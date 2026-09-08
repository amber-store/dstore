package node

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/amber-store/core/amberpack"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/dstore/catalog"
	"github.com/amber-store/dstore/codec"
	"github.com/amber-store/dstore/view"
)

// packStamp is the per-pack reconcile bookkeeping (§6.1, §8.4).
type packStamp struct {
	Inc        uint64 `cbor:"0,keyasint"`
	For        uint64 `cbor:"1,keyasint"`
	Replicated bool   `cbor:"2,keyasint"`
	Checked    int64  `cbor:"3,keyasint,omitempty"`
	Verified   int64  `cbor:"4,keyasint,omitempty"`
}

// xferState is a participant's progress through a transition pass.
type xferState struct {
	ID       uint64          `cbor:"0,keyasint"`
	Round    uint32          `cbor:"1,keyasint"`
	Seal     uint64          `cbor:"2,keyasint"`
	Done     map[uint64]bool `cbor:"3,keyasint,omitempty"`
	Deferred map[uint64]bool `cbor:"4,keyasint,omitempty"` // packs with keys waiting on the primary rule
	Primary  bool            `cbor:"5,keyasint,omitempty"` // primary_done reported
	Cover    bool            `cbor:"6,keyasint,omitempty"` // second seal taken
	Seal2    uint64          `cbor:"7,keyasint,omitempty"`
	Finished bool            `cbor:"8,keyasint,omitempty"`
}

// reconciler runs the reconcile pass (§8.4): one mechanism for
// rebalancing, repair, healing and anti-entropy.
type reconciler struct {
	n *Node

	mu       sync.Mutex
	queue    map[view.NodeID]map[[32]byte]struct{} // forwards scheduled per target
	wiped    map[view.NodeID]time.Time             // targets whose incarnation changed
	xfer     *xferState
	lastPass time.Time
	rate     int64 // bytes/s
}

func newReconciler(n *Node) *reconciler {
	return &reconciler{n: n, queue: map[view.NodeID]map[[32]byte]struct{}{}, wiped: map[view.NodeID]time.Time{}, rate: n.cfg.Rate}
}

func (r *reconciler) stampKey(id uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], id)
	return append(append([]byte(mkPack), r.n.storeID...), b[:]...)
}

func (r *reconciler) stamp(id uint64) (packStamp, bool) {
	var s packStamp
	if err := r.n.meta.GetCBOR(r.stampKey(id), &s); err != nil {
		return packStamp{}, false
	}
	return s, true
}

func (r *reconciler) setStamp(id uint64, s packStamp) {
	_ = r.n.meta.SetCBOR(r.stampKey(id), s)
}

// settled reports whether pack id has been reconciled for the placement
// in force and every target confirmed (§8.4).
func (r *reconciler) settled(id uint64, v *view.View) bool {
	s, ok := r.stamp(id)
	return ok && s.Inc == v.Incarnation && s.For == v.PlacementEpoch && s.Replicated
}

// settledFor reports whether pack id was reconciled for a given change.
func (r *reconciler) settledFor(id uint64, v *view.View, forID uint64) bool {
	s, ok := r.stamp(id)
	return ok && s.Inc == v.Incarnation && s.For == forID && s.Replicated
}

// scheduleForward queues keys to be offered to target soon.
func (r *reconciler) scheduleForward(target view.NodeID, keys [][32]byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	q := r.queue[target]
	if q == nil {
		q = map[[32]byte]struct{}{}
		r.queue[target] = q
	}
	for _, k := range keys {
		q[k] = struct{}{}
	}
}

// pinned handles pins on records this node may not own: the pack is
// marked unreplicated and the keys queued for an offer (§8.4).
func (r *reconciler) pinned(keys [][32]byte) {
	pl := r.n.Placement()
	var offer [][32]byte
	for _, k := range keys {
		if !pl.InWriteSet(k, r.n.id) {
			offer = append(offer, k)
		}
	}
	if len(offer) == 0 {
		return
	}
	for _, k := range offer {
		for _, o := range pl.WriteSet(k) {
			r.scheduleForward(o, [][32]byte{k})
		}
	}
}

// corruptFound marks the pack holding k unreplicated so other holders
// refill it.
func (r *reconciler) corruptFound(k [32]byte) {
	pl := r.n.Placement()
	for _, o := range pl.ReadOrder(k) {
		if o != r.n.id {
			r.scheduleForward(o, [][32]byte{k})
		}
	}
}

// verifyLocal re-verifies this node's copy after a target rejected it.
func (r *reconciler) verifyLocal(k [32]byte) {
	rec, err := r.n.store.GetRecord(key.Key(k))
	if err != nil {
		r.n.markCorrupt(k)
		return
	}
	raw, err := amberpack.ParseRecord(rec)
	if err != nil {
		r.n.markCorrupt(k)
		return
	}
	if _, _, err := verifyRecord(amberpack.RawRecord{Record: raw, Bytes: rec}); err != nil {
		r.n.log.Warn("local record is corrupt", "key", key.Key(k).String()[:16])
		r.n.markCorrupt(k)
		r.corruptFound(k)
	}
}

// targetWiped marks every pack replicated=false for a target whose
// incarnation changed, with a random delay within Δ (§8.4).
func (r *reconciler) targetWiped(id view.NodeID) {
	r.mu.Lock()
	r.wiped[id] = time.Now().Add(time.Duration(rand.Int64N(int64(r.n.cfg.Delta))))
	r.mu.Unlock()
}

// ---- the loop ----

func (r *reconciler) run() {
	n := r.n
	defer n.wg.Done()
	if !n.waitForView(n.ctx) {
		return
	}
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
			r.step()
		}
	}
}

func (r *reconciler) step() {
	n := r.n
	v := n.View()
	if v == nil {
		return
	}
	ctx, cancel := context.WithTimeout(n.ctx, 10*time.Minute)
	defer cancel()
	r.flushQueue(ctx)
	r.firstAudit(ctx)
	r.applyWiped(v)
	if v.RebalancePause {
		return
	}
	if r.transitionStep(ctx, v) {
		return
	}
	r.backgroundStep(ctx, v)
}

// flushQueue offers scheduled keys to their targets.
func (r *reconciler) flushQueue(ctx context.Context) {
	r.mu.Lock()
	q := r.queue
	r.queue = map[view.NodeID]map[[32]byte]struct{}{}
	r.mu.Unlock()
	for target, keys := range q {
		ks := make([][32]byte, 0, len(keys))
		for k := range keys {
			ks = append(ks, k)
		}
		if failed := r.offer(ctx, target, ks); len(failed) > 0 {
			// Retry later, but not forever for a dead target: the pack
			// audit covers it.
			r.mu.Lock()
			if time.Since(r.lastPass) < time.Hour {
				r.mu.Unlock()
				continue
			}
			r.mu.Unlock()
		}
	}
}

// offer runs missing+put for keys at target from this node's store and
// returns the keys that were not confirmed.
func (r *reconciler) offer(ctx context.Context, target view.NodeID, keys [][32]byte) [][32]byte {
	n := r.n
	var failed [][32]byte
	for i := 0; i < len(keys); i += 4096 {
		end := min(i+4096, len(keys))
		batch := keys[i:end]
		fctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		_, errs := n.forwardTo(fctx, target, batch, func(k [32]byte) []byte {
			if n.isCorrupt(k) {
				return nil
			}
			rec, err := n.store.GetRecord(key.Key(k))
			if err != nil {
				if errors.Is(err, packstore.ErrCorrupt) || errors.Is(err, packstore.ErrVerify) {
					n.markCorrupt(k)
				}
				return nil
			}
			r.pace(len(rec))
			return rec
		})
		cancel()
		for k := range errs {
			failed = append(failed, k)
		}
	}
	return failed
}

// pace sleeps to keep the copy rate under --rate.
func (r *reconciler) pace(nbytes int) {
	if r.rate <= 0 {
		return
	}
	cap := r.rate
	if v := r.n.View(); v != nil && v.RateCap > 0 && int64(v.RateCap) < cap {
		cap = int64(v.RateCap)
	}
	time.Sleep(time.Duration(float64(nbytes) / float64(cap) * float64(time.Second)))
}

// firstAudit offers recently written keys to every owner once they are
// older than first_audit (§8.4) — the audit that heals a write whose
// owner was down at the time.
func (r *reconciler) firstAudit(ctx context.Context) {
	n := r.n
	cutoff := time.Now().Add(-n.cfg.FirstAudit)
	n.recentMu.Lock()
	var due [][32]byte
	for k, t := range n.recent {
		if t.Before(cutoff) {
			due = append(due, k)
		}
		if len(due) >= 8192 {
			break
		}
	}
	for _, k := range due {
		delete(n.recent, k)
	}
	n.recentMu.Unlock()
	if len(due) == 0 {
		return
	}
	pl := n.Placement()
	byTarget := map[view.NodeID][][32]byte{}
	for _, k := range due {
		for _, o := range pl.WriteSet(k) {
			if o != n.id {
				byTarget[o] = append(byTarget[o], k)
			}
		}
	}
	for t, ks := range byTarget {
		if failed := r.offer(ctx, t, ks); len(failed) > 0 {
			n.log.Debug("first audit: target short", "target", view.ShortID(t), "keys", len(failed))
		}
	}
}

// applyWiped marks packs unreplicated for wiped targets whose delay passed.
func (r *reconciler) applyWiped(v *view.View) {
	r.mu.Lock()
	var due []view.NodeID
	for id, at := range r.wiped {
		if time.Now().After(at) {
			due = append(due, id)
			delete(r.wiped, id)
		}
	}
	r.mu.Unlock()
	if len(due) == 0 {
		return
	}
	segs, err := r.n.store.Segments()
	if err != nil {
		return
	}
	for _, s := range segs {
		if st, ok := r.stamp(s.ID); ok && st.Replicated {
			st.Replicated = false
			r.setStamp(s.ID, st)
		}
	}
}

// ---- transitions ----

// seal forces the active segment sealed and returns the highest sealed id.
func (r *reconciler) seal() uint64 {
	n := r.n
	n.sweepMu.Lock()
	_, _ = n.store.Compact(func(key.Key) bool { return true }, packstore.CompactOpts{MinDeadRatio: 2})
	n.sweepMu.Unlock()
	segs, err := n.store.Segments()
	if err != nil || len(segs) == 0 {
		return 0
	}
	return segs[len(segs)-1].ID
}

func (r *reconciler) xferKey(id uint64, round uint32) []byte {
	return []byte(fmt.Sprintf("%s%d/%d", mkXfer, id, round))
}

func (r *reconciler) saveXfer(x *xferState) {
	_ = r.n.meta.SetCBOR(r.xferKey(x.ID, x.Round), x)
}

// adoptTransition runs the adoption step (§8.2): seal, record, ack.
func (r *reconciler) adoptTransition(v *view.View) {
	n := r.n
	p := v.Pending
	if !v.IsMember(n.id) {
		return
	}
	var x xferState
	if err := n.meta.GetCBOR(r.xferKey(p.ID, p.Round), &x); err != nil {
		x = xferState{ID: p.ID, Round: p.Round, Seal: r.seal()}
		r.saveXfer(&x)
	}
	if x.Done == nil {
		x.Done = map[uint64]bool{}
	}
	if x.Deferred == nil {
		x.Deferred = map[uint64]bool{}
	}
	r.mu.Lock()
	r.xfer = &x
	r.mu.Unlock()
	if view.Contains(p.ParticipantsAck, n.id) {
		return
	}
	ctx, cancel := context.WithTimeout(n.ctx, time.Minute)
	defer cancel()
	nv, err := n.cat.CASView(ctx, func(v *view.View) (bool, error) {
		if v.Pending == nil || v.Pending.ID != p.ID {
			return false, catalog.ErrAbort
		}
		if view.Contains(v.Pending.ParticipantsAck, n.id) {
			return false, catalog.ErrAbort
		}
		v.Pending.ParticipantsAck = view.AddID(v.Pending.ParticipantsAck, n.id)
		return false, nil
	})
	if err == nil && nv != nil {
		n.adopt(nv, "ack")
	}
}

// transitionCommitted clears the pass state.
func (r *reconciler) transitionCommitted(v *view.View) {
	r.mu.Lock()
	r.xfer = nil
	r.mu.Unlock()
	_ = r.n.meta.DeletePrefix([]byte(mkXfer))
}

// transitionStep advances this node's pass over a frozen transition and
// reports whether it did transition work.
func (r *reconciler) transitionStep(ctx context.Context, v *view.View) bool {
	n := r.n
	p := v.Pending
	if p == nil || !p.Frozen {
		return false
	}
	if !view.Contains(p.Participants, n.id) {
		return false // absent: keep data, do nothing
	}
	r.mu.Lock()
	x := r.xfer
	r.mu.Unlock()
	if x == nil || x.ID != p.ID || x.Round != p.Round {
		r.adoptTransition(v)
		r.mu.Lock()
		x = r.xfer
		r.mu.Unlock()
		if x == nil {
			return false
		}
	}
	if x.Finished || view.Contains(p.Done, n.id) {
		return false
	}
	segs, err := n.store.Segments()
	if err != nil {
		return true
	}
	limit := x.Seal
	if x.Cover {
		limit = x.Seal2
	}
	// The pass over packs ≤ seal in id order; deferred packs revisited.
	allDone := true
	for _, s := range segs {
		if s.ID > limit {
			break
		}
		if x.Done[s.ID] {
			continue
		}
		if x.Cover && s.ID <= x.Seal {
			continue
		}
		allDone = false
		deferred := r.reconcilePack(ctx, s.ID, v, x)
		if deferred {
			x.Deferred[s.ID] = true
		} else {
			x.Done[s.ID] = true
			delete(x.Deferred, s.ID)
		}
		r.saveXfer(x)
		if !deferred {
			return true // one pack per step keeps client traffic first
		}
	}
	if !x.Primary {
		// Every key this node forwards at once has been offered (the rest
		// are deferred): report primary_done so lower-ranked owners can
		// proceed without waiting Δ.
		x.Primary = true
		r.saveXfer(x)
		r.casPending(ctx, p.ID, func(p *view.Pending) { p.PrimaryDone = view.AddID(p.PrimaryDone, n.id) })
	}
	if !allDone {
		return true
	}
	if !x.Cover {
		x.Cover = true
		x.Seal2 = r.seal()
		r.saveXfer(x)
		return true
	}
	x.Finished = true
	r.saveXfer(x)
	r.casPending(ctx, p.ID, func(p *view.Pending) { p.Done = view.AddID(p.Done, n.id) })
	n.log.Info("transition pass done", "id", p.ID, "round", p.Round)
	return true
}

func (r *reconciler) casPending(ctx context.Context, id uint64, fn func(p *view.Pending)) {
	n := r.n
	nv, err := n.cat.CASView(ctx, func(v *view.View) (bool, error) {
		if v.Pending == nil || v.Pending.ID != id {
			return false, catalog.ErrAbort
		}
		fn(v.Pending)
		return false, nil
	})
	if err == nil && nv != nil {
		n.adopt(nv, "pending")
	}
}

// rankAmong returns this node's 1-based position among the participating
// owners of k under cur, and the participants ranked above it.
func rankAmong(pl *view.Placement, k [32]byte, self view.NodeID, participants [][]byte) (pos int, above []view.NodeID) {
	pos = 0
	for _, o := range pl.Owners(k) {
		if !view.Contains(participants, o) {
			continue
		}
		if o == self {
			return len(above) + 1, above
		}
		above = append(above, o)
	}
	return 0, above
}

// reconcilePack offers every record of pack id to its targets (§8.4). It
// returns true if some keys were deferred by the primary-forwarder rule.
func (r *reconciler) reconcilePack(ctx context.Context, id uint64, v *view.View, x *xferState) (deferred bool) {
	n := r.n
	pl := n.Placement()
	p := v.Pending
	inTransition := p != nil && p.Frozen && x != nil
	var keys [][32]byte
	if err := n.store.ScanIndex(id, func(k key.Key, _ uint64, _ uint32) { keys = append(keys, [32]byte(k)) }); err != nil {
		return false
	}
	settledCur := inTransition && r.settledFor(id, v, v.PlacementEpoch)
	byTarget := map[view.NodeID][][32]byte{}
	frozenAt := time.Time{}
	if inTransition {
		frozenAt = time.Unix(0, p.FrozenAt)
	}
	for _, k := range keys {
		if n.isCorrupt(k) {
			continue
		}
		if n.gc.provedDead(k, id, pl) {
			continue
		}
		if inTransition {
			j, above := rankAmong(pl, k, n.id, p.Participants)
			ready := j == 0 || j == 1
			if !ready {
				ready = time.Since(frozenAt) >= time.Duration(j-1)*n.cfg.Delta
			}
			if !ready {
				ready = true
				for _, a := range above {
					if !view.Contains(p.PrimaryDone, a) {
						ready = false
					}
				}
			}
			if !ready {
				deferred = true
				continue
			}
		}
		var targets []view.NodeID
		if inTransition {
			targets = pl.PendingOwners(k)
		} else {
			targets = pl.WriteSet(k)
		}
		for _, t := range targets {
			if t == n.id {
				continue
			}
			if settledCur && pl.IsOwner(k, t) {
				continue
			}
			byTarget[t] = append(byTarget[t], k)
		}
		if !inTransition {
			continue
		}
		// During a transition, current co-owners are offered to as well
		// unless the pack was settled for the current placement.
		if !settledCur {
			for _, t := range pl.Owners(k) {
				if t != n.id && !contains(byTarget[t], k) {
					byTarget[t] = append(byTarget[t], k)
				}
			}
		}
	}
	replicated := true
	for t, ks := range byTarget {
		if failed := r.offer(ctx, t, ks); len(failed) > 0 {
			replicated = false
			n.log.Debug("reconcile: target short", "pack", id, "target", view.ShortID(t), "keys", len(ks), "failed", len(failed))
		}
	}
	n.log.Debug("reconcile pack", "pack", id, "keys", len(keys), "targets", len(byTarget), "deferred", deferred, "replicated", replicated, "transition", inTransition)
	// Verify bytes on the weekly visit.
	verified := int64(0)
	if st, ok := r.stamp(id); !ok || time.Since(time.Unix(0, st.Verified)) > n.cfg.AuditInterval {
		r.scrubPack(id)
		verified = time.Now().UnixNano()
	} else {
		verified = st.Verified
	}
	if deferred {
		return true
	}
	forID := v.PlacementEpoch
	if inTransition {
		forID = p.ID
	}
	r.setStamp(id, packStamp{Inc: v.Incarnation, For: forID, Replicated: replicated, Checked: time.Now().UnixNano(), Verified: verified})
	r.mu.Lock()
	r.lastPass = time.Now()
	r.mu.Unlock()
	return false
}

func contains(ks [][32]byte, k [32]byte) bool {
	for _, x := range ks {
		if x == k {
			return true
		}
	}
	return false
}

// scrubPack verifies every record of a pack (CRC and payload rehash).
func (r *reconciler) scrubPack(id uint64) {
	n := r.n
	_ = n.store.ScanIndex(id, func(k key.Key, off uint64, _ uint32) {
		rec, err := n.store.Record(id, off)
		if err != nil {
			n.markCorrupt([32]byte(k))
			r.corruptFound([32]byte(k))
			return
		}
		raw, err := amberpack.ParseRecord(rec)
		if err != nil {
			n.markCorrupt([32]byte(k))
			r.corruptFound([32]byte(k))
			return
		}
		if _, _, err := verifyRecord(amberpack.RawRecord{Record: raw, Bytes: rec}); err != nil {
			n.markCorrupt([32]byte(k))
			r.corruptFound([32]byte(k))
		}
	})
}

// ---- background ----

// backgroundStep audits one pack that needs it: unsettled, unreplicated,
// never checked, or checked longer ago than audit_interval.
func (r *reconciler) backgroundStep(ctx context.Context, v *view.View) {
	n := r.n
	segs, err := n.store.Segments()
	if err != nil || len(segs) == 0 {
		return
	}
	now := time.Now()
	type cand struct {
		id   uint64
		prio int
	}
	var cands []cand
	for _, s := range segs {
		st, ok := r.stamp(s.ID)
		switch {
		case !ok:
			cands = append(cands, cand{s.ID, 0})
		case st.Inc != v.Incarnation || st.For != v.PlacementEpoch:
			cands = append(cands, cand{s.ID, 1})
		case !st.Replicated:
			cands = append(cands, cand{s.ID, 2})
		case now.Sub(time.Unix(0, st.Checked)) > n.cfg.AuditInterval:
			cands = append(cands, cand{s.ID, 3})
		}
	}
	if len(cands) == 0 {
		return
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].prio != cands[j].prio {
			return cands[i].prio < cands[j].prio
		}
		return cands[i].id < cands[j].id
	})
	c := cands[0]
	// An unreplicated pack is retried with backoff, not every tick.
	if st, ok := r.stamp(c.id); ok && !st.Replicated && now.Sub(time.Unix(0, st.Checked)) < n.cfg.FirstAudit {
		return
	}
	r.reconcilePack(ctx, c.id, v, nil)
}

// pendingPacks counts packs not settled for the current placement.
func (r *reconciler) pendingPacks(segs []packstore.SegmentInfo) int {
	v := r.n.View()
	if v == nil {
		return len(segs)
	}
	n := 0
	for _, s := range segs {
		if !r.settled(s.ID, v) {
			n++
		}
	}
	return n
}

// scrubAge returns the age in seconds of the oldest unverified pack.
func (r *reconciler) scrubAge(segs []packstore.SegmentInfo) int64 {
	var oldest int64
	for _, s := range segs {
		st, ok := r.stamp(s.ID)
		var t time.Time
		if ok && st.Verified > 0 {
			t = time.Unix(0, st.Verified)
		} else {
			t = s.Sealed
		}
		if age := int64(time.Since(t).Seconds()); age > oldest {
			oldest = age
		}
	}
	return oldest
}

var _ = codec.Marshal
