package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/core/reference"
	"github.com/amber-store/dstore/catalog"
	"github.com/amber-store/dstore/codec"
	"github.com/amber-store/dstore/paxos"
	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
)

// maintenance is the lease-driven coordinator (§8.1): at most one
// transition and one GC cycle at a time, voter changes excluding both.
type maintenance struct {
	n *Node

	mu         sync.Mutex
	holder     bool
	busy       int  // exclusive activities in progress
	working    bool // a coordinate() run is in progress
	gcRunning  bool // a GC coordination run is in progress
	lastBackup time.Time
	lastDaily  time.Time
	lastGCPoll time.Time
}

func newMaintenance(n *Node) *maintenance { return &maintenance{n: n} }

func (m *maintenance) isHolder() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.holder
}

func (m *maintenance) run() {
	n := m.n
	defer n.wg.Done()
	if !n.waitForView(n.ctx) {
		return
	}
	// Voters contend first; others after a longer jitter.
	if v := n.View(); v != nil && !v.IsVoter(n.id) {
		select {
		case <-time.After(3 * n.cfg.Lease):
		case <-n.ctx.Done():
			return
		}
	}
	t := time.NewTicker(n.cfg.MaintenanceTick)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			if m.isHolder() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = n.cat.ReleaseLease(ctx, n.id)
				cancel()
			}
			return
		case <-t.C:
			m.tick()
		}
	}
}

// tick renews or acquires the lease and, as the holder, drives the
// cluster activities.
func (m *maintenance) tick() {
	n := m.n
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.Lease)
	defer cancel()
	held, lease, err := n.cat.AcquireLease(ctx, n.id, n.cfg.Lease)
	if err != nil {
		if m.isHolder() {
			n.log.Warn("lease renewal failed; stepping down", "error", err)
		}
		m.setHolder(false)
		return
	}
	if !held {
		m.setHolder(false)
		_ = lease
		m.nodeTick(ctx)
		return
	}
	if !m.isHolder() {
		n.log.Info("holding the maintenance lease")
	}
	m.setHolder(true)
	m.nodeTick(ctx)
	m.mu.Lock()
	start := m.busy == 0 && !m.working
	if start {
		m.working = true
	}
	m.mu.Unlock()
	if !start {
		return
	}
	// Coordination runs off the tick so that lease renewal never waits on
	// a long activity.
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		defer func() {
			m.mu.Lock()
			m.working = false
			m.mu.Unlock()
		}()
		m.coordinate()
	}()
}

func (m *maintenance) setHolder(h bool) {
	m.mu.Lock()
	m.holder = h
	m.mu.Unlock()
}

// nodeTick is what every node does each tick regardless of the lease.
func (m *maintenance) nodeTick(ctx context.Context) {
	m.mu.Lock()
	due := time.Since(m.lastGCPoll) > min(10*time.Second, 5*m.n.cfg.MaintenanceTick)
	if due {
		m.lastGCPoll = time.Now()
	}
	m.mu.Unlock()
	if due {
		m.n.gc.poll(ctx)
	}
}

// coordinate runs the holder's activities.
func (m *maintenance) coordinate() {
	n := m.n
	v := n.View()
	if v == nil {
		return
	}
	lctx, cancel := context.WithTimeout(n.ctx, 10*time.Minute)
	defer cancel()
	if v.VoterSync == view.VoterSyncPending {
		if err := m.resumeVoterSync(lctx); err != nil {
			n.log.Warn("voter sync", "error", err)
		}
		return
	}
	if v.Pending != nil {
		m.driveTransition(lctx, v)
	} else {
		if len(v.RemoveVoters) > 0 {
			id := view.NodeID(v.RemoveVoters[0])
			if err := m.removeVoter(lctx, id, true); err != nil {
				n.log.Warn("deferred voter remove", "node", view.ShortID(id), "error", err)
			}
			return
		}
		if m.nextRamp(lctx, v) {
			return
		}
	}
	m.mu.Lock()
	startGC := !m.gcRunning
	if startGC {
		m.gcRunning = true
	}
	m.mu.Unlock()
	if startGC {
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			gctx, gcancel := context.WithTimeout(n.ctx, n.cfg.MarkTimeout+n.cfg.SweepTimeout+n.cfg.BarrierTimeout+time.Minute)
			defer gcancel()
			m.n.gc.coordinate(gctx)
			m.mu.Lock()
			m.gcRunning = false
			m.mu.Unlock()
		}()
	}
	m.housekeeping(lctx)
}

// ---- lease-holder execution ----

// runAsHolder runs fn on the lease holder: locally when this node holds or
// can take the lease, otherwise forwarded to the holder as fwd.
func (m *maintenance) runAsHolder(ctx context.Context, fwd *wire.Msg, fn func(ctx context.Context) (*view.View, error)) (*view.View, error) {
	n := m.n
	if !m.isHolder() {
		held, lease, err := n.cat.AcquireLease(ctx, n.id, n.cfg.Lease)
		if err != nil {
			return nil, err
		}
		if !held {
			if len(lease.Holder) != 32 {
				return nil, errors.New("lease held by an unknown node")
			}
			holder := view.NodeID(lease.Holder)
			if fwd == nil || (fwd.Type == wire.TAdmin && fwd.Force) {
				return nil, fmt.Errorf("lease held by %s; retry there", view.ShortID(holder))
			}
			return m.forward(ctx, holder, fwd)
		}
		m.setHolder(true)
	}
	m.mu.Lock()
	m.busy++
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.busy--
		m.mu.Unlock()
	}()
	return fn(ctx)
}

// forward sends an operator request to the lease holder.
func (m *maintenance) forward(ctx context.Context, holder view.NodeID, fwd *wire.Msg) (*view.View, error) {
	n := m.n
	f := *fwd
	if f.Type == wire.TAdmin {
		var req AdminRequest
		_ = codec.Unmarshal(f.Params, &req)
		req.Forwarded = true
		f.Params = codec.MustMarshal(req)
		f.Force = true // one hop only
	}
	resp, err := n.pool.Call(ctx, holder, wire.ALPNCluster, n.stampReq(&f))
	if err != nil {
		return nil, fmt.Errorf("forward to lease holder %s: %w", view.ShortID(holder), err)
	}
	switch resp.Type {
	case wire.TViewReply:
		v, err := view.Decode(resp.View)
		if err != nil {
			return nil, err
		}
		n.adopt(v, "forward")
		return v, nil
	case wire.TAdminReply:
		var ar AdminReply
		if err := codec.Unmarshal(resp.Status, &ar); err != nil {
			return nil, err
		}
		if len(ar.View) > 0 {
			v, err := view.Decode(ar.View)
			if err != nil {
				return nil, err
			}
			n.adopt(v, "forward")
			return v, nil
		}
		return n.View(), nil
	}
	return nil, fmt.Errorf("unexpected reply %d from lease holder", resp.Type)
}

// ---- transitions (§8) ----

// proposeTransition publishes a pending placement set built by fn.
func (m *maintenance) proposeTransition(ctx context.Context, reason string, fn func(v *view.View, p *view.Pending) error) (*view.View, error) {
	n := m.n
	nv, err := n.cat.CASView(ctx, func(v *view.View) (bool, error) {
		if v.Pending != nil {
			return false, errors.New("a transition is already in progress")
		}
		if v.VoterSync == view.VoterSyncPending {
			return false, errors.New("a voter change is in progress")
		}
		p := &view.Pending{Nodes: cloneNodes(v.Nodes), Replicas: v.Replicas, Since: time.Now().UnixNano(), Reason: reason}
		if err := fn(v, p); err != nil {
			return false, err
		}
		if err := view.ValidateChange(v.Nodes, p.Nodes, int(v.Replicas), false); err != nil {
			return false, err
		}
		p.ID = v.Epoch + 1
		v.Pending = p
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	n.adopt(nv, "propose")
	n.broadcastCluster(ctx, &wire.Msg{Type: wire.TViewChanged, Epoch: nv.Epoch})
	n.log.Info("transition proposed", "id", nv.Pending.ID, "reason", reason)
	return nv, nil
}

func cloneNodes(nodes []view.Node) []view.Node {
	out := make([]view.Node, len(nodes))
	for i, nd := range nodes {
		out[i] = nd
		out[i].ID = append([]byte{}, nd.ID...)
		out[i].Addrs = append([]string{}, nd.Addrs...)
	}
	return out
}

// nodeChange proposes remove/drain/weight/zone for a node.
func (m *maintenance) nodeChange(ctx context.Context, op string, id view.NodeID, req AdminRequest) (*view.View, error) {
	n := m.n
	v := n.View()
	if _, ok := v.Node(id); !ok {
		return nil, fmt.Errorf("%s is not a member", view.ShortID(id))
	}
	if op == "node-remove" && req.Dead && v.IsVoter(id) {
		// A dead node's voter remove runs first so the remaining quorum is
		// among live nodes (§8.1).
		if err := m.removeVoter(ctx, id, req.AllowUnsafe); err != nil {
			return nil, fmt.Errorf("voter remove: %w", err)
		}
	}
	return m.proposeTransition(ctx, op+" "+view.ShortID(id), func(v *view.View, p *view.Pending) error {
		for i := range p.Nodes {
			if !bytes.Equal(p.Nodes[i].ID, id[:]) {
				continue
			}
			switch op {
			case "node-remove":
				p.Nodes = append(p.Nodes[:i], p.Nodes[i+1:]...)
				v.Ramps = dropRamp(v.Ramps, id)
				return nil
			case "node-drain":
				p.Nodes[i].Weight = 0
				v.Ramps = dropRamp(v.Ramps, id)
				return nil
			case "node-weight":
				p.Nodes[i].Weight = req.Weight
				v.Ramps = dropRamp(v.Ramps, id)
				return nil
			case "node-zone":
				p.Nodes[i].Zone = req.Zone
				return nil
			}
		}
		return fmt.Errorf("%s is not in the placement set", view.ShortID(id))
	})
}

func dropRamp(ramps []view.Ramp, id view.NodeID) []view.Ramp {
	out := ramps[:0]
	for _, r := range ramps {
		if !bytes.Equal(r.Node, id[:]) {
			out = append(out, r)
		}
	}
	return out
}

// nextRamp proposes the next step of a weight ramp, if any is due.
func (m *maintenance) nextRamp(ctx context.Context, v *view.View) bool {
	for _, r := range v.Ramps {
		id := view.NodeID(r.Node)
		nd, ok := v.Node(id)
		if !ok || nd.Weight >= r.Target {
			nv, err := m.n.cat.CASView(ctx, func(v *view.View) (bool, error) {
				v.Ramps = dropRamp(v.Ramps, id)
				return false, nil
			})
			if err == nil {
				m.n.adopt(nv, "ramp-done")
			}
			return true
		}
		next := nextRampWeight(nd.Weight, r.Target)
		_, err := m.proposeTransition(ctx, fmt.Sprintf("ramp %s to %d", view.ShortID(id), next), func(v *view.View, p *view.Pending) error {
			for i := range p.Nodes {
				if bytes.Equal(p.Nodes[i].ID, id[:]) {
					p.Nodes[i].Weight = next
				}
			}
			for i := range v.Ramps {
				if bytes.Equal(v.Ramps[i].Node, id[:]) {
					v.Ramps[i].Step++
				}
			}
			return nil
		})
		if err != nil {
			m.n.log.Warn("ramp step", "error", err)
		}
		return true
	}
	return false
}

// driveTransition freezes, re-freezes and commits (§8.3, §8.5).
func (m *maintenance) driveTransition(ctx context.Context, v *view.View) {
	n := m.n
	p := v.Pending
	members := v.AllMembers()
	now := time.Now()
	if !p.Frozen {
		allAcked := true
		for _, id := range members {
			if !view.Contains(p.ParticipantsAck, id) {
				allAcked = false
				break
			}
		}
		if !allAcked && now.Sub(time.Unix(0, p.Since)) < n.cfg.AdoptTimeout {
			return
		}
		nv, err := n.cat.CASView(ctx, func(v *view.View) (bool, error) {
			if v.Pending == nil || v.Pending.ID != p.ID || v.Pending.Frozen {
				return false, catalog.ErrAbort
			}
			var parts [][]byte
			for _, id := range v.AllMembers() {
				if view.Contains(v.Pending.ParticipantsAck, id) {
					parts = view.AddID(parts, id)
				}
			}
			v.Pending.Participants = parts
			v.Pending.Frozen = true
			v.Pending.FrozenAt = time.Now().UnixNano()
			return true, nil
		})
		if err == nil && nv != nil {
			n.adopt(nv, "freeze")
			n.broadcastCluster(ctx, &wire.Msg{Type: wire.TViewChanged, Epoch: nv.Epoch})
			n.log.Info("transition frozen", "id", p.ID, "participants", len(nv.Pending.Participants), "absent", len(members)-len(nv.Pending.Participants))
		}
		return
	}
	allDone := true
	for _, id := range p.Participants {
		if !view.Contains(p.Done, view.NodeID(id)) {
			allDone = false
			break
		}
	}
	if allDone {
		m.commit(ctx, p.ID, p.Round)
		return
	}
	if now.Sub(time.Unix(0, p.FrozenAt)) > n.cfg.ParticipantTimeout {
		if _, err := m.refreeze(ctx, false); err != nil {
			n.log.Warn("refreeze", "error", err)
		}
	}
}

// refreeze drops participants that have not reported done and starts a
// new round (§8.3).
func (m *maintenance) refreeze(ctx context.Context, force bool) (*view.View, error) {
	n := m.n
	nv, err := n.cat.CASView(ctx, func(v *view.View) (bool, error) {
		p := v.Pending
		if p == nil || !p.Frozen {
			return false, errors.New("no frozen transition")
		}
		var parts [][]byte
		for _, id := range p.Participants {
			if view.Contains(p.Done, view.NodeID(id)) || view.Contains(p.PrimaryDone, view.NodeID(id)) && !force {
				parts = append(parts, id)
			}
		}
		if len(parts) == len(p.Participants) && !force {
			return false, catalog.ErrAbort
		}
		p.Participants = parts
		p.Round++
		p.Done = nil
		p.PrimaryDone = nil
		p.FrozenAt = time.Now().UnixNano()
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	n.adopt(nv, "refreeze")
	n.broadcastCluster(ctx, &wire.Msg{Type: wire.TViewChanged, Epoch: nv.Epoch})
	return nv, nil
}

// commit makes pending the placement (§8.5).
func (m *maintenance) commit(ctx context.Context, id uint64, round uint32) {
	n := m.n
	nv, err := n.cat.CASView(ctx, func(v *view.View) (bool, error) {
		p := v.Pending
		if p == nil || p.ID != id || p.Round != round {
			return false, catalog.ErrAbort
		}
		for _, id := range p.Participants {
			if !view.Contains(p.Done, view.NodeID(id)) {
				return false, catalog.ErrAbort
			}
		}
		removed := map[view.NodeID]bool{}
		for _, nd := range v.Nodes {
			removed[nd.NID()] = true
		}
		for _, nd := range p.Nodes {
			delete(removed, nd.NID())
		}
		for rid := range removed {
			v.Former = append(v.Former, view.Former{ID: rid[:], Until: time.Now().Add(30 * 24 * time.Hour).UnixNano()})
			if v.IsVoter(rid) {
				v.RemoveVoters = view.AddID(v.RemoveVoters, rid)
			}
		}
		v.Nodes = p.Nodes
		v.Replicas = p.Replicas
		v.MinReplicas = min(v.MinReplicas, v.Replicas)
		v.PlacementEpoch = p.ID
		v.Pending = nil
		// Prune expired former entries.
		var former []view.Former
		for _, f := range v.Former {
			if f.Until > time.Now().UnixNano() {
				former = append(former, f)
			}
		}
		v.Former = former
		return true, nil
	})
	if err != nil {
		if !errors.Is(err, catalog.ErrAbort) {
			n.log.Warn("commit", "error", err)
		}
		return
	}
	n.adopt(nv, "commit")
	n.broadcastCluster(ctx, &wire.Msg{Type: wire.TViewChanged, Epoch: nv.Epoch})
	n.log.Info("transition committed", "id", id, "epoch", nv.Epoch, "nodes", len(nv.Nodes))
}

func (m *maintenance) transitionText(v *view.View) string {
	if v.Pending == nil {
		if len(v.Ramps) > 0 {
			return fmt.Sprintf("idle (%d ramp(s) pending)", len(v.Ramps))
		}
		return "idle"
	}
	p := v.Pending
	if !p.Frozen {
		return fmt.Sprintf("id %d (%s): adopting, %d/%d acked", p.ID, p.Reason, len(p.ParticipantsAck), len(v.AllMembers()))
	}
	var waiting []string
	for _, id := range p.Participants {
		if !view.Contains(p.Done, view.NodeID(id)) {
			waiting = append(waiting, view.ShortID(view.NodeID(id)))
		}
	}
	return fmt.Sprintf("id %d round %d (%s): %d/%d done, waiting for %v", p.ID, p.Round, p.Reason, len(p.Done), len(p.Participants), waiting)
}

// ---- voter changes (§5.4) ----

func (m *maintenance) pingMajority(ctx context.Context, ids []view.NodeID) bool {
	n := m.n
	ok := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id view.NodeID) {
			defer wg.Done()
			if id == n.id {
				mu.Lock()
				ok++
				mu.Unlock()
				return
			}
			pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if _, err := n.pool.Call(pctx, id, wire.ALPNCluster, n.stampReq(&wire.Msg{Type: wire.TPing})); err == nil {
				mu.Lock()
				ok++
				mu.Unlock()
			}
		}(id)
	}
	wg.Wait()
	return ok >= len(ids)/2+1
}

// addVoter adds id to the acceptor set with the sync procedure.
func (m *maintenance) addVoter(ctx context.Context, id view.NodeID) error {
	n := m.n
	v := n.View()
	if v.IsVoter(id) {
		return errors.New("already a voter")
	}
	if v.VoterSync == view.VoterSyncPending {
		return errors.New("a voter change is in progress")
	}
	if len(v.Voters) == 1 {
		return m.addVoterFromSingle(ctx, id)
	}
	newSet := append(v.VoterIDs(), id)
	if !m.pingMajority(ctx, newSet) {
		return errors.New("a majority of the new voter set did not answer a ping")
	}
	nv, err := n.cat.CASView(ctx, func(v *view.View) (bool, error) {
		if v.VoterSync == view.VoterSyncPending || v.IsVoter(id) {
			return false, errors.New("voter set changed concurrently")
		}
		v.Voters = append(v.Voters, view.Voter{ID: id[:], Since: v.Epoch + 1})
		v.VoterSync = view.VoterSyncPending
		v.VoterSyncTarget = id[:]
		v.VoterSyncAdd = true
		v.VoterSyncCursor = nil
		v.DeferredVoters = removeID(v.DeferredVoters, id)
		return true, nil
	})
	if err != nil {
		return err
	}
	n.adopt(nv, "voter-add")
	return m.finishVoterSync(ctx, v.VoterIDs())
}

// addVoterFromSingle handles the first step out of a single voter: the
// second node is deferred; the third takes the cluster to three voters in
// one step, seeded from the single acceptor (§5.4).
func (m *maintenance) addVoterFromSingle(ctx context.Context, id view.NodeID) error {
	n := m.n
	v := n.View()
	if len(v.DeferredVoters) == 0 {
		nv, err := n.cat.CASView(ctx, func(v *view.View) (bool, error) {
			v.DeferredVoters = view.AddID(v.DeferredVoters, id)
			return false, nil
		})
		if err != nil {
			return err
		}
		n.adopt(nv, "voter-defer")
		n.log.Info("voter add deferred until a third node joins", "node", view.ShortID(id))
		return nil
	}
	newIDs := append(view.IDsOf(v.DeferredVoters), id)
	single := v.VoterIDs()[0]
	all := append([]view.NodeID{single}, newIDs...)
	if !m.pingMajority(ctx, all) {
		return errors.New("the new voters did not answer a ping")
	}
	// One CAS commits the three-voter view on the single acceptor (quorum
	// 1); nothing can commit at the new epoch until the new voters carry
	// markers, since without them they refuse to promise. Then every row —
	// the new view included — is copied to both, markers are set, and the
	// view installed everywhere.
	nv, err := n.cat.CASView(ctx, func(v *view.View) (bool, error) {
		if len(v.Voters) != 1 {
			return false, errors.New("voter set changed concurrently")
		}
		for _, nid := range newIDs {
			v.Voters = append(v.Voters, view.Voter{ID: nid[:], Since: v.Epoch + 1})
		}
		v.DeferredVoters = nil
		v.VoterSync = view.VoterSyncDone
		return true, nil
	})
	if err != nil {
		return err
	}
	n.adopt(nv, "voters-3")
	rows, err := n.acceptor.AllRows()
	if err != nil {
		return err
	}
	newEpoch := nv.Epoch
	for _, nid := range newIDs {
		for i := 0; i < len(rows); i += 2000 {
			end := min(i+2000, len(rows))
			if _, err := n.Call(ctx, nid, n.stampReq(&wire.Msg{Type: wire.TSeed, Rows: rows[i:end]})); err != nil {
				return fmt.Errorf("seed %s: %w", view.ShortID(nid), err)
			}
		}
		if resp, err := n.Call(ctx, nid, n.stampReq(&wire.Msg{Type: wire.TMarker, Since: newEpoch})); err != nil || resp.Type != wire.TOK {
			return fmt.Errorf("marker %s: %v", view.ShortID(nid), err)
		}
	}
	n.prop.Install(ctx, nv, all)
	n.broadcastCluster(ctx, &wire.Msg{Type: wire.TViewChanged, Epoch: nv.Epoch})
	n.log.Info("catalog now has three voters", "epoch", nv.Epoch)
	return nil
}

// removeVoter removes id from the acceptor set with the sync procedure.
func (m *maintenance) removeVoter(ctx context.Context, id view.NodeID, allowUnsafe bool) error {
	n := m.n
	v := n.View()
	if !v.IsVoter(id) {
		// Clear a stale deferred removal.
		if view.Contains(v.RemoveVoters, id) {
			nv, err := n.cat.CASView(ctx, func(v *view.View) (bool, error) {
				v.RemoveVoters = removeID(v.RemoveVoters, id)
				return false, nil
			})
			if err == nil {
				n.adopt(nv, "voter-remove-done")
			}
		}
		return nil
	}
	if v.VoterSync == view.VoterSyncPending {
		return errors.New("a voter change is in progress")
	}
	if len(v.Voters) == 1 {
		return errors.New("cannot remove the only voter")
	}
	if len(v.Voters) <= 3 && len(v.Nodes) >= 3 && !allowUnsafe {
		return errors.New("removal would leave fewer than 3 voters (use --allow-unsafe)")
	}
	old := v.VoterIDs()
	nv, err := n.cat.CASView(ctx, func(v *view.View) (bool, error) {
		if v.VoterSync == view.VoterSyncPending || !v.IsVoter(id) {
			return false, errors.New("voter set changed concurrently")
		}
		var voters []view.Voter
		for _, vo := range v.Voters {
			if !bytes.Equal(vo.ID, id[:]) {
				voters = append(voters, vo)
			}
		}
		v.Voters = voters
		v.VoterSync = view.VoterSyncPending
		v.VoterSyncTarget = id[:]
		v.VoterSyncAdd = false
		v.VoterSyncCursor = nil
		v.RemoveVoters = removeID(v.RemoveVoters, id)
		return true, nil
	})
	if err != nil {
		return err
	}
	n.adopt(nv, "voter-remove")
	return m.finishVoterSync(ctx, old)
}

// finishVoterSync runs steps 2–4 of §5.4 for the pending voter change.
func (m *maintenance) finishVoterSync(ctx context.Context, oldVoters []view.NodeID) error {
	n := m.n
	v := n.View()
	if v.VoterSync != view.VoterSyncPending {
		return nil
	}
	target := view.NodeID(v.VoterSyncTarget)
	// Step 2: retire the old configuration — a majority of the old voters
	// must install the new epoch.
	all := map[view.NodeID]struct{}{}
	for _, id := range oldVoters {
		all[id] = struct{}{}
	}
	for _, id := range v.VoterIDs() {
		all[id] = struct{}{}
	}
	var ids []view.NodeID
	for id := range all {
		ids = append(ids, id)
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		ok := n.prop.Install(ctx, v, ids)
		installed := 0
		for _, id := range ok {
			for _, o := range oldVoters {
				if o == id {
					installed++
				}
			}
		}
		if installed >= len(oldVoters)/2+1 {
			break
		}
		if time.Now().After(deadline) {
			return errors.New("a majority of the old voters did not install the new view")
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// Step 3: sync every register at the new epoch.
	if err := m.syncRegisters(ctx); err != nil {
		return err
	}
	// Step 4: marker on an added voter, then voter_sync = done.
	if v.VoterSyncAdd {
		since := uint64(0)
		for _, vo := range v.Voters {
			if bytes.Equal(vo.ID, target[:]) {
				since = vo.Since
			}
		}
		mdeadline := time.Now().Add(time.Minute)
		for {
			resp, err := n.Call(ctx, target, n.stampReq(&wire.Msg{Type: wire.TMarker, Since: since}))
			if err == nil && resp.Type == wire.TOK {
				break
			}
			if time.Now().After(mdeadline) {
				return fmt.Errorf("new voter %s did not acknowledge its marker", view.ShortID(target))
			}
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	nv, err := n.cat.CASView(ctx, func(v *view.View) (bool, error) {
		v.VoterSync = view.VoterSyncDone
		v.VoterSyncCursor = nil
		v.VoterSyncTarget = nil
		return false, nil
	})
	if err != nil {
		return err
	}
	n.adopt(nv, "voter-sync-done")
	n.prop.Install(ctx, nv, ids)
	n.broadcastCluster(ctx, &wire.Msg{Type: wire.TViewChanged, Epoch: nv.Epoch})
	n.log.Info("voter change complete", "voters", len(nv.Voters), "epoch", nv.Epoch)
	return nil
}

// resumeVoterSync continues a voter change a previous holder left pending.
func (m *maintenance) resumeVoterSync(ctx context.Context) error {
	v := m.n.View()
	old := v.VoterIDs()
	if v.VoterSyncAdd {
		// The old set is the new set without the target.
		var o []view.NodeID
		for _, id := range old {
			if id != view.NodeID(v.VoterSyncTarget) {
				o = append(o, id)
			}
		}
		old = o
	} else {
		old = append(old, view.NodeID(v.VoterSyncTarget))
	}
	return m.finishVoterSync(ctx, old)
}

// syncRegisters runs the identity transition on every register, resuming
// from the cursor kept in the view.
func (m *maintenance) syncRegisters(ctx context.Context) error {
	n := m.n
	after := n.View().VoterSyncCursor
	count := 0
	for {
		rows, next, err := n.prop.Scan(ctx, nil, after, 500)
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		for _, r := range rows {
			if string(r.Reg) == catalog.RegView {
				continue // the view is what carries the sync itself
			}
			for attempt := 0; ; attempt++ {
				_, err := n.prop.Settle(ctx, r.Reg)
				if err == nil {
					break
				}
				if attempt > 20 {
					return fmt.Errorf("settle %q: %w", r.Reg, err)
				}
				select {
				case <-time.After(200 * time.Millisecond):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			count++
		}
		if next == nil {
			break
		}
		after = next
		cursor := after
		nv, err := n.cat.CASView(ctx, func(v *view.View) (bool, error) {
			if v.VoterSync != view.VoterSyncPending {
				return false, catalog.ErrAbort
			}
			v.VoterSyncCursor = cursor
			return false, nil
		})
		if err == nil {
			n.adopt(nv, "sync-cursor")
		}
	}
	n.log.Info("voter sync: registers re-accepted", "count", count)
	return nil
}

// ---- housekeeping ----

func (m *maintenance) housekeeping(ctx context.Context) {
	n := m.n
	m.mu.Lock()
	backupDue := time.Since(m.lastBackup) > time.Hour
	dailyDue := time.Since(m.lastDaily) > 24*time.Hour
	m.mu.Unlock()
	if backupDue {
		m.mu.Lock()
		m.lastBackup = time.Now()
		m.mu.Unlock()
		if _, err := m.backupCatalog(ctx); err != nil {
			n.log.Warn("catalog backup", "error", err)
		}
	}
	if dailyDue {
		m.mu.Lock()
		m.lastDaily = time.Now()
		m.mu.Unlock()
		m.dailyPass(ctx)
	}
}

// BackupName is the reserved reference of the catalog backup object.
const BackupName = "dstore/catalog-backup"

// backupEntry is one reference in a backup object.
type backupEntry struct {
	Name   string `cbor:"0,keyasint"`
	Record []byte `cbor:"1,keyasint"`
}

// backupCatalog writes the reference set as an object into the data plane
// (§13) and names it with the reserved reference.
func (m *maintenance) backupCatalog(ctx context.Context) ([32]byte, error) {
	n := m.n
	refs, err := n.cat.RefsSnapshot(ctx)
	if err != nil {
		return [32]byte{}, err
	}
	var entries []backupEntry
	for _, e := range refs {
		if e.Name == BackupName {
			continue
		}
		entries = append(entries, backupEntry{Name: e.Name, Record: e.Value.Record})
	}
	data := codec.MustMarshal(entries)
	k, err := key.New(key.Blob, uint64(len(data)), data)
	if err != nil {
		return [32]byte{}, err
	}
	if err := n.store.Put(k, data); err != nil {
		return [32]byte{}, err
	}
	rec, err := n.store.GetRecord(k)
	if err != nil {
		return [32]byte{}, err
	}
	kk := [32]byte(k)
	for _, o := range n.Placement().WriteSet(kk) {
		if o == n.id {
			continue
		}
		fctx, cancel := context.WithTimeout(ctx, n.cfg.ForwardTimeout)
		n.forwardTo(fctx, o, [][32]byte{kk}, func([32]byte) []byte { return rec })
		cancel()
	}
	r := reference.Reference{Name: BackupName, Key: k[:], User: "dstore", CreatedAt: time.Now().UnixNano()}
	enc, err := r.Encode()
	if err != nil {
		return kk, err
	}
	if _, err := n.RefPutLocal(ctx, BackupName, enc, catalog.Cond{Force: true}); err != nil {
		return kk, err
	}
	n.noteBackup(k[:])
	n.broadcastCluster(ctx, &wire.Msg{Type: wire.TBackupNote, Key: k[:]})
	n.log.Info("catalog backup written", "refs", len(entries), "key", k.String()[:16])
	return kk, nil
}

// RestoreCatalog force-writes every reference of a backup object.
func (n *Node) RestoreCatalog(ctx context.Context, data []byte) (int, error) {
	var entries []backupEntry
	if err := codec.Unmarshal(data, &entries); err != nil {
		return 0, err
	}
	written := 0
	for _, e := range entries {
		if _, err := n.cat.RefPut(ctx, e.Name, e.Record, catalog.Cond{Force: true}, 0, keyOfRecord); err != nil {
			return written, fmt.Errorf("%s: %w", e.Name, err)
		}
		written++
	}
	_, _ = n.cat.CASGC(ctx, func(g *catalog.GCState) error { g.BarrierAt = time.Now().UnixNano(); g.Hold = true; return nil })
	return written, nil
}

// dailyPass retries tombstone purges and cleans up join tokens (§5.3, §2).
func (m *maintenance) dailyPass(ctx context.Context) {
	n := m.n
	v := n.View()
	dayAgo := time.Now().Add(-24 * time.Hour).UnixNano()
	var after []byte
	for {
		rows, next, err := n.prop.Scan(ctx, []byte(catalog.PrefixRef), after, 5000)
		if err != nil {
			break
		}
		for _, r := range rows {
			if !r.Row.HasValue {
				continue
			}
			rv, err := catalog.DecodeRef(r.Row.Value)
			if err != nil || !rv.IsTombstone() || rv.DeletedAt > dayAgo {
				continue
			}
			n.cat.TryPurge(ctx, string(r.Reg[len(catalog.PrefixRef):]), r.Row.Accepted)
		}
		if next == nil {
			break
		}
		after = next
	}
	tokens, err := n.cat.ListTokens(ctx)
	if err != nil {
		return
	}
	weekAgo := time.Now().Add(-7 * 24 * time.Hour).UnixNano()
	for id, t := range tokens {
		if t.CreatedAt < weekAgo || tokenUsed(v, []byte(id)) {
			_ = n.cat.DeleteToken(ctx, []byte(id))
		}
	}
}

// ballotOf is a helper for tests.
func ballotOf(b []byte) paxos.Ballot {
	bb, _ := paxos.ParseBallot(b)
	return bb
}

var _ = packstore.ErrNotFound
