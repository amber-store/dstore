package node

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/core/reference"
	"github.com/amber-store/dstore/catalog"
	"github.com/amber-store/dstore/codec"
	"github.com/amber-store/dstore/transport"
	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
)

// barrierRecord is one entry of the node's barrier history (§9.5).
type barrierRecord struct {
	Epoch         uint64 `cbor:"0,keyasint"`
	EligibleBelow uint64 `cbor:"1,keyasint"`
	Confirmed     bool   `cbor:"2,keyasint,omitempty"`
	Overflowed    bool   `cbor:"3,keyasint,omitempty"`
}

// gcMeta is the node's persisted GC bookkeeping.
type gcMeta struct {
	Counter  uint64 `cbor:"0,keyasint"` // barrier counter
	LastAck  int64  `cbor:"1,keyasint,omitempty"`
	LastLive uint64 `cbor:"2,keyasint,omitempty"`
}

// markState is a worker's state for one epoch (§9.4).
type markState struct {
	epoch     uint64
	nonce     []byte
	count     uint64 // this node's barrier count for the epoch
	eligBelow uint64
	placement *view.Placement
	acked     [][]byte

	eligible map[[32]byte]uint64 // records held in packs < eligible_below → pack id
	marked   map[[32]byte]struct{}
	visited  map[[32]byte]struct{}
	forward  map[view.NodeID]map[[32]byte]struct{} // interior keys already forwarded per worker

	queue   [][32]byte             // keys to process
	sender  []byte                 // this sender's id in gc-keys batches
	seqs    map[view.NodeID]uint64 // next batch sequence per target, for the whole epoch
	out     map[view.NodeID]*outBatch
	sent    uint64
	recv    uint64
	seqSeen map[string]uint64 // sender → last seq acked
	missing [][32]byte
	frozen  bool
	busy    int
	dirty   bool
}

type outBatch struct {
	keys   [][32]byte
	expand [][32]byte
}

// sweepInput is what a sweep needs from the mark (§9.6).
type sweepInput struct {
	epoch     uint64
	count     uint64
	eligBelow uint64 // of the previous ack
	placement *view.Placement
	eligible  map[[32]byte]uint64
	marked    map[[32]byte]struct{}
	overflow  bool
}

// gcState is a node's garbage-collection state, worker and coordinator.
type gcState struct {
	n *Node

	mu       sync.Mutex
	meta     gcMeta
	mark     *markState
	lastMark *sweepInput
	swept    map[uint64]bool // epochs this node has reported sweep_done for
	sweeping bool
	started  map[uint64]bool // epochs this coordinator started
	manual   int             // operator runs waiting: the periodic loop yields
}

func newGCState(n *Node) *gcState {
	g := &gcState{n: n, swept: map[uint64]bool{}, started: map[uint64]bool{}}
	_ = n.meta.GetCBOR([]byte(mkGC), &g.meta)
	return g
}

func (g *gcState) counter() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.meta.Counter
}

func (g *gcState) lastLive() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.meta.LastLive
}

func histKey(c uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], c)
	return append([]byte(mkGCHist), b[:]...)
}

func (g *gcState) history(c uint64) (barrierRecord, bool) {
	var r barrierRecord
	if err := g.n.meta.GetCBOR(histKey(c), &r); err != nil {
		return r, false
	}
	return r, true
}

// activeSegmentID returns the id records appended from now on land in
// (or above): one past the highest sealed segment.
func (g *gcState) activeSegmentID() uint64 {
	segs, err := g.n.store.Segments()
	if err != nil || len(segs) == 0 {
		return 1
	}
	return segs[len(segs)-1].ID + 1
}

// ---- node side: barrier ----

func (g *gcState) handleBarrier(ctx context.Context, s transport.Stream, m *wire.Msg) error {
	n := g.n
	// Refuse a barrier less than gc_interval − skew after the previous ack.
	g.mu.Lock()
	last := time.Unix(0, g.meta.LastAck)
	g.mu.Unlock()
	if g.meta.LastAck != 0 && time.Since(last) < n.cfg.GCInterval-5*time.Minute && n.cfg.GCInterval > 10*time.Minute {
		return wire.WriteErr(s, wire.CodeTooSoon, "barrier too soon after the previous ack")
	}
	n.sweepMu.Lock()
	g.mu.Lock()
	c := g.meta.Counter + 1
	rec := barrierRecord{Epoch: m.G, EligibleBelow: g.activeSegmentID()}
	if err := n.meta.SetCBOR(histKey(c), rec); err != nil {
		g.mu.Unlock()
		n.sweepMu.Unlock()
		return wire.WriteErr(s, wire.CodeInternal, err.Error())
	}
	g.meta.Counter = c
	g.meta.LastAck = time.Now().UnixNano()
	_ = n.meta.SetCBOR([]byte(mkGC), g.meta)
	g.mu.Unlock()
	n.sweepMu.Unlock()

	// Ack by CAS into gc.acked; fails once the phase moved on.
	_, err := n.cat.CASGC(ctx, func(st *catalog.GCState) error {
		if st.Epoch != m.G || st.Phase != catalog.PhaseBarrier {
			return errors.New("barrier phase over")
		}
		st.Acked = view.AddID(st.Acked, n.id)
		return nil
	})
	if err != nil {
		return wire.WriteErr(s, wire.CodeUnavailable, err.Error())
	}
	rec.Confirmed = true
	_ = n.meta.SetCBOR(histKey(c), rec)
	return wire.WriteMsg(s, n.stampReply(&wire.Msg{Type: wire.TOK}))
}

// ---- node side: mark ----

func (g *gcState) handleMark(ctx context.Context, s transport.Stream, m *wire.Msg) error {
	n := g.n
	// Find the confirmed count for this epoch.
	g.mu.Lock()
	var count uint64
	var rec barrierRecord
	for c := g.meta.Counter; c > 0 && c+16 > g.meta.Counter; c-- {
		if r, ok := g.history(c); ok && r.Epoch == m.G {
			count, rec = c, r
			break
		}
	}
	g.mu.Unlock()
	if count == 0 || !rec.Confirmed {
		return wire.WriteErr(s, wire.CodeBadRequest, "no confirmed barrier for this epoch")
	}
	pv, err := view.Decode(m.Params)
	if err != nil {
		return wire.WriteErr(s, wire.CodeBadRequest, "params: "+err.Error())
	}
	var acked [][]byte
	_ = codec.Unmarshal(m.Value, &acked)
	ms := &markState{epoch: m.G, nonce: m.Nonce, count: count, eligBelow: rec.EligibleBelow, placement: view.NewPlacement(pv), acked: acked,
		eligible: map[[32]byte]uint64{}, marked: map[[32]byte]struct{}{}, visited: map[[32]byte]struct{}{}, forward: map[view.NodeID]map[[32]byte]struct{}{},
		out: map[view.NodeID]*outBatch{}, seqSeen: map[string]uint64{}}
	// Snapshot the sweepable records: every record in packs below eligible_below.
	segs, err := n.store.Segments()
	if err != nil {
		return wire.WriteErr(s, wire.CodeInternal, err.Error())
	}
	for _, seg := range segs {
		if seg.ID >= rec.EligibleBelow {
			continue
		}
		id := seg.ID
		_ = n.store.ScanIndex(id, func(k key.Key, _ uint64, _ uint32) { ms.eligible[[32]byte(k)] = id })
	}
	g.mu.Lock()
	g.mark = ms
	g.mu.Unlock()
	n.wg.Add(1)
	go g.workerLoop(ms)
	return wire.WriteMsg(s, n.stampReply(&wire.Msg{Type: wire.TAck}))
}

func (g *gcState) current(epoch uint64, nonce []byte) *markState {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.mark == nil || g.mark.epoch != epoch || !bytes.Equal(g.mark.nonce, nonce) {
		return nil
	}
	return g.mark
}

func (g *gcState) handleKeys(ctx context.Context, s transport.Stream, m *wire.Msg) error {
	n := g.n
	ms := g.current(m.G, m.Nonce)
	if ms == nil {
		return wire.WriteErr(s, wire.CodeBadRequest, "no mark state for this epoch")
	}
	keys, err := wire.Keys32(m.Keys)
	if err != nil {
		return wire.WriteErr(s, wire.CodeBadRequest, err.Error())
	}
	sender := string(m.Node)
	g.mu.Lock()
	if ms.frozen {
		g.mu.Unlock()
		return wire.WriteErr(s, wire.CodeMarkFrozen, "mark complete")
	}
	if last, ok := ms.seqSeen[sender]; ok && m.Seq <= last {
		g.mu.Unlock()
		return wire.WriteMsg(s, n.stampReply(&wire.Msg{Type: wire.TAck})) // duplicate
	}
	ms.seqSeen[sender] = m.Seq
	ms.recv++
	for _, k := range keys {
		if _, ok := ms.eligible[k]; ok {
			ms.marked[k] = struct{}{}
		}
	}
	if m.Expand {
		ms.queue = append(ms.queue, keys...)
	}
	ms.dirty = true
	g.mu.Unlock()
	return wire.WriteMsg(s, n.stampReply(&wire.Msg{Type: wire.TAck}))
}

func (g *gcState) handleStatus(ctx context.Context, s transport.Stream, m *wire.Msg) error {
	n := g.n
	ms := g.current(m.G, m.Nonce)
	if ms == nil {
		return wire.WriteErr(s, wire.CodeBadRequest, "no mark state for this epoch")
	}
	g.mu.Lock()
	idle := len(ms.queue) == 0 && ms.busy == 0 && len(ms.out) == 0
	if idle && ms.dirty {
		if err := g.persistMark(ms); err != nil {
			idle = false
		} else {
			ms.dirty = false
		}
	}
	reply := &wire.Msg{Type: wire.TGCStatusRep, Sent: ms.sent, Received: ms.recv, Idle: idle, Marked: uint64(len(ms.marked)), Missing: wire.RawKeys(ms.missing)}
	n.log.Debug("gc status", "epoch", ms.epoch, "sent", ms.sent, "recv", ms.recv, "idle", idle, "queue", len(ms.queue), "busy", ms.busy, "out", len(ms.out), "dirty", ms.dirty)
	if m.Force { // freeze
		ms.frozen = true
	}
	g.mu.Unlock()
	return wire.WriteMsg(s, n.stampReply(reply))
}

func (g *gcState) handleAbort(ctx context.Context, s transport.Stream, m *wire.Msg) error {
	g.mu.Lock()
	if g.mark != nil && g.mark.epoch == m.G {
		g.mark = nil
	}
	g.mu.Unlock()
	return wire.WriteMsg(s, g.n.stampReply(&wire.Msg{Type: wire.TAck}))
}

// worker returns the worker of k: the first node in rank(k, nodes) that
// acked the barrier.
func (ms *markState) worker(k [32]byte) view.NodeID {
	for _, id := range ms.placement.ReadOrder(k) {
		if view.Contains(ms.acked, id) {
			return id
		}
	}
	return view.NodeID{}
}

// workerLoop processes the queue: expands interior objects this node is
// the worker for and delivers keys to their owners.
func (g *gcState) workerLoop(ms *markState) {
	n := g.n
	defer n.wg.Done()
	flush := time.NewTicker(50 * time.Millisecond)
	defer flush.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-flush.C:
		}
		g.mu.Lock()
		if g.mark != ms {
			g.mu.Unlock()
			return
		}
		var batch [][32]byte
		if len(ms.queue) > 0 {
			nb := min(len(ms.queue), 256)
			batch = append(batch, ms.queue[:nb]...)
			ms.queue = ms.queue[nb:]
			ms.busy++
		}
		g.mu.Unlock()
		for _, k := range batch {
			g.process(ms, k)
		}
		if len(batch) > 0 {
			g.mu.Lock()
			ms.busy--
			g.mu.Unlock()
		}
		g.flushOut(ms)
	}
}

func (g *gcState) deliver(ms *markState, k [32]byte) {
	owners := ms.placement.WriteSet(k)
	for _, o := range owners {
		if !view.Contains(ms.acked, o) {
			continue
		}
		if o == g.n.id {
			if _, ok := ms.eligible[k]; ok {
				ms.marked[k] = struct{}{}
			}
			continue
		}
		b := ms.out[o]
		if b == nil {
			b = &outBatch{}
			ms.out[o] = b
		}
		b.keys = append(b.keys, k)
	}
}

// process handles one key (§9.4 pseudo-code).
func (g *gcState) process(ms *markState, k [32]byte) {
	n := g.n
	kk := key.Key(k)
	g.mu.Lock()
	g.deliver(ms, k)
	if kk.Type() == key.Blob || kk.Type() == key.XattrSet {
		g.mu.Unlock()
		return
	}
	w := ms.worker(k)
	if w != n.id {
		fw := ms.forward[w]
		if fw == nil {
			fw = map[[32]byte]struct{}{}
			ms.forward[w] = fw
		}
		if _, done := fw[k]; !done {
			fw[k] = struct{}{}
			b := ms.out[w]
			if b == nil {
				b = &outBatch{}
				ms.out[w] = b
			}
			b.expand = append(b.expand, k)
		}
		g.mu.Unlock()
		return
	}
	if _, seen := ms.visited[k]; seen {
		g.mu.Unlock()
		return
	}
	ms.visited[k] = struct{}{}
	g.mu.Unlock()

	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.ForwardTimeout)
	data, err := n.getData(ctx, k)
	cancel()
	if err != nil {
		g.mu.Lock()
		ms.missing = append(ms.missing, k)
		g.mu.Unlock()
		return
	}
	kids, err := fstree.ChildKeys(kk, data)
	if err != nil {
		g.mu.Lock()
		ms.missing = append(ms.missing, k)
		g.mu.Unlock()
		return
	}
	g.mu.Lock()
	for _, c := range kids {
		ms.queue = append(ms.queue, [32]byte(c))
	}
	g.mu.Unlock()
}

// flushOut sends pending batches, retrying until acked.
func (g *gcState) flushOut(ms *markState) {
	n := g.n
	g.mu.Lock()
	var targets []view.NodeID
	for id, b := range ms.out {
		if len(b.keys) > 0 || len(b.expand) > 0 {
			targets = append(targets, id)
		}
	}
	g.mu.Unlock()
	for _, id := range targets {
		for {
			g.mu.Lock()
			b := ms.out[id]
			if b == nil {
				g.mu.Unlock()
				break
			}
			var keys [][32]byte
			expand := false
			if len(b.expand) > 0 {
				nk := min(len(b.expand), wire.MaxKeys)
				keys, b.expand = b.expand[:nk], b.expand[nk:]
				expand = true
			} else if len(b.keys) > 0 {
				nk := min(len(b.keys), wire.MaxKeys)
				keys, b.keys = b.keys[:nk], b.keys[nk:]
			}
			if len(keys) == 0 {
				delete(ms.out, id)
				g.mu.Unlock()
				break
			}
			ms.sent++
			if ms.seqs == nil {
				ms.seqs = map[view.NodeID]uint64{}
			}
			ms.seqs[id]++
			seq := ms.seqs[id]
			g.mu.Unlock()
			sender := ms.sender
			if sender == nil {
				sender = n.id[:]
			}
			req := n.stampReq(&wire.Msg{Type: wire.TGCKeys, G: ms.epoch, Nonce: ms.nonce, Seq: seq, Keys: wire.RawKeys(keys), Expand: expand, Node: sender})
			for attempt := 0; ; attempt++ {
				ctx, cancel := context.WithTimeout(n.ctx, n.cfg.ForwardTimeout)
				resp, err := n.pool.Call(ctx, id, wire.ALPNCluster, req)
				cancel()
				if err == nil && resp.Type == wire.TAck {
					break
				}
				if wire.IsCode(err, wire.CodeMarkFrozen) || wire.IsCode(err, wire.CodeBadRequest) {
					g.mu.Lock()
					ms.missing = append(ms.missing, [32]byte{}) // a refusal aborts the epoch
					g.mu.Unlock()
					break
				}
				if attempt > 30 || n.ctx.Err() != nil {
					break
				}
				time.Sleep(time.Second)
			}
		}
	}
}

// persistMark writes the mark to <store>/gc/mark-<g> (called under g.mu).
func (g *gcState) persistMark(ms *markState) error {
	dir := filepath.Join(g.n.cfg.StoreDir, "gc")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, fmt.Sprintf("mark-%d", ms.epoch))
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	pv, _ := ms.placement.View().Encode()
	hdr := codec.MustMarshal(map[int]any{0: ms.epoch, 1: ms.count, 2: ms.eligBelow, 3: pv})
	var lb [4]byte
	binary.BigEndian.PutUint32(lb[:], uint32(len(hdr)))
	if _, err := f.Write(lb[:]); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(hdr); err != nil {
		f.Close()
		return err
	}
	for k := range ms.marked {
		if _, err := f.Write(k[:]); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return nil
}

// loadMark reads a persisted mark and rebuilds the eligible set.
func (g *gcState) loadMark(epoch uint64) (*sweepInput, error) {
	path := filepath.Join(g.n.cfg.StoreDir, "gc", fmt.Sprintf("mark-%d", epoch))
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(b) < 4 {
		return nil, io.ErrUnexpectedEOF
	}
	hl := binary.BigEndian.Uint32(b[:4])
	var hdr struct {
		Epoch     uint64 `cbor:"0,keyasint"`
		Count     uint64 `cbor:"1,keyasint"`
		EligBelow uint64 `cbor:"2,keyasint"`
		View      []byte `cbor:"3,keyasint"`
	}
	if err := codec.Unmarshal(b[4:4+hl], &hdr); err != nil {
		return nil, err
	}
	pv, err := view.Decode(hdr.View)
	if err != nil {
		return nil, err
	}
	si := &sweepInput{epoch: hdr.Epoch, count: hdr.Count, placement: view.NewPlacement(pv), eligible: map[[32]byte]uint64{}, marked: map[[32]byte]struct{}{}}
	rest := b[4+hl:]
	for len(rest) >= 32 {
		si.marked[[32]byte(rest[:32])] = struct{}{}
		rest = rest[32:]
	}
	segs, err := g.n.store.Segments()
	if err != nil {
		return nil, err
	}
	for _, seg := range segs {
		if seg.ID >= hdr.EligBelow {
			continue
		}
		id := seg.ID
		_ = g.n.store.ScanIndex(id, func(k key.Key, _ uint64, _ uint32) { si.eligible[[32]byte(k)] = id })
	}
	if prev, ok := g.history(hdr.Count - 1); ok {
		si.eligBelow = prev.EligibleBelow
	}
	return si, nil
}

// ---- node side: sweep ----

// poll reads the gc register and sweeps when the phase says so (§9.6).
func (g *gcState) poll(ctx context.Context) {
	n := g.n
	st, err := n.cat.ReadGC(ctx)
	if err != nil {
		return
	}
	switch st.Phase {
	case catalog.PhaseAborted, catalog.PhaseIdle:
		g.mu.Lock()
		if g.mark != nil && g.mark.epoch <= st.Epoch && st.Phase == catalog.PhaseAborted {
			g.mark = nil
		}
		g.mu.Unlock()
		return
	case catalog.PhaseSweep:
	default:
		return
	}
	if !view.Contains(st.Acked, n.id) {
		return
	}
	g.mu.Lock()
	done := g.swept[st.Epoch] || g.sweeping
	for _, s := range st.SweepDone {
		if bytes.Equal(s.ID, n.id[:]) {
			done = true
		}
	}
	g.mu.Unlock()
	if done {
		return
	}
	// Waves: a quarter of the acked nodes at a time.
	pos := 0
	for i, id := range st.Acked {
		if bytes.Equal(id, n.id[:]) {
			pos = i
		}
	}
	quarter := (len(st.Acked) + 3) / 4
	if quarter == 0 {
		quarter = 1
	}
	if pos >= st.SweepWave*quarter {
		return
	}
	g.mu.Lock()
	g.sweeping = true
	g.mu.Unlock()
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		g.sweep(st.Epoch, st.Garbage)
	}()
}

// sweepInputFor builds the sweep input from the in-memory mark or the
// persisted one.
func (g *gcState) sweepInputFor(epoch uint64) *sweepInput {
	g.mu.Lock()
	ms := g.mark
	g.mu.Unlock()
	if ms != nil && ms.epoch == epoch {
		g.mu.Lock()
		si := &sweepInput{epoch: epoch, count: ms.count, placement: ms.placement, eligible: ms.eligible, marked: ms.marked}
		if prev, ok := g.history(ms.count - 1); ok {
			si.eligBelow = prev.EligibleBelow
		}
		g.mu.Unlock()
		return si
	}
	si, err := g.loadMark(epoch)
	if err != nil {
		return nil
	}
	return si
}

// live is the sweep's predicate (§9.6): a record is dead when it is
// garbage (owned under the mark placement, unmarked, unpinned) or not
// mine (unowned under the current view, unpinned, pack settled).
func (g *gcState) live(si *sweepInput, cur *view.Placement, k [32]byte) bool {
	n := g.n
	if n.isCorrupt(k) {
		return false
	}
	packID, ok := si.eligible[k]
	if !ok {
		return true // young or unknown
	}
	if packID >= si.eligBelow {
		return true
	}
	if n.pinned(k, si.count-1) {
		return true
	}
	owned := si.placement.InWriteSet(k, n.id)
	if owned {
		if _, m := si.marked[k]; m {
			return true
		}
		if si.overflow {
			return true
		}
		return false // clause 1
	}
	if cur.InWriteSet(k, n.id) {
		return true
	}
	if n.rec.settled(packID, cur.View()) {
		return false // clause 2
	}
	return true
}

// sweep compacts this node's packs against the mark and reports.
func (g *gcState) sweep(epoch uint64, garbage float64) {
	n := g.n
	defer func() {
		g.mu.Lock()
		g.sweeping = false
		g.mu.Unlock()
	}()
	si := g.sweepInputFor(epoch)
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.SweepTimeout)
	defer cancel()
	if si == nil {
		_, _ = n.cat.CASGC(ctx, func(st *catalog.GCState) error {
			if st.Epoch != epoch || st.Phase != catalog.PhaseSweep {
				return catalog.ErrAbort
			}
			st.SweepDone = append(st.SweepDone, catalog.NodeStat{ID: n.id[:], NoMark: true})
			return nil
		})
		g.mu.Lock()
		g.swept[epoch] = true
		g.mu.Unlock()
		return
	}
	g.mu.Lock()
	g.lastMark = si
	g.mu.Unlock()
	cur := n.Placement()
	start := time.Now()
	live := func(k key.Key) bool { return g.live(si, cur, [32]byte(k)) }
	ratio := 0.5
	if garbage > 0 {
		ratio = garbage
	}
	if free, total, ok := diskFree(n.cfg.StoreDir); ok && total > 0 && free < total/10 {
		ratio = 0.1
	}
	n.sweepMu.Lock()
	stats, err := n.store.Compact(live, packstore.CompactOpts{MinDeadRatio: ratio, Horizon: time.Now().Add(-n.cfg.Grace)})
	n.sweepMu.Unlock()
	if err != nil {
		n.log.Warn("sweep", "error", err)
	}
	// Drop the pins this sweep no longer needs: those stamped below count−1.
	g.dropPins(si.count - 1)
	liveCount := uint64(len(si.marked))
	g.mu.Lock()
	g.meta.LastLive = liveCount
	_ = n.meta.SetCBOR([]byte(mkGC), g.meta)
	g.swept[epoch] = true
	g.mu.Unlock()
	_, _ = n.cat.CASGC(ctx, func(st *catalog.GCState) error {
		if st.Epoch != epoch || st.Phase != catalog.PhaseSweep {
			return catalog.ErrAbort
		}
		for _, s := range st.SweepDone {
			if bytes.Equal(s.ID, n.id[:]) {
				return catalog.ErrAbort
			}
		}
		st.SweepDone = append(st.SweepDone, catalog.NodeStat{ID: n.id[:], Live: liveCount, Freed: stats.BytesFreed, Packs: stats.SegmentsCompacted, Copied: uint64(stats.RecordsCopied), Duration: int64(time.Since(start))})
		return nil
	})
	n.log.Info("sweep done", "epoch", epoch, "packs", stats.SegmentsCompacted, "freed", stats.BytesFreed, "copied", stats.RecordsCopied, "took", time.Since(start))
}

// dropPins removes pins stamped below the given count.
func (g *gcState) dropPins(below uint64) {
	n := g.n
	b := n.meta.NewBatch()
	_ = n.meta.Scan([]byte(mkPin), func(k, v []byte) bool {
		if len(v) == 8 && getU64(v) < below {
			b.Delete(append([]byte{}, k...))
		}
		return true
	})
	if b.Len() > 0 {
		_ = b.Commit()
	}
}

// provedDead is the reconcile pass's test (§8.4): clause 1 of the sweep
// with the newest complete mark.
func (g *gcState) provedDead(k [32]byte, packID uint64, cur *view.Placement) bool {
	g.mu.Lock()
	si := g.lastMark
	g.mu.Unlock()
	if si == nil || si.overflow {
		return false
	}
	n := g.n
	if !si.placement.InWriteSet(k, n.id) {
		return false
	}
	p, ok := si.eligible[k]
	if !ok || p != packID || p >= si.eligBelow {
		return false
	}
	if _, m := si.marked[k]; m {
		return false
	}
	if n.pinned(k, si.count-1) {
		return false
	}
	return true
}

// ---- coordinator ----

// coordinate runs the coordinator's GC duties on a tick (§9.1, §9.8).
func (g *gcState) coordinate(ctx context.Context) {
	n := g.n
	st, err := n.cat.ReadGC(ctx)
	if err != nil {
		return
	}
	switch st.Phase {
	case catalog.PhaseBarrier, catalog.PhaseMark:
		g.mu.Lock()
		mine := g.started[st.Epoch]
		g.mu.Unlock()
		if !mine {
			// A previous coordinator's epoch: abandon it.
			g.abort(ctx, st.Epoch, "coordinator changed")
		}
		return
	case catalog.PhaseSweep:
		g.observeSweep(ctx, st)
		return
	}
	if st.Hold {
		return
	}
	if time.Since(time.Unix(0, st.BarrierAt)) < n.cfg.GCInterval {
		return
	}
	g.mu.Lock()
	waiting := g.manual > 0
	g.mu.Unlock()
	if waiting {
		return
	}
	if v := n.View(); v != nil && v.VoterSync == view.VoterSyncPending {
		return
	}
	if err := g.runCycle(ctx, false, false); err != nil {
		n.log.Warn("gc cycle", "error", err)
	}
}

// observeSweep releases waves and closes the epoch when every acked node
// is done or sweep_timeout passes.
func (g *gcState) observeSweep(ctx context.Context, st catalog.GCState) {
	n := g.n
	done := map[string]bool{}
	for _, s := range st.SweepDone {
		done[string(s.ID)] = true
	}
	quarter := (len(st.Acked) + 3) / 4
	if quarter == 0 {
		quarter = 1
	}
	released := min(st.SweepWave*quarter, len(st.Acked))
	releasedDone := 0
	for i := 0; i < released; i++ {
		if done[string(st.Acked[i])] {
			releasedDone++
		}
	}
	timedOut := time.Since(time.Unix(0, st.SnapshotAt)) > n.cfg.SweepTimeout
	if len(done) >= len(st.Acked) || timedOut {
		_, _ = n.cat.CASGC(ctx, func(g2 *catalog.GCState) error {
			if g2.Epoch != st.Epoch || g2.Phase != catalog.PhaseSweep {
				return catalog.ErrAbort
			}
			g2.Phase = catalog.PhaseIdle
			g2.Last = g2.SweepDone
			g2.LastOK = g2.Epoch
			return nil
		})
		n.log.Info("gc epoch complete", "epoch", st.Epoch, "nodes", len(done))
		return
	}
	if releasedDone >= released && released < len(st.Acked) {
		_, _ = n.cat.CASGC(ctx, func(g2 *catalog.GCState) error {
			if g2.Epoch != st.Epoch || g2.Phase != catalog.PhaseSweep {
				return catalog.ErrAbort
			}
			g2.SweepWave++
			return nil
		})
	}
}

func (g *gcState) abort(ctx context.Context, epoch uint64, reason string) {
	n := g.n
	_, _ = n.cat.CASGC(ctx, func(st *catalog.GCState) error {
		if st.Epoch != epoch || st.Phase == catalog.PhaseIdle || st.Phase == catalog.PhaseSweep {
			return catalog.ErrAbort
		}
		st.Phase = catalog.PhaseAborted
		st.Error = reason
		return nil
	})
	n.broadcastCluster(ctx, &wire.Msg{Type: wire.TGCAbort, G: epoch})
	n.log.Warn("gc epoch aborted", "epoch", epoch, "reason", reason)
}

// resweep re-runs only the sweep with the marks the nodes hold.
func (g *gcState) resweep(ctx context.Context, garbage float64) error {
	n := g.n
	_, err := n.cat.CASGC(ctx, func(st *catalog.GCState) error {
		if st.Phase != catalog.PhaseIdle || st.LastOK == 0 {
			return errors.New("no completed epoch to re-sweep")
		}
		st.Epoch = st.LastOK
		st.Phase = catalog.PhaseSweep
		st.SweepDone = nil
		st.SweepWave = 1
		st.Garbage = garbage
		st.SnapshotAt = time.Now().UnixNano()
		return nil
	})
	return err
}

// runCycle runs one epoch: barrier, snapshot, mark, then hands the sweep
// to the nodes (§9.2–§9.6).
func (g *gcState) runCycle(ctx context.Context, tolerate, manual bool) error {
	n := g.n
	v := n.View()
	if v == nil {
		return errors.New("no view")
	}
	nonce := make([]byte, 8)
	rand.Read(nonce)
	if manual {
		// An operator's run waits for a running epoch and for the barrier
		// spacing rather than failing; the periodic loop yields meanwhile.
		g.mu.Lock()
		g.manual++
		g.mu.Unlock()
		defer func() {
			g.mu.Lock()
			g.manual--
			g.mu.Unlock()
		}()
		for {
			cur, err := n.cat.ReadGC(ctx)
			if err != nil {
				return err
			}
			busy := cur.Phase == catalog.PhaseBarrier || cur.Phase == catalog.PhaseMark || cur.Phase == catalog.PhaseSweep
			tooSoon := time.Since(time.Unix(0, cur.BarrierAt)) < n.cfg.GCInterval
			if !busy && !tooSoon {
				break
			}
			if cur.Phase == catalog.PhaseSweep {
				g.observeSweep(ctx, cur)
			}
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	var epoch uint64
	st, err := n.cat.CASGC(ctx, func(st *catalog.GCState) error {
		if st.Phase == catalog.PhaseBarrier || st.Phase == catalog.PhaseMark || st.Phase == catalog.PhaseSweep {
			return errors.New("an epoch is in progress")
		}
		if st.Hold {
			return errors.New("gc is on hold (recovery); clear with gc-hold")
		}
		if time.Since(time.Unix(0, st.BarrierAt)) < n.cfg.GCInterval {
			return &wire.Error{Code: wire.CodeTooSoon, Text: fmt.Sprintf("next barrier allowed at %s", time.Unix(0, st.BarrierAt).Add(n.cfg.GCInterval).Format(time.RFC3339))}
		}
		st.Epoch++
		st.Phase = catalog.PhaseBarrier
		st.BarrierAt = time.Now().UnixNano()
		st.Acked = nil
		st.MarkDone = false
		st.SweepDone = nil
		st.SweepWave = 0
		st.Nonce = nonce
		st.Marked = nil
		st.Error = ""
		st.Missing = nil
		st.Tolerate = tolerate
		st.Garbage = 0
		epoch = st.Epoch
		return nil
	})
	if err != nil {
		return err
	}
	g.mu.Lock()
	g.started[epoch] = true
	g.mu.Unlock()
	n.log.Info("gc barrier", "epoch", epoch)

	// Barrier: every data node, in parallel, until all acked or timeout.
	members := v.AllMembers()
	var wg sync.WaitGroup
	for _, id := range members {
		wg.Add(1)
		go func(id view.NodeID) {
			defer wg.Done()
			bctx, cancel := context.WithTimeout(ctx, n.cfg.BarrierTimeout)
			defer cancel()
			req := n.stampReq(&wire.Msg{Type: wire.TGCBarrier, G: epoch})
			if id == n.id {
				g.barrierSelf(bctx, epoch)
				return
			}
			_, _ = n.pool.Call(bctx, id, wire.ALPNCluster, req)
		}(id)
	}
	wg.Wait()

	// Freeze acked; take the roots snapshot (settling undecided registers).
	pv := n.View()
	pvEnc, _ := pv.Encode()
	st, err = n.cat.CASGC(ctx, func(st *catalog.GCState) error {
		if st.Epoch != epoch || st.Phase != catalog.PhaseBarrier {
			return errors.New("epoch changed under the coordinator")
		}
		st.Phase = catalog.PhaseMark
		st.SnapshotAt = time.Now().UnixNano()
		st.Placement = pvEnc
		return nil
	})
	if err != nil {
		return err
	}
	if len(st.Acked) == 0 {
		g.abort(ctx, epoch, "no node acked the barrier")
		return errors.New("no node acked the barrier")
	}
	roots, err := n.cat.RefsSnapshot(ctx)
	if err != nil {
		g.abort(ctx, epoch, "roots snapshot: "+err.Error())
		return err
	}
	// Start every worker.
	ackedEnc := codec.MustMarshal(st.Acked)
	for _, id := range view.IDsOf(st.Acked) {
		req := n.stampReq(&wire.Msg{Type: wire.TGCMark, G: epoch, Nonce: nonce, Params: pvEnc, Value: ackedEnc})
		var resp *wire.Msg
		var err error
		if id == n.id {
			resp, err = g.selfCall(ctx, req)
		} else {
			resp, err = n.pool.Call(ctx, id, wire.ALPNCluster, req)
		}
		if err != nil || resp.Type != wire.TAck {
			g.abort(ctx, epoch, fmt.Sprintf("worker %s did not start: %v", view.ShortID(id), err))
			return fmt.Errorf("worker %s did not start", view.ShortID(id))
		}
	}
	// Deliver the roots as key batches.
	pl := view.NewPlacement(pv)
	// The coordinator is one more sender, distinct from this node's worker.
	ms := &markState{epoch: epoch, nonce: nonce, placement: pl, acked: st.Acked, sender: append(append([]byte{}, n.id[:]...), 'c'), out: map[view.NodeID]*outBatch{}, eligible: map[[32]byte]uint64{}, marked: map[[32]byte]struct{}{}, forward: map[view.NodeID]map[[32]byte]struct{}{}}
	rootKeys := 0
	for _, r := range roots {
		rec, err := reference.Decode(r.Value.Record)
		if err != nil || len(rec.Key) != 32 {
			continue
		}
		k := [32]byte(rec.Key)
		rootKeys++
		w := ms.worker(k)
		if w == n.id {
			g.mu.Lock()
			if g.mark != nil && g.mark.epoch == epoch {
				g.mark.queue = append(g.mark.queue, k)
				g.mark.dirty = true
			}
			g.mu.Unlock()
			continue
		}
		b := ms.out[w]
		if b == nil {
			b = &outBatch{}
			ms.out[w] = b
		}
		b.expand = append(b.expand, k)
	}
	g.flushOut(ms)
	coordSent := ms.sent

	// Termination polling.
	deadline := time.Now().Add(n.cfg.MarkTimeout)
	stable := 0
	var lastSum [2]uint64
	var marked []catalog.NodeStat
	var missing [][32]byte
	for {
		if time.Now().After(deadline) {
			g.abort(ctx, epoch, "mark timeout")
			return errors.New("mark timeout")
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
		var sent, recv uint64 = coordSent, 0
		allIdle := true
		marked = marked[:0]
		missing = missing[:0]
		bad := false
		for _, id := range view.IDsOf(st.Acked) {
			req := n.stampReq(&wire.Msg{Type: wire.TGCStatus, G: epoch, Nonce: nonce})
			var resp *wire.Msg
			var err error
			if id == n.id {
				resp, err = g.selfCall(ctx, req)
			} else {
				sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				resp, err = n.pool.Call(sctx, id, wire.ALPNCluster, req)
				cancel()
			}
			if err != nil || resp.Type != wire.TGCStatusRep {
				bad = true
				break
			}
			sent += resp.Sent
			recv += resp.Received
			if !resp.Idle {
				allIdle = false
			}
			marked = append(marked, catalog.NodeStat{ID: id[:], Marked: resp.Marked})
			for _, mk := range resp.Missing {
				if len(mk) == 32 {
					missing = append(missing, [32]byte(mk))
				} else {
					bad = true
				}
			}
		}
		if bad {
			g.abort(ctx, epoch, "a worker lost its mark state or refused a batch")
			return errors.New("mark aborted: worker error")
		}
		sum := [2]uint64{sent, recv}
		n.log.Debug("gc poll", "epoch", epoch, "sent", sent, "recv", recv, "idle", allIdle, "bad", bad, "stable", stable)
		if allIdle && sent == recv && sum == lastSum {
			stable++
		} else {
			stable = 0
		}
		lastSum = sum
		if stable >= 2 {
			break
		}
	}
	// Missing objects abort unless tolerated (leaves only).
	if len(missing) > 0 {
		interior := false
		for _, k := range missing {
			t := key.Key(k).Type()
			if t != key.Blob && t != key.XattrSet {
				interior = true
			}
		}
		if !tolerate || interior {
			raw := make([][]byte, 0, len(missing))
			for _, k := range missing {
				raw = append(raw, append([]byte{}, k[:]...))
			}
			_, _ = n.cat.CASGC(ctx, func(st *catalog.GCState) error { st.Missing = raw; return nil })
			g.abort(ctx, epoch, fmt.Sprintf("%d objects missing under references", len(missing)))
			return fmt.Errorf("gc: %d objects missing under references; nothing swept", len(missing))
		}
	}
	// Freeze the workers and move to sweep.
	for _, id := range view.IDsOf(st.Acked) {
		req := n.stampReq(&wire.Msg{Type: wire.TGCStatus, G: epoch, Nonce: nonce, Force: true})
		if id == n.id {
			g.selfCall(ctx, req)
		} else {
			sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			_, _ = n.pool.Call(sctx, id, wire.ALPNCluster, req)
			cancel()
		}
	}
	_, err = n.cat.CASGC(ctx, func(st *catalog.GCState) error {
		if st.Epoch != epoch || st.Phase != catalog.PhaseMark {
			return errors.New("epoch changed under the coordinator")
		}
		st.MarkDone = true
		st.Marked = marked
		st.Phase = catalog.PhaseSweep
		st.SweepWave = 1
		st.SnapshotAt = time.Now().UnixNano()
		return nil
	})
	if err != nil {
		return err
	}
	n.log.Info("gc mark complete", "epoch", epoch, "roots", rootKeys, "workers", len(st.Acked))
	if manual {
		// Wait for the sweep to finish so the operator sees a result.
		for {
			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
			cur, err := n.cat.ReadGC(ctx)
			if err != nil {
				return err
			}
			g.observeSweep(ctx, cur)
			n.gc.poll(ctx)
			if cur.Epoch != epoch || cur.Phase != catalog.PhaseSweep {
				return nil
			}
		}
	}
	return nil
}

// barrierSelf runs the barrier handler in-process.
func (g *gcState) barrierSelf(ctx context.Context, epoch uint64) {
	a, b := newLoopback()
	go func() {
		_ = g.handleBarrier(ctx, b, &wire.Msg{Type: wire.TGCBarrier, G: epoch})
		b.Close()
	}()
	_, _ = wire.ReadMsg(a)
	a.Close()
}

// selfCall runs a cluster handler in-process and returns its reply.
func (g *gcState) selfCall(ctx context.Context, req *wire.Msg) (*wire.Msg, error) {
	a, b := newLoopback()
	go func() {
		var err error
		switch req.Type {
		case wire.TGCMark:
			err = g.handleMark(ctx, b, req)
		case wire.TGCStatus:
			err = g.handleStatus(ctx, b, req)
		case wire.TGCKeys:
			err = g.handleKeys(ctx, b, req)
		}
		_ = err
		b.Close()
	}()
	m, err := wire.ReadMsg(a)
	a.Close()
	if err != nil {
		return nil, err
	}
	if m.Type == wire.TErr {
		return m, wire.ErrorFromMsg(m)
	}
	return m, nil
}

// statusText renders the gc register for operators.
func (g *gcState) statusText(st catalog.GCState) string {
	var b bytes.Buffer
	fmt.Fprintf(&b, "epoch %d %s", st.Epoch, catalog.PhaseName(st.Phase))
	if st.BarrierAt > 0 {
		fmt.Fprintf(&b, ", barrier %s", time.Unix(0, st.BarrierAt).Format(time.RFC3339))
	}
	if st.Phase == catalog.PhaseBarrier || st.Phase == catalog.PhaseMark || st.Phase == catalog.PhaseSweep {
		fmt.Fprintf(&b, ", %d acked", len(st.Acked))
	}
	if st.Phase == catalog.PhaseSweep {
		fmt.Fprintf(&b, ", %d/%d swept (wave %d)", len(st.SweepDone), len(st.Acked), st.SweepWave)
	}
	if st.Hold {
		b.WriteString(", ON HOLD")
	}
	if st.Error != "" {
		fmt.Fprintf(&b, ", last error: %s", st.Error)
	}
	if len(st.Missing) > 0 {
		fmt.Fprintf(&b, ", %d missing objects", len(st.Missing))
	}
	if len(st.Last) > 0 {
		var live, freed uint64
		for _, s := range st.Last {
			live += s.Live
			freed += s.Freed
		}
		fmt.Fprintf(&b, "; last epoch %d: %d live records, %d bytes freed", st.LastOK, live, freed)
	}
	return b.String()
}

// ---- in-process loopback stream ----

type loopStream struct {
	r *io.PipeReader
	w *io.PipeWriter
}

func newLoopback() (*loopStream, *loopStream) {
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()
	return &loopStream{r: r1, w: w2}, &loopStream{r: r2, w: w1}
}

func (l *loopStream) Read(p []byte) (int, error)  { return l.r.Read(p) }
func (l *loopStream) Write(p []byte) (int, error) { return l.w.Write(p) }
func (l *loopStream) Close() error                { l.w.Close(); l.r.Close(); return nil }
func (l *loopStream) CloseWrite() error           { return l.w.Close() }
func (l *loopStream) CancelRead(uint64)           { l.r.Close() }

var _ transport.Stream = (*loopStream)(nil)

// sortedIDs is a test helper.
func sortedIDs(ids [][]byte) [][]byte {
	out := append([][]byte{}, ids...)
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i], out[j]) < 0 })
	return out
}
