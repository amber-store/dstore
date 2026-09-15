package node

import (
	"bytes"
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amber-store/core/reference"
	"github.com/amber-store/dstore/paxos"
	"github.com/amber-store/dstore/refglob"
	"github.com/amber-store/dstore/transport"
	"github.com/amber-store/dstore/wire"
)

// Reference watching (architecture/dstore.md §7, Watching): a client sends
// a glob and the names and keys it holds; the node answers with the
// difference, then pushes every later change. The node learns of changes
// from the coordinator of each write (a ref-changed hint on the cluster
// ALPN) and repairs whatever a lost hint missed with a periodic majority
// scan, so that hints are only a latency optimisation.

// refHint is one committed reference write as the watchers learn of it.
type refHint struct {
	name    string
	record  []byte // nil for a deletion
	version []byte // the commit ballot
}

// watchSub is one watch stream's subscription to hints.
type watchSub struct {
	pat *refglob.Pattern
	ch  chan refHint
	// rescan is set when a hint could not be queued; the stream then
	// re-scans instead of relying on the hints it has.
	rescan atomic.Bool
}

// watchers is the registry of the watch streams a node serves.
type watchers struct {
	mu   sync.Mutex
	subs map[*watchSub]struct{}
}

// subscribe registers a pattern and returns its hint channel and the
// function that removes it again.
func (w *watchers) subscribe(pat *refglob.Pattern) (*watchSub, func()) {
	sub := &watchSub{pat: pat, ch: make(chan refHint, 1024)}
	w.mu.Lock()
	if w.subs == nil {
		w.subs = map[*watchSub]struct{}{}
	}
	w.subs[sub] = struct{}{}
	w.mu.Unlock()
	return sub, func() {
		w.mu.Lock()
		delete(w.subs, sub)
		w.mu.Unlock()
	}
}

// hint delivers h to every subscriber whose pattern matches its name.
func (w *watchers) hint(h refHint) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for sub := range w.subs {
		if !sub.pat.Match(h.name) {
			continue
		}
		select {
		case sub.ch <- h:
		default:
			sub.rescan.Store(true)
		}
	}
}

// count returns the number of watch streams.
func (w *watchers) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.subs)
}

// refChanged records a committed reference write: this node's watchers
// learn of it now and every other member gets a ref-changed hint. record
// is nil for a deletion.
func (n *Node) refChanged(name string, record, version []byte) {
	n.watch.hint(refHint{name: name, record: record, version: version})
	ctx := n.ctx
	if ctx == nil { // not started: the CLI on a local node
		ctx = context.Background()
	}
	n.broadcastCluster(ctx, &wire.Msg{Type: wire.TRefChanged, Name: name, Record: record, Version: version})
}

// refState is what a watch stream believes its client holds for a name:
// the key (nil once the client was told the name is gone) and the
// version the belief rests on (zero for a key the client reported).
type refState struct {
	key     []byte
	version paxos.Ballot
}

// refWatch is one watch stream.
type refWatch struct {
	n     *Node
	s     transport.Stream
	pat   *refglob.Pattern
	known map[string]refState
	batch wire.Msg // the ref-changes frame being filled
	size  int
}

// handleRefWatch serves ref-watch. It runs under the node's context, not
// the per-stream timeout, and ends when the client closes the stream, a
// write fails, or the node shuts down.
func (n *Node) handleRefWatch(s transport.Stream, m *wire.Msg) error {
	pat, err := refglob.Compile(m.Pattern)
	if err != nil {
		return wire.WriteErr(s, wire.CodeBadRequest, err.Error())
	}
	if n.View() == nil {
		return wire.WriteErr(s, wire.CodeUnavailable, "no view")
	}
	w := &refWatch{n: n, s: s, pat: pat, known: map[string]refState{}}
	for _, r := range m.Refs {
		if pat.Match(r.Name) && len(r.Key) > 0 {
			w.known[r.Name] = refState{key: r.Key}
		}
	}
	return w.run()
}

func (w *refWatch) run() error {
	n := w.n
	ctx, cancel := context.WithCancel(n.ctx)
	defer cancel()
	sub, unsubscribe := n.watch.subscribe(w.pat)
	defer unsubscribe()
	// The client ends the watch by closing its side of the stream.
	go func() {
		_, _ = io.Copy(io.Discard, w.s)
		cancel()
	}()

	if err := w.reconcile(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return n.catalogErr(w.s, err)
	}
	if err := w.synced(); err != nil {
		return err
	}
	ticker := time.NewTicker(n.cfg.WatchReconcile)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case h := <-sub.ch:
			w.applyHint(h)
			// Drain what else is queued into the same frame.
			for len(sub.ch) > 0 {
				w.applyHint(<-sub.ch)
			}
			if err := w.flush(); err != nil {
				return err
			}
			if sub.rescan.Swap(false) {
				if err := w.rescan(ctx); err != nil {
					return err
				}
			}
		case <-ticker.C:
			if err := w.rescan(ctx); err != nil {
				return err
			}
		}
	}
}

// rescan reconciles against the catalog and sends the heartbeat; a scan
// that fails is retried at the next tick.
func (w *refWatch) rescan(ctx context.Context) error {
	if err := w.reconcile(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		w.n.log.Debug("watch reconcile failed", "pattern", w.pat.String(), "error", err)
		return nil
	}
	return w.synced()
}

// reconcile scans the pattern's prefix and queues the difference between
// the catalog and what the client holds.
func (w *refWatch) reconcile(ctx context.Context) error {
	sctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	seen := map[string]struct{}{}
	after := ""
	for {
		entries, next, err := w.n.cat.RefList(sctx, w.pat.Prefix(), after, 20000)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if !w.pat.Match(e.Name) {
				continue
			}
			r, err := reference.Decode(e.Value.Record)
			if err != nil {
				continue
			}
			seen[e.Name] = struct{}{}
			version, _ := paxos.ParseBallot(e.Value.Version)
			st, ok := w.known[e.Name]
			if ok && !st.version.IsZero() && version.Compare(st.version) < 0 {
				continue // a hint got ahead of a lagging row
			}
			w.known[e.Name] = refState{key: r.Key, version: version}
			if ok && bytes.Equal(st.key, r.Key) {
				continue
			}
			w.queue(wire.RefInfo{Name: e.Name, Key: r.Key, Version: e.Value.Version, CreatedAt: r.CreatedAt, User: r.User})
		}
		if next == "" || len(entries) == 0 {
			break
		}
		after = next
	}
	for name, st := range w.known {
		if _, ok := seen[name]; ok {
			continue
		}
		delete(w.known, name)
		if st.key != nil {
			w.queueDeleted(name)
		}
	}
	return w.flush()
}

// applyHint folds one hint into the client's state and queues what it
// changes. A hint at or below the known version is stale or a duplicate.
func (w *refWatch) applyHint(h refHint) {
	version, err := paxos.ParseBallot(h.version)
	if err != nil || version.IsZero() {
		return
	}
	st, ok := w.known[h.name]
	if ok && !st.version.IsZero() && version.Compare(st.version) <= 0 {
		return
	}
	if h.record == nil {
		// Remember the deletion so that a delayed older put is ignored.
		w.known[h.name] = refState{version: version}
		if ok && st.key != nil {
			w.queueDeleted(h.name)
		}
		return
	}
	r, err := reference.Decode(h.record)
	if err != nil {
		return
	}
	w.known[h.name] = refState{key: r.Key, version: version}
	if ok && bytes.Equal(st.key, r.Key) {
		return
	}
	w.queue(wire.RefInfo{Name: h.name, Key: r.Key, Version: h.version, CreatedAt: r.CreatedAt, User: r.User})
}

func (w *refWatch) queue(r wire.RefInfo) {
	w.batch.Refs = append(w.batch.Refs, r)
	w.size += len(r.Name) + len(r.Key) + len(r.Version) + len(r.User) + 16
	if w.size > wire.MaxPageBytes {
		_ = w.flush()
	}
}

func (w *refWatch) queueDeleted(name string) {
	w.batch.Deleted = append(w.batch.Deleted, name)
	w.size += len(name) + 8
	if w.size > wire.MaxPageBytes {
		_ = w.flush()
	}
}

// flush sends the queued changes, if any.
func (w *refWatch) flush() error {
	if len(w.batch.Refs) == 0 && len(w.batch.Deleted) == 0 {
		return nil
	}
	m := w.batch
	m.Type = wire.TRefChanges
	w.batch = wire.Msg{}
	w.size = 0
	return wire.WriteMsg(w.s, w.n.stampReply(&m))
}

// synced tells the client it holds the current state; it doubles as the
// heartbeat.
func (w *refWatch) synced() error {
	return wire.WriteMsg(w.s, w.n.stampReply(&wire.Msg{Type: wire.TRefSynced}))
}
