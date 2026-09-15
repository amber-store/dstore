package client

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"math/rand/v2"
	"time"

	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
)

// RefChange is one event of a reference watch: a reference whose key the
// client did not hold (new or changed), a deletion, or a Synced marker.
type RefChange struct {
	Name      string
	Key       []byte // nil when Deleted
	Version   []byte
	CreatedAt int64
	User      string
	Deleted   bool
	// Synced marks the point at which the client holds the cluster's
	// current state for the pattern; it is yielded once per connection,
	// after the initial difference, with Node set to the serving node.
	Synced bool
	Node   view.NodeID
}

// errWatchIdle reports a watch stream that went quiet.
var errWatchIdle = errors.New("client: watch stream idle")

// WatchRefs watches the references matching a glob (architecture/dstore.md
// §7, Watching). known maps the names the caller holds to their keys; the
// sequence first yields the difference between it and the cluster —
// references with another key, references the caller did not list,
// deletions of names it listed — then a Synced marker, then every later
// change as it happens. The iterator keeps its own copy of known current
// from what it yields, so when a connection dies it reconnects to another
// node and re-sends the watch from where it was. It ends when ctx is
// cancelled, when the consumer stops, or on a terminal error from the
// cluster (a bad pattern, an allowlist refusal), which it yields.
func (c *Cluster) WatchRefs(ctx context.Context, pattern string, known map[string][]byte) iter.Seq2[RefChange, error] {
	state := make(map[string][]byte, len(known))
	for name, key := range known {
		state[name] = key
	}
	return func(yield func(RefChange, error) bool) {
		delay := time.Second
		for ctx.Err() == nil {
			served := false
			for _, id := range c.preferred(c.allNodes()) {
				res := c.watchOnce(ctx, id, pattern, state, yield)
				for attempt := 0; res == watchRetry && attempt < 3; attempt++ {
					res = c.watchOnce(ctx, id, pattern, state, yield)
				}
				switch res {
				case watchStop:
					return
				case watchFatal:
					return
				case watchServed:
					served = true
				}
				if served {
					break // re-rank the nodes before the next attempt
				}
			}
			if served {
				// The stream ran and died; the node is penalised, so the
				// next attempt goes elsewhere. A short pause keeps a node
				// that keeps dropping streams from spinning the loop.
				delay = time.Second
				select {
				case <-ctx.Done():
					return
				case <-time.After(200 * time.Millisecond):
				}
				continue
			}
			rctx, cancel := context.WithTimeout(ctx, 15*time.Second)
			_ = c.RefreshView(rctx)
			cancel()
			c.log.Warn("watch: no node answered, retrying", "pattern", pattern, "in", delay)
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay + time.Duration(rand.Int64N(int64(delay/2)+1))):
			}
			delay = min(2*delay, 30*time.Second)
		}
	}
}

// allNodes lists the members of the view, or the bootstrap nodes before
// there is one.
func (c *Cluster) allNodes() []view.NodeID {
	ids := c.Nodes()
	if len(ids) == 0 {
		c.mu.RLock()
		for id := range c.bootAddrs {
			ids = append(ids, id)
		}
		c.mu.RUnlock()
	}
	return ids
}

type watchResult int

const (
	watchFailed watchResult = iota // nothing came of this node; try the next
	watchRetry                     // stale view adopted; retry this node
	watchServed                    // the stream ran and then died; reconnect
	watchStop                      // the consumer stopped or ctx ended
	watchFatal                     // a terminal error was yielded
)

type watchFrame struct {
	m   *wire.Msg
	err error
}

// watchOnce runs one watch stream against id, updating state and yielding
// as frames arrive.
func (c *Cluster) watchOnce(ctx context.Context, id view.NodeID, pattern string, state map[string][]byte, yield func(RefChange, error) bool) watchResult {
	octx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	s, err := c.pool.Open(octx, id, wire.ALPNClient)
	cancel()
	if err != nil {
		c.handleErr(id, err)
		return watchFailed
	}
	defer wire.CloseStream(s)
	refs := make([]wire.RefInfo, 0, len(state))
	for name, key := range state {
		refs = append(refs, wire.RefInfo{Name: name, Key: key})
	}
	if err := wire.WriteMsg(s, c.stamp(&wire.Msg{Type: wire.TRefWatch, Pattern: pattern, Refs: refs})); err != nil {
		c.pool.Drop(id, wire.ALPNClient)
		c.handleErr(id, err)
		return watchFailed
	}

	frames := make(chan watchFrame, 1)
	rctx, rcancel := context.WithCancel(ctx)
	defer rcancel()
	go func() {
		for {
			m, err := wire.ReadMsg(s)
			select {
			case frames <- watchFrame{m, err}:
			case <-rctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	abandon := func() {
		s.CancelRead(0)
		_ = s.Close()
	}

	synced := false
	served := func() watchResult {
		if synced {
			return watchServed
		}
		return watchFailed
	}
	idle := time.NewTimer(c.cfg.WatchIdle)
	defer idle.Stop()
	for {
		var f watchFrame
		select {
		case <-ctx.Done():
			abandon()
			return watchStop
		case <-idle.C:
			abandon()
			c.pool.Drop(id, wire.ALPNClient)
			c.handleErr(id, errWatchIdle)
			c.log.Warn("watch: stream idle, reconnecting", "node", view.ShortID(id))
			return served()
		case f = <-frames:
		}
		if f.err != nil {
			abandon()
			c.pool.Drop(id, wire.ALPNClient)
			c.handleErr(id, f.err)
			c.log.Warn("watch: stream ended, reconnecting", "node", view.ShortID(id), "error", f.err)
			return served()
		}
		idle.Reset(c.cfg.WatchIdle)
		m := f.m
		switch m.Type {
		case wire.TErr:
			e := wire.ErrorFromMsg(m)
			c.handleErr(id, e)
			switch e.Code {
			case wire.CodeStaleView:
				return watchRetry
			case wire.CodeBadRequest, wire.CodeUnauthorized:
				yield(RefChange{}, e)
				return watchFatal
			}
			c.log.Warn("watch: node refused, trying the next", "node", view.ShortID(id), "error", e)
			return watchFailed
		case wire.TRefChanges:
			c.ok(id)
			for _, r := range m.Refs {
				state[r.Name] = r.Key
				if !yield(RefChange{Name: r.Name, Key: r.Key, Version: r.Version, CreatedAt: r.CreatedAt, User: r.User}, nil) {
					abandon()
					return watchStop
				}
			}
			for _, name := range m.Deleted {
				delete(state, name)
				if !yield(RefChange{Name: name, Deleted: true}, nil) {
					abandon()
					return watchStop
				}
			}
		case wire.TRefSynced:
			c.ok(id)
			if !synced {
				synced = true
				c.log.Info("watch: synced", "pattern", pattern, "node", view.ShortID(id), "refs", len(state))
				if !yield(RefChange{Synced: true, Node: id}, nil) {
					abandon()
					return watchStop
				}
			}
		default:
			abandon()
			c.pool.Drop(id, wire.ALPNClient)
			c.handleErr(id, fmt.Errorf("%w: type %d", wire.ErrProtocol, m.Type))
			return served()
		}
	}
}
