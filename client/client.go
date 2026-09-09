// Package client is the dstore client library (architecture/dstore.md
// §11): a cluster handle with a view cache, a connection pool per node,
// primary-forwarded writes, ranked reads, and tree push and pull against a
// local packstore.
package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/amber-store/dstore/codec"
	"github.com/amber-store/dstore/ticket"
	"github.com/amber-store/dstore/transport"
	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
)

// Config configures a cluster handle.
type Config struct {
	Endpoint transport.Endpoint
	Ticket   ticket.Ticket
	Conns    int // connections per node, default 4
	Jobs     int // parallel workers, default 8
	Logger   *slog.Logger
	// GCInterval is the cluster's gc_interval, for re-pinning long pushes;
	// default 4 h.
	GCInterval time.Duration
	// RequestTimeout bounds one request; default 2 min.
	RequestTimeout time.Duration
	// BatchBytes is the target size of one put batch; default 16 MiB, at
	// most wire.MaxPutBatch. Conns batches are in flight per primary.
	BatchBytes int
}

// Cluster is a handle on a dstore cluster.
type Cluster struct {
	cfg  Config
	log  *slog.Logger
	ep   transport.Endpoint
	pool *transport.Pool

	mu        sync.RWMutex
	view      *view.View
	pl        *view.Placement
	bootAddrs map[view.NodeID][]string
	backoff   map[view.NodeID]time.Time
	failures  map[view.NodeID]int
	unreach   map[view.NodeID]struct{} // members the last view reply said were unreachable
}

// Dial connects to any bootstrap node of the ticket and fetches the view.
func Dial(ctx context.Context, cfg Config) (*Cluster, error) {
	if cfg.Endpoint == nil {
		return nil, errors.New("client: no endpoint")
	}
	if cfg.Conns <= 0 {
		cfg.Conns = 4
	}
	if cfg.Jobs <= 0 {
		cfg.Jobs = 8
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.GCInterval == 0 {
		cfg.GCInterval = 4 * time.Hour
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 2 * time.Minute
	}
	if cfg.BatchBytes <= 0 {
		cfg.BatchBytes = defaultBatchBytes
	}
	cfg.BatchBytes = min(cfg.BatchBytes, wire.MaxPutBatch)
	c := &Cluster{cfg: cfg, log: cfg.Logger, ep: cfg.Endpoint, bootAddrs: map[view.NodeID][]string{}, backoff: map[view.NodeID]time.Time{}, failures: map[view.NodeID]int{}, unreach: map[view.NodeID]struct{}{}}
	for _, m := range cfg.Ticket.Members {
		if len(m.ID) == 32 {
			c.bootAddrs[view.NodeID(m.ID)] = m.Addrs
		}
	}
	c.pool = transport.NewPool(cfg.Endpoint, c.addrsOf, cfg.Conns)
	var lastErr error
	for _, m := range cfg.Ticket.Members {
		if len(m.ID) != 32 {
			continue
		}
		id := view.NodeID(m.ID)
		dctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		resp, err := c.pool.Call(dctx, id, wire.ALPNClient, &wire.Msg{Type: wire.TView})
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		if err := c.adoptReply(resp); err != nil {
			lastErr = err
			continue
		}
		c.log.Info("connected", append([]any{"node", view.ShortID(id), "nodes", len(c.View().Nodes)}, c.pathAttrs(id)...)...)
		return c, nil
	}
	if lastErr == nil {
		lastErr = errors.New("client: ticket names no nodes")
	}
	c.pool.Close()
	return nil, fmt.Errorf("client: no bootstrap node answered: %w", lastErr)
}

// Close releases the connections (not the endpoint).
func (c *Cluster) Close() { c.pool.Close() }

// View returns the cached view.
func (c *Cluster) View() *view.View {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.view
}

// Placement returns the cached placement tables.
func (c *Cluster) Placement() *view.Placement {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.pl
}

func (c *Cluster) adoptReply(m *wire.Msg) error {
	if m.Type != wire.TViewReply {
		return fmt.Errorf("client: unexpected reply %d", m.Type)
	}
	v, err := view.Decode(m.View)
	if err != nil {
		return err
	}
	c.adopt(v)
	c.mu.Lock()
	c.unreach = map[view.NodeID]struct{}{}
	for _, u := range m.Unreachable {
		if len(u) == 32 {
			c.unreach[view.NodeID(u)] = struct{}{}
		}
	}
	c.mu.Unlock()
	return nil
}

func (c *Cluster) adopt(v *view.View) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.view != nil {
		cmp := c.view.Compare(v.Incarnation, v.Epoch)
		if cmp > 0 || cmp == 0 && v.Version <= c.view.Version {
			return
		}
	}
	c.view = v
	c.pl = view.NewPlacement(v)
}

func (c *Cluster) addrsOf(id view.NodeID) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.view != nil {
		if nd, ok := c.view.Node(id); ok && len(nd.Addrs) > 0 {
			return nd.Addrs
		}
	}
	return c.bootAddrs[id]
}

// RefreshView fetches the view from any node.
func (c *Cluster) RefreshView(ctx context.Context) error {
	resp, err := c.anyNode(ctx, &wire.Msg{Type: wire.TView})
	if err != nil {
		return err
	}
	return c.adoptReply(resp)
}

func (c *Cluster) stamp(m *wire.Msg) *wire.Msg {
	v := c.View()
	if v != nil {
		m.ClusterID = v.ClusterID
		m.Incarnation = v.Incarnation
		m.Epoch = v.Epoch
	}
	return m
}

// handleErr adopts a newer view carried by a stale-view/not-owner reply
// and records node failures for backoff.
func (c *Cluster) handleErr(id view.NodeID, err error) {
	if we, ok := wire.AsError(err); ok {
		if len(we.View) > 0 {
			if v, derr := view.Decode(we.View); derr == nil {
				c.adopt(v)
			}
		}
		return
	}
	c.mu.Lock()
	c.failures[id]++
	d := 5 * time.Second << min(c.failures[id]-1, 4)
	if d > 60*time.Second {
		d = 60 * time.Second
	}
	c.backoff[id] = time.Now().Add(d)
	c.mu.Unlock()
}

func (c *Cluster) ok(id view.NodeID) {
	c.mu.Lock()
	delete(c.backoff, id)
	delete(c.failures, id)
	delete(c.unreach, id)
	c.mu.Unlock()
}

func (c *Cluster) penalty(id view.NodeID) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	p := 0
	if t, ok := c.backoff[id]; ok && time.Now().Before(t) {
		p += 2
	}
	if _, ok := c.unreach[id]; ok {
		p++
	}
	return p
}

// probeHinted checks the members a view reply flagged unreachable before
// a transfer starts. The hint is one node's view and may be stale; an
// owner it demoted would otherwise sit out the transfer (§11.1). A probe
// that answers clears the hint, one that fails adds the usual backoff.
func (c *Cluster) probeHinted(ctx context.Context) {
	c.mu.RLock()
	ids := make([]view.NodeID, 0, len(c.unreach))
	for id := range c.unreach {
		ids = append(ids, id)
	}
	c.mu.RUnlock()
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id view.NodeID) {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			_, _ = c.call(pctx, id, &wire.Msg{Type: wire.TView})
		}(id)
	}
	wg.Wait()
}

// call sends one request to a node and reads one reply.
func (c *Cluster) call(ctx context.Context, id view.NodeID, m *wire.Msg) (*wire.Msg, error) {
	cctx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	defer cancel()
	resp, err := c.pool.Call(cctx, id, wire.ALPNClient, c.stamp(m))
	if err != nil {
		c.handleErr(id, err)
		return resp, err
	}
	c.ok(id)
	if resp.Epoch > 0 || resp.Incarnation > 0 {
		if v := c.View(); v != nil && v.Compare(resp.Incarnation, resp.Epoch) < 0 {
			go c.RefreshView(context.Background())
		}
	}
	return resp, nil
}

// callRetry retries stale-view replies after adopting the newer view.
func (c *Cluster) callRetry(ctx context.Context, id view.NodeID, m *wire.Msg) (*wire.Msg, error) {
	var resp *wire.Msg
	var err error
	for attempt := 0; attempt < 4; attempt++ {
		resp, err = c.call(ctx, id, m)
		if err == nil || !wire.IsCode(err, wire.CodeStaleView) {
			return resp, err
		}
	}
	return resp, err
}

// preferred orders node ids by path preference (§11.1); see rankOwners.
func (c *Cluster) preferred(ids []view.NodeID) []view.NodeID {
	return rankOwners(ids, c.penalty, func(id view.NodeID) (transport.PathInfo, bool) {
		return c.pool.Path(id, wire.ALPNClient)
	})
}

// Primary returns the owner of key this client sends writes to.
func (c *Cluster) Primary(key [32]byte) (view.NodeID, bool) {
	owners := c.Placement().Owners(key)
	if len(owners) == 0 {
		return view.NodeID{}, false
	}
	return c.preferred(owners)[0], true
}

// Owners returns owners(key, nodes).
func (c *Cluster) Owners(key [32]byte) []view.NodeID { return c.Placement().Owners(key) }

// WriteSet returns owners under nodes ∪ pending.nodes.
func (c *Cluster) WriteSet(key [32]byte) []view.NodeID { return c.Placement().WriteSet(key) }

// ReadOrder returns the read order of key, refined by path preference.
func (c *Cluster) ReadOrder(key [32]byte) []view.NodeID {
	order := c.Placement().ReadOrder(key)
	r := int(c.View().Replicas)
	if len(order) <= r {
		return c.preferred(order)
	}
	return append(c.preferred(order[:r]), order[r:]...)
}

// anyNode sends a request to nodes in preference order until one answers.
func (c *Cluster) anyNode(ctx context.Context, m *wire.Msg) (*wire.Msg, error) {
	v := c.View()
	var ids []view.NodeID
	if v != nil {
		for _, nd := range v.Nodes {
			ids = append(ids, nd.NID())
		}
	}
	if len(ids) == 0 {
		for id := range c.bootAddrs {
			ids = append(ids, id)
		}
	}
	var lastErr error
	for _, id := range c.preferred(ids) {
		resp, err := c.call(ctx, id, m)
		for attempt := 0; err != nil && wire.IsCode(err, wire.CodeStaleView) && attempt < 4; attempt++ {
			// The node was ahead of this client's view; handleErr adopted
			// the view it sent, so the retry carries the new epoch.
			resp, err = c.call(ctx, id, m)
		}
		if err == nil {
			return resp, nil
		}
		if _, remote := wire.AsError(err); remote {
			return resp, err // the node answered; its answer stands
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("client: no nodes")
	}
	return nil, lastErr
}

// Status fetches a node's status report.
func (c *Cluster) Status(ctx context.Context, id view.NodeID) ([]byte, error) {
	resp, err := c.call(ctx, id, &wire.Msg{Type: wire.TStatus})
	if err != nil {
		return nil, err
	}
	return resp.Status, nil
}

// Admin runs an operator command on a node (any node when id is zero).
func (c *Cluster) Admin(ctx context.Context, id view.NodeID, req any) ([]byte, error) {
	m := &wire.Msg{Type: wire.TAdmin, Params: codec.MustMarshal(req)}
	var resp *wire.Msg
	var err error
	if id == (view.NodeID{}) {
		resp, err = c.anyNode(ctx, m)
	} else {
		resp, err = c.call(ctx, id, m)
	}
	if err != nil {
		return nil, err
	}
	if resp.Type != wire.TAdminReply {
		return nil, fmt.Errorf("client: unexpected reply %d", resp.Type)
	}
	return resp.Status, nil
}

// Nodes returns the ids of the members under nodes.
func (c *Cluster) Nodes() []view.NodeID {
	v := c.View()
	if v == nil {
		return nil
	}
	ids := make([]view.NodeID, 0, len(v.Nodes))
	for _, nd := range v.Nodes {
		ids = append(ids, nd.NID())
	}
	return ids
}
