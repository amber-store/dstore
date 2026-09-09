// Package transport abstracts the QUIC stream layer the node, client and
// coordinator are written against (architecture/dstore.md §16): an iroh
// implementation for real deployments and an in-memory one that drives a
// whole cluster in one process with partitions and delays.
package transport

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
)

// Stream is one bidirectional stream.
type Stream interface {
	io.ReadWriteCloser
	// CloseWrite finishes the send side.
	CloseWrite() error
	// CancelRead abandons the receive side.
	CancelRead(code uint64)
}

// PathInfo describes a connection's current path.
type PathInfo struct {
	Direct bool
	RTT    time.Duration // 0 until the path has a measurement
}

// Conn is an authenticated connection to a peer.
type Conn interface {
	RemoteID() view.NodeID
	ALPN() string
	OpenStream(ctx context.Context) (Stream, error)
	AcceptStream(ctx context.Context) (Stream, error)
	Close() error
	Path() PathInfo
	// Done is closed when the connection is closed.
	Done() <-chan struct{}
}

// Endpoint dials and accepts connections under one identity.
type Endpoint interface {
	ID() view.NodeID
	// Dial connects to id at the given advisory addresses under alpn.
	Dial(ctx context.Context, id view.NodeID, addrs []string, alpn string) (Conn, error)
	// Accept returns the next incoming connection on any registered ALPN.
	Accept(ctx context.Context) (Conn, error)
	// Addrs returns the endpoint's dialable addresses in the view's string form.
	Addrs() []string
	Close() error
}

// ErrClosed reports an operation on a closed endpoint or connection.
var ErrClosed = errors.New("transport: closed")

// AddrsFunc resolves a node id to its advisory dial addresses.
type AddrsFunc func(id view.NodeID) []string

// Pool keeps connections per (peer, alpn), dialing on demand and dropping
// dead ones.
type Pool struct {
	ep      Endpoint
	addrs   AddrsFunc
	perPeer int
	mu      sync.Mutex
	conns   map[poolKey][]Conn
	next    map[poolKey]int
	dialing map[poolKey]*sync.Mutex
	// Backoff after a failed dial.
	failed map[poolKey]time.Time
}

type poolKey struct {
	id   view.NodeID
	alpn string
}

// NewPool returns a pool over ep resolving addresses through addrs.
func NewPool(ep Endpoint, addrs AddrsFunc, perPeer int) *Pool {
	if perPeer <= 0 {
		perPeer = 1
	}
	return &Pool{ep: ep, addrs: addrs, perPeer: perPeer, conns: map[poolKey][]Conn{}, next: map[poolKey]int{}, dialing: map[poolKey]*sync.Mutex{}, failed: map[poolKey]time.Time{}}
}

// Endpoint returns the pool's endpoint.
func (p *Pool) Endpoint() Endpoint { return p.ep }

// Get returns a live connection to id under alpn, dialing if needed.
func (p *Pool) Get(ctx context.Context, id view.NodeID, alpn string) (Conn, error) {
	k := poolKey{id, alpn}
	p.mu.Lock()
	live := p.conns[k][:0]
	for _, c := range p.conns[k] {
		select {
		case <-c.Done():
		default:
			live = append(live, c)
		}
	}
	p.conns[k] = live
	if len(live) >= p.perPeer {
		i := p.next[k] % len(live)
		p.next[k]++
		c := live[i]
		p.mu.Unlock()
		return c, nil
	}
	if t, ok := p.failed[k]; ok && time.Since(t) < 2*time.Second && len(live) == 0 {
		p.mu.Unlock()
		return nil, errors.New("transport: peer recently unreachable")
	}
	dm := p.dialing[k]
	if dm == nil {
		dm = &sync.Mutex{}
		p.dialing[k] = dm
	}
	p.mu.Unlock()

	dm.Lock()
	defer dm.Unlock()
	p.mu.Lock()
	if len(p.conns[k]) >= p.perPeer {
		c := p.conns[k][0]
		p.mu.Unlock()
		return c, nil
	}
	p.mu.Unlock()
	addrs := p.addrs(id)
	c, err := p.ep.Dial(ctx, id, addrs, alpn)
	if err != nil {
		p.mu.Lock()
		p.failed[k] = time.Now()
		p.mu.Unlock()
		return nil, err
	}
	p.mu.Lock()
	delete(p.failed, k)
	p.conns[k] = append(p.conns[k], c)
	p.mu.Unlock()
	return c, nil
}

// Drop discards every connection to id under alpn.
func (p *Pool) Drop(id view.NodeID, alpn string) {
	k := poolKey{id, alpn}
	p.mu.Lock()
	conns := p.conns[k]
	delete(p.conns, k)
	delete(p.failed, k)
	p.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}

// Path reports the path of a connection to id, if one is open.
func (p *Pool) Path(id view.NodeID, alpn string) (PathInfo, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns[poolKey{id, alpn}] {
		select {
		case <-c.Done():
			continue
		default:
			return c.Path(), true
		}
	}
	return PathInfo{}, false
}

// Close closes every connection.
func (p *Pool) Close() {
	p.mu.Lock()
	all := p.conns
	p.conns = map[poolKey][]Conn{}
	p.mu.Unlock()
	for _, cs := range all {
		for _, c := range cs {
			c.Close()
		}
	}
}

// Call opens a stream to id, writes req, and reads a single reply frame.
// A TErr reply is returned as a *wire.Error. Transport failures drop the
// connection so the next call redials.
func (p *Pool) Call(ctx context.Context, id view.NodeID, alpn string, req *wire.Msg) (*wire.Msg, error) {
	c, err := p.Get(ctx, id, alpn)
	if err != nil {
		return nil, err
	}
	s, err := c.OpenStream(ctx)
	if err != nil {
		p.Drop(id, alpn)
		return nil, err
	}
	defer wire.CloseStream(s)
	if err := wire.WriteMsg(s, req); err != nil {
		return nil, err
	}
	_ = s.CloseWrite()
	done := make(chan struct{})
	var reply *wire.Msg
	var rerr error
	go func() {
		reply, rerr = wire.ReadMsg(s)
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		s.CancelRead(0)
		_ = s.Close()
		<-done
		return nil, ctx.Err()
	}
	if rerr != nil {
		return nil, rerr
	}
	if reply.Type == wire.TErr {
		return reply, wire.ErrorFromMsg(reply)
	}
	return reply, nil
}

// Open opens a stream to id under alpn for a multi-frame exchange.
func (p *Pool) Open(ctx context.Context, id view.NodeID, alpn string) (Stream, error) {
	c, err := p.Get(ctx, id, alpn)
	if err != nil {
		return nil, err
	}
	s, err := c.OpenStream(ctx)
	if err != nil {
		p.Drop(id, alpn)
		return nil, err
	}
	return s, nil
}
