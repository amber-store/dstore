package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/amber-store/dstore/view"
)

// Network is an in-process transport: every endpoint registers with it,
// and dials are delivered directly. Partitions and delays are injected
// per node pair.
type Network struct {
	mu        sync.Mutex
	endpoints map[view.NodeID]*MemEndpoint
	down      map[view.NodeID]bool
	cut       map[[2]view.NodeID]bool
	delay     time.Duration
}

// NewNetwork returns an empty network.
func NewNetwork() *Network {
	return &Network{endpoints: map[view.NodeID]*MemEndpoint{}, down: map[view.NodeID]bool{}, cut: map[[2]view.NodeID]bool{}}
}

// SetDown makes a node unreachable (and unable to dial) while down is true.
func (n *Network) SetDown(id view.NodeID, down bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.down[id] = down
	if down {
		if ep := n.endpoints[id]; ep != nil {
			ep.dropAll()
		}
		for _, ep := range n.endpoints {
			ep.dropPeer(id)
		}
	}
}

// Partition cuts the link between a and b in both directions.
func (n *Network) Partition(a, b view.NodeID, cut bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cut[[2]view.NodeID{a, b}] = cut
	n.cut[[2]view.NodeID{b, a}] = cut
	if cut {
		if ep := n.endpoints[a]; ep != nil {
			ep.dropPeer(b)
		}
		if ep := n.endpoints[b]; ep != nil {
			ep.dropPeer(a)
		}
	}
}

// SetDelay adds a fixed latency to every dial.
func (n *Network) SetDelay(d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delay = d
}

// Bind registers a new endpoint under id with the given ALPNs.
func (n *Network) Bind(id view.NodeID, alpns ...string) *MemEndpoint {
	ep := &MemEndpoint{net: n, id: id, alpns: map[string]bool{}, accept: make(chan Conn, 64), conns: map[*memConn]struct{}{}}
	for _, a := range alpns {
		ep.alpns[a] = true
	}
	n.mu.Lock()
	n.endpoints[id] = ep
	n.mu.Unlock()
	return ep
}

func (n *Network) reachable(from, to view.NodeID) (*MemEndpoint, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.down[from] {
		return nil, errors.New("mem: local endpoint is down")
	}
	if n.down[to] || n.cut[[2]view.NodeID{from, to}] {
		return nil, fmt.Errorf("mem: %s unreachable", view.ShortID(to))
	}
	ep := n.endpoints[to]
	if ep == nil || ep.closed.Load() {
		return nil, fmt.Errorf("mem: %s not bound", view.ShortID(to))
	}
	return ep, nil
}

// MemEndpoint is an in-memory endpoint.
type MemEndpoint struct {
	net    *Network
	id     view.NodeID
	alpns  map[string]bool
	accept chan Conn
	mu     sync.Mutex
	conns  map[*memConn]struct{}
	closed atomicBool
}

func (e *MemEndpoint) ID() view.NodeID { return e.id }
func (e *MemEndpoint) Addrs() []string { return []string{"mem:" + view.ShortID(e.id)} }

func (e *MemEndpoint) Dial(ctx context.Context, id view.NodeID, addrs []string, alpn string) (Conn, error) {
	if e.closed.Load() {
		return nil, ErrClosed
	}
	peer, err := e.net.reachable(e.id, id)
	if err != nil {
		return nil, err
	}
	if !peer.alpns[alpn] {
		return nil, fmt.Errorf("mem: %s does not speak %s", view.ShortID(id), alpn)
	}
	e.net.mu.Lock()
	d := e.net.delay
	e.net.mu.Unlock()
	if d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	local := &memConn{ep: e, remote: id, alpn: alpn, streams: make(chan Stream, 256), done: make(chan struct{})}
	remote := &memConn{ep: peer, remote: e.id, alpn: alpn, streams: make(chan Stream, 256), done: make(chan struct{})}
	local.peer, remote.peer = remote, local
	e.track(local)
	peer.track(remote)
	select {
	case peer.accept <- remote:
	case <-ctx.Done():
		local.Close()
		return nil, ctx.Err()
	}
	return local, nil
}

func (e *MemEndpoint) track(c *memConn) {
	e.mu.Lock()
	e.conns[c] = struct{}{}
	e.mu.Unlock()
}

func (e *MemEndpoint) untrack(c *memConn) {
	e.mu.Lock()
	delete(e.conns, c)
	e.mu.Unlock()
}

func (e *MemEndpoint) dropAll() {
	e.mu.Lock()
	cs := make([]*memConn, 0, len(e.conns))
	for c := range e.conns {
		cs = append(cs, c)
	}
	e.mu.Unlock()
	for _, c := range cs {
		c.Close()
	}
}

func (e *MemEndpoint) dropPeer(id view.NodeID) {
	e.mu.Lock()
	var cs []*memConn
	for c := range e.conns {
		if c.remote == id {
			cs = append(cs, c)
		}
	}
	e.mu.Unlock()
	for _, c := range cs {
		c.Close()
	}
}

func (e *MemEndpoint) Accept(ctx context.Context) (Conn, error) {
	select {
	case c := <-e.accept:
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (e *MemEndpoint) Close() error {
	e.closed.Store(true)
	e.dropAll()
	return nil
}

type memConn struct {
	ep      *MemEndpoint
	peer    *memConn
	remote  view.NodeID
	alpn    string
	streams chan Stream
	done    chan struct{}
	once    sync.Once
}

func (c *memConn) RemoteID() view.NodeID { return c.remote }
func (c *memConn) ALPN() string          { return c.alpn }
func (c *memConn) Path() PathInfo        { return PathInfo{Direct: true, RTT: time.Millisecond} }
func (c *memConn) Done() <-chan struct{} { return c.done }

func (c *memConn) OpenStream(ctx context.Context) (Stream, error) {
	select {
	case <-c.done:
		return nil, ErrClosed
	default:
	}
	a, b := newStreamPair()
	select {
	case c.peer.streams <- b:
	case <-c.done:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return a, nil
}

func (c *memConn) AcceptStream(ctx context.Context) (Stream, error) {
	select {
	case s := <-c.streams:
		return s, nil
	case <-c.done:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *memConn) Close() error {
	c.once.Do(func() {
		close(c.done)
		c.ep.untrack(c)
		if c.peer != nil {
			go c.peer.Close()
		}
	})
	return nil
}

// memStream is one end of a bidirectional in-memory stream: a buffered
// byte pipe in each direction with independent close semantics.
type memStream struct {
	r *pipe
	w *pipe
}

func newStreamPair() (*memStream, *memStream) {
	ab := newPipe()
	ba := newPipe()
	return &memStream{r: ba, w: ab}, &memStream{r: ab, w: ba}
}

func (s *memStream) Read(p []byte) (int, error)  { return s.r.Read(p) }
func (s *memStream) Write(p []byte) (int, error) { return s.w.Write(p) }
func (s *memStream) CloseWrite() error           { s.w.CloseWrite(); return nil }
func (s *memStream) CancelRead(code uint64)      { s.r.CancelRead() }
func (s *memStream) Close() error {
	s.w.CloseWrite()
	return nil
}

// pipe is a buffered byte channel with EOF on writer close and an error
// on reader cancel.
type pipe struct {
	mu       sync.Mutex
	cond     *sync.Cond
	buf      []byte
	closed   bool
	canceled bool
}

const pipeLimit = 4 << 20

func newPipe() *pipe {
	p := &pipe{}
	p.cond = sync.NewCond(&p.mu)
	return p
}

func (p *pipe) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for len(b) > 0 {
		for len(p.buf) >= pipeLimit && !p.canceled && !p.closed {
			p.cond.Wait()
		}
		if p.canceled {
			return n, errors.New("mem: stream reset by peer")
		}
		if p.closed {
			return n, io.ErrClosedPipe
		}
		take := min(pipeLimit-len(p.buf), len(b))
		p.buf = append(p.buf, b[:take]...)
		b = b[take:]
		n += take
		p.cond.Broadcast()
	}
	return n, nil
}

func (p *pipe) Read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.buf) == 0 && !p.closed && !p.canceled {
		p.cond.Wait()
	}
	if p.canceled {
		return 0, errors.New("mem: read canceled")
	}
	if len(p.buf) == 0 {
		return 0, io.EOF
	}
	n := copy(b, p.buf)
	p.buf = p.buf[n:]
	p.cond.Broadcast()
	return n, nil
}

func (p *pipe) CloseWrite() {
	p.mu.Lock()
	p.closed = true
	p.cond.Broadcast()
	p.mu.Unlock()
}

func (p *pipe) CancelRead() {
	p.mu.Lock()
	p.canceled = true
	p.buf = nil
	p.cond.Broadcast()
	p.mu.Unlock()
}

// atomicBool is a tiny helper to avoid importing sync/atomic types in the
// struct layout.
type atomicBool struct {
	mu sync.Mutex
	v  bool
}

func (a *atomicBool) Load() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.v
}

func (a *atomicBool) Store(v bool) {
	a.mu.Lock()
	a.v = v
	a.mu.Unlock()
}
