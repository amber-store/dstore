package transport

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sync"
	"time"

	"github.com/amber-store/dstore/view"
	"github.com/tmc/go-iroh/iroh"
	irohkey "github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
)

// IrohConfig configures an iroh endpoint.
type IrohConfig struct {
	SecretKey irohkey.SecretKey
	ALPNs     []string
	// RelayMode selects relays; nil disables relays (direct-only, the
	// configuration end-to-end tests use).
	RelayMode *relay.Mode
	// Advertise lists direct addresses to publish; nil means the bound port on
	// every non-loopback interface address.
	Advertise []netip.AddrPort
	// BindAddr binds the UDP socket to a specific address (tests).
	BindAddr netip.AddrPort
	// Loopback advertises 127.0.0.1:<port> (tests on one machine).
	Loopback bool
	// DirectTimeout bounds a first dial at direct addresses before racing the
	// relay (§11.3); default 2 s.
	DirectTimeout time.Duration
}

// IrohEndpoint implements Endpoint over go-iroh.
type IrohEndpoint struct {
	ep    *iroh.Endpoint
	id    view.NodeID
	cfg   IrohConfig
	addrs []string
	mu    sync.Mutex
	// Accepted connections whose ALPN the caller dispatches on.
}

// BindIroh binds an iroh endpoint.
func BindIroh(ctx context.Context, cfg IrohConfig) (*IrohEndpoint, error) {
	opts := []iroh.Option{iroh.WithSecretKey(cfg.SecretKey), iroh.WithALPNs(cfg.ALPNs...)}
	if cfg.RelayMode != nil {
		opts = append(opts, iroh.WithRelayMode(*cfg.RelayMode))
	} else {
		opts = append(opts, iroh.WithRelayMode(relay.ModeDisabled()))
	}
	if cfg.BindAddr.IsValid() {
		opts = append(opts, iroh.WithBindAddr(cfg.BindAddr))
	}
	opts = append(opts, iroh.WithTransportConfig(&iroh.QUICTransportConfig{
		KeepAlivePeriod:    5 * time.Second,
		MaxIdleTimeout:     60 * time.Second,
		MaxIncomingStreams: 1024,
	}))
	ep, err := iroh.Bind(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("transport: bind: %w", err)
	}
	e := &IrohEndpoint{ep: ep, cfg: cfg}
	e.id = view.NodeID(ep.ID().Bytes())
	port := ep.LocalAddr().Port()
	var direct []netip.AddrPort
	switch {
	case cfg.Advertise != nil:
		direct = cfg.Advertise
	case cfg.Loopback:
		direct = []netip.AddrPort{netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), port)}
	default:
		direct = localAddrPorts(port)
	}
	for _, ap := range direct {
		ep.AddExternalAddr(ap)
		e.addrs = append(e.addrs, netaddr.IPAddr{Addr: ap}.String())
	}
	if cfg.RelayMode != nil {
		octx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_ = ep.Online(octx)
		cancel()
		for _, u := range ep.Addr().RelayURLs() {
			e.addrs = append(e.addrs, netaddr.RelayAddr{URL: u}.String())
		}
	}
	return e, nil
}

// Raw returns the underlying iroh endpoint.
func (e *IrohEndpoint) Raw() *iroh.Endpoint { return e.ep }

// ID returns the endpoint id.
func (e *IrohEndpoint) ID() view.NodeID { return e.id }

// Addrs returns the advertised addresses.
func (e *IrohEndpoint) Addrs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := append([]string{}, e.addrs...)
	// Relay URLs may appear after bind.
	for _, u := range e.ep.Addr().RelayURLs() {
		s := netaddr.RelayAddr{URL: u}.String()
		found := false
		for _, a := range out {
			if a == s {
				found = true
			}
		}
		if !found {
			out = append(out, s)
		}
	}
	return out
}

// ParseAddrs parses view address strings into transport addresses,
// skipping ones it does not understand.
func ParseAddrs(addrs []string) []netaddr.TransportAddr {
	var out []netaddr.TransportAddr
	for _, s := range addrs {
		ta, err := netaddr.ParseTransportAddr(s)
		if err != nil {
			// Accept bare ip:port too.
			if ap, err2 := netip.ParseAddrPort(s); err2 == nil {
				out = append(out, netaddr.IPAddr{Addr: ap})
			}
			continue
		}
		out = append(out, ta)
	}
	return out
}

// Dial connects to id. Direct addresses are raced first with a short
// timeout; the relay is tried only on retry (§11.3).
func (e *IrohEndpoint) Dial(ctx context.Context, id view.NodeID, addrs []string, alpn string) (Conn, error) {
	eid, err := irohkey.NewEndpointID(id)
	if err != nil {
		return nil, err
	}
	cands := ParseAddrs(addrs)
	var direct, relays []netaddr.TransportAddr
	for _, c := range cands {
		if _, ok := c.(netaddr.RelayAddr); ok {
			relays = append(relays, c)
		} else {
			direct = append(direct, c)
		}
	}
	timeout := e.cfg.DirectTimeout
	if timeout == 0 {
		timeout = 2 * time.Second
	}
	if len(direct) > 0 {
		dctx, cancel := context.WithTimeout(ctx, timeout)
		c, err := raceConnect(dctx, e.ep, eid, direct, alpn)
		cancel()
		if err == nil {
			return &irohConn{c: c}, nil
		}
		if len(relays) == 0 {
			return nil, err
		}
	}
	if len(relays) > 0 {
		c, err := raceConnect(ctx, e.ep, eid, append(relays, direct...), alpn)
		if err != nil {
			return nil, err
		}
		return &irohConn{c: c}, nil
	}
	// No addresses known: let iroh's own resolution try.
	c, err := e.ep.Connect(ctx, netaddr.NewEndpointAddr(eid), alpn)
	if err != nil {
		return nil, err
	}
	return &irohConn{c: c}, nil
}

// raceConnect dials every candidate concurrently and keeps the first.
func raceConnect(ctx context.Context, ep *iroh.Endpoint, id irohkey.EndpointID, cands []netaddr.TransportAddr, alpn string) (*iroh.Conn, error) {
	if len(cands) == 0 {
		return nil, fmt.Errorf("transport: no candidate addresses for %s", id.Short())
	}
	type result struct {
		conn *iroh.Conn
		err  error
	}
	results := make(chan result, len(cands))
	cancels := make([]context.CancelFunc, len(cands))
	for i, ta := range cands {
		actx, cancel := context.WithCancel(ctx)
		cancels[i] = cancel
		go func(ta netaddr.TransportAddr) {
			conn, err := ep.Connect(actx, netaddr.NewEndpointAddr(id, ta), alpn)
			if err == nil {
				if err = awaitHandshake(actx, conn); err != nil {
					conn = nil
				}
			}
			if err != nil {
				err = fmt.Errorf("dial %s: %w", ta, err)
			}
			results <- result{conn, err}
		}(ta)
	}
	var errs []error
	for range cands {
		r := <-results
		if r.err != nil {
			errs = append(errs, r.err)
			continue
		}
		for _, cancel := range cancels {
			cancel()
		}
		remaining := len(cands) - len(errs) - 1
		go func(n int) {
			for ; n > 0; n-- {
				if late := <-results; late.conn != nil {
					late.conn.Close()
				}
			}
		}(remaining)
		return r.conn, nil
	}
	for _, cancel := range cancels {
		cancel()
	}
	return nil, errors.Join(errs...)
}

// awaitHandshake waits until conn has completed its handshake. With a cached
// session ticket for the peer, go-iroh's Connect returns at the 0-RTT window,
// before a single packet has come back, so a returned connection proves
// nothing about the address it was dialed at: a candidate only wins the race
// once the peer has answered on it. On ctx expiry the connection is closed.
func awaitHandshake(ctx context.Context, conn *iroh.Conn) error {
	select {
	case <-conn.HandshakeComplete():
		return nil
	case <-conn.Context().Done():
		return context.Cause(conn.Context())
	case <-ctx.Done():
		conn.Close()
		return ctx.Err()
	}
}

// Accept returns the next incoming connection.
func (e *IrohEndpoint) Accept(ctx context.Context) (Conn, error) {
	c, err := e.ep.Accept(ctx)
	if err != nil {
		return nil, err
	}
	return &irohConn{c: c}, nil
}

// Close shuts the endpoint down.
func (e *IrohEndpoint) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return e.ep.Shutdown(ctx)
}

type irohConn struct {
	c *iroh.Conn
}

func (c *irohConn) RemoteID() view.NodeID { return view.NodeID(c.c.RemoteID().Bytes()) }
func (c *irohConn) ALPN() string          { return c.c.ALPN() }
func (c *irohConn) OpenStream(ctx context.Context) (Stream, error) {
	s, err := c.c.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return s, nil
}
func (c *irohConn) AcceptStream(ctx context.Context) (Stream, error) {
	s, err := c.c.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	return s, nil
}
func (c *irohConn) Close() error { return c.c.Close() }

// Path reports the selected path. Its RTT stays 0 until the path has a
// measurement: the connection-level smoothed RTT is the initial guess
// (100 ms) after a path migration, which would rank a LAN node as far.
func (c *irohConn) Path() PathInfo {
	info := PathInfo{Direct: true}
	for _, p := range c.c.Paths() {
		if p.Selected {
			info.Direct = !p.Relayed
			if p.HasRTT {
				info.RTT = p.RTT
			}
		}
	}
	return info
}
func (c *irohConn) Done() <-chan struct{} { return c.c.Context().Done() }

// localAddrPorts pairs the machine's unicast interface addresses with port.
func localAddrPorts(port uint16) []netip.AddrPort {
	var out []netip.AddrPort
	for _, ip := range interfaceIPs() {
		out = append(out, netip.AddrPortFrom(ip, port))
	}
	if len(out) == 0 {
		out = append(out, netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), port))
	}
	return out
}
