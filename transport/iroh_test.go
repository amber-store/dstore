package transport

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/amber-store/dstore/wire"
	irohkey "github.com/tmc/go-iroh/key"
)

// TestIrohLoopback exchanges one frame between two real iroh endpoints on
// the loopback interface with no relay.
func TestIrohLoopback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bind := func(alpns ...string) *IrohEndpoint {
		sk, err := irohkey.GenerateSecretKey()
		if err != nil {
			t.Fatal(err)
		}
		ep, err := BindIroh(ctx, IrohConfig{SecretKey: sk, ALPNs: alpns, Loopback: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ep.Close() })
		return ep
	}
	server := bind(wire.ALPNClient, wire.ALPNCluster)
	client := bind()

	done := make(chan error, 1)
	go func() {
		c, err := server.Accept(ctx)
		if err != nil {
			done <- err
			return
		}
		if c.ALPN() != wire.ALPNClient || c.RemoteID() != client.ID() {
			rid := c.RemoteID()
			done <- fmt.Errorf("alpn %q remote %x", c.ALPN(), rid[:4])
			return
		}
		s, err := c.AcceptStream(ctx)
		if err != nil {
			done <- err
			return
		}
		m, err := wire.ReadMsg(s)
		if err != nil {
			done <- err
			return
		}
		err = wire.WriteMsg(s, &wire.Msg{Type: wire.TPong, Epoch: m.Epoch + 1})
		wire.CloseStream(s)
		done <- err
	}()

	conn, err := client.Dial(ctx, server.ID(), server.Addrs(), wire.ALPNClient)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	s, err := conn.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.WriteMsg(s, &wire.Msg{Type: wire.TPing, Epoch: 41}); err != nil {
		t.Fatal(err)
	}
	_ = s.CloseWrite()
	m, err := wire.ReadMsg(s)
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != wire.TPong || m.Epoch != 42 {
		t.Fatalf("reply %+v", m)
	}
	wire.CloseStream(s)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if p := conn.Path(); !p.Direct {
		t.Fatalf("expected a direct path, got %+v", p)
	}
}

// TestIrohDialWaitsForHandshake covers the second dial to a node whose
// session ticket the client already holds: go-iroh's Connect then returns
// at the 0-RTT window, before the peer has answered at all. Dial must not
// hand such a connection out as a proven path. A dial at a dead address
// has to fail within DirectTimeout, and a dial that succeeds has to return
// a connection whose handshake has completed.
func TestIrohDialWaitsForHandshake(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bind := func(cfg IrohConfig) *IrohEndpoint {
		sk, err := irohkey.GenerateSecretKey()
		if err != nil {
			t.Fatal(err)
		}
		cfg.SecretKey = sk
		cfg.Loopback = true
		ep, err := BindIroh(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ep.Close() })
		return ep
	}
	server := bind(IrohConfig{ALPNs: []string{wire.ALPNClient}})
	client := bind(IrohConfig{DirectTimeout: 500 * time.Millisecond})

	go func() {
		for {
			c, err := server.Accept(ctx)
			if err != nil {
				return
			}
			go func() {
				s, err := c.AcceptStream(ctx)
				if err != nil {
					return
				}
				if _, err := wire.ReadMsg(s); err == nil {
					_ = wire.WriteMsg(s, &wire.Msg{Type: wire.TPong})
				}
				wire.CloseStream(s)
			}()
		}
	}()

	ping := func(conn Conn) {
		t.Helper()
		s, err := conn.OpenStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := wire.WriteMsg(s, &wire.Msg{Type: wire.TPing}); err != nil {
			t.Fatal(err)
		}
		_ = s.CloseWrite()
		if _, err := wire.ReadMsg(s); err != nil {
			t.Fatal(err)
		}
		wire.CloseStream(s)
	}

	// Dial until a connection resumes with 0-RTT: the session ticket arrives
	// asynchronously after the first handshake. Every connection Dial returns
	// must have completed its handshake.
	for resumed := false; !resumed; {
		conn, err := client.Dial(ctx, server.ID(), server.Addrs(), wire.ALPNClient)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		ic := conn.(*irohConn).c
		select {
		case <-ic.HandshakeComplete():
		default:
			t.Fatal("Dial returned a connection before its handshake completed")
		}
		ping(conn)
		resumed = ic.Used0RTT()
		conn.Close()
		if ctx.Err() != nil {
			t.Fatalf("no 0-RTT resumption before %v", ctx.Err())
		}
		time.Sleep(20 * time.Millisecond)
	}

	// A dead address with a cached ticket: Connect returns instantly at the
	// 0-RTT window, but nothing ever answers there.
	dead := deadLoopbackAddr(t)
	start := time.Now()
	conn, err := client.Dial(ctx, server.ID(), []string{dead}, wire.ALPNClient)
	if err == nil {
		conn.Close()
		t.Fatalf("dial at dead address %s returned a connection", dead)
	}
	if el := time.Since(start); el > 3*time.Second {
		t.Fatalf("dial at dead address took %v, want about DirectTimeout", el)
	}

	// Dead and live addresses raced: the live one must win.
	conn, err = client.Dial(ctx, server.ID(), append([]string{dead}, server.Addrs()...), wire.ALPNClient)
	if err != nil {
		t.Fatalf("dial with a dead and a live address: %v", err)
	}
	defer conn.Close()
	ping(conn)
}

// deadLoopbackAddr returns a loopback UDP address nothing listens on.
func deadLoopbackAddr(t *testing.T) string {
	t.Helper()
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := pc.LocalAddr().String()
	pc.Close()
	return addr
}

// TestIrohPathRTTIsUnknownUntilSampled checks that a fresh connection does
// not report QUIC's initial round-trip guess as a measurement: the client
// ranks owners by it, and a guess of 100 ms would put a LAN node in the
// far class.
func TestIrohPathRTTIsUnknownUntilSampled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bind := func(alpns ...string) *IrohEndpoint {
		sk, err := irohkey.GenerateSecretKey()
		if err != nil {
			t.Fatal(err)
		}
		ep, err := BindIroh(ctx, IrohConfig{SecretKey: sk, ALPNs: alpns, Loopback: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ep.Close() })
		return ep
	}
	server := bind(wire.ALPNClient)
	client := bind()
	go func() {
		c, err := server.Accept(ctx)
		if err != nil {
			return
		}
		defer c.Close()
		s, err := c.AcceptStream(ctx)
		if err != nil {
			return
		}
		if _, err := wire.ReadMsg(s); err == nil {
			_ = wire.WriteMsg(s, &wire.Msg{Type: wire.TPong})
		}
		wire.CloseStream(s)
		<-ctx.Done()
	}()
	conn, err := client.Dial(ctx, server.ID(), server.Addrs(), wire.ALPNClient)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	check := func(when string) {
		t.Helper()
		ic := conn.(*irohConn)
		t.Logf("%s: path %+v stats min=%v latest=%v smoothed=%v paths=%+v", when, conn.Path(), ic.c.Stats().MinRTT, ic.c.Stats().LatestRTT, ic.c.Stats().SmoothedRTT, ic.c.Paths())
		if p := conn.Path(); p.RTT != 0 && p.RTT >= 50*time.Millisecond {
			t.Fatalf("%s: a loopback connection reports an RTT of %v: the initial guess, not a sample", when, p.RTT)
		}
	}
	check("after dial")
	s, err := conn.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.WriteMsg(s, &wire.Msg{Type: wire.TPing}); err != nil {
		t.Fatal(err)
	}
	_ = s.CloseWrite()
	if _, err := wire.ReadMsg(s); err != nil {
		t.Fatal(err)
	}
	wire.CloseStream(s)
	check("after one exchange")
}
