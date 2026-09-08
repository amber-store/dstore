package transport

import (
	"context"
	"fmt"
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
