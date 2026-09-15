package node_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/amber-store/core/reference"
	"github.com/amber-store/dstore/catalog"
	"github.com/amber-store/dstore/client"
	"github.com/amber-store/dstore/node"
	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
)

// watchEvents collects a watch's events on a channel.
type watchEvents struct {
	ch   chan client.RefChange
	done chan error
}

func startWatch(ctx context.Context, c *client.Cluster, pattern string, known map[string][]byte) *watchEvents {
	w := &watchEvents{ch: make(chan client.RefChange, 1024), done: make(chan error, 1)}
	go func() {
		var last error
		for ev, err := range c.WatchRefs(ctx, pattern, known) {
			if err != nil {
				last = err
				break
			}
			w.ch <- ev
		}
		w.done <- last
	}()
	return w
}

// until reads events until pred accepts one, returning every event read.
func (w *watchEvents) until(t *testing.T, d time.Duration, what string, pred func(client.RefChange) bool) []client.RefChange {
	t.Helper()
	var out []client.RefChange
	deadline := time.After(d)
	for {
		select {
		case ev := <-w.ch:
			out = append(out, ev)
			if pred(ev) {
				return out
			}
		case <-deadline:
			t.Fatalf("timeout waiting for %s; got %+v", what, out)
		}
	}
}

func (w *watchEvents) synced(t *testing.T, d time.Duration) []client.RefChange {
	t.Helper()
	return w.until(t, d, "synced", func(ev client.RefChange) bool { return ev.Synced })
}

func (w *watchEvents) change(t *testing.T, d time.Duration, name string) client.RefChange {
	t.Helper()
	evs := w.until(t, d, "change of "+name, func(ev client.RefChange) bool { return ev.Name == name && !ev.Deleted })
	return evs[len(evs)-1]
}

func randomKey() []byte {
	k := make([]byte, 32)
	rand.Read(k)
	return k
}

func putLocal(t *testing.T, n *node.Node, name string, root []byte, user string) {
	t.Helper()
	rec := reference.Reference{Name: name, Key: root, User: user, CreatedAt: time.Now().UnixNano()}
	enc, err := rec.Encode()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := n.RefPutLocal(ctx, name, enc, catalog.Cond{Force: true}); err != nil {
		t.Fatalf("ref put %s on %s: %v", name, view.ShortID(n.ID()), err)
	}
}

func otherNode(h *harness, not view.NodeID) *node.Node {
	for _, n := range h.nodes {
		if n.ID() != not {
			return n
		}
	}
	return nil
}

func TestClusterWatchRefs(t *testing.T) {
	// A long reconcile interval: everything below must arrive by hint.
	h := cluster3With(t, func(c *node.Config) { c.WatchReconcile = time.Minute })
	defer h.close()
	c := h.client(t, 100)
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, rootA, _ := pushTree(t, c, 5, 2000, "trees/a")
	_, rootB, _ := pushTree(t, c, 5, 2000, "trees/b")
	pushTree(t, c, 3, 1000, "other/x")

	known := map[string][]byte{"trees/a": randomKey(), "trees/gone": randomKey()}
	w := startWatch(ctx, c, "trees/**", known)
	initial := w.synced(t, 20*time.Second)
	got := map[string]client.RefChange{}
	for _, ev := range initial {
		if !ev.Synced {
			got[ev.Name] = ev
		}
	}
	if len(got) != 3 {
		t.Fatalf("initial difference: %+v", initial)
	}
	if ev := got["trees/a"]; ev.Deleted || !bytes.Equal(ev.Key, rootA[:]) || ev.User != "tester" || len(ev.Version) == 0 {
		t.Fatalf("trees/a: %+v", ev)
	}
	if ev := got["trees/b"]; ev.Deleted || !bytes.Equal(ev.Key, rootB[:]) {
		t.Fatalf("trees/b: %+v", ev)
	}
	if ev := got["trees/gone"]; !ev.Deleted {
		t.Fatalf("trees/gone: %+v", ev)
	}
	if initial[len(initial)-1].Node == (view.NodeID{}) {
		t.Fatal("synced without the serving node")
	}

	// A write coordinated by any node reaches the watcher by hint.
	for i, n := range h.nodes {
		name := "trees/c" + string(rune('0'+i))
		putLocal(t, n, name, rootA[:], "tester2")
		ev := w.change(t, 5*time.Second, name)
		if !bytes.Equal(ev.Key, rootA[:]) || ev.User != "tester2" {
			t.Fatalf("%s: %+v", name, ev)
		}
	}
	// A deletion.
	if err := c.RefDelete(ctx, "trees/b", client.Cond{Force: true}); err != nil {
		t.Fatal(err)
	}
	w.until(t, 5*time.Second, "deletion of trees/b", func(ev client.RefChange) bool { return ev.Name == "trees/b" && ev.Deleted })
	// The same key again is not a change: the next event is the one after.
	putLocal(t, h.nodes[1], "trees/a", rootA[:], "tester3")
	putLocal(t, h.nodes[2], "trees/d", rootB[:], "tester2")
	evs := w.until(t, 5*time.Second, "trees/d", func(ev client.RefChange) bool { return ev.Name == "trees/d" })
	if len(evs) != 1 {
		t.Fatalf("unexpected events before trees/d: %+v", evs[:len(evs)-1])
	}
	// A name outside the pattern is silent, so the next event is the next
	// matching one.
	pushTree(t, c, 3, 1000, "other/y")
	putLocal(t, h.nodes[0], "trees/e", rootB[:], "tester2")
	evs = w.until(t, 5*time.Second, "trees/e", func(ev client.RefChange) bool { return ev.Name == "trees/e" })
	if len(evs) != 1 {
		t.Fatalf("unexpected events before trees/e: %+v", evs[:len(evs)-1])
	}

	cancel()
	select {
	case err := <-w.done:
		if err != nil {
			t.Fatalf("watch ended with %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("watch did not end on cancel")
	}
}

func TestClusterWatchBadPattern(t *testing.T) {
	h := cluster3(t)
	defer h.close()
	c := h.client(t, 100)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	w := startWatch(ctx, c, "trees/[", nil)
	select {
	case err := <-w.done:
		if !wire.IsCode(err, wire.CodeBadRequest) {
			t.Fatalf("got %v, want bad-request", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("watch did not end")
	}
}

// TestClusterWatchLostHint cuts the link between the coordinator of a
// write and the node serving the watch: the hint is lost and the
// periodic reconcile delivers the change.
func TestClusterWatchLostHint(t *testing.T) {
	h := cluster3With(t, func(c *node.Config) { c.WatchReconcile = 500 * time.Millisecond })
	defer h.close()
	c := h.client(t, 100)
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, root, _ := pushTree(t, c, 5, 2000, "trees/a")
	w := startWatch(ctx, c, "trees/*", nil)
	initial := w.synced(t, 20*time.Second)
	serving := initial[len(initial)-1].Node
	coord := otherNode(h, serving)
	h.net.Partition(coord.ID(), serving, true)
	defer h.net.Partition(coord.ID(), serving, false)
	putLocal(t, coord, "trees/b", root[:], "tester2")
	ev := w.change(t, 15*time.Second, "trees/b")
	if !bytes.Equal(ev.Key, root[:]) {
		t.Fatalf("trees/b: %+v", ev)
	}
}

// TestClusterWatchReconnect takes the serving node down: the client moves
// the watch to another node and receives what changed meanwhile.
func TestClusterWatchReconnect(t *testing.T) {
	h := cluster3With(t, func(c *node.Config) { c.WatchReconcile = time.Minute })
	defer h.close()
	c := h.clientWith(t, 100, func(c *client.Config) { c.WatchIdle = 5 * time.Second })
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, root, _ := pushTree(t, c, 5, 2000, "trees/a")
	w := startWatch(ctx, c, "trees/*", nil)
	initial := w.synced(t, 20*time.Second)
	serving := initial[len(initial)-1].Node
	h.net.SetDown(serving, true)
	defer h.net.SetDown(serving, false)
	writer := otherNode(h, serving)
	putLocal(t, writer, "trees/b", root[:], "tester2")

	evs := w.synced(t, 30*time.Second)
	again := evs[len(evs)-1]
	if again.Node == serving || again.Node == (view.NodeID{}) {
		t.Fatalf("resynced via %s, the node taken down", view.ShortID(again.Node))
	}
	seen := false
	for _, ev := range evs {
		if ev.Name == "trees/b" && !ev.Deleted {
			seen = true
		}
		if ev.Name == "trees/a" {
			t.Fatalf("trees/a re-sent after reconnect: %+v", ev)
		}
	}
	if !seen {
		// The write may have landed after the new stream synced.
		w.change(t, 10*time.Second, "trees/b")
	}
	// The new stream is live: a further change arrives.
	putLocal(t, writer, "trees/c", root[:], "tester2")
	w.change(t, 10*time.Second, "trees/c")
}
