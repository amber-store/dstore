package node_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/dstore/catalog"
	"github.com/amber-store/dstore/client"
	"github.com/amber-store/dstore/codec"
	"github.com/amber-store/dstore/node"
	"github.com/amber-store/dstore/ticket"
	"github.com/amber-store/dstore/transport"
	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
)

func nid(i byte) view.NodeID {
	var id view.NodeID
	id[0], id[31] = i, i
	return id
}

type harness struct {
	t     *testing.T
	net   *transport.Network
	nodes []*node.Node
	dirs  []string
	log   *slog.Logger
}

func testLogger(t *testing.T) *slog.Logger {
	lvl := slog.LevelWarn
	if os.Getenv("DSTORE_TEST_LOG") != "" {
		lvl = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}

func newHarness(t *testing.T) *harness {
	return &harness{t: t, net: transport.NewNetwork(), log: testLogger(t)}
}

func (h *harness) config(i byte, dir string) node.Config {
	ep := h.net.Bind(nid(i), wire.ALPNClient, wire.ALPNCluster)
	return node.Config{
		StoreDir: dir, Endpoint: ep, Logger: h.log, NoSync: true, SegmentSize: 256 << 10,
		MaintenanceTick: 200 * time.Millisecond, Lease: 3 * time.Second, ViewRefresh: time.Second,
		AdoptTimeout: 3 * time.Second, ParticipantTimeout: 30 * time.Second, Delta: 2 * time.Second,
		FirstAudit: 500 * time.Millisecond, GCInterval: 2 * time.Second, BarrierTimeout: 5 * time.Second,
		MarkTimeout: 30 * time.Second, SweepTimeout: 30 * time.Second, Grace: time.Millisecond,
		ForwardTimeout: 10 * time.Second, PutTTL: 10 * time.Minute,
	}
}

func (h *harness) open(i byte) *node.Node {
	dir := filepath.Join(h.t.TempDir(), fmt.Sprintf("n%d", i))
	n, err := node.Open(h.config(i, dir))
	if err != nil {
		h.t.Fatal(err)
	}
	h.nodes = append(h.nodes, n)
	h.dirs = append(h.dirs, dir)
	return n
}

func (h *harness) close() {
	for _, n := range h.nodes {
		n.Close()
	}
}

func (h *harness) admin(n *node.Node, req node.AdminRequest) node.AdminReply {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	r, err := n.Admin(ctx, req)
	if err != nil {
		h.t.Fatalf("admin %s: %v", req.Op, err)
	}
	return r
}

// waitFor polls until cond holds or the deadline passes.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (h *harness) waitSteady(nodes int, voters int) {
	h.t.Helper()
	waitFor(h.t, 60*time.Second, fmt.Sprintf("%d nodes, %d voters, no pending", nodes, voters), func() bool {
		for _, n := range h.nodes {
			v := n.View()
			if v == nil || len(v.Nodes) != nodes || len(v.Voters) != voters || v.Pending != nil || v.VoterSync != view.VoterSyncDone || len(v.Ramps) > 0 {
				return false
			}
		}
		return true
	})
}

// cluster3 builds a three-node cluster: init on node 1, join 2 and 3.
func cluster3(t *testing.T) *harness {
	h := newHarness(t)
	ctx := context.Background()
	n1 := h.open(1)
	if _, err := n1.InitCluster(ctx, 3, 2, 100, "", false); err != nil {
		t.Fatal(err)
	}
	if err := n1.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for i := byte(2); i <= 3; i++ {
		tok := h.admin(n1, node.AdminRequest{Op: "token-create"})
		n := h.open(i)
		if err := n.Start(ctx); err != nil {
			t.Fatal(err)
		}
		jctx, cancel := context.WithTimeout(ctx, 60*time.Second)
		if _, err := n.Join(jctx, n1.ID(), n1.Endpoint().Addrs(), tok.Token, 100, "", false, true); err != nil {
			t.Fatalf("join %d: %v", i, err)
		}
		cancel()
		h.waitSteady(int(i), map[byte]int{2: 1, 3: 3}[i])
	}
	return h
}

func (h *harness) client(t *testing.T, i byte) *client.Cluster {
	ep := h.net.Bind(nid(i), wire.ALPNClient)
	n1 := h.nodes[0]
	id1 := n1.ID()
	tk := ticket.Ticket{Members: []ticket.Member{{ID: id1[:], Addrs: n1.Endpoint().Addrs()}}}
	c, err := client.Dial(context.Background(), client.Config{Endpoint: ep, Ticket: tk, Logger: h.log, RequestTimeout: 20 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// makeTree builds a random directory tree in a local packstore.
func makeTree(t *testing.T, files int, size int) (*packstore.Store, key.Key, string) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	os.MkdirAll(filepath.Join(src, "sub"), 0o755)
	for i := 0; i < files; i++ {
		b := make([]byte, size+i*37)
		rand.Read(b)
		p := filepath.Join(src, fmt.Sprintf("f%03d", i))
		if i%3 == 0 {
			p = filepath.Join(src, "sub", fmt.Sprintf("g%03d", i))
		}
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	st, err := packstore.Open(filepath.Join(dir, "packstore"), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	root, _, err := ingest.Dir(st, src, ingest.Opts{Jobs: 2})
	if err != nil {
		t.Fatal(err)
	}
	return st, root, src
}

func TestClusterPushPull(t *testing.T) {
	h := cluster3(t)
	defer h.close()
	ctx := context.Background()
	c := h.client(t, 100)
	defer c.Close()

	local, root, _ := makeTree(t, 30, 20000)
	st, err := c.Push(ctx, local, root, "trees/a", "tester", client.Cond{Versioned: true}, nil)
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if st.Uploaded == 0 || len(st.Version) == 0 {
		t.Fatalf("push stats %+v", st)
	}
	// Every key placed on min_replicas owners: check with the nodes' stores.
	keys, _ := fstree.ReachableKeys(root, local.Get)
	pl := c.Placement()
	for _, k := range keys {
		holders := 0
		for _, n := range h.nodes {
			if has, _ := n.Store().Has(k); has {
				holders++
			}
		}
		if holders < int(pl.View().MinReplicas) {
			t.Fatalf("key %s on %d nodes", k.String()[:16], holders)
		}
	}
	// A second push of the same tree uploads nothing.
	st2, err := c.Push(ctx, local, root, "trees/a", "tester", client.Cond{Versioned: true, ExpectedVersion: st.Version}, nil)
	if err != nil {
		t.Fatalf("second push: %v", err)
	}
	if st2.Uploaded != 0 {
		t.Fatalf("second push uploaded %d", st2.Uploaded)
	}
	// A CAS with the wrong version fails.
	if _, err := c.Push(ctx, local, root, "trees/a", "tester", client.Cond{Versioned: true, ExpectedVersion: st.Version}, nil); err == nil {
		t.Fatal("expected cas mismatch")
	}
	// Pull into a fresh store and compare.
	pulled, err := packstore.Open(filepath.Join(t.TempDir(), "pulled"), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer pulled.Close()
	ps, err := c.Pull(ctx, pulled, "trees/a", nil)
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if ps.Root != root {
		t.Fatalf("pulled root %s, want %s", ps.Root, root)
	}
	for _, k := range keys {
		a, err := local.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		b, err := pulled.Get(k)
		if err != nil {
			t.Fatalf("pulled store lacks %s", k)
		}
		if !bytes.Equal(a, b) {
			t.Fatalf("object %s differs", k)
		}
	}
	// Listing and get.
	refs, err := c.RefList(ctx, "trees/")
	if err != nil || len(refs) != 1 || refs[0].Name != "trees/a" {
		t.Fatalf("list: %v %v", refs, err)
	}
	r, err := c.RefGet(ctx, "trees/a")
	if err != nil || !bytes.Equal(r.Ref.Key, root[:]) {
		t.Fatalf("get: %v %v", r, err)
	}
	if err := c.RefDelete(ctx, "trees/a", client.Cond{Force: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RefGet(ctx, "trees/a"); err != client.ErrUnknownRef {
		t.Fatalf("after delete: %v", err)
	}
}

func TestClusterGC(t *testing.T) {
	h := cluster3(t)
	defer h.close()
	ctx := context.Background()
	c := h.client(t, 100)
	defer c.Close()

	keep, keepRoot, _ := makeTree(t, 10, 30000)
	drop, dropRoot, _ := makeTree(t, 10, 30000)
	if _, err := c.Push(ctx, keep, keepRoot, "keep", "t", client.Cond{Force: true}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Push(ctx, drop, dropRoot, "drop", "t", client.Cond{Force: true}, nil); err != nil {
		t.Fatal(err)
	}
	dropKeys, _ := fstree.ReachableKeys(dropRoot, drop.Get)
	keepKeys, _ := fstree.ReachableKeys(keepRoot, keep.Get)

	gcRun := func() catalog.GCState {
		t.Helper()
		var last error
		for attempt := 0; attempt < 20; attempt++ {
			r, err := h.nodes[0].Admin(ctx, node.AdminRequest{Op: "gc-run"})
			if err == nil {
				var st catalog.GCState
				codec.Unmarshal(r.GC, &st)
				return st
			}
			last = err
			time.Sleep(time.Second)
		}
		t.Fatalf("gc-run: %v", last)
		return catalog.GCState{}
	}
	// Two cycles with everything referenced: nothing is reaped.
	gcRun()
	time.Sleep(2500 * time.Millisecond)
	gcRun()
	for _, n := range h.nodes {
		for _, k := range append(dropKeys, keepKeys...) {
			if n.Placement().IsOwner([32]byte(k), n.ID()) {
				if has, _ := n.Store().Has(k); !has {
					t.Fatalf("live key %s reaped at %s", k.String()[:16], view.ShortID(n.ID()))
				}
			}
		}
	}
	// Delete one reference; after two barriers its objects are exempt no
	// more, and a sweep reclaims them.
	if err := c.RefDelete(ctx, "drop", client.Cond{Force: true}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 60*time.Second, "garbage reaped", func() bool {
		time.Sleep(2500 * time.Millisecond)
		st := gcRun()
		if st.Phase != catalog.PhaseIdle {
			return false
		}
		reaped := 0
		for _, n := range h.nodes {
			for _, k := range dropKeys {
				if has, _ := n.Store().Has(k); !has {
					reaped++
				}
			}
		}
		return reaped > 0
	})
	for _, n := range h.nodes {
		for _, k := range keepKeys {
			if n.Placement().IsOwner([32]byte(k), n.ID()) {
				if has, _ := n.Store().Has(k); !has {
					t.Fatalf("kept key %s reaped", k.String()[:16])
				}
			}
		}
	}
	// The kept tree still pulls.
	pulled, _ := packstore.Open(filepath.Join(t.TempDir(), "p"), packstore.WithSync(false))
	defer pulled.Close()
	if _, err := c.Pull(ctx, pulled, "keep", nil); err != nil {
		t.Fatalf("pull after gc: %v", err)
	}
}

func TestClusterRemoveNode(t *testing.T) {
	h := cluster3(t)
	defer h.close()
	ctx := context.Background()
	c := h.client(t, 100)
	defer c.Close()

	local, root, _ := makeTree(t, 20, 20000)
	if _, err := c.Push(ctx, local, root, "t", "t", client.Cond{Force: true}, nil); err != nil {
		t.Fatal(err)
	}
	// Remove node 3: a transition moves its share to the survivors, then
	// its vote is removed.
	victim := h.nodes[2]
	vid := victim.ID()
	h.admin(h.nodes[0], node.AdminRequest{Op: "node-remove", Node: vid[:], AllowUnsafe: true})
	waitFor(t, 90*time.Second, "removal committed", func() bool {
		v := h.nodes[0].View()
		return v != nil && v.Pending == nil && len(v.Nodes) == 2 && len(v.Voters) == 2 && v.VoterSync == view.VoterSyncDone && len(v.RemoveVoters) == 0
	})
	victim.Close()
	h.nodes = h.nodes[:2]
	// Every key is on both survivors (R=3 capped at 2 nodes).
	keys, _ := fstree.ReachableKeys(root, local.Get)
	waitFor(t, 60*time.Second, "keys on survivors", func() bool {
		for _, n := range h.nodes {
			for _, k := range keys {
				if has, _ := n.Store().Has(k); !has {
					return false
				}
			}
		}
		return true
	})
	if err := c.RefreshView(ctx); err != nil {
		t.Fatal(err)
	}
	pulled, _ := packstore.Open(filepath.Join(t.TempDir(), "p"), packstore.WithSync(false))
	defer pulled.Close()
	if _, err := c.Pull(ctx, pulled, "t", nil); err != nil {
		t.Fatalf("pull after removal: %v", err)
	}
}

func TestClusterNodeDownDuringWrite(t *testing.T) {
	h := cluster3(t)
	defer h.close()
	ctx := context.Background()
	c := h.client(t, 100)
	defer c.Close()

	// Take node 3 down; writes proceed at min_replicas = 2.
	h.net.SetDown(nid(3), true)
	local, root, _ := makeTree(t, 12, 20000)
	if _, err := c.Push(ctx, local, root, "t", "t", client.Cond{Force: true}, nil); err != nil {
		t.Fatalf("push with a node down: %v", err)
	}
	// Bring it back: the first audit fills the gaps.
	h.net.SetDown(nid(3), false)
	keys, _ := fstree.ReachableKeys(root, local.Get)
	n3 := h.nodes[2]
	waitFor(t, 60*time.Second, "node 3 healed", func() bool {
		for _, k := range keys {
			if n3.Placement().InWriteSet([32]byte(k), n3.ID()) {
				if has, _ := n3.Store().Has(k); !has {
					return false
				}
			}
		}
		return true
	})
}

// TestClusterPushProgress checks the progress reports a push emits: bytes
// are counted per record as they go to the wire, the totals are known from
// negotiation on, and the per-node shares add up to the whole.
func TestClusterPushProgress(t *testing.T) {
	h := cluster3(t)
	defer h.close()
	ctx := context.Background()
	c := h.client(t, 100)
	defer c.Close()

	local, root, _ := makeTree(t, 30, 20000)
	var mu sync.Mutex
	var reports []client.ProgressReport
	prog := func(r client.ProgressReport) {
		mu.Lock()
		defer mu.Unlock()
		reports = append(reports, r)
	}
	st, err := c.Push(ctx, local, root, "trees/p", "tester", client.Cond{Force: true}, prog)
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reports) < 10 {
		t.Fatalf("only %d progress reports for %d objects", len(reports), st.Keys)
	}
	var lastBytes int64
	for i, r := range reports {
		if r.Bytes < lastBytes {
			t.Fatalf("report %d: bytes went from %d to %d", i, lastBytes, r.Bytes)
		}
		lastBytes = r.Bytes
		if r.TotalObjects != st.Keys {
			t.Fatalf("report %d: total objects %d, want %d", i, r.TotalObjects, st.Keys)
		}
	}
	last := reports[len(reports)-1]
	if last.Objects != last.TotalObjects || last.Bytes != last.TotalBytes || last.TotalBytes != st.Bytes || st.Bytes == 0 {
		t.Fatalf("final report %+v, stats %+v", last, st)
	}
	var sum int64
	for _, n := range last.Nodes {
		sum += n.Bytes
		if n.InFlight != 0 || n.Awaiting != 0 {
			t.Fatalf("node %s still has %d batches in flight, %d awaiting", view.ShortID(n.ID), n.InFlight, n.Awaiting)
		}
	}
	if len(last.Nodes) == 0 || sum != last.Bytes {
		t.Fatalf("node bytes %d over %d nodes, want %d", sum, len(last.Nodes), last.Bytes)
	}
	// The push spread over every owner: with R = 3 on three nodes each node
	// is the primary for a share of the keys (§11.1), not only the node the
	// client dialed first.
	if len(last.Nodes) != len(h.nodes) {
		t.Fatalf("push talked to %d of %d nodes", len(last.Nodes), len(h.nodes))
	}
	for _, n := range last.Nodes {
		if n.Bytes == 0 {
			t.Fatalf("node %s received nothing", view.ShortID(n.ID))
		}
	}
}
