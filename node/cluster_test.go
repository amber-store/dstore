package node_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/amber-store/core/amberpack"
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
		ForwardTimeout: 10 * time.Second, PutTTL: 10 * time.Minute, PutChunkBytes: 256 << 10,
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
	return h.clientWith(t, i, nil)
}

// clientWith dials a client whose config tweak has adjusted.
func (h *harness) clientWith(t *testing.T, i byte, tweak func(*client.Config)) *client.Cluster {
	ep := h.net.Bind(nid(i), wire.ALPNClient)
	n1 := h.nodes[0]
	id1 := n1.ID()
	tk := ticket.Ticket{Members: []ticket.Member{{ID: id1[:], Addrs: n1.Endpoint().Addrs()}}}
	cfg := client.Config{Endpoint: ep, Ticket: tk, Logger: h.log, RequestTimeout: 20 * time.Second}
	if tweak != nil {
		tweak(&cfg)
	}
	c, err := client.Dial(context.Background(), cfg)
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
	// Bytes count what went over the wire: the records, not the logical
	// lengths in the keys (a tree object's key carries its subtree's size).
	keys, _ := fstree.ReachableKeys(root, local.Get)
	var wire int64
	for _, k := range keys {
		rec, err := local.GetRecord(k)
		if err != nil {
			t.Fatal(err)
		}
		wire += int64(len(rec))
	}
	if st.Bytes != wire {
		t.Fatalf("push stats count %d bytes, the records are %d bytes", st.Bytes, wire)
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

// TestClusterPushPipelines checks that a push keeps several batches in
// flight per primary, so that the wait for one batch's store and
// replication overlaps the next batch's transfer.
func TestClusterPushPipelines(t *testing.T) {
	h := cluster3(t)
	defer h.close()
	ctx := context.Background()
	c := h.clientWith(t, 100, func(cfg *client.Config) { cfg.BatchBytes = 64 << 10 })
	defer c.Close()

	local, root, _ := makeTree(t, 30, 20000)
	maxInFlight := 0 // prog runs under the transfer's lock
	prog := func(r client.ProgressReport) {
		for _, n := range r.Nodes {
			maxInFlight = max(maxInFlight, n.InFlight)
		}
	}
	if _, err := c.Push(ctx, local, root, "trees/pipe", "tester", client.Cond{Force: true}, prog); err != nil {
		t.Fatalf("push: %v", err)
	}
	if maxInFlight < 2 {
		t.Fatalf("at most %d batch in flight per node: batches are not pipelined", maxInFlight)
	}
}

// TestClusterPutStreamsWhileReceiving drives one put stream by hand and
// requires a replica to hold the batch's first record while the second
// half of the batch is still being sent: the primary appends and forwards
// records as they arrive instead of after the whole batch is in.
func TestClusterPutStreamsWhileReceiving(t *testing.T) {
	h := cluster3(t)
	defer h.close()
	ctx := context.Background()
	n1, n2 := h.nodes[0], h.nodes[1]
	ep := h.net.Bind(nid(100), wire.ALPNClient)
	pool := transport.NewPool(ep, func(view.NodeID) []string { return n1.Endpoint().Addrs() }, 1)
	defer pool.Close()

	// 64 blobs of 64 KiB: the first half fills two 1 MiB wire frames.
	var recs [][]byte
	var first key.Key
	for i := 0; i < 64; i++ {
		data := make([]byte, 64<<10)
		rand.Read(data)
		k, err := key.New(key.Blob, uint64(len(data)), data)
		if err != nil {
			t.Fatal(err)
		}
		rec, err := amberpack.EncodeRecord(k, data)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = k
		}
		recs = append(recs, rec)
	}
	s, err := pool.Open(ctx, n1.ID(), wire.ALPNClient)
	if err != nil {
		t.Fatal(err)
	}
	defer wire.CloseStream(s)
	v := n1.View()
	if err := wire.WriteMsg(s, &wire.Msg{Type: wire.TPut, ClusterID: v.ClusterID, Incarnation: v.Incarnation, Epoch: v.Epoch}); err != nil {
		t.Fatal(err)
	}
	var midway error
	err = wire.SendPackRecords(s, func(yield func([]byte, error) bool) {
		for i, rec := range recs {
			if i == len(recs)/2 {
				deadline := time.Now().Add(10 * time.Second)
				for {
					if has, _ := n2.Store().Has(first); has {
						break
					}
					if time.Now().After(deadline) {
						midway = errors.New("the replica did not hold the first record while the batch was still being sent")
						break
					}
					time.Sleep(20 * time.Millisecond)
				}
			}
			if !yield(rec, nil) {
				return
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = s.CloseWrite()
	resp, err := wire.Expect(s, wire.TPutResult)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if midway != nil {
		t.Fatal(midway)
	}
	if len(resp.Holders) != len(recs) || len(resp.Failed) != 0 || len(resp.Rejected) != 0 {
		t.Fatalf("reply: %d holders, %d failed, %d rejected", len(resp.Holders), len(resp.Failed), len(resp.Rejected))
	}
	for _, hl := range resp.Holders {
		if len(hl.Holders) != 3 {
			t.Fatalf("key %x held by %d owners, want 3", hl.Key[:8], len(hl.Holders))
		}
	}
}


// pushTree pushes a fresh random tree and returns its local store, root
// and keys.
func pushTree(t *testing.T, c *client.Cluster, files, size int, name string) (*packstore.Store, key.Key, [][32]byte) {
	t.Helper()
	local, root, _ := makeTree(t, files, size)
	if _, err := c.Push(context.Background(), local, root, name, "tester", client.Cond{Force: true}, nil); err != nil {
		t.Fatalf("push: %v", err)
	}
	keys, err := fstree.ReachableKeys(root, local.Get)
	if err != nil {
		t.Fatal(err)
	}
	all := make([][32]byte, len(keys))
	for i, k := range keys {
		all[i] = [32]byte(k)
	}
	return local, root, all
}

// TestClusterGetYieldsBeforeEveryBatchIsFetched checks that Get streams:
// with one worker and a stream-open latency, the first record must arrive
// after one latency, not after every per-node batch has completed.
func TestClusterGetYieldsBeforeEveryBatchIsFetched(t *testing.T) {
	h := cluster3(t)
	defer h.close()
	ctx := context.Background()
	c := h.clientWith(t, 100, func(cfg *client.Config) { cfg.Jobs = 1 })
	defer c.Close()
	_, _, keys := pushTree(t, c, 30, 20000, "trees/get")

	const delay = 300 * time.Millisecond
	h.net.SetDelay(delay)
	defer h.net.SetDelay(0)
	start := time.Now()
	seq, _ := c.Get(ctx, keys)
	var first time.Duration
	for _, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		first = time.Since(start)
		break
	}
	if first == 0 || first >= 2*delay {
		t.Fatalf("first record after %v with a %v stream latency: Get waited for every batch", first, delay)
	}
}

// TestClusterGetStopsEarlyCleanly breaks out of a Get and then uses the
// client again: the fetch's goroutines must not leak or deadlock.
func TestClusterGetStopsEarlyCleanly(t *testing.T) {
	h := cluster3(t)
	defer h.close()
	ctx := context.Background()
	c := h.client(t, 100)
	_, _, keys := pushTree(t, c, 30, 20000, "trees/early")

	for round := 0; round < 3; round++ {
		seq, _ := c.Get(ctx, keys)
		n := 0
		for _, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			n++
			if n == 2 {
				break
			}
		}
		if n != 2 {
			t.Fatalf("round %d: got %d records before breaking", round, n)
		}
	}
	seq, missing := c.Get(ctx, keys)
	n := 0
	for _, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	if n != len(keys) || len(missing()) != 0 {
		t.Fatalf("full get after early breaks: %d of %d records, %d missing", n, len(keys), len(missing()))
	}
	done := make(chan struct{})
	go func() { c.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Close hung after early breaks")
	}
}

// TestClusterPullWithNodeDown pulls a tree with one owner down: every key
// is served by its next owner in the read order.
func TestClusterPullWithNodeDown(t *testing.T) {
	h := cluster3(t)
	defer h.close()
	ctx := context.Background()
	c := h.client(t, 100)
	defer c.Close()
	local, root, keys := pushTree(t, c, 30, 20000, "trees/down")

	h.net.SetDown(nid(2), true)
	defer h.net.SetDown(nid(2), false)
	pulled, err := packstore.Open(filepath.Join(t.TempDir(), "pulled"), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer pulled.Close()
	ps, err := c.Pull(ctx, pulled, "trees/down", nil)
	if err != nil {
		t.Fatalf("pull with a node down: %v", err)
	}
	if ps.Root != root || ps.Fetched != len(keys) {
		t.Fatalf("pulled root %s, %d of %d objects", ps.Root, ps.Fetched, len(keys))
	}
	for _, k := range keys {
		a, _ := local.Get(key.Key(k))
		b, err := pulled.Get(key.Key(k))
		if err != nil || !bytes.Equal(a, b) {
			t.Fatalf("object %x missing or different after pull", k[:8])
		}
	}
}
