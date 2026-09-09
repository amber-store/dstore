// Package node implements a dstore storage node (architecture/dstore.md):
// a packstore with a meta database and a CASPaxos acceptor, serving the
// client and cluster ALPNs, coordinating reference writes, and running the
// maintenance activities — transitions, the reconcile pass and garbage
// collection — when it holds the lease.
package node

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amber-store/core/packstore"
	"github.com/amber-store/dstore/catalog"
	"github.com/amber-store/dstore/meta"
	"github.com/amber-store/dstore/paxos"
	"github.com/amber-store/dstore/transport"
	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
)

// Meta keys.
const (
	mkView        = "view"
	mkBallot      = "ballot"
	mkStoreID     = "store_id"
	mkIncarnation = "incarnation"
	mkRegistered  = "registered/" // + store_id: this store id is registered in the view
	mkCorrupt     = "corrupt/"
	mkPack        = "pack/"
	mkPin         = "pin/"
	mkGC          = "gc"
	mkGCHist      = "gchist/"
	mkXfer        = "xfer/"
	mkRecent      = "recent/"
	mkBackup      = "backup/"
	mkRetired     = "retired"
	mkNoVote      = "novote"
)

// Config configures a node.
type Config struct {
	StoreDir string
	PaxosDir string
	Endpoint transport.Endpoint
	Logger   *slog.Logger
	Jobs     int
	Rate     int64 // reconcile bytes/s; 0 = unlimited
	MinFree  int64 // bytes of free space below which uploads are refused
	Gateway  bool
	// Knobs (zero = default).
	PutTTL             time.Duration
	GCInterval         time.Duration
	BarrierTimeout     time.Duration
	MarkTimeout        time.Duration
	SweepTimeout       time.Duration
	Lease              time.Duration
	AdoptTimeout       time.Duration
	ParticipantTimeout time.Duration
	Delta              time.Duration
	FirstAudit         time.Duration
	AuditInterval      time.Duration
	SealAfter          time.Duration
	Grace              time.Duration
	ViewRefresh        time.Duration
	ForwardTimeout     time.Duration
	MaintenanceTick    time.Duration
	// PutChunkBytes is how many received bytes a put appends to the store
	// at a time while the rest of the batch is still arriving; default
	// 8 MiB.
	PutChunkBytes int
	// NoSync disables packstore fsyncs (tests only).
	NoSync bool
	// SegmentSize is the packstore segment (pack) size: the active pack is
	// sealed once it reaches this many bytes. Default DefaultSegmentSize.
	SegmentSize int64
}

// DefaultSegmentSize is the pack size a node uses unless Config.SegmentSize
// says otherwise: 2 GiB.
const DefaultSegmentSize int64 = 2 << 30

func (c *Config) defaults() {
	def := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	def(&c.PutTTL, time.Hour)
	def(&c.GCInterval, 4*time.Hour)
	def(&c.BarrierTimeout, time.Minute)
	def(&c.MarkTimeout, time.Hour)
	def(&c.SweepTimeout, 6*time.Hour)
	def(&c.Lease, 60*time.Second)
	def(&c.AdoptTimeout, 5*time.Minute)
	def(&c.ParticipantTimeout, time.Hour)
	def(&c.Delta, 10*time.Minute)
	def(&c.FirstAudit, 5*time.Minute)
	def(&c.AuditInterval, 7*24*time.Hour)
	def(&c.SealAfter, time.Hour)
	def(&c.Grace, time.Hour)
	def(&c.ViewRefresh, 30*time.Second)
	def(&c.ForwardTimeout, 30*time.Second)
	if c.PutChunkBytes <= 0 {
		c.PutChunkBytes = 8 << 20
	}
	if c.SegmentSize <= 0 {
		c.SegmentSize = DefaultSegmentSize
	}
	def(&c.MaintenanceTick, 5*time.Second)
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.PaxosDir == "" {
		c.PaxosDir = filepath.Join(c.StoreDir, "paxos")
	}
}

// Node is one dstore node.
type Node struct {
	cfg  Config
	log  *slog.Logger
	id   view.NodeID
	ep   transport.Endpoint
	pool *transport.Pool

	store    *packstore.Store
	meta     *meta.DB
	acceptor *paxos.Acceptor
	prop     *paxos.Proposer
	cat      *catalog.Catalog
	storeID  []byte

	viewMu    sync.RWMutex
	view      *view.View
	placement *view.Placement

	ballotMu   sync.Mutex
	ballotNext uint64
	ballotMax  uint64

	sweepMu sync.RWMutex // the sweep lock (§9.3)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Reachability, as this node sees it.
	unreachMu   sync.Mutex
	unreachable map[view.NodeID]time.Time
	// Addresses of nodes not yet in the view (joiners being admitted).
	joinAddrs map[view.NodeID][]string

	// Write admission.
	writeSlots   chan struct{} // client-ALPN put admission
	forwardSlots chan struct{} // cluster-ALPN put admission: forwards and reconcile
	writable     atomic.Bool

	// Reference coordination.
	completeMu    sync.Mutex
	completeCache map[[32]byte]time.Time

	// Keys written recently, awaiting their first audit (§8.4).
	recentMu sync.Mutex
	recent   map[[32]byte]time.Time

	// Maintenance.
	maint *maintenance
	gc    *gcState
	rec   *reconciler

	// Stats.
	stats nodeStats

	started atomic.Bool
	closed  atomic.Bool
}

type nodeStats struct {
	puts, gets, missing, refPuts atomic.Uint64
	bytesIn, bytesOut            atomic.Uint64
	forwarded                    atomic.Uint64
}

// Open opens the node's stores. The endpoint must already be bound.
func Open(cfg Config) (*Node, error) {
	cfg.defaults()
	if cfg.Endpoint == nil {
		return nil, errors.New("node: no endpoint")
	}
	n := &Node{cfg: cfg, log: cfg.Logger, ep: cfg.Endpoint, id: cfg.Endpoint.ID(),
		unreachable: map[view.NodeID]time.Time{}, completeCache: map[[32]byte]time.Time{}, recent: map[[32]byte]time.Time{}, joinAddrs: map[view.NodeID][]string{}}
	n.log = n.log.With("node", view.ShortID(n.id))
	st, err := packstore.Open(filepath.Join(cfg.StoreDir, "packstore"),
		packstore.WithSync(!cfg.NoSync), packstore.WithSegmentSize(cfg.SegmentSize))
	if err != nil {
		return nil, err
	}
	n.store = st
	md, err := meta.Open(filepath.Join(cfg.StoreDir, "meta"))
	if err != nil {
		st.Close()
		return nil, err
	}
	n.meta = md
	sid, err := md.Get([]byte(mkStoreID))
	if err != nil {
		sid = make([]byte, 16)
		rand.Read(sid)
		if err := md.Set([]byte(mkStoreID), sid); err != nil {
			n.closeStores()
			return nil, err
		}
	}
	n.storeID = sid
	acc, err := paxos.OpenAcceptor(cfg.PaxosDir, n.id)
	if err != nil {
		n.closeStores()
		return nil, err
	}
	n.acceptor = acc
	acc.Alert = func(msg string) { n.log.Warn(msg) }
	acc.OnInstall = func(v *view.View) { n.adopt(v, "install") }

	if b, err := md.Get([]byte(mkView)); err == nil {
		if v, err := view.Decode(b); err == nil {
			n.setView(v)
		}
	}
	n.pool = transport.NewPool(n.ep, n.addrsOf, 2)
	n.prop = paxos.NewProposer(n.id, n, n, n, paxos.Config{})
	n.cat = catalog.New(n.prop, n)
	jobs := cfg.Jobs
	if jobs <= 0 {
		jobs = 8
	}
	n.writeSlots = make(chan struct{}, 2*jobs)
	n.forwardSlots = make(chan struct{}, 2*jobs)
	n.writable.Store(true)
	n.maint = newMaintenance(n)
	n.gc = newGCState(n)
	n.rec = newReconciler(n)
	return n, nil
}

func (n *Node) closeStores() {
	if n.acceptor != nil {
		n.acceptor.Close()
	}
	if n.meta != nil {
		n.meta.Close()
	}
	if n.store != nil {
		n.store.Close()
	}
}

// ID returns the node's identity.
func (n *Node) ID() view.NodeID { return n.id }

// Store returns the packstore.
func (n *Node) Store() *packstore.Store { return n.store }

// Catalog returns the node's catalog handle.
func (n *Node) Catalog() *catalog.Catalog { return n.cat }

// Acceptor returns the node's acceptor.
func (n *Node) Acceptor() *paxos.Acceptor { return n.acceptor }

// Endpoint returns the node's endpoint.
func (n *Node) Endpoint() transport.Endpoint { return n.ep }

// Log returns the node's logger.
func (n *Node) Log() *slog.Logger { return n.log }

// ---- views ----

// View returns the current view (paxos.Views).
func (n *Node) View() *view.View {
	n.viewMu.RLock()
	defer n.viewMu.RUnlock()
	return n.view
}

// Placement returns the current placement tables.
func (n *Node) Placement() *view.Placement {
	n.viewMu.RLock()
	defer n.viewMu.RUnlock()
	return n.placement
}

func (n *Node) setView(v *view.View) {
	n.viewMu.Lock()
	defer n.viewMu.Unlock()
	if n.view != nil && n.view.Epoch == v.Epoch && n.view.Incarnation == v.Incarnation && n.placement != nil {
		n.view = v
		n.placement = view.NewPlacement(v) // node lists may not change, but cheap
		return
	}
	n.view = v
	n.placement = view.NewPlacement(v)
}

// Adopt adopts a newer view (paxos.Views).
func (n *Node) Adopt(v *view.View) { n.adopt(v, "adopt") }

func (n *Node) adopt(v *view.View, how string) {
	cur := n.View()
	if cur != nil {
		c := cur.Compare(v.Incarnation, v.Epoch)
		if c > 0 || c == 0 && v.Version <= cur.Version {
			return
		}
		if len(cur.ClusterID) > 0 && len(v.ClusterID) > 0 && string(cur.ClusterID) != string(v.ClusterID) {
			n.log.Error("refusing a view from another cluster")
			return
		}
	}
	enc, err := v.Encode()
	if err != nil {
		return
	}
	if err := n.meta.Set([]byte(mkView), enc); err != nil {
		n.log.Error("persist view", "error", err)
		return
	}
	n.setView(v)
	_ = n.acceptor.Install(v)
	epochChanged := cur == nil || cur.Epoch != v.Epoch || cur.Incarnation != v.Incarnation
	if epochChanged {
		n.log.Info("adopted view", "epoch", v.Epoch, "version", v.Version, "nodes", len(v.Nodes), "voters", len(v.Voters), "pending", v.Pending != nil, "via", how)
	}
	if n.started.Load() {
		if epochChanged {
			go n.onEpoch(cur, v)
		}
	}
}

// onEpoch runs the adoption side effects of a new epoch (§8.2).
func (n *Node) onEpoch(old, v *view.View) {
	if v.Pending != nil && (old == nil || old.Pending == nil || old.Pending.ID != v.Pending.ID || old.Pending.Round != v.Pending.Round) {
		n.rec.adoptTransition(v)
	}
	if v.Pending == nil && old != nil && old.Pending != nil {
		n.rec.transitionCommitted(v)
	}
	// A member's incarnation change marks every pack replicated=false
	// for that target (§8.4).
	if old != nil {
		for _, nn := range v.Nodes {
			if on, ok := old.Node(nn.NID()); ok && on.Incarnation != nn.Incarnation {
				n.rec.targetWiped(nn.NID())
			}
		}
	}
	if old != nil && old.IsMember(n.id) && !v.IsMember(n.id) && !n.meta.Has([]byte(mkRetired)) {
		n.log.Warn("this node is no longer a member of the committed view; it will hand its data back and then serve nothing")
	}
}

func (n *Node) addrsOf(id view.NodeID) []string {
	if v := n.View(); v != nil {
		if nd, ok := v.Node(id); ok && len(nd.Addrs) > 0 {
			return nd.Addrs
		}
	}
	n.unreachMu.Lock()
	defer n.unreachMu.Unlock()
	return n.joinAddrs[id]
}

// rememberAddrs records a joiner's addresses until the view carries them.
func (n *Node) rememberAddrs(id view.NodeID, addrs []string) {
	n.unreachMu.Lock()
	n.joinAddrs[id] = addrs
	delete(n.unreachable, id)
	n.unreachMu.Unlock()
	n.pool.Drop(id, wire.ALPNCluster)
}

// ---- ballots (paxos.Ballots) ----

const ballotBlock = 1000

// NextBallot hands out a persisted, never reused proposer counter.
func (n *Node) NextBallot() (uint64, error) {
	n.ballotMu.Lock()
	defer n.ballotMu.Unlock()
	if n.ballotNext == 0 || n.ballotNext >= n.ballotMax {
		cur := n.meta.GetU64([]byte(mkBallot))
		next := cur + ballotBlock
		if err := n.meta.SetU64([]byte(mkBallot), next); err != nil {
			return 0, err
		}
		n.ballotNext, n.ballotMax = cur+1, next
	}
	b := n.ballotNext
	n.ballotNext++
	return b, nil
}

// BumpBallot raises the counter above c.
func (n *Node) BumpBallot(c uint64) error {
	n.ballotMu.Lock()
	defer n.ballotMu.Unlock()
	if c < n.ballotNext {
		return nil
	}
	next := c + ballotBlock
	if err := n.meta.SetU64([]byte(mkBallot), next); err != nil {
		return err
	}
	n.ballotNext, n.ballotMax = c+1, next
	return nil
}

// ---- paxos transport over the cluster ALPN ----

// Call implements paxos.Transport.
func (n *Node) Call(ctx context.Context, to view.NodeID, req *wire.Msg) (*wire.Msg, error) {
	if to == n.id {
		return n.acceptor.Handle(req), nil
	}
	resp, err := n.pool.Call(ctx, to, wire.ALPNCluster, req)
	if err != nil {
		var we *wire.Error
		if errors.As(err, &we) {
			return resp, nil // catalog error codes are classified by the proposer
		}
		n.markUnreachable(to, err)
		return nil, err
	}
	n.markReachable(to)
	return resp, nil
}

func (n *Node) markUnreachable(id view.NodeID, err error) {
	n.unreachMu.Lock()
	n.unreachable[id] = time.Now()
	n.unreachMu.Unlock()
}

func (n *Node) markReachable(id view.NodeID) {
	n.unreachMu.Lock()
	delete(n.unreachable, id)
	n.unreachMu.Unlock()
}

// Unreachable returns the members this node cannot currently reach: those
// whose last failed call is recent and that have not answered since.
func (n *Node) Unreachable() [][]byte {
	n.unreachMu.Lock()
	defer n.unreachMu.Unlock()
	var out [][]byte
	for id, t := range n.unreachable {
		if time.Since(t) > 2*time.Minute {
			delete(n.unreachable, id)
			continue
		}
		id := id
		out = append(out, id[:])
	}
	return out
}

// ---- lifecycle ----

// Start begins serving and the background loops.
func (n *Node) Start(ctx context.Context) error {
	n.ctx, n.cancel = context.WithCancel(ctx)
	n.started.Store(true)
	n.wg.Add(1)
	go n.acceptLoop()
	n.wg.Add(1)
	go n.viewRefreshLoop()
	n.wg.Add(1)
	go n.maint.run()
	n.wg.Add(1)
	go n.rec.run()
	n.wg.Add(1)
	go n.selfEntryLoop()
	n.log.Info("node started", "id", view.IDString(n.id), "addrs", n.ep.Addrs())
	return nil
}

// Close stops the node.
func (n *Node) Close() error {
	if !n.closed.CompareAndSwap(false, true) {
		return nil
	}
	if n.cancel != nil {
		n.cancel()
	}
	n.pool.Close()
	n.ep.Close()
	n.wg.Wait()
	n.closeStores()
	return nil
}

// Wait blocks until the node stops.
func (n *Node) Wait() { n.wg.Wait() }

func (n *Node) viewRefreshLoop() {
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.ViewRefresh)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
			n.refreshView()
		}
	}
}

// refreshView fast-reads the view register and adopts a newer one.
func (n *Node) refreshView() {
	ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
	defer cancel()
	v, err := n.cat.ReadView(ctx)
	if err != nil {
		return
	}
	n.adopt(v, "refresh")
}

// selfEntryLoop keeps this node's addrs and writable flag current in the
// view (§5.5), rate-limited to once a minute.
func (n *Node) selfEntryLoop() {
	defer n.wg.Done()
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	first := time.After(2 * time.Second)
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-first:
			n.updateSelfEntry()
		case <-t.C:
			n.updateSelfEntry()
		}
	}
}

func (n *Node) updateSelfEntry() {
	v := n.View()
	if v == nil {
		return
	}
	me, ok := v.Node(n.id)
	if !ok {
		return
	}
	addrs := n.ep.Addrs()
	n.checkFreeSpace()
	writable := n.writable.Load()
	if sameStrings(me.Addrs, addrs) && me.Writable == writable {
		return
	}
	ctx, cancel := context.WithTimeout(n.ctx, 20*time.Second)
	defer cancel()
	nv, err := n.cat.CASView(ctx, func(v *view.View) (bool, error) {
		changed := false
		upd := func(nodes []view.Node) {
			for i := range nodes {
				if nodes[i].NID() == n.id {
					if !sameStrings(nodes[i].Addrs, addrs) || nodes[i].Writable != writable {
						nodes[i].Addrs = addrs
						nodes[i].Writable = writable
						changed = true
					}
				}
			}
		}
		upd(v.Nodes)
		if v.Pending != nil {
			upd(v.Pending.Nodes)
		}
		if !changed {
			return false, catalog.ErrAbort
		}
		return false, nil
	})
	if err == nil && nv != nil {
		n.adopt(nv, "self")
	}
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// checkFreeSpace clears the writable flag below the free-space reserve.
func (n *Node) checkFreeSpace() {
	free, total, ok := diskFree(filepath.Join(n.cfg.StoreDir, "packstore"))
	if !ok {
		return
	}
	reserve := n.cfg.MinFree
	if reserve == 0 {
		reserve = min(total/20, 100<<30)
	}
	n.writable.Store(free > reserve)
}

// ---- pins (§9.5) ----

func tailOf(k [32]byte) []byte { return k[24:32] }

// pinKeys records pins for keys found present, stamped with the current
// barrier counter, synced before the caller replies. It is called under
// the sweep lock shared.
func (n *Node) pinKeys(keys [][32]byte) error {
	if len(keys) == 0 {
		return nil
	}
	c := n.gc.counter()
	b := n.meta.NewBatch()
	var cb [8]byte
	putU64(cb[:], c)
	for _, k := range keys {
		b.Set(append([]byte(mkPin), tailOf(k)...), cb[:])
	}
	return b.Commit()
}

// pinned reports whether k's tail carries a pin at or above count.
func (n *Node) pinned(k [32]byte, atLeast uint64) bool {
	b, err := n.meta.Get(append([]byte(mkPin), tailOf(k)...))
	if err != nil || len(b) != 8 {
		return false
	}
	return getU64(b) >= atLeast
}

func putU64(b []byte, v uint64) {
	for i := 7; i >= 0; i-- {
		b[i] = byte(v)
		v >>= 8
	}
}

func getU64(b []byte) uint64 {
	var v uint64
	for i := 0; i < 8; i++ {
		v = v<<8 | uint64(b[i])
	}
	return v
}

// ---- corrupt records ----

func (n *Node) isCorrupt(k [32]byte) bool {
	return n.meta.Has(append([]byte(mkCorrupt), k[:]...))
}

func (n *Node) markCorrupt(k [32]byte) {
	_ = n.meta.Set(append([]byte(mkCorrupt), k[:]...), []byte{1})
}

func (n *Node) clearCorrupt(k [32]byte) {
	_ = n.meta.Delete(append([]byte(mkCorrupt), k[:]...))
}

// clearCorruptKeys drops the corrupt markers of the keys that carry one in
// a single synced write. A marker is rare, so a batch of fresh records
// usually costs no write at all here; one synced delete per key made a
// 60 MiB batch take a minute per replica.
func (n *Node) clearCorruptKeys(keys [][32]byte) {
	var b *meta.Batch
	for _, k := range keys {
		if !n.isCorrupt(k) {
			continue
		}
		if b == nil {
			b = n.meta.NewBatch()
		}
		b.Delete(append([]byte(mkCorrupt), k[:]...))
	}
	if b != nil {
		_ = b.Commit()
	}
}

// ---- helpers ----

// InitCluster writes the first view of a new cluster onto this node's
// acceptor (a single voter) and adopts it.
func (n *Node) InitCluster(ctx context.Context, replicas, minReplicas uint8, weight uint32, zone string, allowUnsafe bool) (*view.View, error) {
	if n.View() != nil {
		return nil, errors.New("node: already a member of a cluster")
	}
	if replicas == 0 {
		replicas = 3
	}
	if minReplicas == 0 {
		minReplicas = view.DefaultMinReplicas(replicas)
	}
	if minReplicas < 2 && !allowUnsafe {
		return nil, errors.New("node: min_replicas below 2 needs --allow-unsafe")
	}
	cid := make([]byte, 16)
	rand.Read(cid)
	v := &view.View{
		ClusterID: cid, Incarnation: 1, Epoch: 1, Version: 1, PlacementEpoch: 1,
		Replicas: replicas, MinReplicas: minReplicas,
		Voters: []view.Voter{{ID: n.id[:], Since: 1}},
		Nodes:  []view.Node{{ID: n.id[:], Weight: weight, Addrs: n.ep.Addrs(), Zone: zone, Writable: true, Incarnation: 1}},
	}
	if err := n.acceptor.SetMarker(1); err != nil {
		return nil, err
	}
	if err := n.acceptor.Install(v); err != nil {
		return nil, err
	}
	n.setView(v)
	enc, _ := v.Encode()
	if err := n.meta.Set([]byte(mkView), enc); err != nil {
		return nil, err
	}
	if err := n.meta.Set([]byte(mkRegistered+string(n.storeID)), []byte{1}); err != nil {
		return nil, err
	}
	if err := n.cat.InitView(ctx, v); err != nil {
		return nil, err
	}
	n.log.Info("cluster initialised", "cluster", hex.EncodeToString(cid))
	return v, nil
}

// StoreDir returns the store directory.
func (n *Node) StoreDir() string { return n.cfg.StoreDir }

// Config returns the node's configuration.
func (n *Node) Config() Config { return n.cfg }

// dirExists reports whether p is a directory.
func dirExists(p string) bool {
	st, err := os.Stat(p)
	return err == nil && st.IsDir()
}

// errf formats an error.
func errf(format string, args ...any) error { return fmt.Errorf(format, args...) }

// OpenOffline opens a store's meta, packstore and acceptor without a
// network endpoint, for offline operations (ticket derivation, catalog
// restore). The node must not be running.
func OpenOffline(dir string) (*Node, error) {
	sk, err := os.ReadFile(filepath.Join(dir, "identity"))
	if err != nil {
		return nil, fmt.Errorf("node: no identity in %s: %w", dir, err)
	}
	_ = sk
	return Open(Config{StoreDir: dir, Endpoint: offlineEndpoint{}})
}

// offlineEndpoint is a transport.Endpoint that reaches nothing.
type offlineEndpoint struct{}

func (offlineEndpoint) ID() view.NodeID { return view.NodeID{} }
func (offlineEndpoint) Dial(context.Context, view.NodeID, []string, string) (transport.Conn, error) {
	return nil, errors.New("node: offline")
}
func (offlineEndpoint) Accept(ctx context.Context) (transport.Conn, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (offlineEndpoint) Addrs() []string { return nil }
func (offlineEndpoint) Close() error    { return nil }
