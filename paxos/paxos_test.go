package paxos

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
)

// memTransport delivers catalog requests to in-process acceptors.
type memTransport struct {
	mu        sync.Mutex
	acceptors map[view.NodeID]*Acceptor
	down      map[view.NodeID]bool
}

func (t *memTransport) Call(ctx context.Context, to view.NodeID, req *wire.Msg) (*wire.Msg, error) {
	t.mu.Lock()
	a, ok := t.acceptors[to]
	down := t.down[to]
	t.mu.Unlock()
	if !ok || down {
		return nil, errors.New("unreachable")
	}
	return a.Handle(req), nil
}

type memViews struct {
	mu sync.Mutex
	v  *view.View
}

func (m *memViews) View() *view.View {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.v
}
func (m *memViews) Adopt(v *view.View) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.v == nil || m.v.Compare(v.Incarnation, v.Epoch) < 0 {
		m.v = v
	}
}

type memBallots struct{ c atomic.Uint64 }

func (b *memBallots) NextBallot() (uint64, error) { return b.c.Add(1), nil }
func (b *memBallots) BumpBallot(n uint64) error {
	for {
		cur := b.c.Load()
		if cur >= n || b.c.CompareAndSwap(cur, n) {
			return nil
		}
	}
}

func nid(i int) view.NodeID {
	var id view.NodeID
	id[0] = byte(i)
	id[31] = byte(i)
	return id
}

func cluster(t *testing.T, n int) (*memTransport, *view.View, []*Acceptor) {
	t.Helper()
	tr := &memTransport{acceptors: map[view.NodeID]*Acceptor{}, down: map[view.NodeID]bool{}}
	v := &view.View{ClusterID: []byte("cluster0000000000"), Incarnation: 1, Epoch: 1, Version: 1, Replicas: 3, MinReplicas: 2}
	var accs []*Acceptor
	for i := 1; i <= n; i++ {
		id := nid(i)
		v.Voters = append(v.Voters, view.Voter{ID: id[:], Since: 1})
		v.Nodes = append(v.Nodes, view.Node{ID: id[:], Weight: 100, Writable: true})
	}
	for i := 1; i <= n; i++ {
		id := nid(i)
		a, err := OpenAcceptor(filepath.Join(t.TempDir(), fmt.Sprintf("paxos%d", i)), id)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { a.Close() })
		if err := a.SetMarker(1); err != nil {
			t.Fatal(err)
		}
		if err := a.Install(v); err != nil {
			t.Fatal(err)
		}
		tr.acceptors[id] = a
		accs = append(accs, a)
	}
	return tr, v, accs
}

func proposer(tr *memTransport, v *view.View, i int) *Proposer {
	return NewProposer(nid(i), tr, &memViews{v: v}, &memBallots{}, Config{AcceptorTimeout: time.Second})
}

func TestCASAndRead(t *testing.T) {
	tr, v, _ := cluster(t, 3)
	p := proposer(tr, v, 1)
	ctx := context.Background()
	reg := []byte("ref/a")

	res, err := p.Propose(ctx, reg, func(cur Row, _ Ballot) ([]byte, bool, error) {
		if cur.HasValue {
			return nil, false, errors.New("exists")
		}
		return []byte("v1"), true, nil
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(res.Value) != "v1" {
		t.Fatalf("value %q", res.Value)
	}
	row, err := p.Read(ctx, reg)
	if err != nil || string(row.Value) != "v1" || row.Accepted.Compare(res.Ballot) != 0 {
		t.Fatalf("read %v %v", row, err)
	}
	// A second create must fail with the current value.
	_, err = p.Propose(ctx, reg, func(cur Row, _ Ballot) ([]byte, bool, error) {
		if cur.HasValue {
			return nil, false, errors.New("exists")
		}
		return []byte("v2"), true, nil
	}, 0)
	if err == nil || err.Error() != "exists" {
		t.Fatalf("expected exists, got %v", err)
	}
}

func TestConcurrentProposersOneWins(t *testing.T) {
	tr, v, _ := cluster(t, 5)
	ctx := context.Background()
	reg := []byte("ref/hot")
	var wins atomic.Int32
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := map[string]bool{}
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := proposer(tr, v, i)
			for range 5 {
				res, err := p.Propose(ctx, reg, func(cur Row, _ Ballot) ([]byte, bool, error) {
					if cur.HasValue {
						return nil, false, errors.New("exists")
					}
					return fmt.Appendf(nil, "winner-%d", i), true, nil
				}, 0)
				if err == nil {
					wins.Add(1)
				} else if err.Error() != "exists" {
					t.Errorf("proposer %d: %v", i, err)
				}
				// Every answer names a decided value.
				mu.Lock()
				seen[string(res.Value)] = true
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if wins.Load() > 1 {
		t.Fatalf("%d winners", wins.Load())
	}
	if len(seen) != 1 {
		t.Fatalf("proposers saw %d distinct values: %v", len(seen), seen)
	}
	p := proposer(tr, v, 1)
	row, err := p.Read(ctx, reg)
	if err != nil || !seen[string(row.Value)] {
		t.Fatalf("final %q %v", row.Value, err)
	}
}

func TestMinorityDownStillCommits(t *testing.T) {
	tr, v, _ := cluster(t, 3)
	tr.down[nid(3)] = true
	p := proposer(tr, v, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := p.Propose(ctx, []byte("x"), func(Row, Ballot) ([]byte, bool, error) { return []byte("a"), true, nil }, 0); err != nil {
		t.Fatal(err)
	}
	tr.down[nid(2)] = true
	_, err := p.Propose(ctx, []byte("x"), func(Row, Ballot) ([]byte, bool, error) { return []byte("b"), true, nil }, 0)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected unavailable, got %v", err)
	}
	// The value is still readable once a majority is back; the down
	// acceptor missed nothing that was committed.
	tr.down[nid(2)] = false
	row, err := p.Read(ctx, []byte("x"))
	if err != nil || string(row.Value) != "a" {
		t.Fatalf("read after partition: %v %v", row, err)
	}
}

func TestScanMergeAndSettle(t *testing.T) {
	tr, v, accs := cluster(t, 3)
	p := proposer(tr, v, 1)
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		reg := []byte(fmt.Sprintf("ref/%02d", i))
		if _, err := p.Propose(ctx, reg, func(Row, Ballot) ([]byte, bool, error) { return []byte{byte(i)}, true, nil }, 0); err != nil {
			t.Fatal(err)
		}
	}
	// Plant a minority-accepted value directly on one acceptor.
	b := Ballot{Counter: 1000, Proposer: nid(2)}
	accs[0].Handle(&wire.Msg{Type: wire.TAccept, Incarnation: 1, Epoch: 1, Reg: []byte("ref/05"), Ballot: b.Bytes(), Value: []byte("rogue"), HasValue: true})

	rows, next, err := p.Scan(ctx, []byte("ref/"), nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	if next != nil {
		t.Fatalf("unexpected cursor %q", next)
	}
	if len(rows) != 10 {
		t.Fatalf("%d rows", len(rows))
	}
	undecided := 0
	for _, r := range rows {
		if r.Undecided {
			undecided++
			if string(r.Reg) != "ref/05" {
				t.Fatalf("undecided %s", r.Reg)
			}
			settled, err := p.Settle(ctx, r.Reg)
			if err != nil {
				t.Fatal(err)
			}
			// The higher-ballot minority value is what a settle adopts.
			if string(settled.Value) != "rogue" {
				t.Fatalf("settled to %q", settled.Value)
			}
		}
	}
	if undecided != 1 {
		t.Fatalf("%d undecided", undecided)
	}
	// Paging.
	rows, next, err = p.Scan(ctx, []byte("ref/"), nil, 4)
	if err != nil || len(rows) != 4 || next == nil {
		t.Fatalf("page: %d rows next=%q err=%v", len(rows), next, err)
	}
	rows2, _, err := p.Scan(ctx, []byte("ref/"), next, 100)
	if err != nil || len(rows2) != 6 {
		t.Fatalf("page 2: %d rows err=%v", len(rows2), err)
	}
}

func TestEpochGating(t *testing.T) {
	tr, v, accs := cluster(t, 3)
	ctx := context.Background()
	p := proposer(tr, v, 1)
	if _, err := p.Propose(ctx, []byte("k"), func(Row, Ballot) ([]byte, bool, error) { return []byte("1"), true, nil }, 0); err != nil {
		t.Fatal(err)
	}
	// Install a newer epoch on two acceptors; a proposer at the old epoch
	// learns it through stale-view and retries successfully.
	v2 := v.Clone()
	v2.Epoch, v2.Version = 2, 2
	accs[0].Install(v2)
	accs[1].Install(v2)
	old := proposer(tr, v, 2)
	res, err := old.Propose(ctx, []byte("k"), func(cur Row, _ Ballot) ([]byte, bool, error) { return append(cur.Value, '2'), true, nil }, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(res.Value) != "12" {
		t.Fatalf("value %q", res.Value)
	}
	if old.views.View().Epoch != 2 {
		t.Fatal("proposer did not adopt the newer view")
	}
	// The third acceptor was told the view through need-view + install.
	if accs[2].Installed().Epoch != 2 {
		t.Fatal("acceptor 3 did not install the view")
	}
}

func TestPurgeAndFloor(t *testing.T) {
	tr, v, accs := cluster(t, 3)
	ctx := context.Background()
	p := proposer(tr, v, 1)
	reg := []byte("ref/t")
	res, err := p.Propose(ctx, reg, func(Row, Ballot) ([]byte, bool, error) { return []byte("tomb"), true, nil }, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n := p.Purge(ctx, reg, res.Ballot); n != 3 {
		t.Fatalf("purge confirmed by %d", n)
	}
	for _, a := range accs {
		if _, _, has, _ := a.LocalRead(reg); has {
			t.Fatal("row survived purge")
		}
	}
	// A proposer with a small ballot space is refused below the floor and
	// climbs above it.
	small := proposer(tr, v, 2)
	res2, err := small.Propose(ctx, reg, func(cur Row, _ Ballot) ([]byte, bool, error) {
		if cur.HasValue {
			return nil, false, errors.New("resurrected")
		}
		return []byte("new"), true, nil
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if res2.Ballot.Compare(res.Ballot) <= 0 {
		t.Fatalf("new ballot %v not above purged %v", res2.Ballot, res.Ballot)
	}
}

func TestAmnesiacRefusesPrepare(t *testing.T) {
	tr, v, accs := cluster(t, 3)
	// Acceptor 3's marker disagrees with the view.
	accs[2].SetMarker(99)
	if !accs[2].Amnesiac() {
		t.Fatal("expected amnesiac")
	}
	p := proposer(tr, v, 1)
	ctx := context.Background()
	if _, err := p.Propose(ctx, []byte("k"), func(Row, Ballot) ([]byte, bool, error) { return []byte("1"), true, nil }, 0); err != nil {
		t.Fatal(err)
	}
	// It still accepted (accept is honoured) so a later sync can confirm it.
	if _, val, has, _ := accs[2].LocalRead([]byte("k")); !has || string(val) != "1" {
		t.Fatal("amnesiac acceptor did not honour accept")
	}
	tr.down[nid(2)] = true
	_, err := p.Propose(ctx, []byte("k"), func(Row, Ballot) ([]byte, bool, error) { return []byte("2"), true, nil }, 0)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("amnesiac reply counted as a promise: %v", err)
	}
}

func TestNotAfter(t *testing.T) {
	tr, v, _ := cluster(t, 3)
	p := proposer(tr, v, 1)
	ctx := context.Background()
	_, err := p.Propose(ctx, []byte("k"), func(Row, Ballot) ([]byte, bool, error) { return []byte("1"), true, nil }, time.Now().Add(-time.Second).UnixNano())
	if !errors.Is(err, ErrExpired) {
		t.Fatalf("expected expired, got %v", err)
	}
}
