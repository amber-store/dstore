package node_test

import (
	"bytes"
	"context"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/amber-store/core/commit"
	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/dstore/catalog"
	"github.com/amber-store/dstore/client"
	"github.com/amber-store/dstore/codec"
	"github.com/amber-store/dstore/node"
	"github.com/amber-store/dstore/worktree"
)

func readCommit(t *testing.T, tr *worktree.Tree, k key.Key) commit.Commit {
	t.Helper()
	data, err := tr.Get(k)
	if err != nil {
		t.Fatalf("commit %s: %v", k, err)
	}
	c, err := commit.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// A reference naming a commit is a branch: working copies clone and pull
// its tree, every push adds a commit on top, and the cluster keeps the
// whole history alive.
func TestWorktreeBranch(t *testing.T) {
	h := cluster3(t)
	defer h.close()
	ctx := context.Background()
	ca := h.client(t, 110)
	defer ca.Close()

	// A message turns a new reference into a branch.
	a := t.TempDir()
	writeWC(t, a, "f.txt", "one\n")
	ta, _, err := worktree.Init(ctx, ca, a, worktree.Config{Name: "trees/branch"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ta.Close()
	pr, err := ta.Push(ctx, ca, "alice", "first", false, 2, nil)
	if err != nil || pr.Commit.Type() != key.Commit {
		t.Fatalf("push: %v %+v", err, pr)
	}
	first, tree1 := pr.Commit, pr.Root
	if c := readCommit(t, ta, first); c.Tree != tree1 || len(c.Parents) != 0 || c.Message != "first" || c.Author.Name != "alice" || c.Committer != c.Author {
		t.Fatalf("first commit: %+v", c)
	}
	if r, err := ca.RefGet(ctx, "trees/branch"); err != nil || !bytes.Equal(r.Ref.Key, first[:]) {
		t.Fatalf("reference: %v %v", r, err)
	}
	if st, _ := ta.Status(2); len(st.Changes) != 0 || st.Remote != worktree.RemoteUpToDate || !ta.State.IsBranch() {
		t.Fatalf("status after push: %+v %+v", st, ta.State)
	}
	if pr, err = ta.Push(ctx, ca, "alice", "", false, 2, nil); err != nil || !pr.Nothing {
		t.Fatalf("second push: %v %+v", err, pr)
	}

	// A clone checks out the commit's tree.
	cb := h.client(t, 111)
	defer cb.Close()
	tb, fr, err := worktree.Clone(ctx, cb, filepath.Join(t.TempDir(), "b"), worktree.Config{Name: "trees/branch"}, nil)
	if err != nil {
		t.Fatalf("clone: %v", err)
	}
	defer tb.Close()
	if fr.Key != first || fr.Tree != tree1 || tb.State.Base != tree1 || tb.State.RemoteCommit != first {
		t.Fatalf("clone: %+v %+v", fr, tb.State)
	}
	if readWC(t, tb.Root, "f.txt") != "one\n" {
		t.Fatal("clone content")
	}

	// B's push, without a message, still commits on top of first.
	writeWC(t, tb.Root, "f.txt", "two\n")
	pr, err = tb.Push(ctx, cb, "bob", "", false, 2, nil)
	if err != nil || pr.Commit.Type() != key.Commit {
		t.Fatalf("push from B: %v %+v", err, pr)
	}
	second := pr.Commit
	if c := readCommit(t, tb, second); c.Tree != pr.Root || !slices.Equal(c.Parents, []key.Key{first}) || c.Author.Name != "bob" {
		t.Fatalf("second commit: %+v", c)
	}

	// A pulls B's commit.
	if plr, err := ta.Pull(ctx, ca, false, 2, nil); err != nil || len(plr.Conflicts) != 0 {
		t.Fatalf("pull: %v %+v", err, plr)
	}
	if readWC(t, a, "f.txt") != "two\n" || ta.State.RemoteCommit != second {
		t.Fatalf("after pull: %+v", ta.State)
	}

	// An interrupted push (reference written, state lost) is recognised on
	// retry although the retry's commit would carry a new timestamp.
	writeWC(t, a, "f.txt", "three\n")
	before := ta.State
	if pr, err = ta.Push(ctx, ca, "alice", "", false, 2, nil); err != nil || pr.Recovered {
		t.Fatalf("push: %v %+v", err, pr)
	}
	third := pr.Commit
	time.Sleep(time.Millisecond) // a distinct timestamp for the retry
	ta.State = before
	if pr, err = ta.Push(ctx, ca, "alice", "", false, 2, nil); err != nil || !pr.Recovered || pr.Commit != third || ta.State.RemoteCommit != third {
		t.Fatalf("retried push: %v %+v", err, pr)
	}
	if c := readCommit(t, ta, third); !slices.Equal(c.Parents, []key.Key{second}) {
		t.Fatalf("third commit: %+v", c)
	}

	// History stays live: the first commit's tree is reachable only through
	// the branch's ancestry, yet survives collections that reap a deleted
	// reference's objects.
	histKeys, err := fstree.ReachableKeys(first, ta.Get)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(histKeys, tree1) {
		t.Fatal("the first commit's tree is not reachable from it")
	}
	drop, dropRoot, _ := makeTree(t, 5, 30000)
	if _, err := ca.Push(ctx, drop, dropRoot, "drop", "t", client.Cond{Force: true}, nil); err != nil {
		t.Fatal(err)
	}
	dropKeys, _ := fstree.ReachableKeys(dropRoot, drop.Get)
	if err := ca.RefDelete(ctx, "drop", client.Cond{Force: true}); err != nil {
		t.Fatal(err)
	}
	gcRun := func() catalog.GCState {
		t.Helper()
		var last error
		for range 20 {
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
	waitFor(t, 60*time.Second, "garbage reaped", func() bool {
		time.Sleep(2500 * time.Millisecond)
		if gcRun().Phase != catalog.PhaseIdle {
			return false
		}
		for _, n := range h.nodes {
			for _, k := range dropKeys {
				if has, _ := n.Store().Has(k); !has {
					return true
				}
			}
		}
		return false
	})
	for _, n := range h.nodes {
		for _, k := range histKeys {
			if n.Placement().IsOwner([32]byte(k), n.ID()) {
				if has, _ := n.Store().Has(k); !has {
					t.Fatalf("history key %s (%s) reaped", k.String()[:16], k.Type())
				}
			}
		}
	}
}
