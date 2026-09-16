package node_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/amber-store/dstore/client"
	"github.com/amber-store/dstore/worktree"
)

func writeWC(t *testing.T, root, rel, content string) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readWC(t *testing.T, root, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("%s: %v", rel, err)
	}
	return string(b)
}

func wcKinds(cs []worktree.Change) map[string]worktree.Kind {
	m := map[string]worktree.Kind{}
	for _, c := range cs {
		m[c.Path] = c.Kind
	}
	return m
}

func TestWorktreeInitPushCloneEditPull(t *testing.T) {
	h := cluster3(t)
	defer h.close()
	ctx := context.Background()
	ca := h.client(t, 100)
	defer ca.Close()

	// A: an existing directory becomes the working copy of a new name.
	a := t.TempDir()
	writeWC(t, a, "hello.txt", "hello\n")
	writeWC(t, a, "sub/deep.txt", "deep\n")
	ta, fr, err := worktree.Init(ctx, ca, a, worktree.Config{Name: "trees/wc"}, nil)
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	defer ta.Close()
	if fr.Exists {
		t.Fatal("a new name must not exist")
	}
	st, err := ta.Status(2)
	if err != nil {
		t.Fatal(err)
	}
	if k := wcKinds(st.Changes); k["hello.txt"] != worktree.Added || k["sub"] != worktree.Added || k["sub/deep.txt"] != worktree.Added || st.Remote != worktree.RemoteAbsent {
		t.Fatalf("status after init: %+v", st)
	}
	pr, err := ta.Push(ctx, ca, "tester", false, 2, nil)
	if err != nil || pr.Nothing {
		t.Fatalf("push: %v %+v", err, pr)
	}
	if st, _ = ta.Status(2); len(st.Changes) != 0 || st.MetaOnly != 0 || st.Remote != worktree.RemoteUpToDate {
		t.Fatalf("status after push: %+v", st)
	}
	if pr, err = ta.Push(ctx, ca, "tester", false, 2, nil); err != nil || !pr.Nothing {
		t.Fatalf("second push: %v %+v", err, pr)
	}

	// B: a clone sees the push.
	cb := h.client(t, 101)
	defer cb.Close()
	b := filepath.Join(t.TempDir(), "wc")
	tb, _, err := worktree.Clone(ctx, cb, b, worktree.Config{Name: "trees/wc"}, nil)
	if err != nil {
		t.Fatalf("clone: %v", err)
	}
	defer tb.Close()
	if readWC(t, b, "hello.txt") != "hello\n" || readWC(t, b, "sub/deep.txt") != "deep\n" {
		t.Fatal("clone content")
	}
	if st, _ = tb.Status(2); len(st.Changes) != 0 || st.Remote != worktree.RemoteUpToDate {
		t.Fatalf("status after clone: %+v", st)
	}

	// B edits and pushes.
	writeWC(t, b, "hello.txt", "hello world\n")
	writeWC(t, b, "new.txt", "n\n")
	if err := os.Remove(filepath.Join(b, "sub", "deep.txt")); err != nil {
		t.Fatal(err)
	}
	st, _ = tb.Status(2)
	if k := wcKinds(st.Changes); k["hello.txt"] != worktree.Modified || k["new.txt"] != worktree.Added || k["sub/deep.txt"] != worktree.Deleted || len(st.Changes) != 3 {
		t.Fatalf("status after edits: %+v", st)
	}
	if _, err := tb.Push(ctx, cb, "tester", false, 2, nil); err != nil {
		t.Fatalf("push from B: %v", err)
	}

	// A fetches, sees the move, pulls.
	if fr, err = ta.Fetch(ctx, ca, nil); err != nil || fr.UpToDate {
		t.Fatalf("fetch: %v %+v", err, fr)
	}
	st, _ = ta.Status(2)
	if k := wcKinds(st.Incoming); st.Remote != worktree.RemoteMoved || k["hello.txt"] != worktree.Modified || k["new.txt"] != worktree.Added || k["sub/deep.txt"] != worktree.Deleted {
		t.Fatalf("status after fetch: %+v", st)
	}
	plr, err := ta.Pull(ctx, ca, false, 2, nil)
	if err != nil || len(plr.Conflicts) != 0 {
		t.Fatalf("pull: %v %+v", err, plr)
	}
	if readWC(t, a, "hello.txt") != "hello world\n" || readWC(t, a, "new.txt") != "n\n" {
		t.Fatal("pull content")
	}
	if _, err := os.Stat(filepath.Join(a, "sub", "deep.txt")); !os.IsNotExist(err) {
		t.Fatalf("sub/deep.txt should be gone: %v", err)
	}
	if st, _ = ta.Status(2); len(st.Changes) != 0 || st.Remote != worktree.RemoteUpToDate {
		t.Fatalf("status after pull: %+v", st)
	}
	if plr, err = ta.Pull(ctx, ca, false, 2, nil); err != nil || !plr.UpToDate {
		t.Fatalf("second pull: %v %+v", err, plr)
	}
}

// cloneTwo pushes a source tree under name and clones it twice.
func cloneTwo(t *testing.T, h *harness, name string) (ta, tb *worktree.Tree, ca, cb *client.Cluster) {
	t.Helper()
	ctx := context.Background()
	c0 := h.client(t, 102)
	defer c0.Close()
	src := t.TempDir()
	writeWC(t, src, "f.txt", "base\n")
	writeWC(t, src, "other.txt", "other\n")
	t0, _, err := worktree.Init(ctx, c0, src, worktree.Config{Name: name}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := t0.Push(ctx, c0, "tester", false, 2, nil); err != nil {
		t.Fatal(err)
	}
	t0.Close()
	ca, cb = h.client(t, 103), h.client(t, 104)
	if ta, _, err = worktree.Clone(ctx, ca, filepath.Join(t.TempDir(), "a"), worktree.Config{Name: name}, nil); err != nil {
		t.Fatal(err)
	}
	if tb, _, err = worktree.Clone(ctx, cb, filepath.Join(t.TempDir(), "b"), worktree.Config{Name: name}, nil); err != nil {
		t.Fatal(err)
	}
	return ta, tb, ca, cb
}

func TestWorktreeConflict(t *testing.T) {
	h := cluster3(t)
	defer h.close()
	ctx := context.Background()
	ta, tb, ca, cb := cloneTwo(t, h, "trees/conflict")
	defer ta.Close()
	defer tb.Close()
	defer ca.Close()
	defer cb.Close()

	writeWC(t, ta.Root, "f.txt", "A\n")
	writeWC(t, tb.Root, "f.txt", "B\n")
	writeWC(t, tb.Root, "b.txt", "b\n")
	if _, err := ta.Push(ctx, ca, "a", false, 2, nil); err != nil {
		t.Fatalf("push A: %v", err)
	}
	// B has not fetched: the cluster refuses its push.
	if _, err := tb.Push(ctx, cb, "b", false, 2, nil); !errors.Is(err, worktree.ErrRefChanged) {
		t.Fatalf("push B: err = %v, want ErrRefChanged", err)
	}
	plr, err := tb.Pull(ctx, cb, false, 2, nil)
	if !errors.Is(err, worktree.ErrConflict) || len(plr.Conflicts) != 1 || plr.Conflicts[0].Path != "f.txt" {
		t.Fatalf("pull B: %v %+v", err, plr)
	}
	if readWC(t, tb.Root, "f.txt") != "B\n" {
		t.Fatal("a refused pull must not touch the directory")
	}
	if plr, err = tb.Pull(ctx, cb, true, 2, nil); err != nil {
		t.Fatalf("forced pull: %v", err)
	}
	if readWC(t, tb.Root, "f.txt") != "A\n" || readWC(t, tb.Root, "b.txt") != "b\n" {
		t.Fatal("forced pull: remote side on the conflict, local addition kept")
	}
	if _, err := tb.Push(ctx, cb, "b", false, 2, nil); err != nil {
		t.Fatalf("push B after pull: %v", err)
	}
	if _, err := ta.Pull(ctx, ca, false, 2, nil); err != nil {
		t.Fatalf("pull A: %v", err)
	}
	if readWC(t, ta.Root, "f.txt") != "A\n" || readWC(t, ta.Root, "b.txt") != "b\n" {
		t.Fatal("A after pull")
	}
}

func TestWorktreePushRecoversAfterLostState(t *testing.T) {
	h := cluster3(t)
	defer h.close()
	ctx := context.Background()
	c := h.client(t, 105)
	defer c.Close()
	dir := t.TempDir()
	writeWC(t, dir, "f.txt", "1\n")
	tr, _, err := worktree.Init(ctx, c, dir, worktree.Config{Name: "trees/recover"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	if _, err := tr.Push(ctx, c, "tester", false, 2, nil); err != nil {
		t.Fatal(err)
	}
	writeWC(t, dir, "f.txt", "2\n")
	before := tr.State
	pr, err := tr.Push(ctx, c, "tester", false, 2, nil)
	if err != nil || pr.Recovered {
		t.Fatalf("push: %v %+v", err, pr)
	}
	// The reference write landed but the state write was lost.
	tr.State = before
	if err := tr.SaveState(); err != nil {
		t.Fatal(err)
	}
	pr, err = tr.Push(ctx, c, "tester", false, 2, nil)
	if err != nil || !pr.Recovered {
		t.Fatalf("retried push: %v %+v", err, pr)
	}
	if st, _ := tr.Status(2); len(st.Changes) != 0 || st.Remote != worktree.RemoteUpToDate {
		t.Fatalf("status after recovery: %+v", st)
	}
}
