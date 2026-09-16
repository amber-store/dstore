package worktree

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"golang.org/x/sys/unix"
)

func openStore(t *testing.T) *packstore.Store {
	t.Helper()
	st, err := packstore.Open(filepath.Join(t.TempDir(), "ps"), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func writeFile(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func ingestDir(t *testing.T, st *packstore.Store, dir string) key.Key {
	t.Helper()
	root, _, err := ingest.Dir(st, dir, ingest.Opts{Jobs: 2})
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func kinds(changes []Change) map[string]Kind {
	m := map[string]Kind{}
	for _, c := range changes {
		m[c.Path] = c.Kind
	}
	return m
}

func TestDiffTrees_EveryKind(t *testing.T) {
	st := openStore(t)
	a := t.TempDir()
	writeFile(t, filepath.Join(a, "same.txt"), "same", 0o644)
	writeFile(t, filepath.Join(a, "edit.txt"), "one", 0o644)
	writeFile(t, filepath.Join(a, "gone.txt"), "bye", 0o644)
	writeFile(t, filepath.Join(a, "mode.sh"), "#!/bin/sh", 0o644)
	writeFile(t, filepath.Join(a, "sub", "deep.txt"), "deep", 0o644)
	writeFile(t, filepath.Join(a, "olddir", "x"), "x", 0o644)
	if err := os.Symlink("t1", filepath.Join(a, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("was-link", filepath.Join(a, "becomes-file")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(a, "flip", "inner"), "inner", 0o644) // a dir that becomes a file
	ka := ingestDir(t, st, a)

	b := t.TempDir()
	writeFile(t, filepath.Join(b, "same.txt"), "same", 0o644)
	writeFile(t, filepath.Join(b, "edit.txt"), "two", 0o644)
	writeFile(t, filepath.Join(b, "new.txt"), "hi", 0o644)
	writeFile(t, filepath.Join(b, "mode.sh"), "#!/bin/sh", 0o755)
	writeFile(t, filepath.Join(b, "sub", "deep.txt"), "deep", 0o644)
	writeFile(t, filepath.Join(b, "newdir", "y"), "y", 0o644)
	writeFile(t, filepath.Join(b, "becomes-file"), "now a file", 0o644)
	writeFile(t, filepath.Join(b, "flip"), "flat", 0o644)
	if err := os.Symlink("t2", filepath.Join(b, "link")); err != nil {
		t.Fatal(err)
	}
	kb := ingestDir(t, st, b)

	changes, err := DiffTrees(st.Get, ka, kb)
	if err != nil {
		t.Fatal(err)
	}
	got := kinds(changes)
	want := map[string]Kind{
		"edit.txt": Modified, "gone.txt": Deleted, "new.txt": Added, "mode.sh": ModeChanged,
		"link": Modified, "becomes-file": TypeChanged,
		"olddir": Deleted, "olddir/x": Deleted, "newdir": Added, "newdir/y": Added,
		"flip": TypeChanged, "flip/inner": Deleted,
	}
	for p, k := range want {
		if got[p] != k {
			t.Errorf("%s: kind %v, want %v", p, got[p], k)
		}
	}
	for p := range got {
		if _, ok := want[p]; !ok && p != "same.txt" && p != "sub" && p != "sub/deep.txt" {
			t.Errorf("unexpected change %s: %v", p, got[p])
		}
		// same.txt, sub and sub/deep.txt may only appear as metadata-only
		// (mtimes differ between the two fixtures), never as content changes.
		if (p == "same.txt" || p == "sub" || p == "sub/deep.txt") && got[p] != MetaChanged {
			t.Errorf("%s: kind %v, want at most MetaChanged", p, got[p])
		}
	}
	// Order is bytewise by path, an added directory before its children.
	for i := 1; i < len(changes); i++ {
		if changes[i-1].Path >= changes[i].Path {
			t.Errorf("out of order: %s then %s", changes[i-1].Path, changes[i].Path)
		}
	}
}

func TestDiffTrees_PrunesEqualSubtrees(t *testing.T) {
	st := openStore(t)
	a := t.TempDir()
	writeFile(t, filepath.Join(a, "sub", "x"), "x", 0o644)
	writeFile(t, filepath.Join(a, "top"), "1", 0o644)
	ka := ingestDir(t, st, a)
	writeFile(t, filepath.Join(a, "top"), "2", 0o644)
	kb := ingestDir(t, st, a)
	// A pruned subtree is never fetched: only the two roots are read.
	calls := 0
	get := func(k key.Key) ([]byte, error) {
		calls++
		return st.Get(k)
	}
	changes, err := DiffTrees(get, ka, kb)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].Path != "top" || changes[0].Kind != Modified {
		t.Fatalf("changes = %+v", changes)
	}
	if calls != 2 {
		t.Fatalf("get called %d times, want 2 (the two roots only)", calls)
	}
}

func TestCompare(t *testing.T) {
	file := func(ck byte, mode uint64, mtime int64) *fstree.Entry {
		k := make([]byte, 32)
		k[1] = ck
		return &fstree.Entry{Name: []byte("f"), Mode: unix.S_IFREG | mode, Mtime: mtime, ContentKey: k}
	}
	cases := []struct {
		name   string
		a, b   *fstree.Entry
		kind   Kind
		differ bool
	}{
		{"equal", file(1, 0o644, 1), file(1, 0o644, 1), 0, false},
		{"content", file(1, 0o644, 1), file(2, 0o644, 1), Modified, true},
		{"mode", file(1, 0o644, 1), file(1, 0o755, 1), ModeChanged, true},
		{"mtime", file(1, 0o644, 1), file(1, 0o644, 2), MetaChanged, true},
		{"type", file(1, 0o644, 1), &fstree.Entry{Name: []byte("f"), Mode: unix.S_IFLNK | 0o777, LinkTarget: []byte("x")}, TypeChanged, true},
	}
	for _, c := range cases {
		k, ok := Compare(c.a, c.b)
		if ok != c.differ || (ok && k != c.kind) {
			t.Errorf("%s: (%v, %v), want (%v, %v)", c.name, k, ok, c.kind, c.differ)
		}
	}
}
