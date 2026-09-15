package worktree

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"golang.org/x/sys/unix"
)

// applyFixtureA is the first version of a tree, with every entry type the
// applier handles except devices.
func applyFixtureA(t *testing.T) string {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "keep.txt"), "keep", 0o644)
	writeFile(t, filepath.Join(dir, "edit.txt"), "one", 0o644)
	writeFile(t, filepath.Join(dir, "gone.txt"), "bye", 0o644)
	writeFile(t, filepath.Join(dir, "run.sh"), "#!/bin/sh", 0o644)
	writeFile(t, filepath.Join(dir, "sub", "deep.txt"), "deep", 0o600)
	writeFile(t, filepath.Join(dir, "olddir", "x"), "x", 0o644)
	writeFile(t, filepath.Join(dir, "flip", "inner"), "inner", 0o644)
	writeFile(t, filepath.Join(dir, "flop"), "flop", 0o644)
	if err := os.Symlink("keep.txt", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(dir, "pipe"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Unix(1_600_000_000, 123456789)
	for _, p := range []string{"keep.txt", "edit.txt", "sub/deep.txt", "sub", "run.sh"} {
		if err := os.Chtimes(filepath.Join(dir, p), old, old); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// applyFixtureB is the second version: edits, deletions, additions, a
// chmod, a retargeted link, a directory that became a file and a file that
// became a directory, and a read-only directory.
func applyFixtureB(t *testing.T) string {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "keep.txt"), "keep", 0o644)
	writeFile(t, filepath.Join(dir, "edit.txt"), "two", 0o644)
	writeFile(t, filepath.Join(dir, "run.sh"), "#!/bin/sh", 0o755)
	writeFile(t, filepath.Join(dir, "sub", "deep.txt"), "deep", 0o600)
	writeFile(t, filepath.Join(dir, "newdir", "y"), "y", 0o644)
	writeFile(t, filepath.Join(dir, "flip"), "flat", 0o644)
	writeFile(t, filepath.Join(dir, "flop", "inner"), "inner", 0o644)
	writeFile(t, filepath.Join(dir, "ro", "child"), "c", 0o644)
	if err := os.Symlink("edit.txt", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(dir, "pipe"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Unix(1_600_000_000, 123456789)
	for _, p := range []string{"keep.txt", "sub/deep.txt", "sub"} {
		if err := os.Chtimes(filepath.Join(dir, p), old, old); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(filepath.Join(dir, "ro"), 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(dir, "ro"), 0o700) })
	return dir
}

func TestApply_CloneThenUpdateReproducesTrees(t *testing.T) {
	st := openStore(t)
	a := applyFixtureA(t)
	ka := ingestDir(t, st, a)
	empty, eb := EmptyTree()
	if err := st.Put(empty, eb); err != nil {
		t.Fatal(err)
	}

	work := t.TempDir()
	initial, err := DiffTrees(st.Get, empty, ka)
	if err != nil {
		t.Fatal(err)
	}
	if err := Apply(work, initial, st.Get); err != nil {
		t.Fatalf("initial apply: %v", err)
	}
	if got := ingestDir(t, st, work); got != ka {
		t.Fatalf("after clone, work ingests to %s, want %s", got, ka)
	}

	b := applyFixtureB(t)
	kb := ingestDir(t, st, b)
	update, err := DiffTrees(st.Get, ka, kb)
	if err != nil {
		t.Fatal(err)
	}
	if err := Apply(work, update, st.Get); err != nil {
		t.Fatalf("update apply: %v", err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(work, "ro"), 0o700) })
	if got := ingestDir(t, st, work); got != kb {
		t.Fatalf("after update, work ingests to %s, want %s", got, kb)
	}
	// Re-applying is a no-op.
	if err := Apply(work, update, st.Get); err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if got := ingestDir(t, st, work); got != kb {
		t.Fatalf("after re-apply, work ingests to %s, want %s", got, kb)
	}
	if fi, err := os.Lstat(filepath.Join(work, "ro")); err != nil || fi.Mode().Perm() != 0o500 {
		t.Fatalf("read-only dir: %v %v", fi, err)
	}
}

func TestApply_KeepsNonEmptyDirectoryOnDelete(t *testing.T) {
	st := openStore(t)
	a := applyFixtureA(t)
	ka := ingestDir(t, st, a)
	work := t.TempDir()
	empty, eb := EmptyTree()
	if err := st.Put(empty, eb); err != nil {
		t.Fatal(err)
	}
	initial, _ := DiffTrees(st.Get, empty, ka)
	if err := Apply(work, initial, st.Get); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(work, "olddir", "mine"), "mine", 0o644)
	if err := os.RemoveAll(filepath.Join(a, "olddir")); err != nil {
		t.Fatal(err)
	}
	kb := ingestDir(t, st, a)
	update, _ := DiffTrees(st.Get, ka, kb)
	if err := Apply(work, update, st.Get); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(work, "olddir", "mine")); err != nil {
		t.Fatalf("local file under a remotely deleted dir was lost: %v", err)
	}
	if _, err := os.Stat(filepath.Join(work, "olddir", "x")); !os.IsNotExist(err) {
		t.Fatalf("olddir/x should be gone: %v", err)
	}
}

func TestApply_RefusesUnsafePaths(t *testing.T) {
	work := t.TempDir()
	e := &fstree.Entry{Name: []byte("x"), Mode: unix.S_IFREG | 0o644}
	for _, p := range []string{"../x", "a/../x", "a//x", "./x", ""} {
		if err := Apply(work, []Change{{Path: p, Kind: Added, New: e}}, nil); err == nil {
			t.Errorf("path %q accepted", p)
		}
	}
}

func TestApply_RefusesSymlinkedAncestor(t *testing.T) {
	work := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(work, "lnk")); err != nil {
		t.Fatal(err)
	}
	blob, err := fstree.EncodeBlob([]byte("pwned"))
	if err != nil {
		t.Fatal(err)
	}
	get := func(k key.Key) ([]byte, error) { return blob.Bytes, nil }
	e := &fstree.Entry{Name: []byte("f"), Mode: unix.S_IFREG | 0o644, ContentKey: blob.Key[:]}
	err = Apply(work, []Change{{Path: "lnk/f", Kind: Added, New: e}}, get)
	if err == nil || !strings.Contains(err.Error(), "non-directory") {
		t.Fatalf("err = %v", err)
	}
	if leaked, _ := os.ReadDir(outside); len(leaked) != 0 {
		t.Fatalf("wrote through the link: %v", leaked)
	}
}
