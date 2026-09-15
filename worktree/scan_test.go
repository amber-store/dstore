package worktree

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"golang.org/x/sys/unix"
)

// scanFixture writes a small tree, ingests it and returns the dir and base.
func scanFixture(t *testing.T, st *packstore.Store) (string, key.Key) {
	t.Helper()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "a.txt"), "alpha", 0o644)
	writeFile(t, filepath.Join(dir, "run.sh"), "#!/bin/sh", 0o644)
	writeFile(t, filepath.Join(dir, "sub", "b.txt"), "beta", 0o644)
	writeFile(t, filepath.Join(dir, "gone.txt"), "bye", 0o644)
	if err := os.Symlink("a.txt", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	return dir, ingestDir(t, st, dir)
}

func scan(t *testing.T, dir string, base key.Key, st *packstore.Store, syncedAt time.Time) map[string]Kind {
	t.Helper()
	changes, err := Scan(dir, base, st.Get, syncedAt, 2)
	if err != nil {
		t.Fatal(err)
	}
	return kinds(changes)
}

func TestScan_CleanTreeHasNoChanges(t *testing.T) {
	st := openStore(t)
	dir, base := scanFixture(t, st)
	if got := scan(t, dir, base, st, time.Now()); len(got) != 0 {
		t.Fatalf("changes on a clean tree: %v", got)
	}
}

func TestScan_EveryKind(t *testing.T) {
	st := openStore(t)
	dir, base := scanFixture(t, st)
	writeFile(t, filepath.Join(dir, "a.txt"), "alpha 2", 0o644)           // modified
	if err := os.Chmod(filepath.Join(dir, "run.sh"), 0o755); err != nil { // mode
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "gone.txt")); err != nil { // deleted
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "new.txt"), "new", 0o644)       // new
	writeFile(t, filepath.Join(dir, "newdir", "c.txt"), "c", 0o644) // new dir + file
	past := time.Unix(1_600_000_000, 0)
	if err := os.Chtimes(filepath.Join(dir, "sub", "b.txt"), past, past); err != nil { // meta
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("run.sh", filepath.Join(dir, "link")); err != nil { // retargeted
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, Dir), 0o755); err != nil { // metadata dir: invisible
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, Dir, "junk"), "x", 0o644)

	got := scan(t, dir, base, st, time.Now())
	want := map[string]Kind{
		"a.txt": Modified, "run.sh": ModeChanged, "gone.txt": Deleted, "new.txt": Added,
		"newdir": Added, "newdir/c.txt": Added, "sub/b.txt": MetaChanged, "link": Modified,
	}
	for p, k := range want {
		if got[p] != k {
			t.Errorf("%s: %v, want %v", p, got[p], k)
		}
	}
	for p := range got {
		if _, ok := want[p]; !ok && p != "sub" {
			t.Errorf("unexpected %s: %v", p, got[p])
		}
	}
}

func TestScan_IgnoredBasePathIsDeleted(t *testing.T) {
	st := openStore(t)
	dir, base := scanFixture(t, st)
	writeFile(t, filepath.Join(dir, ".amberignore"), "gone.txt\n", 0o644)
	got := scan(t, dir, base, st, time.Now())
	if got["gone.txt"] != Deleted || got[".amberignore"] != Added {
		t.Fatalf("got %v", got)
	}
}

func TestScan_RacyMtime(t *testing.T) {
	st := openStore(t)
	dir, base := scanFixture(t, st)
	path := filepath.Join(dir, "a.txt")
	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	// Same size, same mtime, different bytes.
	writeFile(t, path, "ALPHA", 0o644)
	if err := os.Chtimes(path, fi.ModTime(), fi.ModTime()); err != nil {
		t.Fatal(err)
	}
	// A sync long after the edit trusts size+mtime: the edit is invisible.
	if got := scan(t, dir, base, st, time.Now().Add(time.Hour)); len(got) != 0 {
		t.Fatalf("far-future syncedAt: %v, want no changes (the stat heuristic)", got)
	}
	// A sync within the window hashes the file.
	if got := scan(t, dir, base, st, time.Now()); got["a.txt"] != Modified {
		t.Fatalf("recent syncedAt: %v, want a.txt modified", got)
	}
}

func TestScan_TypeChangeExpands(t *testing.T) {
	st := openStore(t)
	dir, base := scanFixture(t, st)
	if err := os.Remove(filepath.Join(dir, "a.txt")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "a.txt", "inner"), "i", 0o644) // file → dir
	if err := os.RemoveAll(filepath.Join(dir, "sub")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "sub"), "flat", 0o644) // dir → file
	got := scan(t, dir, base, st, time.Now())
	want := map[string]Kind{"a.txt": TypeChanged, "a.txt/inner": Added, "sub": TypeChanged, "sub/b.txt": Deleted}
	for p, k := range want {
		if got[p] != k {
			t.Errorf("%s: %v, want %v", p, got[p], k)
		}
	}
}

func TestScan_Xattr(t *testing.T) {
	st := openStore(t)
	dir, base := scanFixture(t, st)
	path := filepath.Join(dir, "a.txt")
	if err := unix.Setxattr(path, "user.wc", []byte("1"), 0); err != nil {
		if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) {
			t.Skip("no xattr support here")
		}
		t.Fatal(err)
	}
	if got := scan(t, dir, base, st, time.Now()); got["a.txt"] != MetaChanged {
		t.Fatalf("got %v, want a.txt meta", got)
	}
}
