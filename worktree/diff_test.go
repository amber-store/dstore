package worktree

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
)

func diffFixtures(t *testing.T) (st *packstore.Store, a, b key.Key, bdir string) {
	st = openStore(t)
	adir := t.TempDir()
	writeFile(t, filepath.Join(adir, "edit.txt"), "one\ntwo\nthree\n", 0o644)
	writeFile(t, filepath.Join(adir, "bin"), "a\x00b", 0o644)
	writeFile(t, filepath.Join(adir, "gone.txt"), "bye\n", 0o644)
	writeFile(t, filepath.Join(adir, "run.sh"), "#!/bin/sh\n", 0o644)
	writeFile(t, filepath.Join(adir, "sub", "x"), "x\n", 0o644)
	if err := os.Symlink("t1", filepath.Join(adir, "link")); err != nil {
		t.Fatal(err)
	}
	a = ingestDir(t, st, adir)
	bdir = t.TempDir()
	writeFile(t, filepath.Join(bdir, "edit.txt"), "one\n2\nthree\n", 0o644)
	writeFile(t, filepath.Join(bdir, "bin"), "a\x00c", 0o644)
	writeFile(t, filepath.Join(bdir, "new.txt"), "hi\n", 0o644)
	writeFile(t, filepath.Join(bdir, "run.sh"), "#!/bin/sh\n", 0o755)
	writeFile(t, filepath.Join(bdir, "sub", "x"), "x\n", 0o644)
	if err := os.Chmod(filepath.Join(bdir, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("t2", filepath.Join(bdir, "link")); err != nil {
		t.Fatal(err)
	}
	b = ingestDir(t, st, bdir)
	return st, a, b, bdir
}

func TestUnified_TreeToTree(t *testing.T) {
	st, a, b, _ := diffFixtures(t)
	changes, err := DiffTrees(st.Get, a, b)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := Unified(&buf, changes, TreeSource{st.Get}, TreeSource{st.Get}); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{
		"diff a/edit.txt b/edit.txt\n--- a/edit.txt\n+++ b/edit.txt\n", "-two\n+2\n",
		"Binary files a/bin and b/bin differ\n",
		"--- /dev/null\n+++ b/new.txt\n", "+hi\n",
		"--- a/gone.txt\n+++ /dev/null\n", "-bye\n",
		"diff a/run.sh b/run.sh\nold mode 0644\nnew mode 0755\n",
		"diff a/link b/link\n", "-t1", "+t2",
		"diff a/sub b/sub\nold mode 0755\nnew mode 0700\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "--- a/sub\n") || strings.Contains(out, "--- a/run.sh\n") {
		t.Errorf("mode-only changes must have no body:\n%s", out)
	}
}

func TestUnified_TreeToDisk(t *testing.T) {
	st, a, _, bdir := diffFixtures(t)
	changes, err := Scan(bdir, a, st.Get, time.Now(), 2)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := Unified(&buf, changes, TreeSource{st.Get}, DiskSource{bdir}); err != nil {
		t.Fatal(err)
	}
	if out := buf.String(); !strings.Contains(out, "-two\n+2\n") || !strings.Contains(out, "+t2") {
		t.Errorf("disk side not read:\n%s", out)
	}
}

func TestStat(t *testing.T) {
	st, a, b, _ := diffFixtures(t)
	changes, _ := DiffTrees(st.Get, a, b)
	var buf bytes.Buffer
	if err := Stat(&buf, changes, TreeSource{st.Get}, TreeSource{st.Get}); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{" edit.txt | +1 -1\n", " bin | binary\n", " new.txt | +1 -0\n", " gone.txt | +0 -1\n", " link | +1 -1\n", "files changed"} {
		if !strings.Contains(out, want) {
			t.Errorf("stat lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "sub |") {
		t.Errorf("a directory mode change has no stat line:\n%s", out)
	}
}
