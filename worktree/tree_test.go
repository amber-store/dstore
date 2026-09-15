package worktree

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCreateOpenRoundTrip(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Ticket: "dstore1abc", Name: "trees/demo", NoRelay: true, User: "me"}
	tr, err := Create(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	empty, _ := EmptyTree()
	if tr.State.Base != empty {
		t.Fatalf("base after Create = %s, want the empty tree %s", tr.State.Base, empty)
	}
	if _, err := tr.Get(empty); err != nil {
		t.Fatalf("empty tree not stored: %v", err)
	}
	// Until SaveState, the copy is incomplete.
	tr.Close()
	if _, err := Open(dir); !errors.Is(err, ErrIncomplete) {
		t.Fatalf("Open before SaveState: err = %v, want ErrIncomplete", err)
	}
	tr, err = Create(dir, cfg) // re-creating an existing .dstore must fail
	if err == nil {
		tr.Close()
		t.Fatal("Create over an existing .dstore succeeded")
	}
	// Finish the copy.
	tr, err = openRaw(dir)
	if err != nil {
		t.Fatal(err)
	}
	tr.State.HasRemote = true
	tr.State.Remote = empty
	tr.State.RemoteVersion = []byte{1, 2, 3}
	tr.State.SyncedAt = time.Unix(1_700_000_000, 5).UTC()
	if err := tr.SaveState(); err != nil {
		t.Fatal(err)
	}
	tr.Close()

	sub := filepath.Join(dir, "a", "b")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	got, err := Open(sub) // found from a subdirectory
	if err != nil {
		t.Fatal(err)
	}
	defer got.Close()
	if got.Root != dir {
		t.Fatalf("Root = %q, want %q", got.Root, dir)
	}
	if got.Config != cfg {
		t.Fatalf("Config = %+v, want %+v", got.Config, cfg)
	}
	if !got.State.HasRemote || got.State.Remote != empty || string(got.State.RemoteVersion) != "\x01\x02\x03" || !got.State.SyncedAt.Equal(time.Unix(1_700_000_000, 5)) {
		t.Fatalf("State = %+v", got.State)
	}
}

func TestFindOutsideWorkingCopy(t *testing.T) {
	if _, err := Find(t.TempDir()); !errors.Is(err, ErrNotWorkingCopy) {
		t.Fatalf("err = %v, want ErrNotWorkingCopy", err)
	}
}

func TestRemove(t *testing.T) {
	dir := t.TempDir()
	tr, err := Create(dir, Config{Name: "x"})
	if err != nil {
		t.Fatal(err)
	}
	tr.Close()
	if err := Remove(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, Dir)); !os.IsNotExist(err) {
		t.Fatalf(".dstore still there: %v", err)
	}
}
