package worktree

import (
	"errors"
	"testing"
	"time"
)

// One command at a time has a working copy open. Until core v0.0.10 the
// packstore's single-owner lock saw to that on the side; with a packstore
// that any number of processes may open, two commands would race on
// .dstore/state and on the working directory. Two opens in one process stand
// for two processes: flock is per open file description.
func TestWorkingCopyTakesOneCommandAtATime(t *testing.T) {
	dir := t.TempDir()
	made, err := Create(dir, Config{Name: "trees/x"})
	if err != nil {
		t.Fatal(err)
	}
	if other, err := Open(dir); !errors.Is(err, ErrInUse) {
		if other != nil {
			other.Close()
		}
		t.Fatalf("open during a clone or init: %v, want ErrInUse", err)
	}
	made.State.SyncedAt = time.Now()
	if err := made.SaveState(); err != nil {
		t.Fatal(err)
	}
	if err := made.Close(); err != nil {
		t.Fatal(err)
	}

	first, err := Open(dir)
	if err != nil {
		t.Fatalf("open after the first command ended: %v", err)
	}
	if second, err := Open(dir); !errors.Is(err, ErrInUse) {
		if second != nil {
			second.Close()
		}
		t.Fatalf("second open of one working copy: %v, want ErrInUse", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := Open(dir)
	if err != nil {
		t.Fatalf("open after close: %v", err)
	}
	again.Close()
}
