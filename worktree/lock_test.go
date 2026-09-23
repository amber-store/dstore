package worktree

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
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

const lockHolderEnv = "DSTORE_TEST_LOCK_HOLDER"

// TestLockHolderProcess is the other process of the test below: with
// lockHolderEnv set it opens that working copy, says so, and holds it until
// its stdin closes. In a normal run it does nothing.
func TestLockHolderProcess(t *testing.T) {
	dir := os.Getenv(lockHolderEnv)
	if dir == "" {
		t.Skip("the helper of TestWorkingCopyLockHoldsAcrossProcesses")
	}
	tr, err := Open(dir)
	if err != nil {
		fmt.Println("HOLDER FAILED:", err)
		os.Exit(1)
	}
	fmt.Println("HOLDING")
	_, _ = io.Copy(io.Discard, os.Stdin)
	tr.Close()
}

// Two commands are two processes. A lock that only this process sees (a
// name of its own, say) would pass the test above and exclude nothing.
func TestWorkingCopyLockHoldsAcrossProcesses(t *testing.T) {
	dir := t.TempDir()
	made, err := Create(dir, Config{Name: "trees/x"})
	if err != nil {
		t.Fatal(err)
	}
	made.State.SyncedAt = time.Now()
	if err := made.SaveState(); err != nil {
		t.Fatal(err)
	}
	if err := made.Close(); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(os.Args[0], "-test.run", "^TestLockHolderProcess$", "-test.v")
	cmd.Env = append(os.Environ(), lockHolderEnv+"="+dir)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	holding := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if line := sc.Text(); strings.Contains(line, "HOLD") {
				holding <- line
				break
			}
		}
		close(holding)
		_, _ = io.Copy(io.Discard, stdout)
	}()
	select {
	case line, ok := <-holding:
		if !ok || line != "HOLDING" {
			t.Fatalf("the other process did not take the working copy: %q", line)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the other process never reported")
	}

	if tr, err := Open(dir); !errors.Is(err, ErrInUse) {
		if tr != nil {
			tr.Close()
		}
		t.Fatalf("open while another process holds the working copy: %v, want ErrInUse", err)
	}
	stdin.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatalf("the other process: %v", err)
	}
	tr, err := Open(dir)
	if err != nil {
		t.Fatalf("open after the other process ended: %v", err)
	}
	tr.Close()
}
