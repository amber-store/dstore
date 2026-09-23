package client

import (
	"errors"
	"strings"
	"testing"

	"github.com/amber-store/core/commit"
	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
)

func TestTreeOf(t *testing.T) {
	dir, err := fstree.EncodeDirLeaf(nil)
	if err != nil {
		t.Fatal(err)
	}
	id := commit.Identity{Name: "tester", When: 1}
	ck, raw, err := commit.Commit{Tree: dir.Key, Author: id, Committer: id}.Object()
	if err != nil {
		t.Fatal(err)
	}
	objs := map[key.Key][]byte{ck: raw}
	get := func(k key.Key) ([]byte, error) {
		if b, ok := objs[k]; ok {
			return b, nil
		}
		return nil, errors.New("absent")
	}
	if got, err := TreeOf(dir.Key, get); err != nil || got != dir.Key {
		t.Fatalf("a tree stands for itself: %s %v", got, err)
	}
	if got, err := TreeOf(ck, get); err != nil || got != dir.Key {
		t.Fatalf("a commit stands for its tree: %s %v", got, err)
	}
	// A conflicted commit stands for its first side, and its terms count
	// towards the footprint.
	xk, xraw, err := commit.Commit{Tree: dir.Key, Author: id, Committer: id, ConflictTerms: []key.Key{dir.Key, dir.Key}}.Object()
	if err != nil {
		t.Fatal(err)
	}
	objs[xk] = xraw
	if got, err := TreeOf(xk, get); err != nil || got != dir.Key {
		t.Fatalf("a conflicted commit stands for its first side: %s %v", got, err)
	}
	// The same bytes under a key of core v0.0.9's rule, the commit's own
	// length: refused, and the message says what to do.
	old, err := key.New(key.Commit, uint64(len(raw)), raw)
	if err != nil {
		t.Fatal(err)
	}
	if old == ck {
		t.Fatal("the test needs a footprint that differs from the commit's own length")
	}
	objs[old] = raw
	if _, err := TreeOf(old, get); err == nil || !strings.Contains(err.Error(), "footprint") || !strings.Contains(err.Error(), "created again") {
		t.Fatalf("a commit keyed by the older rule: %v", err)
	}
	delete(objs, ck)
	if _, err := TreeOf(ck, get); err == nil {
		t.Fatal("an absent commit must fail")
	}
}
