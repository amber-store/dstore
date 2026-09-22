package client

import (
	"errors"
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
	delete(objs, ck)
	if _, err := TreeOf(ck, get); err == nil {
		t.Fatal("an absent commit must fail")
	}
}
