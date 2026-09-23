package node

import (
	"strings"
	"testing"

	"github.com/amber-store/core/amberpack"
	"github.com/amber-store/core/commit"
	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
)

func rawRecord(t *testing.T, k key.Key, data []byte) amberpack.RawRecord {
	t.Helper()
	b, err := amberpack.EncodeRecord(k, data)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := amberpack.ParseRecord(b)
	if err != nil {
		t.Fatal(err)
	}
	return amberpack.RawRecord{Record: rec, Bytes: b}
}

// A Commit's length field is its footprint — its own serialized length plus
// the length field of every tree it records (core v0.0.10) — and a node
// holds a key to that rule before it stores or forwards the record.
func TestVerifyRecordCommitFootprint(t *testing.T) {
	dir, err := fstree.EncodeDirLeaf(nil)
	if err != nil {
		t.Fatal(err)
	}
	id := commit.Identity{Name: "tester", When: 1}
	plain := commit.Commit{Tree: dir.Key, Author: id, Committer: id, Message: "m"}
	conflicted := plain
	conflicted.ConflictTerms = []key.Key{dir.Key, dir.Key}
	for name, c := range map[string]commit.Commit{"plain": plain, "conflicted": conflicted} {
		k, data, err := c.Object()
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := verifyRecord(rawRecord(t, k, data)); err != nil {
			t.Fatalf("%s: valid commit refused: %v", name, err)
		}
		if k.Length() == uint64(len(data)) {
			t.Fatalf("%s: the test needs a footprint that differs from the commit's own length", name)
		}
		// core v0.0.9's rule, the commit's own length; and one off the footprint.
		for _, length := range []uint64{uint64(len(data)), k.Length() + 1, k.Length() - 1} {
			bad, err := key.New(key.Commit, length, data)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := verifyRecord(rawRecord(t, bad, data)); err == nil || !strings.Contains(err.Error(), "footprint") {
				t.Fatalf("%s: commit with length field %d (footprint %d): %v", name, length, k.Length(), err)
			}
		}
	}
	// Bytes that are no commit cannot be held to the rule: refused, whatever
	// their length field says.
	junk := []byte("not a commit")
	jk, err := key.New(key.Commit, uint64(len(junk)), junk)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := verifyRecord(rawRecord(t, jk, junk)); err == nil {
		t.Fatal("bytes that do not decode accepted under a Commit key")
	}
}
