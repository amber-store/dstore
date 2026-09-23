package node

import (
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

// A Commit's length field is its own serialized length, checked like a
// Blob's: a key with the right hash and a wrong length is refused.
func TestVerifyRecordCommitLength(t *testing.T) {
	dir, err := fstree.EncodeDirLeaf(nil)
	if err != nil {
		t.Fatal(err)
	}
	id := commit.Identity{Name: "tester", When: 1}
	k, data, err := commit.Commit{Tree: dir.Key, Author: id, Committer: id, Message: "m"}.Object()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := verifyRecord(rawRecord(t, k, data)); err != nil {
		t.Fatalf("valid commit refused: %v", err)
	}
	bad, err := key.New(key.Commit, uint64(len(data))+1, data)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := verifyRecord(rawRecord(t, bad, data)); err == nil {
		t.Fatal("commit with a wrong length field accepted")
	}
}
