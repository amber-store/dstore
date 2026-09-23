package node_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/amber-store/core/commit"
	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/reference"
	"github.com/amber-store/dstore/client"
)

// A node that upgraded from v0.1.10 may hold commits keyed by core v0.0.9's
// rule. The completeness walk can fetch one but not read it, and must refuse
// the reference: treated like an absent object, as it once was, the commit
// passed the has-and-pin negotiation with nothing below it checked, and a
// reference was written on a tree that no node holds.
func TestRefPutRefusesACommitOfTheOlderKeyRule(t *testing.T) {
	h := cluster3(t)
	defer h.close()
	ctx := context.Background()
	ca := h.client(t, 122)
	defer ca.Close()

	blob, err := fstree.EncodeBlob([]byte("never uploaded"))
	if err != nil {
		t.Fatal(err)
	}
	dir, err := fstree.EncodeDirLeaf([]fstree.Entry{{Name: []byte("f"), Mode: 0o100644, ContentKey: blob.Key[:]}})
	if err != nil {
		t.Fatal(err)
	}
	id := commit.Identity{Name: "alice", When: 1}
	good, raw, err := commit.Commit{Tree: dir.Key, Author: id, Committer: id, Message: "its tree is on no node"}.Object()
	if err != nil {
		t.Fatal(err)
	}
	old, err := key.New(key.Commit, uint64(len(raw)), raw)
	if err != nil {
		t.Fatal(err)
	}
	// Straight into the stores, as an upgrade finds them: put would refuse.
	for _, n := range h.nodes {
		for _, k := range []key.Key{good, old} {
			if err := n.Store().Put(k, raw); err != nil {
				t.Fatal(err)
			}
		}
	}
	put := func(name string, k key.Key) error {
		rec, err := reference.Reference{Name: name, Key: k[:], User: "alice", CreatedAt: time.Now().UnixNano()}.Encode()
		if err != nil {
			t.Fatal(err)
		}
		_, err = ca.RefPut(ctx, rec, client.Cond{Force: true})
		return err
	}
	// The control: the same bytes under their footprint key are read, and
	// the absent tree makes the reference incomplete.
	if err := put("trees/control", good); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("a commit whose tree is absent: %v, want incomplete", err)
	}
	err = put("trees/oldrule", old)
	if err == nil {
		t.Fatal("a reference was accepted on a commit keyed by the older rule")
	}
	if !strings.Contains(err.Error(), "malformed object under the reference") || !strings.Contains(err.Error(), "created again") {
		t.Fatalf("the refusal does not say why: %v", err)
	}
}
