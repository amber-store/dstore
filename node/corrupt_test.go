package node

import (
	"log/slog"
	"testing"

	"github.com/amber-store/core/amberpack"
	"github.com/amber-store/core/key"
	"github.com/amber-store/dstore/transport"
	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
)

// TestStoreBatchClearsCorruptMarkers: storing a fresh record drops its
// corrupt marker, if any, and leaves other markers alone.
func TestStoreBatchClearsCorruptMarkers(t *testing.T) {
	var id view.NodeID
	id[0] = 7
	ep := transport.NewNetwork().Bind(id, wire.ALPNClient, wire.ALPNCluster)
	n, err := Open(Config{StoreDir: t.TempDir(), Endpoint: ep, Logger: slog.New(slog.DiscardHandler), NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	data := []byte("a record that was once found corrupt")
	k, err := key.New(key.Blob, uint64(len(data)), data)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := amberpack.EncodeRecord(k, data)
	if err != nil {
		t.Fatal(err)
	}
	other := [32]byte{1, 2, 3}
	n.markCorrupt([32]byte(k))
	n.markCorrupt(other)
	if !n.isCorrupt([32]byte(k)) {
		t.Fatal("marker not set")
	}

	stored, dedup, err := n.storeBatch(&putBatch{keys: [][32]byte{[32]byte(k)}, records: map[[32]byte][]byte{[32]byte(k): rec}})
	if err != nil {
		t.Fatal(err)
	}
	if stored != 1 || dedup != 0 {
		t.Fatalf("stored %d dedup %d, want 1 0", stored, dedup)
	}
	if n.isCorrupt([32]byte(k)) {
		t.Error("marker survived the store")
	}
	if !n.isCorrupt(other) {
		t.Error("an unrelated marker was cleared")
	}
	if has, _ := n.store.Has(k); !has {
		t.Error("record not stored")
	}
	// A batch of unmarked keys must not touch the markers either.
	data2 := []byte("a record nobody marked")
	k2, _ := key.New(key.Blob, uint64(len(data2)), data2)
	rec2, _ := amberpack.EncodeRecord(k2, data2)
	if _, _, err := n.storeBatch(&putBatch{keys: [][32]byte{[32]byte(k2)}, records: map[[32]byte][]byte{[32]byte(k2): rec2}}); err != nil {
		t.Fatal(err)
	}
	if !n.isCorrupt(other) {
		t.Error("marker cleared by a batch of unmarked keys")
	}
}
