// Package meta is a node's own bookkeeping database (architecture/dstore.md
// §6.1): the adopted view, the proposer counter, pack stamps, pins, the GC
// barrier history and transition progress, in a synced Pebble DB.
package meta

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/amber-store/dstore/codec"
	"github.com/cockroachdb/pebble/v2"
)

// ErrNotFound reports an absent key.
var ErrNotFound = errors.New("meta: not found")

type discardLogger struct{}

func (discardLogger) Infof(string, ...any)  {}
func (discardLogger) Errorf(string, ...any) {}
func (discardLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

// DB is the bookkeeping database.
type DB struct {
	db *pebble.DB
	wo *pebble.WriteOptions
	mu sync.Mutex // serialises read-check-write sequences that need it
}

// Open opens (creating if needed) the database at dir.
func Open(dir string) (*DB, error) {
	db, err := pebble.Open(dir, &pebble.Options{Logger: discardLogger{}})
	if err != nil {
		return nil, fmt.Errorf("meta: open %s: %w", dir, err)
	}
	return &DB{db: db, wo: pebble.Sync}, nil
}

// Close closes the database.
func (d *DB) Close() error { return d.db.Close() }

// Get returns the value of key.
func (d *DB) Get(key []byte) ([]byte, error) {
	b, closer, err := d.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	out := append([]byte{}, b...)
	closer.Close()
	return out, nil
}

// Has reports whether key exists.
func (d *DB) Has(key []byte) bool {
	_, err := d.Get(key)
	return err == nil
}

// Set writes key synchronously.
func (d *DB) Set(key, value []byte) error { return d.db.Set(key, value, d.wo) }

// SetNoSync writes key without an fsync.
func (d *DB) SetNoSync(key, value []byte) error { return d.db.Set(key, value, pebble.NoSync) }

// Delete removes key.
func (d *DB) Delete(key []byte) error { return d.db.Delete(key, d.wo) }

// Batch is a set of writes committed together.
type Batch struct {
	b *pebble.Batch
	d *DB
}

// NewBatch starts a batch.
func (d *DB) NewBatch() *Batch { return &Batch{b: d.db.NewBatch(), d: d} }

// Set adds a write.
func (b *Batch) Set(key, value []byte) { b.b.Set(key, value, nil) }

// Delete adds a delete.
func (b *Batch) Delete(key []byte) { b.b.Delete(key, nil) }

// Commit writes the batch synchronously.
func (b *Batch) Commit() error { return b.b.Commit(b.d.wo) }

// Len returns the number of writes.
func (b *Batch) Len() int { return int(b.b.Count()) }

// GetCBOR decodes the CBOR value of key into v.
func (d *DB) GetCBOR(key []byte, v any) error {
	b, err := d.Get(key)
	if err != nil {
		return err
	}
	return codec.Unmarshal(b, v)
}

// SetCBOR writes v as CBOR under key.
func (d *DB) SetCBOR(key []byte, v any) error {
	b, err := codec.Marshal(v)
	if err != nil {
		return err
	}
	return d.Set(key, b)
}

// GetU64 reads a big-endian counter; 0 when absent.
func (d *DB) GetU64(key []byte) uint64 {
	b, err := d.Get(key)
	if err != nil || len(b) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// SetU64 writes a big-endian counter.
func (d *DB) SetU64(key []byte, v uint64) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return d.Set(key, b[:])
}

// Increment bumps a counter durably and returns the new value.
func (d *DB) Increment(key []byte) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	v := d.GetU64(key) + 1
	if err := d.SetU64(key, v); err != nil {
		return 0, err
	}
	return v, nil
}

// Raise sets a counter to at least v.
func (d *DB) Raise(key []byte, v uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.GetU64(key) >= v {
		return nil
	}
	return d.SetU64(key, v)
}

// Scan calls fn for every key with the prefix, in order, until fn returns
// false.
func (d *DB) Scan(prefix []byte, fn func(key, value []byte) bool) error {
	upper := append(append([]byte{}, prefix...), 0xff, 0xff, 0xff, 0xff)
	it, err := d.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upper})
	if err != nil {
		return err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		if !fn(it.Key(), it.Value()) {
			break
		}
	}
	return nil
}

// Count counts the keys with the prefix.
func (d *DB) Count(prefix []byte) int {
	n := 0
	_ = d.Scan(prefix, func([]byte, []byte) bool { n++; return true })
	return n
}

// DeletePrefix removes every key with the prefix.
func (d *DB) DeletePrefix(prefix []byte) error {
	b := d.NewBatch()
	_ = d.Scan(prefix, func(k, _ []byte) bool {
		b.Delete(append([]byte{}, k...))
		return true
	})
	if b.Len() == 0 {
		return nil
	}
	return b.Commit()
}

// Lock serialises a read-check-write sequence.
func (d *DB) Lock() func() {
	d.mu.Lock()
	return d.mu.Unlock
}
