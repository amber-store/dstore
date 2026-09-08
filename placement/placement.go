// Package placement implements weighted rendezvous hashing over hash slots,
// as specified in architecture/dstore.md §4. Every computation is
// integer-only so that the ranking is bit-exact across CPUs, compilers and
// implementations; the golden vectors in placement_test.go pin it down.
package placement

import (
	"encoding/binary"
	"math/bits"
	"sort"

	"github.com/zeebo/blake3"
)

// SlotBits is the width of a placement slot: keys are placed per slot, and
// a slot is the top SlotBits bits of the key's hash tail.
const SlotBits = 20

// Slots is the number of placement slots.
const Slots = 1 << SlotBits

// saltDomain is the BLAKE3 domain prefix of the per-node salt.
const saltDomain = "amber-dstore/placement/1"

// NodeID is a 32-byte endpoint identity, as the view carries it.
type NodeID [32]byte

// Member is one node of a placement set, as the ranking sees it.
type Member struct {
	ID     NodeID
	Weight uint32 // capacity in GiB; 0 owns nothing
	Zone   string // failure domain; empty means the id itself
}

// Slot returns the placement slot of a 32-byte key: the top 20 bits of the
// big-endian u64 at key[24:32].
func Slot(key [32]byte) uint32 {
	return uint32(binary.BigEndian.Uint64(key[24:32]) >> (64 - SlotBits))
}

// Salt is the per-node placement salt: the first 8 bytes of
// BLAKE3("amber-dstore/placement/1" ‖ id), big-endian.
func Salt(id NodeID) uint64 {
	h := blake3.New()
	h.Write([]byte(saltDomain))
	h.Write(id[:])
	var out [8]byte
	h.Digest().Read(out[:])
	return binary.BigEndian.Uint64(out[:])
}

// Fmix64 is the murmur3 64-bit finalizer.
func Fmix64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}

// Log2Fix returns ⌊log2(x)·2^32⌋ for x in [1, 2^64) by the bit-by-bit
// squaring algorithm. It panics on x == 0.
func Log2Fix(x uint64) uint64 {
	if x == 0 {
		panic("placement: Log2Fix(0)")
	}
	i := uint64(bits.Len64(x) - 1)
	m := x << (63 - i)
	var f uint64
	for j := 1; j <= 32; j++ {
		hi, lo := bits.Mul64(m, m)
		if hi&(1<<63) != 0 {
			m = hi
			f |= 1 << (32 - j)
		} else {
			m = hi<<1 | lo>>63
		}
	}
	return i<<32 | f
}

// L is the −log2(u) term of a node's score for a slot, in Q7.32:
// 64·2^32 − log2fix(h+1) with h = fmix64(slot ⊕ salt). h = 2^64−1 gives 0.
func L(slot uint32, salt uint64) uint64 {
	h := Fmix64(uint64(slot) ^ salt)
	if h == ^uint64(0) {
		return 0
	}
	return 64<<32 - Log2Fix(h+1)
}

// Ranked is one entry of a slot's ranking.
type Ranked struct {
	Index int    // position in the input member list
	L     uint64 // the node's L for the slot
}

// less reports whether member a ranks above member b for the slot, given
// their weights and L values: a.weight·L_b > b.weight·L_a; ties by smaller id.
func less(a, b Member, la, lb uint64) bool {
	ahi, alo := bits.Mul64(uint64(a.Weight), lb)
	bhi, blo := bits.Mul64(uint64(b.Weight), la)
	if ahi != bhi {
		return ahi > bhi
	}
	if alo != blo {
		return alo > blo
	}
	return compareID(a.ID, b.ID) < 0
}

func compareID(a, b NodeID) int {
	for i := range a {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

// Set is a placement set prepared for ranking: the members with their salts.
type Set struct {
	Members []Member
	salts   []uint64
}

// NewSet prepares members for ranking. Members with weight 0 are kept (they
// are members) but never rank; Rank returns them after the weighted ones in
// id order, as §4's read order requires.
func NewSet(members []Member) *Set {
	s := &Set{Members: members, salts: make([]uint64, len(members))}
	for i, m := range members {
		s.salts[i] = Salt(m.ID)
	}
	return s
}

// Rank returns the full ranking of the set for a slot: weighted members by
// descending score, then weight-0 members in id order. The result indexes
// into s.Members.
func (s *Set) Rank(slot uint32) []int {
	n := len(s.Members)
	ls := make([]uint64, n)
	weighted := make([]int, 0, n)
	zero := make([]int, 0)
	for i, m := range s.Members {
		if m.Weight == 0 {
			zero = append(zero, i)
			continue
		}
		ls[i] = L(slot, s.salts[i])
		weighted = append(weighted, i)
	}
	sort.SliceStable(weighted, func(a, b int) bool {
		ia, ib := weighted[a], weighted[b]
		return less(s.Members[ia], s.Members[ib], ls[ia], ls[ib])
	})
	sort.Slice(zero, func(a, b int) bool {
		return compareID(s.Members[zero[a]].ID, s.Members[zero[b]].ID) < 0
	})
	return append(weighted, zero...)
}

// Owners returns the first r members of the slot's ranking whose zone has
// not already been taken by an earlier pick, as indexes into s.Members.
// Weight-0 members never own anything.
func (s *Set) Owners(slot uint32, r int) []int {
	rank := s.Rank(slot)
	out := make([]int, 0, r)
	zones := make(map[string]struct{}, r)
	for _, i := range rank {
		if len(out) >= r {
			break
		}
		m := s.Members[i]
		if m.Weight == 0 {
			break
		}
		z := m.Zone
		if z == "" {
			z = string(m.ID[:])
		}
		if _, taken := zones[z]; taken {
			continue
		}
		zones[z] = struct{}{}
		out = append(out, i)
	}
	return out
}

// Table caches per-slot owners for a set and replica count: placement for
// a key becomes a table lookup, computed lazily on first use.
type Table struct {
	set    *Set
	r      int
	owners [][]int
	rank   [][]int
}

// NewTable returns a lazily filled owner table over set with r replicas.
func NewTable(set *Set, r int) *Table {
	return &Table{set: set, r: r, owners: make([][]int, Slots), rank: make([][]int, Slots)}
}

// Set returns the table's placement set.
func (t *Table) Set() *Set { return t.set }

// Replicas returns the table's replica count.
func (t *Table) Replicas() int { return t.r }

// Owners returns the owners of slot as indexes into the set's members.
// Concurrent callers may compute the same slot twice; the result is the
// same and the write is a pointer store, so no lock is taken.
func (t *Table) Owners(slot uint32) []int {
	if o := t.owners[slot]; o != nil {
		return o
	}
	o := t.set.Owners(slot, t.r)
	if o == nil {
		o = []int{}
	}
	t.owners[slot] = o
	return o
}

// Rank returns the full ranking of slot, cached.
func (t *Table) Rank(slot uint32) []int {
	if r := t.rank[slot]; r != nil {
		return r
	}
	r := t.set.Rank(slot)
	if r == nil {
		r = []int{}
	}
	t.rank[slot] = r
	return r
}

// OwnerIDs returns the owner ids of a key.
func (t *Table) OwnerIDs(key [32]byte) []NodeID {
	idx := t.Owners(Slot(key))
	out := make([]NodeID, len(idx))
	for i, j := range idx {
		out[i] = t.set.Members[j].ID
	}
	return out
}

// RankIDs returns the full read order of a key.
func (t *Table) RankIDs(key [32]byte) []NodeID {
	idx := t.Rank(Slot(key))
	out := make([]NodeID, len(idx))
	for i, j := range idx {
		out[i] = t.set.Members[j].ID
	}
	return out
}

// IsOwner reports whether id owns key.
func (t *Table) IsOwner(key [32]byte, id NodeID) bool {
	for _, j := range t.Owners(Slot(key)) {
		if t.set.Members[j].ID == id {
			return true
		}
	}
	return false
}
