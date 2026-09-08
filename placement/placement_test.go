package placement

import (
	"encoding/binary"
	"encoding/hex"
	"math"
	"math/rand"
	"testing"
)

func idFrom(b byte) NodeID {
	var id NodeID
	for i := range id {
		id[i] = b
	}
	return id
}

func TestLog2FixExact(t *testing.T) {
	// Exact powers of two have a zero fraction.
	for i := 0; i < 64; i++ {
		got := Log2Fix(1 << uint(i))
		if got != uint64(i)<<32 {
			t.Fatalf("Log2Fix(2^%d) = %#x", i, got)
		}
	}
	// Compare with float64 log2 for a spread of values; the fixed-point
	// result must be within one ulp of 2^-32 below the true value.
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 10000; i++ {
		x := rng.Uint64()
		if x == 0 {
			continue
		}
		want := math.Log2(float64(x)) * (1 << 32)
		got := float64(Log2Fix(x))
		if got > want+1 || got < want-4096 { // float64 has ~53 bits; allow its slack at 2^64
			t.Fatalf("Log2Fix(%d) = %v, float says %v", x, got, want)
		}
	}
}

func TestFmix64Vectors(t *testing.T) {
	// Reference values of the murmur3 finalizer.
	cases := map[uint64]uint64{
		0:                  0,
		1:                  0xb456bcfc34c2cb2c,
		0xdeadbeef:         0xd24bd59f862a1dac,
		0xffffffffffffffff: 0x64b5720b4b825f21,
	}
	for in, want := range cases {
		if got := Fmix64(in); got != want {
			t.Errorf("Fmix64(%#x) = %#x, want %#x", in, got, want)
		}
	}
}

// TestGoldenVectors pins salt, h, L and rankings for a fixed view. A port
// must reproduce these byte for byte.
func TestGoldenVectors(t *testing.T) {
	members := []Member{
		{ID: idFrom(0x01), Weight: 1000},
		{ID: idFrom(0x02), Weight: 2000},
		{ID: idFrom(0x03), Weight: 500},
		{ID: idFrom(0x04), Weight: 4000},
		{ID: idFrom(0x05), Weight: 1000},
	}
	set := NewSet(members)
	type vec struct {
		slot uint32
		rank []int
	}
	golden := []vec{}
	for _, slot := range []uint32{0, 1, 12345, 0xfffff, 524288, 777777} {
		golden = append(golden, vec{slot, set.Rank(slot)})
	}
	// Salt vectors.
	wantSalt := map[byte]string{
		0x01: hex.EncodeToString(u64be(Salt(idFrom(0x01)))),
	}
	_ = wantSalt
	// The frozen expectations below were produced by this implementation
	// and are the contract for every other one.
	frozen := map[uint32][]int{
		0:       {3, 4, 2, 1, 0},
		1:       {3, 0, 4, 1, 2},
		12345:   {1, 3, 4, 2, 0},
		0xfffff: {3, 1, 4, 0, 2},
		524288:  {3, 0, 1, 4, 2},
		777777:  {4, 1, 2, 3, 0},
	}
	for _, g := range golden {
		want, ok := frozen[g.slot]
		if !ok {
			t.Fatalf("slot %d not frozen: got %v", g.slot, g.rank)
		}
		if !equalInts(g.rank, want) {
			t.Errorf("slot %d: rank %v, frozen %v", g.slot, g.rank, want)
		}
	}
	saltGolden := map[byte]uint64{
		0x01: 0x91d3f42d715bdc8a,
	}
	for b, want := range saltGolden {
		if got := Salt(idFrom(b)); got != want {
			t.Errorf("Salt(%02x) = %#x, want %#x", b, got, want)
		}
	}
	if got := L(12345, Salt(idFrom(0x01))); got != 0x22356754f {
		t.Errorf("L(12345, salt1) = %#x", got)
	}
}

func u64be(x uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], x)
	return b[:]
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestWeightedDistribution(t *testing.T) {
	members := []Member{
		{ID: idFrom(1), Weight: 100},
		{ID: idFrom(2), Weight: 200},
		{ID: idFrom(3), Weight: 300},
		{ID: idFrom(4), Weight: 400},
	}
	set := NewSet(members)
	counts := make([]int, len(members))
	const n = 200000
	for slot := uint32(0); slot < n; slot++ {
		counts[set.Rank(slot)[0]]++
	}
	total := 0
	for _, w := range members {
		total += int(w.Weight)
	}
	for i, m := range members {
		want := float64(n) * float64(m.Weight) / float64(total)
		got := float64(counts[i])
		if math.Abs(got-want)/want > 0.03 {
			t.Errorf("member %d: %v wins, want ~%v", i, got, want)
		}
	}
}

func TestAddingNodeMovesOnlyToIt(t *testing.T) {
	base := []Member{
		{ID: idFrom(1), Weight: 100},
		{ID: idFrom(2), Weight: 100},
		{ID: idFrom(3), Weight: 100},
		{ID: idFrom(4), Weight: 100},
	}
	grown := append(append([]Member{}, base...), Member{ID: idFrom(5), Weight: 100})
	a := NewSet(base)
	b := NewSet(grown)
	moved, total := 0, 0
	for slot := uint32(0); slot < 50000; slot++ {
		oa := a.Owners(slot, 3)
		ob := b.Owners(slot, 3)
		total++
		// Every owner in the old set either stays an owner or was displaced by 5.
		for _, i := range oa {
			found := false
			for _, j := range ob {
				if base[i].ID == grown[j].ID {
					found = true
				}
			}
			if !found {
				moved++
				has5 := false
				for _, j := range ob {
					if grown[j].ID == idFrom(5) {
						has5 = true
					}
				}
				if !has5 {
					t.Fatalf("slot %d: owner moved but not to the new node", slot)
				}
			}
		}
	}
	if moved == 0 || moved > total {
		t.Fatalf("moved %d of %d", moved, total)
	}
}

func TestZoneRule(t *testing.T) {
	members := []Member{
		{ID: idFrom(1), Weight: 100, Zone: "a"},
		{ID: idFrom(2), Weight: 100, Zone: "a"},
		{ID: idFrom(3), Weight: 100, Zone: "b"},
		{ID: idFrom(4), Weight: 100, Zone: "b"},
		{ID: idFrom(5), Weight: 0, Zone: "c"},
	}
	set := NewSet(members)
	for slot := uint32(0); slot < 2000; slot++ {
		o := set.Owners(slot, 3)
		if len(o) != 2 {
			t.Fatalf("slot %d: %d owners with two zones", slot, len(o))
		}
		if members[o[0]].Zone == members[o[1]].Zone {
			t.Fatalf("slot %d: same zone twice", slot)
		}
		r := set.Rank(slot)
		if r[len(r)-1] != 4 {
			t.Fatalf("weight-0 member must rank last")
		}
	}
}

func TestSlot(t *testing.T) {
	var k [32]byte
	binary.BigEndian.PutUint64(k[24:], 0xffffffffffffffff)
	if Slot(k) != Slots-1 {
		t.Fatal("slot of all-ones tail")
	}
	binary.BigEndian.PutUint64(k[24:], 1<<44)
	if Slot(k) != 1 {
		t.Fatal("slot of 2^44")
	}
}
