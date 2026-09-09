package client

import "testing"

func keysN(n int) [][32]byte {
	out := make([][32]byte, n)
	for i := range out {
		out[i][0] = byte(i + 1)
	}
	return out
}

func TestBatchesBalancesBySizerNotByKeyLength(t *testing.T) {
	// The sizer decides: five keys of 10 bytes each at a 25-byte limit
	// give batches of 2, 2 and 1.
	got := batches(keysN(5), func([32]byte) int { return 10 }, 25, 100)
	if len(got) != 3 || len(got[0]) != 2 || len(got[1]) != 2 || len(got[2]) != 1 {
		t.Fatalf("got batch sizes %v", lens(got))
	}
}

func TestBatchesCapsKeysPerBatch(t *testing.T) {
	got := batches(keysN(7), func([32]byte) int { return 1 }, 1<<20, 3)
	if len(got) != 3 || len(got[0]) != 3 || len(got[1]) != 3 || len(got[2]) != 1 {
		t.Fatalf("got batch sizes %v", lens(got))
	}
}

func TestBatchesSendsAnOversizedRecordAlone(t *testing.T) {
	sizes := map[byte]int{1: 5, 2: 100, 3: 5}
	got := batches(keysN(3), func(k [32]byte) int { return sizes[k[0]] }, 20, 100)
	if len(got) != 3 {
		t.Fatalf("got batch sizes %v, want one key per batch", lens(got))
	}
}

func TestBatchesKeepsOrder(t *testing.T) {
	keys := keysN(4)
	got := batches(keys, func([32]byte) int { return 1 }, 2, 100)
	if got[0][0] != keys[0] || got[0][1] != keys[1] || got[1][0] != keys[2] || got[1][1] != keys[3] {
		t.Fatalf("order changed: %v", got)
	}
}

func lens(b [][][32]byte) []int {
	out := make([]int, len(b))
	for i, x := range b {
		out[i] = len(x)
	}
	return out
}
