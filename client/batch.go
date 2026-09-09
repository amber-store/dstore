package client

// RecordSizer reports the wire size of a key's record: what a batch
// carries and what progress counts. A blob's key length is close, but a
// tree object's key carries the size of its whole subtree, so the local
// packstore's index is the source (packstore.StoredSize).
type RecordSizer func(k [32]byte) int

// batches splits keys, in order, into batches of at most maxBytes by the
// sizer and at most maxKeys keys. A record larger than maxBytes is a
// batch of its own.
func batches(keys [][32]byte, size RecordSizer, maxBytes, maxKeys int) [][][32]byte {
	var out [][][32]byte
	var cur [][32]byte
	bytes := 0
	for _, k := range keys {
		n := size(k)
		if len(cur) > 0 && (bytes+n > maxBytes || len(cur) >= maxKeys) {
			out = append(out, cur)
			cur, bytes = nil, 0
		}
		cur = append(cur, k)
		bytes += n
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out
}
