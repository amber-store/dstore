package client

import (
	"fmt"

	"github.com/amber-store/core/commit"
	"github.com/amber-store/core/key"
)

// TreeOf returns the directory root that k stands for: a Commit's recorded
// tree, or k itself for any other key. A reference naming a commit is a
// branch; everything that reads files through a reference goes via this.
//
// The commit is held to core's key rule, as core's own readers hold it: its
// length field is its footprint, its own bytes plus its trees. One keyed by
// core v0.0.9's rule is refused here, before a working copy builds history
// on a parent that no reference could ever name.
func TreeOf(k key.Key, get func(key.Key) ([]byte, error)) (key.Key, error) {
	if k.Type() != key.Commit {
		return k, nil
	}
	data, err := get(k)
	if err != nil {
		return key.Key{}, fmt.Errorf("reading commit %s: %w", k, err)
	}
	c, err := commit.Decode(data)
	if err != nil {
		return key.Key{}, fmt.Errorf("commit %s: %w", k, err)
	}
	want, err := commit.Footprint(uint64(len(data)), c.Trees())
	if err != nil {
		return key.Key{}, fmt.Errorf("commit %s: %w", k, err)
	}
	if k.Length() != want {
		return key.Key{}, fmt.Errorf("commit %s: length field %d is not the commit's footprint %d (its own %d bytes plus its trees); a commit keyed by an older rule has to be created again", k, k.Length(), want, len(data))
	}
	return c.Tree, nil
}
