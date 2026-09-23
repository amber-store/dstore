package client

import (
	"fmt"

	"github.com/amber-store/core/commit"
	"github.com/amber-store/core/key"
)

// TreeOf returns the directory root that k stands for: a Commit's recorded
// tree, or k itself for any other key. A reference naming a commit is a
// branch; everything that reads files through a reference goes via this.
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
	return c.Tree, nil
}
