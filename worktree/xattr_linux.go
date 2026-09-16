//go:build linux

package worktree

import "golang.org/x/sys/unix"

// readXattrs reads a non-symlink entry's xattrs the way ingest does on Linux.
func readXattrs(path string) (map[string][]byte, error) {
	return readXattrsWith(path, unix.Llistxattr, unix.Lgetxattr)
}

// setXattr sets one xattr on a non-symlink entry.
func setXattr(path, name string, value []byte) error {
	return unix.Lsetxattr(path, name, value, 0)
}
