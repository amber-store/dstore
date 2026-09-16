//go:build darwin

package worktree

import "golang.org/x/sys/unix"

// readXattrs reads a non-symlink entry's xattrs the way ingest does on macOS.
func readXattrs(path string) (map[string][]byte, error) {
	return readXattrsWith(path, unix.Listxattr, unix.Getxattr)
}

// setXattr sets one xattr on a non-symlink entry.
func setXattr(path, name string, value []byte) error {
	return unix.Setxattr(path, name, value, 0)
}
