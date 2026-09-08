package node

import "golang.org/x/sys/unix"

// diskFree returns the free and total bytes of the filesystem holding path.
func diskFree(path string) (free, total int64, ok bool) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, 0, false
	}
	return int64(st.Bavail) * int64(st.Bsize), int64(st.Blocks) * int64(st.Bsize), true
}
