package worktree

import (
	"bytes"
	"errors"

	"golang.org/x/sys/unix"
)

// readXattrsWith lists xattrs with list and reads each with get, as ingest
// does. ENOTSUP from a filesystem without xattr support means none.
func readXattrsWith(path string, list func(string, []byte) (int, error), get func(string, string, []byte) (int, error)) (map[string][]byte, error) {
	sz, err := list(path, nil)
	if err != nil {
		return nil, ignoreUnsupported(err)
	}
	if sz == 0 {
		return nil, nil
	}
	buf := make([]byte, sz)
	sz, err = list(path, buf)
	if err != nil {
		return nil, ignoreUnsupported(err)
	}
	var names []string
	for _, n := range bytes.Split(buf[:sz], []byte{0}) {
		if len(n) > 0 {
			names = append(names, string(n))
		}
	}
	if len(names) == 0 {
		return nil, nil
	}
	m := make(map[string][]byte, len(names))
	for _, name := range names {
		sz, err := get(path, name, nil)
		if err != nil {
			return nil, err
		}
		val := make([]byte, sz)
		sz, err = get(path, name, val)
		if err != nil {
			return nil, err
		}
		m[name] = val[:sz]
	}
	return m, nil
}

func ignoreUnsupported(err error) error {
	if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) {
		return nil
	}
	return err
}
