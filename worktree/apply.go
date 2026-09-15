package worktree

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/amber-store/core/cborx"
	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"golang.org/x/sys/unix"
)

// Apply writes changes to the working directory root. Deletions go first,
// deepest paths first, and remove a directory only when it is empty; then
// additions and modifications in path order, creating missing parents;
// directory permission bits and mtimes are applied last so that a read-only
// or past-dated directory neither blocks nor is disturbed by its children.
// Regular files are written to a temporary file beside the target and
// renamed into place. Ownership is restored only when running as root,
// xattrs best-effort. Re-applying a list is a no-op.
func Apply(root string, changes []Change, get Getter) error {
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	for _, c := range changes {
		if err := checkPath(c.Path); err != nil {
			return err
		}
	}
	var dels, rest []Change
	for _, c := range changes {
		if c.Kind == Deleted {
			dels = append(dels, c)
		} else {
			rest = append(rest, c)
		}
	}
	sort.Slice(dels, func(i, j int) bool { return dels[i].Path > dels[j].Path })
	sort.Slice(rest, func(i, j int) bool { return rest[i].Path < rest[j].Path })

	target := func(p string) (string, error) {
		t := filepath.Join(root, filepath.FromSlash(p))
		return t, rejectSymlinkComponents(root, t)
	}
	// Parent directories without owner write permission are opened for the
	// duration and restored before the deferred directory metadata sets the
	// final mode of changed directories, so a re-run into a read-only
	// directory works.
	restore := map[string]uint32{}
	writable := func(dir string) error {
		if _, done := restore[dir]; done {
			return nil
		}
		fi, err := os.Lstat(dir)
		if err != nil {
			return err
		}
		mode := uint32(fi.Sys().(*syscall.Stat_t).Mode) & 0o7777
		if !fi.IsDir() || mode&0o700 == 0o700 {
			return nil
		}
		restore[dir] = mode
		return unix.Chmod(dir, mode|0o700)
	}
	for _, c := range dels {
		t, err := target(c.Path)
		if errors.Is(err, errNotDir) {
			continue // an ancestor became a file: nothing below it can exist
		}
		if err != nil {
			return err
		}
		if err := writable(filepath.Dir(t)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if err := os.Remove(t); err != nil && !errors.Is(err, fs.ErrNotExist) && !errors.Is(err, unix.ENOTEMPTY) && !errors.Is(err, unix.EEXIST) {
			return fmt.Errorf("%s: %w", c.Path, err)
		}
	}
	var dirs []Change
	for _, c := range rest {
		t, err := target(c.Path)
		if err != nil {
			return err
		}
		e := c.New
		if err := os.MkdirAll(filepath.Dir(t), 0o755); err != nil {
			return err
		}
		if err := writable(filepath.Dir(t)); err != nil {
			return err
		}
		if err := clearTarget(t, e); err != nil {
			return fmt.Errorf("%s: %w", c.Path, err)
		}
		contentChange := c.Kind != ModeChanged && c.Kind != MetaChanged
		switch e.Mode & unix.S_IFMT {
		case unix.S_IFDIR:
			if err := os.Mkdir(t, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
				return fmt.Errorf("%s: %w", c.Path, err)
			}
			dirs = append(dirs, c)
			continue
		case unix.S_IFREG:
			if contentChange {
				if err := writeRegular(t, e, get); err != nil {
					return fmt.Errorf("%s: %w", c.Path, err)
				}
			}
		case unix.S_IFLNK:
			if contentChange {
				if err := os.Remove(t); err != nil && !errors.Is(err, fs.ErrNotExist) {
					return fmt.Errorf("%s: %w", c.Path, err)
				}
				if err := os.Symlink(string(e.LinkTarget), t); err != nil {
					return fmt.Errorf("%s: %w", c.Path, err)
				}
			}
		case unix.S_IFIFO:
			if contentChange {
				if err := os.Remove(t); err != nil && !errors.Is(err, fs.ErrNotExist) {
					return fmt.Errorf("%s: %w", c.Path, err)
				}
				if err := unix.Mkfifo(t, uint32(e.Mode&0o7777)); err != nil {
					return fmt.Errorf("%s: mkfifo: %w", c.Path, err)
				}
			}
		case unix.S_IFCHR, unix.S_IFBLK:
			if contentChange {
				if err := os.Remove(t); err != nil && !errors.Is(err, fs.ErrNotExist) {
					return fmt.Errorf("%s: %w", c.Path, err)
				}
				var major, minor uint32
				if len(e.Rdev) == 2 {
					major, minor = uint32(e.Rdev[0]), uint32(e.Rdev[1])
				}
				if err := unix.Mknod(t, uint32(e.Mode&(unix.S_IFMT|0o7777)), int(unix.Mkdev(major, minor))); err != nil {
					return fmt.Errorf("%s: mknod: %w", c.Path, err)
				}
			}
		case unix.S_IFSOCK:
			continue // sockets carry no payload and cannot be recreated
		default:
			return fmt.Errorf("%s: unsupported type %#o", c.Path, e.Mode&unix.S_IFMT)
		}
		if err := applyMeta(t, e, get); err != nil {
			return fmt.Errorf("%s: %w", c.Path, err)
		}
	}
	for dir, mode := range restore {
		if err := unix.Chmod(dir, mode); err != nil {
			return err
		}
	}
	sort.Slice(dirs, func(i, j int) bool { return dirs[i].Path > dirs[j].Path })
	for _, c := range dirs {
		t := filepath.Join(root, filepath.FromSlash(c.Path))
		if err := applyMeta(t, c.New, get); err != nil {
			return fmt.Errorf("%s: %w", c.Path, err)
		}
	}
	return nil
}

// checkPath refuses names an entry may not have: empty components, "." and
// "..", so that a change cannot leave the working copy.
func checkPath(p string) error {
	if p == "" {
		return errors.New("empty path")
	}
	for _, part := range strings.Split(p, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("refusing unsafe path %q", p)
		}
	}
	return nil
}

var errNotDir = errors.New("not a directory")

// rejectSymlinkComponents fails if any existing component of path below
// root is not a real directory: a symlink could lead outside the copy; any
// other non-directory is reported as errNotDir.
func rejectSymlinkComponents(root, path string) error {
	for p := filepath.Dir(path); strings.HasPrefix(p, root+string(os.PathSeparator)); p = filepath.Dir(p) {
		fi, err := os.Lstat(p)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if fi.Mode()&fs.ModeSymlink != 0 {
			return fmt.Errorf("refusing to write through non-directory %s", p)
		}
		if !fi.IsDir() {
			return fmt.Errorf("%s: %w", p, errNotDir)
		}
	}
	return nil
}

// clearTarget removes what is at t when its type differs from e's; an
// existing entry of the right type is kept for the caller to overwrite.
func clearTarget(t string, e *fstree.Entry) error {
	fi, err := os.Lstat(t)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	have := uint64(fi.Sys().(*syscall.Stat_t).Mode) & unix.S_IFMT
	if have != e.Mode&unix.S_IFMT {
		return os.RemoveAll(t)
	}
	return nil
}

// writeRegular streams the file content under e's key to a temporary file
// beside t and renames it over t.
func writeRegular(t string, e *fstree.Entry, get Getter) error {
	ck, err := key.Parse(e.ContentKey)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(t), ".dstore-tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if err := fstree.WriteContent(f, ck, get); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, t); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// applyMeta sets ownership (as root), permission bits, xattrs and mtime on
// t from e. chown runs before chmod because it clears setuid/setgid; mtime
// is set last.
func applyMeta(t string, e *fstree.Entry, get Getter) error {
	isLink := e.Mode&unix.S_IFMT == unix.S_IFLNK
	if os.Geteuid() == 0 {
		if err := os.Lchown(t, int(e.UID), int(e.GID)); err != nil {
			return fmt.Errorf("chown: %w", err)
		}
	}
	if !isLink {
		if err := unix.Chmod(t, uint32(e.Mode&0o7777)); err != nil {
			return fmt.Errorf("chmod: %w", err)
		}
		xattrs, err := entryXattrs(e, get)
		if err != nil {
			return err
		}
		for name, val := range xattrs {
			if err := setXattr(t, name, val); err != nil {
				if errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES) || errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) {
					continue // best effort
				}
				return fmt.Errorf("xattr %q: %w", name, err)
			}
		}
	}
	ts := unix.NsecToTimespec(e.Mtime)
	flags := 0
	if isLink {
		flags = unix.AT_SYMLINK_NOFOLLOW
	}
	if err := unix.UtimesNanoAt(unix.AT_FDCWD, t, []unix.Timespec{ts, ts}, flags); err != nil {
		return fmt.Errorf("set mtime: %w", err)
	}
	return nil
}

// entryXattrs decodes an entry's xattrs, inline or spilled.
func entryXattrs(e *fstree.Entry, get Getter) (map[string][]byte, error) {
	switch {
	case len(e.XattrsIn) > 0:
		return cborx.DecodeXattrs(e.XattrsIn)
	case len(e.XattrsKey) == 32:
		k, err := key.Parse(e.XattrsKey)
		if err != nil {
			return nil, err
		}
		b, err := get(k)
		if err != nil {
			return nil, err
		}
		return cborx.DecodeXattrs(b)
	}
	return nil, nil
}
