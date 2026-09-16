package worktree

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/amber-store/core/amberignore"
	"github.com/amber-store/core/cborx"
	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"golang.org/x/sys/unix"
)

// RacyWindow is how close to the sync time a recorded mtime may be before
// the file is hashed regardless of its stat data (git's racily-clean rule).
const RacyWindow = 2 * time.Second

type scanner struct {
	root     string
	get      Getter
	syncedAt time.Time
	jobs     int
}

// Scan lists the changes from the tree base to the working directory root,
// in path order. The walk applies .amberignore and skips the root's .dstore,
// so a base path that is now ignored is reported as deleted. A regular file
// whose size and mtime match the base entry is taken as unchanged without
// being read, unless the base mtime lies within RacyWindow of syncedAt.
func Scan(root string, base key.Key, get Getter, syncedAt time.Time, jobs int) ([]Change, error) {
	ign, err := amberignore.Root(root)
	if err != nil {
		return nil, err
	}
	s := &scanner{root: root, get: get, syncedAt: syncedAt, jobs: jobs}
	var out []Change
	if err := s.dir(root, "", base, ign, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// listDir returns the entries of abs that ingest would see: sorted bytewise,
// ignored names dropped, the metadata directory dropped at the root.
func (s *scanner) listDir(abs string, ign *amberignore.Matcher) ([]os.DirEntry, error) {
	ents, err := os.ReadDir(abs) // sorted by name
	if err != nil {
		return nil, err
	}
	kept := ents[:0]
	for _, de := range ents {
		if abs == s.root && de.Name() == Dir {
			continue
		}
		if ign.Ignored(de.Name(), de.IsDir()) {
			continue
		}
		kept = append(kept, de)
	}
	return kept, nil
}

func (s *scanner) dir(abs, prefix string, dirKey key.Key, ign *amberignore.Matcher, out *[]Change) error {
	disk, err := s.listDir(abs, ign)
	if err != nil {
		return err
	}
	base, err := fstree.CollectEntries(dirKey, s.get)
	if err != nil {
		return err
	}
	i, j := 0, 0
	for i < len(base) || j < len(disk) {
		var cmp int
		switch {
		case i == len(base):
			cmp = 1
		case j == len(disk):
			cmp = -1
		default:
			cmp = bytes.Compare(base[i].Name, []byte(disk[j].Name()))
		}
		switch {
		case cmp < 0:
			if err := expand(s.get, prefix, &base[i], Deleted, out); err != nil {
				return err
			}
			i++
		case cmp > 0:
			if err := s.added(abs, prefix, disk[j].Name(), ign, out); err != nil {
				return err
			}
			j++
		default:
			if err := s.both(abs, prefix, &base[i], ign, out); err != nil {
				return err
			}
			i++
			j++
		}
	}
	return nil
}

// added reports the disk entry name under abs, and everything below it, as
// added.
func (s *scanner) added(abs, prefix, name string, ign *amberignore.Matcher, out *[]Change) error {
	full := filepath.Join(abs, name)
	e, err := s.entry(full, name, true)
	if err != nil {
		return err
	}
	p := joinPath(prefix, name)
	*out = append(*out, Change{Path: p, Kind: Added, New: &e.Entry})
	if IsDir(&e.Entry) {
		return s.addedChildren(full, p, name, ign, out)
	}
	return nil
}

func (s *scanner) addedChildren(full, p, name string, ign *amberignore.Matcher, out *[]Change) error {
	sub, err := ign.Descend(full, name)
	if err != nil {
		return err
	}
	ents, err := s.listDir(full, sub)
	if err != nil {
		return err
	}
	for _, de := range ents {
		if err := s.added(full, p, de.Name(), sub, out); err != nil {
			return err
		}
	}
	return nil
}

// both compares the base entry b with the disk entry of the same name.
func (s *scanner) both(abs, prefix string, b *fstree.Entry, ign *amberignore.Matcher, out *[]Change) error {
	name := string(b.Name)
	full := filepath.Join(abs, name)
	p := joinPath(prefix, name)
	if b.Mode&unix.S_IFMT != s.diskType(full) {
		e, err := s.entry(full, name, true)
		if err != nil {
			return err
		}
		*out = append(*out, Change{Path: p, Kind: TypeChanged, Old: b, New: &e.Entry})
		if err := expandChildren(s.get, p, b, Deleted, out); err != nil {
			return err
		}
		if IsDir(&e.Entry) {
			return s.addedChildren(full, p, name, ign, out)
		}
		return nil
	}
	e, err := s.entry(full, name, false)
	if err != nil {
		return err
	}
	switch e.Mode & unix.S_IFMT {
	case unix.S_IFREG:
		bk, err := key.Parse(b.ContentKey)
		if err != nil {
			return err
		}
		if e.size != bk.Length() || e.Mtime != b.Mtime || b.Mtime > s.syncedAt.Add(-RacyWindow).UnixNano() {
			ck, err := hashFile(full, s.jobs)
			if err != nil {
				return err
			}
			e.ContentKey = ck[:]
		} else {
			e.ContentKey = b.ContentKey
		}
	case unix.S_IFDIR:
		e.ContentKey = b.ContentKey // the directory's own change is mode or metadata
	}
	if k, ok := Compare(b, &e.Entry); ok {
		*out = append(*out, Change{Path: p, Kind: k, Old: b, New: &e.Entry})
	}
	if IsDir(b) {
		sub, err := ign.Descend(full, name)
		if err != nil {
			return err
		}
		bk, err := key.Parse(b.ContentKey)
		if err != nil {
			return err
		}
		return s.dir(full, p, bk, sub, out)
	}
	return nil
}

// diskType returns the S_IFMT bits of the entry at full (0 when absent).
func (s *scanner) diskType(full string) uint64 {
	info, err := os.Lstat(full)
	if err != nil {
		return 0
	}
	return uint64(info.Sys().(*syscall.Stat_t).Mode) & unix.S_IFMT
}

// diskEntry is a disk entry as ingest would record it, plus its size.
type diskEntry struct {
	fstree.Entry
	size uint64
}

// entry reads the entry at full: type, mode, ownership, mtime, link target,
// device numbers and xattrs, as ingest records them. With hash set, a
// regular file's content key is computed too.
func (s *scanner) entry(full, name string, hash bool) (*diskEntry, error) {
	info, err := os.Lstat(full)
	if err != nil {
		return nil, err
	}
	sys := info.Sys().(*syscall.Stat_t)
	e := &diskEntry{Entry: fstree.Entry{
		Name:  []byte(name),
		Mode:  uint64(sys.Mode),
		UID:   uint64(sys.Uid),
		GID:   uint64(sys.Gid),
		Mtime: info.ModTime().UnixNano(),
	}, size: uint64(info.Size())}
	switch e.Mode & unix.S_IFMT {
	case unix.S_IFREG:
		if hash {
			ck, err := hashFile(full, s.jobs)
			if err != nil {
				return nil, err
			}
			e.ContentKey = ck[:]
		}
	case unix.S_IFLNK:
		target, err := os.Readlink(full)
		if err != nil {
			return nil, err
		}
		e.LinkTarget = []byte(target)
	case unix.S_IFCHR, unix.S_IFBLK:
		rdev := uint64(sys.Rdev)
		e.Rdev = []uint64{uint64(unix.Major(rdev)), uint64(unix.Minor(rdev))}
	}
	if e.Mode&unix.S_IFMT != unix.S_IFLNK {
		xattrs, err := readXattrs(full)
		if err != nil {
			return nil, err
		}
		if len(xattrs) > 0 {
			enc := cborx.EncodeXattrs(xattrs)
			if len(enc) <= ingest.DefaultXattrInlineMax {
				e.XattrsIn = enc
			} else {
				obj, err := fstree.EncodeXattrSet(xattrs)
				if err != nil {
					return nil, err
				}
				e.XattrsKey = obj.Key[:]
			}
		}
	}
	return e, nil
}

// hashFile returns the content key ingest would give the regular file at
// path, without storing anything.
func hashFile(path string, jobs int) (key.Key, error) {
	seq, root, err := ingest.Objects(path, ingest.Opts{Jobs: jobs})
	if err != nil {
		return key.Key{}, err
	}
	for _, err := range seq {
		if err != nil {
			return key.Key{}, err
		}
	}
	return *root, nil
}
