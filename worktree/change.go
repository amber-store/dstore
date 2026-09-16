package worktree

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"golang.org/x/sys/unix"
)

// Kind classifies one path's difference between two sides.
type Kind int

const (
	Added       Kind = iota // absent on the old side
	Deleted                 // absent on the new side
	Modified                // same type, different content (file bytes, link target, device numbers)
	TypeChanged             // different S_IFMT
	ModeChanged             // same type and content, different permission bits
	MetaChanged             // same type, content and mode; uid, gid, mtime or xattrs differ
)

func (k Kind) String() string {
	switch k {
	case Added:
		return "new"
	case Deleted:
		return "deleted"
	case Modified:
		return "modified"
	case TypeChanged:
		return "type"
	case ModeChanged:
		return "mode"
	case MetaChanged:
		return "meta"
	}
	return fmt.Sprintf("Kind(%d)", int(k))
}

// Change is one path's difference. Old is nil for Added, New for Deleted.
// Path is root-relative and /-separated.
type Change struct {
	Path     string
	Kind     Kind
	Old, New *fstree.Entry
}

// IsDir reports whether e is a directory entry.
func IsDir(e *fstree.Entry) bool { return e != nil && e.Mode&unix.S_IFMT == unix.S_IFDIR }

// TypeName names an entry's file type.
func TypeName(mode uint64) string {
	switch mode & unix.S_IFMT {
	case unix.S_IFREG:
		return "file"
	case unix.S_IFDIR:
		return "directory"
	case unix.S_IFLNK:
		return "symlink"
	case unix.S_IFIFO:
		return "fifo"
	case unix.S_IFSOCK:
		return "socket"
	case unix.S_IFCHR:
		return "char device"
	case unix.S_IFBLK:
		return "block device"
	}
	return fmt.Sprintf("type %#o", mode&unix.S_IFMT)
}

// SameContent reports whether two entries of the same type carry the same
// content: the content key of a file, the target of a link, the numbers of
// a device. Directories compare equal here; their contents are compared by
// recursion.
func SameContent(a, b *fstree.Entry) bool {
	switch a.Mode & unix.S_IFMT {
	case unix.S_IFREG:
		return bytes.Equal(a.ContentKey, b.ContentKey)
	case unix.S_IFLNK:
		return bytes.Equal(a.LinkTarget, b.LinkTarget)
	case unix.S_IFCHR, unix.S_IFBLK:
		return slices.Equal(a.Rdev, b.Rdev)
	}
	return true
}

// Equivalent reports whether two present entries agree in type, content and
// permission bits — what the merge treats as the same edit made twice.
func Equivalent(a, b *fstree.Entry) bool {
	return a.Mode&unix.S_IFMT == b.Mode&unix.S_IFMT && SameContent(a, b) && a.Mode&0o7777 == b.Mode&0o7777
}

// Compare classifies the difference between two present entries; ok is
// false when they are identical.
func Compare(old, new *fstree.Entry) (kind Kind, ok bool) {
	switch {
	case old.Mode&unix.S_IFMT != new.Mode&unix.S_IFMT:
		return TypeChanged, true
	case !SameContent(old, new):
		return Modified, true
	case old.Mode&0o7777 != new.Mode&0o7777:
		return ModeChanged, true
	case old.UID != new.UID || old.GID != new.GID || old.Mtime != new.Mtime ||
		!bytes.Equal(old.XattrsIn, new.XattrsIn) || !bytes.Equal(old.XattrsKey, new.XattrsKey):
		return MetaChanged, true
	}
	return 0, false
}

func joinPath(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "/" + name
}

// DiffTrees lists the changes from directory tree a to directory tree b in
// path order, skipping subtrees whose keys are equal. An added or deleted
// directory yields a change for itself followed by one per path below it;
// a type change is followed by the former contents as deleted and the new
// contents as added.
func DiffTrees(get Getter, a, b key.Key) ([]Change, error) {
	if a == b {
		return nil, nil
	}
	var out []Change
	if err := diffDirs(get, "", a, b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func diffDirs(get Getter, prefix string, a, b key.Key, out *[]Change) error {
	ea, err := fstree.CollectEntries(a, get)
	if err != nil {
		return err
	}
	eb, err := fstree.CollectEntries(b, get)
	if err != nil {
		return err
	}
	i, j := 0, 0
	for i < len(ea) || j < len(eb) {
		var cmp int
		switch {
		case i == len(ea):
			cmp = 1
		case j == len(eb):
			cmp = -1
		default:
			cmp = bytes.Compare(ea[i].Name, eb[j].Name)
		}
		switch {
		case cmp < 0:
			if err := expand(get, prefix, &ea[i], Deleted, out); err != nil {
				return err
			}
			i++
		case cmp > 0:
			if err := expand(get, prefix, &eb[j], Added, out); err != nil {
				return err
			}
			j++
		default:
			x, y := &ea[i], &eb[j]
			p := joinPath(prefix, string(x.Name))
			if k, ok := Compare(x, y); ok {
				*out = append(*out, Change{Path: p, Kind: k, Old: x, New: y})
				if k == TypeChanged {
					// A directory that became something else loses its
					// contents; something that became a directory gains them.
					if err := expandChildren(get, p, x, Deleted, out); err != nil {
						return err
					}
					if err := expandChildren(get, p, y, Added, out); err != nil {
						return err
					}
				}
			}
			if IsDir(x) && IsDir(y) && !bytes.Equal(x.ContentKey, y.ContentKey) {
				kx, err := key.Parse(x.ContentKey)
				if err != nil {
					return err
				}
				ky, err := key.Parse(y.ContentKey)
				if err != nil {
					return err
				}
				if err := diffDirs(get, p, kx, ky, out); err != nil {
					return err
				}
			}
			i++
			j++
		}
	}
	return nil
}

// expand appends a change of kind (Added or Deleted) for e and, when e is a
// directory, for every path below it.
func expand(get Getter, prefix string, e *fstree.Entry, kind Kind, out *[]Change) error {
	p := joinPath(prefix, string(e.Name))
	c := Change{Path: p, Kind: kind}
	if kind == Added {
		c.New = e
	} else {
		c.Old = e
	}
	*out = append(*out, c)
	return expandChildren(get, p, e, kind, out)
}

// expandChildren appends a change of kind for every path below the
// directory entry e at path p; nothing for a non-directory.
func expandChildren(get Getter, p string, e *fstree.Entry, kind Kind, out *[]Change) error {
	if !IsDir(e) {
		return nil
	}
	k, err := key.Parse(e.ContentKey)
	if err != nil {
		return err
	}
	entries, err := fstree.CollectEntries(k, get)
	if err != nil {
		return err
	}
	for i := range entries {
		if err := expand(get, p, &entries[i], kind, out); err != nil {
			return err
		}
	}
	return nil
}
