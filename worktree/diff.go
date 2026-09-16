package worktree

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/aymanbagabas/go-udiff"
	"golang.org/x/sys/unix"
)

// MaxDiffBytes is the largest file content a diff reads on either side.
const MaxDiffBytes = 16 << 20

var ErrTooLarge = errors.New("too large to diff")

// Source reads the content behind an entry: a regular file's bytes or a
// symlink's target; nil for other types; ErrTooLarge over MaxDiffBytes.
type Source interface {
	Content(p string, e *fstree.Entry) ([]byte, error)
}

// TreeSource reads content from a tree in a store.
type TreeSource struct{ Get Getter }

func (s TreeSource) Content(p string, e *fstree.Entry) ([]byte, error) {
	switch e.Mode & unix.S_IFMT {
	case unix.S_IFLNK:
		return e.LinkTarget, nil
	case unix.S_IFREG:
		ck, err := key.Parse(e.ContentKey)
		if err != nil {
			return nil, err
		}
		if ck.Length() > MaxDiffBytes {
			return nil, ErrTooLarge
		}
		var buf bytes.Buffer
		if err := fstree.WriteContent(&buf, ck, s.Get); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	return nil, nil
}

// DiskSource reads content from the working directory.
type DiskSource struct{ Root string }

func (s DiskSource) Content(p string, e *fstree.Entry) ([]byte, error) {
	full := filepath.Join(s.Root, filepath.FromSlash(p))
	switch e.Mode & unix.S_IFMT {
	case unix.S_IFLNK:
		t, err := os.Readlink(full)
		return []byte(t), err
	case unix.S_IFREG:
		fi, err := os.Lstat(full)
		if err != nil {
			return nil, err
		}
		if fi.Size() > MaxDiffBytes {
			return nil, ErrTooLarge
		}
		return os.ReadFile(full)
	}
	return nil, nil
}

// Unified writes git-style unified diffs for changes: a "diff a/P b/P"
// line, "old mode"/"new mode" lines when the permission bits differ, then
// "--- a/P" / "+++ b/P" (or /dev/null) and hunks. Binary or oversized
// content is summarised. Metadata-only changes are skipped; a directory's
// own change shows its mode lines only.
func Unified(w io.Writer, changes []Change, old, new Source) error {
	for _, c := range changes {
		if err := unifiedOne(w, c, old, new); err != nil {
			return err
		}
	}
	return nil
}

// sides returns the content-bearing entries of a change: directories and
// absent sides are nil.
func sides(c Change) (oldE, newE *fstree.Entry) {
	if c.Old != nil && !IsDir(c.Old) {
		oldE = c.Old
	}
	if c.New != nil && !IsDir(c.New) {
		newE = c.New
	}
	return
}

func modeLines(w io.Writer, c Change) {
	if c.Old != nil && c.New != nil && c.Old.Mode&0o7777 != c.New.Mode&0o7777 {
		fmt.Fprintf(w, "old mode %04o\nnew mode %04o\n", c.Old.Mode&0o7777, c.New.Mode&0o7777)
	}
}

func unifiedOne(w io.Writer, c Change, old, new Source) error {
	if c.Kind == MetaChanged {
		return nil
	}
	oldE, newE := sides(c)
	if oldE == nil && newE == nil {
		if c.Kind == ModeChanged {
			fmt.Fprintf(w, "diff a/%s b/%s\n", c.Path, c.Path)
			modeLines(w, c)
		}
		return nil
	}
	fmt.Fprintf(w, "diff a/%s b/%s\n", c.Path, c.Path)
	modeLines(w, c)
	label := func(side string, e *fstree.Entry) string {
		if e == nil {
			return "/dev/null"
		}
		return side + "/" + c.Path
	}
	oldB, oldErr := content(old, c.Path, oldE)
	newB, newErr := content(new, c.Path, newE)
	if errors.Is(oldErr, ErrTooLarge) || errors.Is(newErr, ErrTooLarge) || isBinary(oldB) || isBinary(newB) {
		fmt.Fprintf(w, "Binary files %s and %s differ\n", label("a", oldE), label("b", newE))
		return nil
	}
	if oldErr != nil {
		return fmt.Errorf("%s: %w", c.Path, oldErr)
	}
	if newErr != nil {
		return fmt.Errorf("%s: %w", c.Path, newErr)
	}
	if bytes.Equal(oldB, newB) {
		return nil
	}
	_, err := io.WriteString(w, udiff.Unified(label("a", oldE), label("b", newE), string(oldB), string(newB)))
	return err
}

func content(s Source, p string, e *fstree.Entry) ([]byte, error) {
	if e == nil {
		return nil, nil
	}
	return s.Content(p, e)
}

// isBinary applies git's heuristic: a NUL byte in the first 8 KiB.
func isBinary(b []byte) bool {
	if len(b) > 8192 {
		b = b[:8192]
	}
	return bytes.IndexByte(b, 0) >= 0
}

// Stat writes one line per content change — " PATH | +A -D" or
// " PATH | binary" — and a total.
func Stat(w io.Writer, changes []Change, old, new Source) error {
	files, ins, dels := 0, 0, 0
	for _, c := range changes {
		if c.Kind == MetaChanged {
			continue
		}
		oldE, newE := sides(c)
		if oldE == nil && newE == nil {
			continue
		}
		oldB, oldErr := content(old, c.Path, oldE)
		newB, newErr := content(new, c.Path, newE)
		if errors.Is(oldErr, ErrTooLarge) || errors.Is(newErr, ErrTooLarge) || isBinary(oldB) || isBinary(newB) {
			fmt.Fprintf(w, " %s | binary\n", c.Path)
			files++
			continue
		}
		if oldErr != nil {
			return fmt.Errorf("%s: %w", c.Path, oldErr)
		}
		if newErr != nil {
			return fmt.Errorf("%s: %w", c.Path, newErr)
		}
		if bytes.Equal(oldB, newB) {
			continue
		}
		a, d := 0, 0
		for _, line := range strings.SplitAfter(udiff.Unified("a", "b", string(oldB), string(newB)), "\n") {
			switch {
			case strings.HasPrefix(line, "+++") || strings.HasPrefix(line, "---"):
			case strings.HasPrefix(line, "+"):
				a++
			case strings.HasPrefix(line, "-"):
				d++
			}
		}
		fmt.Fprintf(w, " %s | +%d -%d\n", c.Path, a, d)
		files++
		ins += a
		dels += d
	}
	fmt.Fprintf(w, " %d files changed, %d insertions(+), %d deletions(-)\n", files, ins, dels)
	return nil
}
