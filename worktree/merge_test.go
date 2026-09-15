package worktree

import (
	"testing"

	"github.com/amber-store/core/fstree"
	"golang.org/x/sys/unix"
)

func fileEntry(name string, ck byte, mode uint64) *fstree.Entry {
	k := make([]byte, 32)
	k[1] = ck
	return &fstree.Entry{Name: []byte(name), Mode: unix.S_IFREG | mode, ContentKey: k}
}

func dirEntry(name string) *fstree.Entry {
	k := make([]byte, 32)
	k[0] = 0x20 // DirLeaf type nibble, length 0
	return &fstree.Entry{Name: []byte(name), Mode: unix.S_IFDIR | 0o755, ContentKey: k}
}

func paths(cs []Change) []string {
	var out []string
	for _, c := range cs {
		out = append(out, c.Path)
	}
	return out
}

func conflictPaths(cs []Conflict) []string {
	var out []string
	for _, c := range cs {
		out = append(out, c.Path)
	}
	return out
}

func eq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestMerge(t *testing.T) {
	base := fileEntry("f", 1, 0o644)
	v2 := fileEntry("f", 2, 0o644)
	v3 := fileEntry("f", 3, 0o644)
	cases := []struct {
		name             string
		local, incoming  []Change
		apply, conflicts []string
	}{
		{"remote only", nil, []Change{{Path: "a", Kind: Modified, Old: base, New: v2}}, []string{"a"}, nil},
		{"local only", []Change{{Path: "a", Kind: Modified, Old: base, New: v2}}, nil, nil, nil},
		{"both differ", []Change{{Path: "a", Kind: Modified, Old: base, New: v2}}, []Change{{Path: "a", Kind: Modified, Old: base, New: v3}}, nil, []string{"a"}},
		{"same edit twice", []Change{{Path: "a", Kind: Modified, Old: base, New: v2}}, []Change{{Path: "a", Kind: Modified, Old: base, New: v2}}, []string{"a"}, nil},
		{"local touch", []Change{{Path: "a", Kind: MetaChanged, Old: base, New: base}}, []Change{{Path: "a", Kind: Modified, Old: base, New: v2}}, []string{"a"}, nil},
		{"both delete", []Change{{Path: "a", Kind: Deleted, Old: base}}, []Change{{Path: "a", Kind: Deleted, Old: base}}, nil, nil},
		{"local delete, remote edit", []Change{{Path: "a", Kind: Deleted, Old: base}}, []Change{{Path: "a", Kind: Modified, Old: base, New: v2}}, nil, []string{"a"}},
		{"local edit, remote delete", []Change{{Path: "a", Kind: Modified, Old: base, New: v2}}, []Change{{Path: "a", Kind: Deleted, Old: base}}, nil, []string{"a"}},
		{"remote add under locally deleted dir",
			[]Change{{Path: "d", Kind: Deleted, Old: dirEntry("d")}, {Path: "d/x", Kind: Deleted, Old: base}},
			[]Change{{Path: "d/y", Kind: Added, New: v2}}, nil, []string{"d/y"}},
		{"remote delete under locally deleted dir",
			[]Change{{Path: "d", Kind: Deleted, Old: dirEntry("d")}, {Path: "d/x", Kind: Deleted, Old: base}},
			[]Change{{Path: "d/x", Kind: Deleted, Old: base}}, nil, nil},
		{"remote add under locally retyped dir",
			[]Change{{Path: "d", Kind: TypeChanged, Old: dirEntry("d"), New: v2}, {Path: "d/x", Kind: Deleted, Old: base}},
			[]Change{{Path: "d/y", Kind: Added, New: v2}}, nil, []string{"d/y"}},
		{"remote retypes dir with local edits below",
			[]Change{{Path: "d/x", Kind: Modified, Old: base, New: v2}},
			[]Change{{Path: "d", Kind: TypeChanged, Old: dirEntry("d"), New: v3}, {Path: "d/x", Kind: Deleted, Old: base}},
			nil, []string{"d", "d/x"}},
		{"remote deletes dir, local adds below",
			[]Change{{Path: "d/new", Kind: Added, New: v2}},
			[]Change{{Path: "d", Kind: Deleted, Old: dirEntry("d")}, {Path: "d/x", Kind: Deleted, Old: base}},
			[]string{"d", "d/x"}, nil},
	}
	for _, c := range cases {
		apply, conflicts := Merge(c.local, c.incoming)
		if !eq(paths(apply), c.apply) || !eq(conflictPaths(conflicts), c.conflicts) {
			t.Errorf("%s: apply %v conflicts %v, want %v and %v", c.name, paths(apply), conflictPaths(conflicts), c.apply, c.conflicts)
		}
	}
}
