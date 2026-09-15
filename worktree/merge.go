package worktree

import (
	"sort"
	"strings"
)

// Conflict is an incoming change that cannot be applied over a local one.
type Conflict struct {
	Path            string
	Local, Incoming Change
}

// Merge decides which incoming changes (base→remote) can be applied over the
// local ones (base→working directory). Rules: no local change, or only a
// metadata change, at the path — apply; both sides deleted — nothing; both
// sides changed to equivalent entries — apply (the remote's metadata wins);
// an incoming addition or modification below a directory the local side
// deleted or retyped — conflict; an incoming retype of a directory with
// local changes below it — conflict; anything else — conflict. Local-only
// changes are kept. The caller resolves conflicts in the remote's favour by
// applying each Conflict's Incoming.
func Merge(local, incoming []Change) (apply []Change, conflicts []Conflict) {
	byPath := make(map[string]Change, len(local))
	paths := make([]string, 0, len(local))
	for _, c := range local {
		byPath[c.Path] = c
		paths = append(paths, c.Path)
	}
	sort.Strings(paths)
	// goneAbove finds a local change that deleted or retyped an ancestor
	// directory of p.
	goneAbove := func(p string) (Change, bool) {
		for i := strings.LastIndexByte(p, '/'); i > 0; i = strings.LastIndexByte(p[:i], '/') {
			if l, ok := byPath[p[:i]]; ok && (l.Kind == Deleted || (l.Kind == TypeChanged && IsDir(l.Old))) {
				return l, true
			}
		}
		return Change{}, false
	}
	// firstBelow finds a local change strictly below the directory p.
	firstBelow := func(p string) (Change, bool) {
		i := sort.SearchStrings(paths, p+"/")
		if i < len(paths) && strings.HasPrefix(paths[i], p+"/") {
			return byPath[paths[i]], true
		}
		return Change{}, false
	}
	for _, in := range incoming {
		if l, ok := byPath[in.Path]; ok {
			switch {
			case l.Kind == MetaChanged:
				apply = append(apply, in)
			case l.Kind == Deleted && in.Kind == Deleted:
				// already gone
			case l.Kind == Deleted || in.Kind == Deleted:
				conflicts = append(conflicts, Conflict{Path: in.Path, Local: l, Incoming: in})
			case Equivalent(l.New, in.New):
				apply = append(apply, in)
			default:
				conflicts = append(conflicts, Conflict{Path: in.Path, Local: l, Incoming: in})
			}
			continue
		}
		if l, ok := goneAbove(in.Path); ok {
			if in.Kind != Deleted {
				conflicts = append(conflicts, Conflict{Path: in.Path, Local: l, Incoming: in})
			}
			continue
		}
		if in.Kind == TypeChanged && IsDir(in.Old) {
			if l, ok := firstBelow(in.Path); ok {
				conflicts = append(conflicts, Conflict{Path: in.Path, Local: l, Incoming: in})
				continue
			}
		}
		apply = append(apply, in)
	}
	return apply, conflicts
}
