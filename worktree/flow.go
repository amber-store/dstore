package worktree

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"time"

	"github.com/amber-store/core/commit"
	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/dstore/client"
	"github.com/amber-store/dstore/ticket"
	"github.com/amber-store/dstore/view"
)

var (
	ErrNoRemote      = errors.New("the reference does not exist on the cluster: nothing to pull")
	ErrRemoteMoved   = errors.New("the cluster's tree moved since your last sync: pull first, or --force")
	ErrRemoteDeleted = errors.New("the reference was deleted on the cluster: --force to recreate it")
	ErrConflict      = errors.New("conflicting changes: resolve them, or --force to take the cluster's side")
	ErrRefChanged    = errors.New("reference changed on the cluster since your last fetch: pull first, or --force")
)

// FetchResult reports what a fetch found.
type FetchResult struct {
	Exists   bool    // the reference exists on the cluster
	UpToDate bool    // its tree was already the stored remote
	Key      key.Key // what the reference names: a tree, or a commit on a branch
	Tree     key.Key // the tree it stands for
	Stats    client.PullStats
}

// fetch reads the reference and pulls its tree into the packstore, updating
// the state in memory only.
func (t *Tree) fetch(ctx context.Context, cl *client.Cluster, prog client.Progress) (FetchResult, error) {
	var r FetchResult
	ref, err := cl.RefGet(ctx, t.Config.Name)
	if errors.Is(err, client.ErrUnknownRef) {
		t.State.HasRemote, t.State.Remote, t.State.RemoteCommit, t.State.RemoteVersion = false, key.Key{}, key.Key{}, nil
		return r, nil
	}
	if err != nil {
		return r, err
	}
	k, err := key.Parse(ref.Ref.Key)
	if err != nil {
		return r, err
	}
	r.Exists, r.Key = true, k
	if t.State.HasRemote && t.State.RemoteKey() == k {
		r.UpToDate = true
	} else if err := cl.PullTree(ctx, t.Store, k, &r.Stats, prog); err != nil {
		// On a branch this pulls the commit's whole history too.
		return r, err
	}
	tree, err := client.TreeOf(k, t.Get)
	if err != nil {
		return r, err
	}
	var rc key.Key
	if k.Type() == key.Commit {
		rc = k
	}
	r.Tree = tree
	t.State.HasRemote, t.State.Remote, t.State.RemoteCommit, t.State.RemoteVersion = true, tree, rc, ref.Version
	return r, nil
}

// Fetch records the reference's current tree as remote.
func (t *Tree) Fetch(ctx context.Context, cl *client.Cluster, prog client.Progress) (FetchResult, error) {
	r, err := t.fetch(ctx, cl, prog)
	if err != nil {
		return r, err
	}
	return r, t.SaveState()
}

// Clone creates a working copy of the reference in dir, which must not
// exist or must be empty. The state file is written last, so an interrupted
// clone is recognisable; on any error what clone created is removed.
func Clone(ctx context.Context, cl *client.Cluster, dir string, cfg Config, prog client.Progress) (*Tree, FetchResult, error) {
	var r FetchResult
	created := false
	fi, err := os.Stat(dir)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, r, err
		}
		created = true
	case err != nil:
		return nil, r, err
	case !fi.IsDir():
		return nil, r, fmt.Errorf("%s is not a directory", dir)
	default:
		ents, err := os.ReadDir(dir)
		if err != nil {
			return nil, r, err
		}
		if len(ents) > 0 {
			return nil, r, fmt.Errorf("%s is not empty", dir)
		}
	}
	t, err := Create(dir, cfg)
	if err != nil {
		if created {
			os.RemoveAll(dir)
		}
		return nil, r, err
	}
	fail := func(err error) (*Tree, FetchResult, error) {
		t.Close()
		if created {
			os.RemoveAll(dir)
		} else {
			Remove(dir)
		}
		return nil, r, err
	}
	if r, err = t.fetch(ctx, cl, prog); err != nil {
		return fail(err)
	}
	if !r.Exists {
		return fail(fmt.Errorf("%w: %s", client.ErrUnknownRef, cfg.Name))
	}
	changes, err := DiffTrees(t.Get, t.State.Base, t.State.Remote)
	if err != nil {
		return fail(err)
	}
	if err := Apply(t.Root, changes, t.Get); err != nil {
		return fail(err)
	}
	t.State.Base, t.State.SyncedAt = t.State.Remote, time.Now()
	if err := t.SaveState(); err != nil {
		return fail(err)
	}
	return t, r, nil
}

// Init makes the existing directory dir a working copy of the reference,
// with the empty tree as base, and fetches so that remote records whether
// the name exists. On error nothing is left behind.
func Init(ctx context.Context, cl *client.Cluster, dir string, cfg Config, prog client.Progress) (*Tree, FetchResult, error) {
	t, err := Create(dir, cfg)
	if err != nil {
		return nil, FetchResult{}, err
	}
	r, err := t.fetch(ctx, cl, prog)
	if err == nil {
		t.State.SyncedAt = time.Now()
		err = t.SaveState()
	}
	if err != nil {
		t.Close()
		Remove(dir)
		return nil, r, err
	}
	return t, r, nil
}

// PullResult reports a pull.
type PullResult struct {
	Fetch     FetchResult
	UpToDate  bool
	Applied   []Change
	Conflicts []Conflict
}

// Pull fetches, then applies the remote's changes since base over the
// working directory, keeping local changes; with force, conflicts take the
// remote's side, otherwise they abort the pull before anything is written.
func (t *Tree) Pull(ctx context.Context, cl *client.Cluster, force bool, jobs int, prog client.Progress) (PullResult, error) {
	var r PullResult
	fr, err := t.Fetch(ctx, cl, prog)
	r.Fetch = fr
	if err != nil {
		return r, err
	}
	if !t.State.HasRemote {
		return r, ErrNoRemote
	}
	if t.State.Remote == t.State.Base {
		r.UpToDate = true
		return r, nil
	}
	local, err := Scan(t.Root, t.State.Base, t.Get, t.State.SyncedAt, jobs)
	if err != nil {
		return r, err
	}
	incoming, err := DiffTrees(t.Get, t.State.Base, t.State.Remote)
	if err != nil {
		return r, err
	}
	apply, conflicts := Merge(local, incoming)
	r.Conflicts = conflicts
	if len(conflicts) > 0 {
		if !force {
			return r, ErrConflict
		}
		for _, c := range conflicts {
			apply = append(apply, c.Incoming)
		}
	}
	if err := Apply(t.Root, apply, t.Get); err != nil {
		return r, err
	}
	r.Applied = apply
	t.State.Base, t.State.SyncedAt = t.State.Remote, time.Now()
	return r, t.SaveState()
}

// PushResult reports a push.
type PushResult struct {
	Root      key.Key
	Commit    key.Key // the commit pushed on a branch; zero for a plain tree
	Nothing   bool    // the tree equals base and base is what the cluster holds
	Recovered bool    // the cluster already held this tree from an interrupted push
	Built     packstore.WriteStats
	Stats     client.PushStats
}

// Push builds the working directory's tree, uploads it and writes the
// reference under compare-and-swap on the stored remote version. It refuses
// when base and remote differ (a fetch showed the cluster moved) unless
// force, which replaces the reference unconditionally.
//
// When the reference is a branch (it names a commit), the tree is recorded
// as a new commit whose parent is the fetched one, with user as author and
// committer and message as its message, and the reference moves to that
// commit. A non-empty message makes a commit on a plain or new reference
// too, turning it into a branch.
func (t *Tree) Push(ctx context.Context, cl *client.Cluster, user, message string, force bool, jobs int, prog client.Progress) (PushResult, error) {
	var r PushResult
	empty, _ := EmptyTree()
	synced := (t.State.HasRemote && t.State.Remote == t.State.Base) || (!t.State.HasRemote && t.State.Base == empty)
	if !synced && !force {
		if !t.State.HasRemote {
			return r, ErrRemoteDeleted
		}
		return r, ErrRemoteMoved
	}
	root, stats, err := ingest.Dir(t.Store, t.Root, ingest.Opts{Jobs: jobs, Exclude: []string{Dir}})
	if err != nil {
		return r, err
	}
	r.Root, r.Built = root, stats
	if root == t.State.Base && synced {
		r.Nothing = true
		return r, nil
	}
	target := root
	var parents []key.Key
	if t.State.IsBranch() || message != "" {
		if t.State.IsBranch() {
			parents = []key.Key{t.State.RemoteCommit}
		}
		if target, err = t.commit(root, parents, user, message); err != nil {
			return r, err
		}
		r.Commit = target
	}
	cond := client.Cond{Force: force}
	if !force {
		cond.Versioned = true
		cond.ExpectedVersion = t.State.RemoteVersion // nil: the name must be new
	}
	ps, err := cl.Push(ctx, t.Store, target, t.Config.Name, user, cond, prog)
	if err != nil {
		var cm *client.CASMismatch
		if !errors.As(err, &cm) {
			return r, err
		}
		cur, perr := key.Parse(cm.Current)
		if !cm.HasCurrent || perr != nil || !t.samePush(cur, target, root, parents) {
			return r, fmt.Errorf("%w (%v)", ErrRefChanged, err)
		}
		r.Recovered = true
		t.State.RemoteVersion = cm.Version
		if target.Type() == key.Commit {
			target, r.Commit = cur, cur
		}
	} else {
		r.Stats = ps
		t.State.RemoteVersion = ps.Version
	}
	var rc key.Key
	if target.Type() == key.Commit {
		rc = target
	}
	t.State.Base, t.State.Remote, t.State.RemoteCommit, t.State.HasRemote, t.State.SyncedAt = root, root, rc, true, time.Now()
	return r, t.SaveState()
}

// commit records tree as a commit with the given parents in the local store
// and returns its key.
func (t *Tree) commit(tree key.Key, parents []key.Key, user, message string) (key.Key, error) {
	now := time.Now()
	_, off := now.Zone()
	id := commit.Identity{Name: user, When: now.UnixNano(), TZOffset: off / 60}
	k, raw, err := commit.Commit{Tree: tree, Parents: parents, Author: id, Committer: id, Message: message}.Object()
	if err != nil {
		return key.Key{}, fmt.Errorf("commit: %w", err)
	}
	if err := t.Store.Put(k, raw); err != nil {
		return key.Key{}, err
	}
	return k, nil
}

// samePush reports whether the cluster's current key cur is what this push
// would have written: the same key, or, on a branch, a commit made by an
// interrupted earlier push of the same tree onto the same parents (its
// timestamp, and so its key, differ from this attempt's).
func (t *Tree) samePush(cur, target, tree key.Key, parents []key.Key) bool {
	if cur == target {
		return true
	}
	if cur.Type() != key.Commit || target.Type() != key.Commit {
		return false
	}
	data, err := t.Get(cur) // an earlier attempt stored its commit locally
	if err != nil {
		return false
	}
	c, err := commit.Decode(data)
	return err == nil && c.Tree == tree && slices.Equal(c.Parents, parents)
}

// RemoteState is how the fetched tree relates to base.
type RemoteState int

const (
	RemoteUpToDate RemoteState = iota
	RemoteMoved
	RemoteAbsent
)

// Status is what `dstore status` prints.
type Status struct {
	Changes  []Change // local changes, metadata-only ones left out
	MetaOnly int      // paths differing only in mtime, ownership or xattrs
	Remote   RemoteState
	Incoming []Change // base→remote when Remote is RemoteMoved
}

func (t *Tree) Status(jobs int) (Status, error) {
	var s Status
	changes, err := Scan(t.Root, t.State.Base, t.Get, t.State.SyncedAt, jobs)
	if err != nil {
		return s, err
	}
	for _, c := range changes {
		if c.Kind == MetaChanged {
			s.MetaOnly++
		} else {
			s.Changes = append(s.Changes, c)
		}
	}
	switch {
	case !t.State.HasRemote:
		s.Remote = RemoteAbsent
	case t.State.Remote == t.State.Base:
		s.Remote = RemoteUpToDate
	default:
		s.Remote = RemoteMoved
		if s.Incoming, err = DiffTrees(t.Get, t.State.Base, t.State.Remote); err != nil {
			return s, err
		}
	}
	return s, nil
}

// TicketFromView derives a bootstrap ticket from a view: up to four members.
func TicketFromView(v *view.View) ticket.Ticket {
	tk := ticket.Ticket{ClusterID: v.ClusterID, Incarnation: v.Incarnation}
	for _, nd := range v.Nodes {
		tk.Members = append(tk.Members, ticket.Member{ID: nd.ID, Addrs: nd.Addrs})
		if len(tk.Members) >= 4 {
			break
		}
	}
	return tk
}

// RefreshTicket stores the ticket derived from the connected cluster's view
// when it differs from the stored one, so the copy survives membership
// changes.
func (t *Tree) RefreshTicket(cl *client.Cluster) error {
	v := cl.View()
	if v == nil {
		return nil
	}
	s := TicketFromView(v).Encode()
	if s == t.Config.Ticket {
		return nil
	}
	t.Config.Ticket = s
	return t.SaveConfig()
}
