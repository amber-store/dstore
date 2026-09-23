// Package worktree implements dstore working copies: a directory holding a
// reference's tree, with a local packstore and a little state in .dstore/,
// and the git-like operations over it (design in
// docs/superpowers/specs/2026-09-15-working-copy-design.md).
package worktree

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
)

// Dir is the metadata directory at the root of a working copy.
const Dir = ".dstore"

const (
	configFile = "config"
	stateFile  = "state"
	storeDir   = "packstore"
)

// Getter reads an object's payload by key.
type Getter = func(key.Key) ([]byte, error)

var (
	ErrNotWorkingCopy = errors.New("not a dstore working copy (no .dstore in this or any parent directory)")
	ErrIncomplete     = errors.New("incomplete clone: delete the directory and clone again")
)

// Config is what a working copy remembers about its cluster and reference.
type Config struct {
	Ticket      string `json:"ticket"`
	Name        string `json:"name"`
	Relay       string `json:"relay,omitempty"`
	NoRelay     bool   `json:"no_relay,omitempty"`
	NoDiscovery bool   `json:"no_discovery,omitempty"`
	User        string `json:"user,omitempty"`
}

// State is the working copy's position: base is the tree the directory was
// last synced to; remote the reference's tree as of the last fetch, with
// its cluster version (HasRemote false: the reference does not exist).
// When the reference names a commit (a branch), RemoteCommit is that commit
// and Remote its tree; otherwise RemoteCommit is the zero key.
type State struct {
	Base          key.Key
	Remote        key.Key
	RemoteCommit  key.Key
	HasRemote     bool
	RemoteVersion []byte
	SyncedAt      time.Time
}

// IsBranch reports whether the fetched reference names a commit.
func (s State) IsBranch() bool {
	return s.HasRemote && s.RemoteCommit.Type() == key.Commit
}

// RemoteKey is the key the reference named at the last fetch: the commit
// on a branch, else the tree.
func (s State) RemoteKey() key.Key {
	if s.IsBranch() {
		return s.RemoteCommit
	}
	return s.Remote
}

type stateJSON struct {
	Base          string `json:"base"`
	Remote        string `json:"remote"`
	RemoteCommit  string `json:"remote_commit,omitempty"`
	RemoteVersion string `json:"remote_version"`
	SyncedAt      string `json:"synced_at"`
}

// Tree is an open working copy. Store is its packstore; its lock makes the
// working copy single-user.
type Tree struct {
	Root   string
	Config Config
	State  State
	Store  *packstore.Store
}

// EmptyTree returns the empty directory object: its key and bytes.
func EmptyTree() (key.Key, []byte) {
	obj, err := fstree.EncodeDirLeaf(nil)
	if err != nil {
		panic(err) // a constant encoding
	}
	return obj.Key, obj.Bytes
}

// Find returns the root of the working copy containing dir: the nearest
// ancestor (dir included) holding a .dstore directory.
func Find(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	for {
		if fi, err := os.Stat(filepath.Join(abs, Dir)); err == nil && fi.IsDir() {
			return abs, nil
		}
		parent := filepath.Dir(abs)
		if parent == abs {
			return "", ErrNotWorkingCopy
		}
		abs = parent
	}
}

// Open finds and opens the working copy containing dir.
func Open(dir string) (*Tree, error) {
	t, err := openRaw(dir)
	if err != nil {
		return nil, err
	}
	st, err := loadState(t.Root)
	if err != nil {
		t.Close()
		return nil, err
	}
	t.State = st
	return t, nil
}

// openRaw opens a working copy without requiring its state file (a clone in
// progress).
func openRaw(dir string) (*Tree, error) {
	root, err := Find(dir)
	if err != nil {
		return nil, err
	}
	var cfg Config
	b, err := os.ReadFile(filepath.Join(root, Dir, configFile))
	if err != nil {
		return nil, fmt.Errorf("working copy %s: %w", root, err)
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("working copy %s: bad config: %w", root, err)
	}
	store, err := packstore.Open(filepath.Join(root, Dir, storeDir), packstore.WithSync(true))
	if err != nil {
		return nil, err
	}
	empty, _ := EmptyTree()
	return &Tree{Root: root, Config: cfg, State: State{Base: empty}, Store: store}, nil
}

// Create makes a fresh .dstore in dir (which must not already be a working
// copy or lie inside one), stores the empty tree and writes the config. The
// state file is not written: the caller writes it once the copy is complete.
func Create(dir string, cfg Config) (*Tree, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if root, err := Find(abs); err == nil {
		return nil, fmt.Errorf("%s is inside the working copy at %s", abs, root)
	}
	meta := filepath.Join(abs, Dir)
	if err := os.MkdirAll(meta, 0o755); err != nil {
		return nil, err
	}
	if err := writeJSON(filepath.Join(meta, configFile), cfg); err != nil {
		return nil, err
	}
	store, err := packstore.Open(filepath.Join(meta, storeDir), packstore.WithSync(true))
	if err != nil {
		return nil, err
	}
	empty, bytes := EmptyTree()
	if err := store.Put(empty, bytes); err != nil {
		store.Close()
		return nil, err
	}
	return &Tree{Root: abs, Config: cfg, State: State{Base: empty, SyncedAt: time.Now()}, Store: store}, nil
}

// Remove deletes dir's .dstore (a failed clone or init).
func Remove(dir string) error {
	return os.RemoveAll(filepath.Join(dir, Dir))
}

func (t *Tree) Close() error { return t.Store.Close() }

// Get reads an object's payload from the local packstore.
func (t *Tree) Get(k key.Key) ([]byte, error) { return t.Store.Get(k) }

func (t *Tree) SaveConfig() error {
	return writeJSON(filepath.Join(t.Root, Dir, configFile), t.Config)
}

func (t *Tree) SaveState() error {
	s := t.State
	j := stateJSON{Base: s.Base.String(), SyncedAt: s.SyncedAt.UTC().Format(time.RFC3339Nano)}
	if s.HasRemote {
		j.Remote = s.Remote.String()
		if s.IsBranch() {
			j.RemoteCommit = s.RemoteCommit.String()
		}
		j.RemoteVersion = hex.EncodeToString(s.RemoteVersion)
	}
	return writeJSON(filepath.Join(t.Root, Dir, stateFile), j)
}

func loadState(root string) (State, error) {
	b, err := os.ReadFile(filepath.Join(root, Dir, stateFile))
	if errors.Is(err, os.ErrNotExist) {
		return State{}, ErrIncomplete
	}
	if err != nil {
		return State{}, err
	}
	var j stateJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return State{}, fmt.Errorf("bad state file: %w", err)
	}
	var s State
	if s.Base, err = parseKey(j.Base); err != nil {
		return State{}, fmt.Errorf("bad state file: base: %w", err)
	}
	if j.Remote != "" {
		s.HasRemote = true
		if s.Remote, err = parseKey(j.Remote); err != nil {
			return State{}, fmt.Errorf("bad state file: remote: %w", err)
		}
		if j.RemoteCommit != "" {
			if s.RemoteCommit, err = parseKey(j.RemoteCommit); err != nil {
				return State{}, fmt.Errorf("bad state file: remote_commit: %w", err)
			}
			if s.RemoteCommit.Type() != key.Commit {
				return State{}, fmt.Errorf("bad state file: remote_commit %s is a %s", s.RemoteCommit, s.RemoteCommit.Type())
			}
		}
		if s.RemoteVersion, err = hex.DecodeString(j.RemoteVersion); err != nil {
			return State{}, fmt.Errorf("bad state file: remote_version: %w", err)
		}
	}
	if s.SyncedAt, err = time.Parse(time.RFC3339Nano, j.SyncedAt); err != nil {
		return State{}, fmt.Errorf("bad state file: synced_at: %w", err)
	}
	return s, nil
}

func parseKey(s string) (key.Key, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return key.Key{}, err
	}
	return key.Parse(b)
}

// writeJSON writes v to path through a temporary file and a rename.
func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
