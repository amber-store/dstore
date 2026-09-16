# Working Copies Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Git-like `clone`, `init`, `fetch`, `pull`, `push`, `status` and `diff` on one dstore reference, with a `.dstore/` working-copy directory holding a local packstore and a small state file.

**Architecture:** A new `worktree` package holds the working copy (`tree.go`), change detection (`change.go`, `scan.go`), the three-way merge (`merge.go`), the on-disk applier (`apply.go`), diff rendering (`diff.go`) and the cluster flows (`flow.go`); the CLI in `cmd/dstore/wc.go` is a thin layer over it. Trees are built with core's `ingest.Dir`, which gets a new `Exclude` option so `.dstore` is never ingested. Today's store-based push and pull move under `dstore store`.

**Tech Stack:** Go 1.26, `github.com/amber-store/core` (fstree, ingest, packstore, amberignore, cborx), `github.com/aymanbagabas/go-udiff` v0.4.1, `github.com/urfave/cli/v2`, `golang.org/x/sys/unix`.

**Spec:** `docs/superpowers/specs/2026-09-15-working-copy-design.md`

## Global Constraints

- Build/test environment on this Mac (Xcode license blocks cgo): every `go build`, `go test`, `go vet` in dstore must run with
  `CC=/Library/Developer/CommandLineTools/usr/bin/clang CGO_CFLAGS="-isysroot /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk" CGO_LDFLAGS="-isysroot /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk"`. Export these once per shell (referred to below as "the cgo env").
- The core repo clone is at `~/jobs-build/amber-store-core` (HEAD = v0.0.7); the core change goes on branch `ingest-exclude` there. Until core v0.0.8 exists, dstore's `go.mod` carries `replace github.com/amber-store/core => /Users/dragan/jobs-build/amber-store-core`; Task 12 removes it.
- Never commit binaries; `go build ./...` produces none, and `go build -o` outputs must be deleted.
- Commit messages end with the two attribution lines (Co-Authored-By and Claude-Session) given in the session.
- Paths inside a working copy are root-relative, `/`-separated. Entry names are validated: no empty component, no `.`/`..`, no `/`.
- The metadata directory name is `.dstore` (constant `worktree.Dir`).
- Racy-mtime window: 2 s. Diff size cap: 16 MiB per side. Binary sniff: a NUL byte in the first 8 KiB.

## File structure

| file | responsibility |
|---|---|
| core `ingest/ingest.go` | `Opts.Exclude`, `ScanWith` |
| core `ingest/parallel.go`, `ingest/scan.go` | apply the exclusion at the root directory |
| core `ingest/exclude_test.go` | tests for the above |
| `worktree/tree.go` | `.dstore` layout, `Config`, `State`, `Tree`, `Find`/`Open`/`Create`, saving, the empty tree |
| `worktree/change.go` | `Kind`, `Change`, entry comparison, `DiffTrees` |
| `worktree/scan.go`, `xattr_darwin.go`, `xattr_linux.go` | the working directory against a tree |
| `worktree/merge.go` | `Conflict`, `Merge` |
| `worktree/apply.go` | writing changes to disk |
| `worktree/diff.go` | `Source`, `Unified`, `Stat` |
| `worktree/flow.go` | `Clone`, `Init`, `Fetch`, `Pull`, `Push`, `Status`, `RefreshTicket` |
| `node/worktree_test.go` | end-to-end over the in-memory cluster |
| `cmd/dstore/wc.go` | the commands, ticket precedence |
| `cmd/dstore/client.go` | `store` group, `dialTicket` refactor |
| `cmd/dstore/main.go` | command list |
| `README.md`, `architecture/dstore.md` | docs |

---

### Task 1: core — `ingest.Opts.Exclude` and `ScanWith`

**Files:**
- Modify: `~/jobs-build/amber-store-core/ingest/ingest.go` (Opts, Objects, Scan)
- Modify: `~/jobs-build/amber-store-core/ingest/parallel.go` (pbuilder)
- Modify: `~/jobs-build/amber-store-core/ingest/scan.go` (scanner)
- Test: `~/jobs-build/amber-store-core/ingest/exclude_test.go`

**Interfaces:**
- Produces: `ingest.Opts.Exclude []string` — names directly under the root never ingested, regardless of `NoIgnore`; `ingest.ScanWith(dir string, opts Opts) (files, bytes int64, err error)`.

- [x] **Step 1: Branch**

```bash
cd ~/jobs-build/amber-store-core && git checkout main && git pull --ff-only && git checkout -b ingest-exclude
```

- [x] **Step 2: Write the failing tests**

`ingest/exclude_test.go`:

```go
package ingest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/amber-store/core/packstore"
)

// excludeFixture is a tree with a metadata dir at the root and a same-named
// dir one level down, which must not be excluded.
func excludeFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.WriteFile(filepath.Join(dir, "a.txt"), []byte("alpha"), 0o644))
	must(os.MkdirAll(filepath.Join(dir, ".meta"), 0o755))
	must(os.WriteFile(filepath.Join(dir, ".meta", "junk"), make([]byte, 1000), 0o644))
	must(os.MkdirAll(filepath.Join(dir, "sub", ".meta"), 0o755))
	must(os.WriteFile(filepath.Join(dir, "sub", ".meta", "keep"), []byte("keep"), 0o644))
	return dir
}

func TestExclude_SkipsRootNameOnly(t *testing.T) {
	dir := excludeFixture(t)
	st, err := packstore.Open(filepath.Join(t.TempDir(), "ps"), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	got, _, err := Dir(st, dir, Opts{Exclude: []string{".meta"}})
	if err != nil {
		t.Fatal(err)
	}
	// The same tree without the root .meta must give the same key.
	if err := os.RemoveAll(filepath.Join(dir, ".meta")); err != nil {
		t.Fatal(err)
	}
	want, _, err := Dir(st, dir, Opts{})
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("root with Exclude %s != root without .meta %s", got, want)
	}
}

func TestExclude_IgnoresNoIgnore(t *testing.T) {
	dir := excludeFixture(t)
	st, err := packstore.Open(filepath.Join(t.TempDir(), "ps"), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	got, _, err := Dir(st, dir, Opts{Exclude: []string{".meta"}, NoIgnore: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(dir, ".meta")); err != nil {
		t.Fatal(err)
	}
	want, _, err := Dir(st, dir, Opts{NoIgnore: true})
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("Exclude must apply with NoIgnore: %s != %s", got, want)
	}
}

func TestScanWith_HonorsExclude(t *testing.T) {
	dir := excludeFixture(t)
	files, bytes, err := ScanWith(dir, Opts{Exclude: []string{".meta"}})
	if err != nil {
		t.Fatal(err)
	}
	// a.txt (5 bytes) and sub/.meta/keep (4 bytes); the root .meta/junk is skipped.
	if files != 2 || bytes != 9 {
		t.Fatalf("files=%d bytes=%d, want 2 and 9", files, bytes)
	}
}
```

- [x] **Step 3: Run the tests to see them fail**

Run: `cd ~/jobs-build/amber-store-core && go test ./ingest -run 'TestExclude|TestScanWith' -v`
Expected: compile errors (`Opts` has no field `Exclude`; `ScanWith` undefined).

- [x] **Step 4: Implement**

In `ingest/ingest.go`, add to `Opts` after `Progress`:

```go
	// Exclude lists names directly under the root that are never ingested,
	// whatever NoIgnore says. It applies to the root directory only; the
	// same name deeper in the tree is ingested normally.
	Exclude []string
```

Add a helper below `jobs()`:

```go
// excludeSet turns Opts.Exclude into a set; nil when empty.
func (o Opts) excludeSet() map[string]bool {
	if len(o.Exclude) == 0 {
		return nil
	}
	m := make(map[string]bool, len(o.Exclude))
	for _, n := range o.Exclude {
		m[n] = true
	}
	return m
}
```

In `Objects`, change the directory `buildRoot` to pass the root and the set:

```go
		buildRoot = func(emit fstree.Emit) (key.Key, error) {
			b := &pbuilder{d: d, emit: emit, sem: make(chan struct{}, jobs), root: path, exclude: opts.excludeSet()}
			return b.buildDir(path, ign, emit)
		}
```

In `ingest/parallel.go`, add two fields to `pbuilder`:

```go
	// root and exclude implement Opts.Exclude: names in exclude are skipped
	// when path == root.
	root    string
	exclude map[string]bool
```

and in `pbuilder.buildDir` change the filter loop to:

```go
	for _, de := range ents {
		if path == b.root && b.exclude[de.Name()] {
			continue
		}
		if !ign.Ignored(de.Name(), de.IsDir()) {
			kept = append(kept, de)
		}
	}
```

In `ingest/scan.go`, add `ScanWith` after `Scan` and thread the exclusion through the scanner:

```go
// ScanWith is Scan with every relevant option of opts applied: NoIgnore,
// Jobs and Exclude, so that the totals match what Objects(dir, opts) reads.
func ScanWith(dir string, opts Opts) (files int64, bytes int64, err error) {
	var ign *amberignore.Matcher
	if !opts.NoIgnore {
		if ign, err = amberignore.Root(dir); err != nil {
			return 0, 0, err
		}
	}
	jobs := opts.jobs()
	s := &scanner{sem: make(chan struct{}, jobs), root: dir, exclude: opts.excludeSet()}
	s.walk(dir, ign)
	if e := s.err(); e != nil {
		return 0, 0, e
	}
	return s.files.Load(), s.bytes.Load(), nil
}
```

Add to the `scanner` struct:

```go
	root    string
	exclude map[string]bool
```

and at the top of the loop in `scanner.walk`:

```go
	for _, de := range ents {
		if dir == s.root && s.exclude[de.Name()] {
			continue
		}
		if ign.Ignored(de.Name(), de.IsDir()) {
			continue
		}
```

- [x] **Step 5: Run the ingest tests**

Run: `cd ~/jobs-build/amber-store-core && go test ./ingest && go vet ./ingest`
Expected: PASS.

- [x] **Step 6: Document and commit**

In core's `README.md`, in the ingest row of the package table, append: "`Opts.Exclude` skips names at the root (a working copy's metadata directory)." Then:

```bash
cd ~/jobs-build/amber-store-core && git add ingest README.md && git commit -m "ingest: Opts.Exclude skips root names; ScanWith

A caller that keeps its own metadata directory inside the tree it
ingests (dstore working copies keep .dstore) needs a way to leave it out
without editing the user's .amberignore. Exclude applies to the root
directory only and regardless of NoIgnore. ScanWith sizes a build with
the same options."
```

(append the attribution lines).

---

### Task 2: dstore dependencies

**Files:**
- Modify: `go.mod`, `go.sum`

- [x] **Step 1: Point core at the local branch and add go-udiff**

```bash
cd ~/amber-store/dstore
go mod edit -replace github.com/amber-store/core=/Users/dragan/jobs-build/amber-store-core
go get github.com/aymanbagabas/go-udiff@v0.4.1
go mod tidy
```

`go mod tidy` will drop go-udiff again because nothing imports it yet; that is fine, Task 8 runs `go get` again. What matters now is the `replace` line.

- [x] **Step 2: Verify the build still passes with the cgo env**

Run: `go build ./... && go vet ./...`
Expected: no output.

- [x] **Step 3: Commit**

```bash
git add go.mod go.sum && git commit -m "Build against the ingest-exclude branch of core (temporary replace)"
```

(append the attribution lines).

---

### Task 3: `worktree/tree.go` — the working copy on disk

**Files:**
- Create: `worktree/tree.go`
- Test: `worktree/tree_test.go`

**Interfaces:**
- Produces:
  - `const Dir = ".dstore"`
  - `type Getter = func(key.Key) ([]byte, error)`
  - `type Config struct { Ticket, Name, Relay string; NoRelay, NoDiscovery bool; User string }` (JSON tags `ticket`, `name`, `relay`, `no_relay`, `no_discovery`, `user`)
  - `type State struct { Base key.Key; Remote key.Key; HasRemote bool; RemoteVersion []byte; SyncedAt time.Time }`
  - `type Tree struct { Root string; Config Config; State State; Store *packstore.Store }`
  - `func Find(dir string) (string, error)`, `func Open(dir string) (*Tree, error)`, `func Create(dir string, cfg Config) (*Tree, error)`, `func Remove(dir string) error`
  - `func (t *Tree) Close() error`, `func (t *Tree) Get(k key.Key) ([]byte, error)`, `func (t *Tree) SaveState() error`, `func (t *Tree) SaveConfig() error`
  - `func EmptyTree() (key.Key, []byte)`
  - `var ErrNotWorkingCopy, ErrIncomplete error`

- [x] **Step 1: Write the failing tests**

`worktree/tree_test.go`:

```go
package worktree

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCreateOpenRoundTrip(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Ticket: "dstore1abc", Name: "trees/demo", NoRelay: true, User: "me"}
	tr, err := Create(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	empty, _ := EmptyTree()
	if tr.State.Base != empty {
		t.Fatalf("base after Create = %s, want the empty tree %s", tr.State.Base, empty)
	}
	if _, err := tr.Get(empty); err != nil {
		t.Fatalf("empty tree not stored: %v", err)
	}
	// Until SaveState, the copy is incomplete.
	tr.Close()
	if _, err := Open(dir); !errors.Is(err, ErrIncomplete) {
		t.Fatalf("Open before SaveState: err = %v, want ErrIncomplete", err)
	}
	tr, err = Create(dir, cfg) // re-creating an existing .dstore must fail
	if err == nil {
		tr.Close()
		t.Fatal("Create over an existing .dstore succeeded")
	}
	// Finish the copy.
	tr, err = openRaw(dir)
	if err != nil {
		t.Fatal(err)
	}
	tr.State.HasRemote = true
	tr.State.Remote = empty
	tr.State.RemoteVersion = []byte{1, 2, 3}
	tr.State.SyncedAt = time.Unix(1_700_000_000, 5).UTC()
	if err := tr.SaveState(); err != nil {
		t.Fatal(err)
	}
	tr.Close()

	sub := filepath.Join(dir, "a", "b")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	got, err := Open(sub) // found from a subdirectory
	if err != nil {
		t.Fatal(err)
	}
	defer got.Close()
	if got.Root != dir {
		t.Fatalf("Root = %q, want %q", got.Root, dir)
	}
	if got.Config != cfg {
		t.Fatalf("Config = %+v, want %+v", got.Config, cfg)
	}
	if !got.State.HasRemote || got.State.Remote != empty || string(got.State.RemoteVersion) != "\x01\x02\x03" || !got.State.SyncedAt.Equal(time.Unix(1_700_000_000, 5)) {
		t.Fatalf("State = %+v", got.State)
	}
}

func TestFindOutsideWorkingCopy(t *testing.T) {
	if _, err := Find(t.TempDir()); !errors.Is(err, ErrNotWorkingCopy) {
		t.Fatalf("err = %v, want ErrNotWorkingCopy", err)
	}
}

func TestRemove(t *testing.T) {
	dir := t.TempDir()
	tr, err := Create(dir, Config{Name: "x"})
	if err != nil {
		t.Fatal(err)
	}
	tr.Close()
	if err := Remove(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, Dir)); !os.IsNotExist(err) {
		t.Fatalf(".dstore still there: %v", err)
	}
}
```

- [x] **Step 2: Run to see it fail**

Run: `go test ./worktree`
Expected: compile errors (package has no Go files).

- [x] **Step 3: Implement `worktree/tree.go`**

```go
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
type State struct {
	Base          key.Key
	Remote        key.Key
	HasRemote     bool
	RemoteVersion []byte
	SyncedAt      time.Time
}

type stateJSON struct {
	Base          string `json:"base"`
	Remote        string `json:"remote"`
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
```

- [x] **Step 4: Run the tests**

Run: `go test ./worktree -v`
Expected: PASS (3 tests).

- [x] **Step 5: Commit**

```bash
git add worktree && git commit -m "worktree: the working copy layout, config and state"
```

(append the attribution lines).

---

### Task 4: `worktree/change.go` — changes and the tree diff

**Files:**
- Create: `worktree/change.go`
- Test: `worktree/change_test.go`

**Interfaces:**
- Produces:
  - `type Kind int` with `Added, Deleted, Modified, TypeChanged, ModeChanged, MetaChanged`; `func (k Kind) String() string` → `new`, `deleted`, `modified`, `type`, `mode`, `meta`
  - `type Change struct { Path string; Kind Kind; Old, New *fstree.Entry }`
  - `func IsDir(e *fstree.Entry) bool`, `func TypeName(mode uint64) string`, `func Compare(old, new *fstree.Entry) (Kind, bool)`, `func SameContent(a, b *fstree.Entry) bool`, `func Equivalent(a, b *fstree.Entry) bool`
  - `func DiffTrees(get Getter, a, b key.Key) ([]Change, error)`
  - `func expand(get Getter, prefix string, e *fstree.Entry, kind Kind, out *[]Change) error` and `func expandChildren(get Getter, p string, e *fstree.Entry, kind Kind, out *[]Change) error` (unexported, reused by scan). A type change of a directory is followed by Deleted changes for its former contents; a path that became a directory by Added changes for its new contents.
  - `func joinPath(prefix, name string) string`

- [x] **Step 1: Write the failing tests**

`worktree/change_test.go`:

```go
package worktree

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"golang.org/x/sys/unix"
)

// fixture builds a directory with a few files and ingests it into st,
// returning the source dir and the root key.
func openStore(t *testing.T) *packstore.Store {
	t.Helper()
	st, err := packstore.Open(filepath.Join(t.TempDir(), "ps"), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func writeFile(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func ingestDir(t *testing.T, st *packstore.Store, dir string) key.Key {
	t.Helper()
	root, _, err := ingest.Dir(st, dir, ingest.Opts{Jobs: 2})
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func kinds(changes []Change) map[string]Kind {
	m := map[string]Kind{}
	for _, c := range changes {
		m[c.Path] = c.Kind
	}
	return m
}

func TestDiffTrees_EveryKind(t *testing.T) {
	st := openStore(t)
	a := t.TempDir()
	writeFile(t, filepath.Join(a, "same.txt"), "same", 0o644)
	writeFile(t, filepath.Join(a, "edit.txt"), "one", 0o644)
	writeFile(t, filepath.Join(a, "gone.txt"), "bye", 0o644)
	writeFile(t, filepath.Join(a, "mode.sh"), "#!/bin/sh", 0o644)
	writeFile(t, filepath.Join(a, "sub", "deep.txt"), "deep", 0o644)
	writeFile(t, filepath.Join(a, "olddir", "x"), "x", 0o644)
	if err := os.Symlink("t1", filepath.Join(a, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("was-link", filepath.Join(a, "becomes-file")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(a, "flip", "inner"), "inner", 0o644) // a dir that becomes a file
	ka := ingestDir(t, st, a)

	b := t.TempDir()
	writeFile(t, filepath.Join(b, "same.txt"), "same", 0o644)
	writeFile(t, filepath.Join(b, "edit.txt"), "two", 0o644)
	writeFile(t, filepath.Join(b, "new.txt"), "hi", 0o644)
	writeFile(t, filepath.Join(b, "mode.sh"), "#!/bin/sh", 0o755)
	writeFile(t, filepath.Join(b, "sub", "deep.txt"), "deep", 0o644)
	writeFile(t, filepath.Join(b, "newdir", "y"), "y", 0o644)
	writeFile(t, filepath.Join(b, "becomes-file"), "now a file", 0o644)
	writeFile(t, filepath.Join(b, "flip"), "flat", 0o644)
	if err := os.Symlink("t2", filepath.Join(b, "link")); err != nil {
		t.Fatal(err)
	}
	kb := ingestDir(t, st, b)

	changes, err := DiffTrees(st.Get, ka, kb)
	if err != nil {
		t.Fatal(err)
	}
	got := kinds(changes)
	want := map[string]Kind{
		"edit.txt": Modified, "gone.txt": Deleted, "new.txt": Added, "mode.sh": ModeChanged,
		"link": Modified, "becomes-file": TypeChanged,
		"olddir": Deleted, "olddir/x": Deleted, "newdir": Added, "newdir/y": Added,
		"flip": TypeChanged, "flip/inner": Deleted,
	}
	for p, k := range want {
		if got[p] != k {
			t.Errorf("%s: kind %v, want %v", p, got[p], k)
		}
	}
	for p := range got {
		if _, ok := want[p]; !ok && p != "same.txt" && p != "sub" && p != "sub/deep.txt" {
			t.Errorf("unexpected change %s: %v", p, got[p])
		}
		// same.txt, sub and sub/deep.txt may only appear as metadata-only
		// (mtimes differ between the two fixtures), never as content changes.
		if (p == "same.txt" || p == "sub" || p == "sub/deep.txt") && got[p] != MetaChanged {
			t.Errorf("%s: kind %v, want at most MetaChanged", p, got[p])
		}
	}
	// Order is bytewise by path, an added directory before its children.
	for i := 1; i < len(changes); i++ {
		if changes[i-1].Path >= changes[i].Path {
			t.Errorf("out of order: %s then %s", changes[i-1].Path, changes[i].Path)
		}
	}
}

func TestDiffTrees_PrunesEqualSubtrees(t *testing.T) {
	st := openStore(t)
	a := t.TempDir()
	writeFile(t, filepath.Join(a, "sub", "x"), "x", 0o644)
	writeFile(t, filepath.Join(a, "top"), "1", 0o644)
	ka := ingestDir(t, st, a)
	writeFile(t, filepath.Join(a, "top"), "2", 0o644)
	kb := ingestDir(t, st, a)
	// Make sub's objects unreadable: a pruned subtree is never fetched.
	calls := 0
	get := func(k key.Key) ([]byte, error) {
		calls++
		return st.Get(k)
	}
	changes, err := DiffTrees(get, ka, kb)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].Path != "top" || changes[0].Kind != Modified {
		t.Fatalf("changes = %+v", changes)
	}
	if calls != 2 {
		t.Fatalf("get called %d times, want 2 (the two roots only)", calls)
	}
}

func TestCompare(t *testing.T) {
	file := func(ck byte, mode uint64, mtime int64) *fstree.Entry {
		k := make([]byte, 32)
		k[1] = ck
		return &fstree.Entry{Name: []byte("f"), Mode: unix.S_IFREG | mode, Mtime: mtime, ContentKey: k}
	}
	cases := []struct {
		name   string
		a, b   *fstree.Entry
		kind   Kind
		differ bool
	}{
		{"equal", file(1, 0o644, 1), file(1, 0o644, 1), 0, false},
		{"content", file(1, 0o644, 1), file(2, 0o644, 1), Modified, true},
		{"mode", file(1, 0o644, 1), file(1, 0o755, 1), ModeChanged, true},
		{"mtime", file(1, 0o644, 1), file(1, 0o644, 2), MetaChanged, true},
		{"type", file(1, 0o644, 1), &fstree.Entry{Name: []byte("f"), Mode: unix.S_IFLNK | 0o777, LinkTarget: []byte("x")}, TypeChanged, true},
	}
	for _, c := range cases {
		k, ok := Compare(c.a, c.b)
		if ok != c.differ || (ok && k != c.kind) {
			t.Errorf("%s: (%v, %v), want (%v, %v)", c.name, k, ok, c.kind, c.differ)
		}
	}
}
```

(add `"github.com/amber-store/core/fstree"` to the imports.)

- [x] **Step 2: Run to see it fail**

Run: `go test ./worktree -run 'TestDiffTrees|TestCompare'`
Expected: compile errors (undefined: Kind, DiffTrees, Compare).

- [x] **Step 3: Implement `worktree/change.go`**

```go
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
// directory yields a change for itself followed by one per path below it.
func DiffTrees(get Getter, a, b key.Key) ([]Change, error) {
	var out []Change
	if a == b {
		return nil, nil
	}
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
```

- [x] **Step 4: Run the tests**

Run: `go test ./worktree -run 'TestDiffTrees|TestCompare' -v`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add worktree && git commit -m "worktree: changes and the tree-to-tree diff"
```

(append the attribution lines).

---

### Task 5: `worktree/scan.go` — the working directory against a tree

**Files:**
- Create: `worktree/scan.go`, `worktree/xattr.go`, `worktree/xattr_darwin.go`, `worktree/xattr_linux.go`
- Test: `worktree/scan_test.go`

**Interfaces:**
- Consumes: `Change`, `Kind`, `Compare`, `IsDir`, `expand`, `expandChildren`, `joinPath` (Task 4); `Dir`, `Getter` (Task 3).
- Produces: `const RacyWindow = 2 * time.Second`; `func Scan(root string, base key.Key, get Getter, syncedAt time.Time, jobs int) ([]Change, error)` — changes from the tree `base` to the directory `root`, with `Old` the base entry and `New` the entry as it is on disk (content key computed, xattrs encoded as ingest would). `syncedAt` must be a real time (the flows pass the state's `SyncedAt`; `diff --remote` passes `time.Now()`).

- [x] **Step 1: Write the failing tests**

`worktree/scan_test.go`:

```go
package worktree

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// scanFixture writes a small tree, ingests it and returns the dir and base.
func scanFixture(t *testing.T, st *packstore.Store) (string, key.Key) {
	t.Helper()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "a.txt"), "alpha", 0o644)
	writeFile(t, filepath.Join(dir, "run.sh"), "#!/bin/sh", 0o644)
	writeFile(t, filepath.Join(dir, "sub", "b.txt"), "beta", 0o644)
	writeFile(t, filepath.Join(dir, "gone.txt"), "bye", 0o644)
	if err := os.Symlink("a.txt", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	return dir, ingestDir(t, st, dir)
}

func scan(t *testing.T, dir string, base key.Key, st *packstore.Store, syncedAt time.Time) map[string]Kind {
	t.Helper()
	changes, err := Scan(dir, base, st.Get, syncedAt, 2)
	if err != nil {
		t.Fatal(err)
	}
	return kinds(changes)
}

func TestScan_CleanTreeHasNoChanges(t *testing.T) {
	st := openStore(t)
	dir, base := scanFixture(t, st)
	if got := scan(t, dir, base, st, time.Now()); len(got) != 0 {
		t.Fatalf("changes on a clean tree: %v", got)
	}
}

func TestScan_EveryKind(t *testing.T) {
	st := openStore(t)
	dir, base := scanFixture(t, st)
	writeFile(t, filepath.Join(dir, "a.txt"), "alpha 2", 0o644)                      // modified
	if err := os.Chmod(filepath.Join(dir, "run.sh"), 0o755); err != nil {            // mode
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "gone.txt")); err != nil {                // deleted
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "new.txt"), "new", 0o644)                        // new
	writeFile(t, filepath.Join(dir, "newdir", "c.txt"), "c", 0o644)                  // new dir + file
	past := time.Unix(1_600_000_000, 0)
	if err := os.Chtimes(filepath.Join(dir, "sub", "b.txt"), past, past); err != nil { // meta
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("run.sh", filepath.Join(dir, "link")); err != nil {         // retargeted
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, Dir), 0o755); err != nil {              // metadata dir: invisible
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, Dir, "junk"), "x", 0o644)

	got := scan(t, dir, base, st, time.Now())
	want := map[string]Kind{
		"a.txt": Modified, "run.sh": ModeChanged, "gone.txt": Deleted, "new.txt": Added,
		"newdir": Added, "newdir/c.txt": Added, "sub/b.txt": MetaChanged, "link": Modified,
	}
	for p, k := range want {
		if got[p] != k {
			t.Errorf("%s: %v, want %v", p, got[p], k)
		}
	}
	for p := range got {
		if _, ok := want[p]; !ok && p != "sub" {
			t.Errorf("unexpected %s: %v", p, got[p])
		}
	}
}

func TestScan_IgnoredBasePathIsDeleted(t *testing.T) {
	st := openStore(t)
	dir, base := scanFixture(t, st)
	writeFile(t, filepath.Join(dir, ".amberignore"), "gone.txt\n", 0o644)
	got := scan(t, dir, base, st, time.Now())
	if got["gone.txt"] != Deleted || got[".amberignore"] != Added {
		t.Fatalf("got %v", got)
	}
}

func TestScan_RacyMtime(t *testing.T) {
	st := openStore(t)
	dir, base := scanFixture(t, st)
	path := filepath.Join(dir, "a.txt")
	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	// Same size, same mtime, different bytes.
	writeFile(t, path, "ALPHA", 0o644)
	if err := os.Chtimes(path, fi.ModTime(), fi.ModTime()); err != nil {
		t.Fatal(err)
	}
	// A sync long after the edit trusts size+mtime: the edit is invisible.
	if got := scan(t, dir, base, st, time.Now().Add(time.Hour)); got["a.txt"] != 0 || len(got) != 0 {
		t.Fatalf("far-future syncedAt: %v, want no changes (the stat heuristic)", got)
	}
	// A sync within the window hashes the file.
	if got := scan(t, dir, base, st, time.Now()); got["a.txt"] != Modified {
		t.Fatalf("recent syncedAt: %v, want a.txt modified", got)
	}
}

func TestScan_TypeChangeExpands(t *testing.T) {
	st := openStore(t)
	dir, base := scanFixture(t, st)
	if err := os.Remove(filepath.Join(dir, "a.txt")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "a.txt", "inner"), "i", 0o644) // file → dir
	if err := os.RemoveAll(filepath.Join(dir, "sub")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "sub"), "flat", 0o644) // dir → file
	got := scan(t, dir, base, st, time.Now())
	want := map[string]Kind{"a.txt": TypeChanged, "a.txt/inner": Added, "sub": TypeChanged, "sub/b.txt": Deleted}
	for p, k := range want {
		if got[p] != k {
			t.Errorf("%s: %v, want %v", p, got[p], k)
		}
	}
}

func TestScan_Xattr(t *testing.T) {
	st := openStore(t)
	dir, base := scanFixture(t, st)
	path := filepath.Join(dir, "a.txt")
	if err := unix.Setxattr(path, "user.wc", []byte("1"), 0); err != nil {
		if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) {
			t.Skip("no xattr support here")
		}
		t.Fatal(err)
	}
	if got := scan(t, dir, base, st, time.Now()); got["a.txt"] != MetaChanged {
		t.Fatalf("got %v, want a.txt meta", got)
	}
}
```

(add `"github.com/amber-store/core/key"` and `"github.com/amber-store/core/packstore"` to the imports.)

- [x] **Step 2: Run to see it fail**

Run: `go test ./worktree -run TestScan`
Expected: compile error (undefined: Scan).

- [x] **Step 3: Implement the xattr readers**

`worktree/xattr.go` (mirrors core's `ingest`, whose readers are unexported):

```go
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
```

`worktree/xattr_darwin.go`:

```go
//go:build darwin

package worktree

import "golang.org/x/sys/unix"

// readXattrs reads a non-symlink entry's xattrs the way ingest does on macOS.
func readXattrs(path string) (map[string][]byte, error) {
	return readXattrsWith(path, unix.Listxattr, unix.Getxattr)
}

// setXattr sets one xattr on a non-symlink entry.
func setXattr(path, name string, value []byte) error {
	return unix.Setxattr(path, name, value, 0)
}
```

`worktree/xattr_linux.go`:

```go
//go:build linux

package worktree

import "golang.org/x/sys/unix"

// readXattrs reads a non-symlink entry's xattrs the way ingest does on Linux.
func readXattrs(path string) (map[string][]byte, error) {
	return readXattrsWith(path, unix.Llistxattr, unix.Lgetxattr)
}

// setXattr sets one xattr on a non-symlink entry.
func setXattr(path, name string, value []byte) error {
	return unix.Lsetxattr(path, name, value, 0)
}
```

- [x] **Step 4: Implement `worktree/scan.go`**

```go
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
```

- [x] **Step 5: Run the tests**

Run: `go test ./worktree -run TestScan -v`
Expected: PASS (TestScan_Xattr may skip on a filesystem without xattrs).

- [x] **Step 6: Commit**

```bash
git add worktree && git commit -m "worktree: scan the working directory against a tree"
```

(append the attribution lines).

---

### Task 6: `worktree/merge.go` — the three-way merge

**Files:**
- Create: `worktree/merge.go`
- Test: `worktree/merge_test.go`

**Interfaces:**
- Consumes: `Change`, `Kind`, `IsDir`, `Equivalent` (Task 4).
- Produces: `type Conflict struct { Path string; Local, Incoming Change }`; `func Merge(local, incoming []Change) (apply []Change, conflicts []Conflict)`.

- [x] **Step 1: Write the failing tests**

`worktree/merge_test.go`:

```go
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
		name           string
		local, incoming []Change
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
```

- [x] **Step 2: Run to see it fail**

Run: `go test ./worktree -run TestMerge`
Expected: compile error (undefined: Merge, Conflict).

- [x] **Step 3: Implement `worktree/merge.go`**

```go
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
```

- [x] **Step 4: Run the tests**

Run: `go test ./worktree -run TestMerge -v`
Expected: PASS. (In "remote retypes dir with local edits below", `d` conflicts by the retype rule and `d/x` by the per-path rule.)

- [x] **Step 5: Commit**

```bash
git add worktree && git commit -m "worktree: the three-way merge for pull"
```

(append the attribution lines).

---

### Task 7: `worktree/apply.go` — writing changes to disk

**Files:**
- Create: `worktree/apply.go`
- Test: `worktree/apply_test.go`

**Interfaces:**
- Consumes: `Change`, `Kind`, `IsDir` (Task 4); `setXattr` (Task 5); `Getter` (Task 3).
- Produces: `func Apply(root string, changes []Change, get Getter) error` — deletions first (deepest paths first; a directory only when empty), then additions and modifications in path order, directory permission bits and mtimes last. Regular files go through a temporary file (`.dstore-tmp-*` beside the target) and a rename. Sockets are skipped. Re-applying the same list is a no-op. Refuses unsafe names and writes through symlinked ancestors.

- [x] **Step 1: Write the failing tests**

`worktree/apply_test.go`:

```go
package worktree

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"golang.org/x/sys/unix"
)

// applyFixtureA is the first version of a tree, with every entry type the
// applier handles except devices.
func applyFixtureA(t *testing.T) string {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "keep.txt"), "keep", 0o644)
	writeFile(t, filepath.Join(dir, "edit.txt"), "one", 0o644)
	writeFile(t, filepath.Join(dir, "gone.txt"), "bye", 0o644)
	writeFile(t, filepath.Join(dir, "run.sh"), "#!/bin/sh", 0o644)
	writeFile(t, filepath.Join(dir, "sub", "deep.txt"), "deep", 0o600)
	writeFile(t, filepath.Join(dir, "olddir", "x"), "x", 0o644)
	writeFile(t, filepath.Join(dir, "flip", "inner"), "inner", 0o644)
	writeFile(t, filepath.Join(dir, "flop"), "flop", 0o644)
	if err := os.Symlink("keep.txt", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(dir, "pipe"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Unix(1_600_000_000, 123456789)
	for _, p := range []string{"keep.txt", "edit.txt", "sub/deep.txt", "sub", "run.sh"} {
		if err := os.Chtimes(filepath.Join(dir, p), old, old); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// applyFixtureB is the second version: edits, deletions, additions, a
// chmod, a retargeted link, a directory that became a file and a file that
// became a directory, and a read-only directory.
func applyFixtureB(t *testing.T) string {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "keep.txt"), "keep", 0o644)
	writeFile(t, filepath.Join(dir, "edit.txt"), "two", 0o644)
	writeFile(t, filepath.Join(dir, "run.sh"), "#!/bin/sh", 0o755)
	writeFile(t, filepath.Join(dir, "sub", "deep.txt"), "deep", 0o600)
	writeFile(t, filepath.Join(dir, "newdir", "y"), "y", 0o644)
	writeFile(t, filepath.Join(dir, "flip"), "flat", 0o644)
	writeFile(t, filepath.Join(dir, "flop", "inner"), "inner", 0o644)
	writeFile(t, filepath.Join(dir, "ro", "child"), "c", 0o644)
	if err := os.Symlink("edit.txt", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(dir, "pipe"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Unix(1_600_000_000, 123456789)
	for _, p := range []string{"keep.txt", "sub/deep.txt", "sub"} {
		if err := os.Chtimes(filepath.Join(dir, p), old, old); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(filepath.Join(dir, "ro"), 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(dir, "ro"), 0o700) })
	return dir
}

func TestApply_CloneThenUpdateReproducesTrees(t *testing.T) {
	st := openStore(t)
	a := applyFixtureA(t)
	ka := ingestDir(t, st, a)
	empty, _ := EmptyTree()
	if err := st.Put(empty, func() []byte { _, b := EmptyTree(); return b }()); err != nil {
		t.Fatal(err)
	}

	work := t.TempDir()
	initial, err := DiffTrees(st.Get, empty, ka)
	if err != nil {
		t.Fatal(err)
	}
	if err := Apply(work, initial, st.Get); err != nil {
		t.Fatalf("initial apply: %v", err)
	}
	if got := ingestDir(t, st, work); got != ka {
		t.Fatalf("after clone, work ingests to %s, want %s", got, ka)
	}

	b := applyFixtureB(t)
	kb := ingestDir(t, st, b)
	update, err := DiffTrees(st.Get, ka, kb)
	if err != nil {
		t.Fatal(err)
	}
	if err := Apply(work, update, st.Get); err != nil {
		t.Fatalf("update apply: %v", err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(work, "ro"), 0o700) })
	if got := ingestDir(t, st, work); got != kb {
		t.Fatalf("after update, work ingests to %s, want %s", got, kb)
	}
	// Re-applying is a no-op.
	if err := Apply(work, update, st.Get); err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if got := ingestDir(t, st, work); got != kb {
		t.Fatalf("after re-apply, work ingests to %s, want %s", got, kb)
	}
	if fi, err := os.Lstat(filepath.Join(work, "ro")); err != nil || fi.Mode().Perm() != 0o500 {
		t.Fatalf("read-only dir: %v %v", fi, err)
	}
}

func TestApply_KeepsNonEmptyDirectoryOnDelete(t *testing.T) {
	st := openStore(t)
	a := applyFixtureA(t)
	ka := ingestDir(t, st, a)
	work := t.TempDir()
	empty, _ := EmptyTree()
	_, eb := EmptyTree()
	st.Put(empty, eb)
	initial, _ := DiffTrees(st.Get, empty, ka)
	if err := Apply(work, initial, st.Get); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(work, "olddir", "mine"), "mine", 0o644)
	if err := os.RemoveAll(filepath.Join(a, "olddir")); err != nil {
		t.Fatal(err)
	}
	kb := ingestDir(t, st, a)
	update, _ := DiffTrees(st.Get, ka, kb)
	if err := Apply(work, update, st.Get); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(work, "olddir", "mine")); err != nil {
		t.Fatalf("local file under a remotely deleted dir was lost: %v", err)
	}
	if _, err := os.Stat(filepath.Join(work, "olddir", "x")); !os.IsNotExist(err) {
		t.Fatalf("olddir/x should be gone: %v", err)
	}
}

func TestApply_RefusesUnsafePaths(t *testing.T) {
	work := t.TempDir()
	e := &fstree.Entry{Name: []byte("x"), Mode: unix.S_IFREG | 0o644}
	for _, p := range []string{"../x", "a/../x", "a//x", "./x", ""} {
		if err := Apply(work, []Change{{Path: p, Kind: Added, New: e}}, nil); err == nil {
			t.Errorf("path %q accepted", p)
		}
	}
}

func TestApply_RefusesSymlinkedAncestor(t *testing.T) {
	work := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(work, "lnk")); err != nil {
		t.Fatal(err)
	}
	blob, err := fstree.EncodeBlob([]byte("pwned"))
	if err != nil {
		t.Fatal(err)
	}
	get := func(k key.Key) ([]byte, error) { return blob.Bytes, nil }
	e := &fstree.Entry{Name: []byte("f"), Mode: unix.S_IFREG | 0o644, ContentKey: blob.Key[:]}
	err = Apply(work, []Change{{Path: "lnk/f", Kind: Added, New: e}}, get)
	if err == nil || !strings.Contains(err.Error(), "non-directory") {
		t.Fatalf("err = %v", err)
	}
	if leaked, _ := os.ReadDir(outside); len(leaked) != 0 {
		t.Fatalf("wrote through the link: %v", leaked)
	}
}
```

- [x] **Step 2: Run to see it fail**

Run: `go test ./worktree -run TestApply`
Expected: compile error (undefined: Apply).

- [x] **Step 3: Implement `worktree/apply.go`**

```go
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

// rejectSymlinkComponents fails if any existing component of path below
// root is not a real directory (a symlink could lead outside the copy).
func rejectSymlinkComponents(root, path string) error {
	for p := filepath.Dir(path); strings.HasPrefix(p, root+string(os.PathSeparator)); p = filepath.Dir(p) {
		fi, err := os.Lstat(p)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if !fi.IsDir() {
			return fmt.Errorf("refusing to write through non-directory %s", p)
		}
	}
	return nil
}

// clearTarget removes what is at t when its type differs from e's, or when
// it is a non-directory that a new version replaces; an existing directory
// of the right type is kept.
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
```

- [x] **Step 4: Run the tests**

Run: `go test ./worktree -run TestApply -v`
Expected: PASS. If `TestApply_CloneThenUpdateReproducesTrees` fails on the root key, diff the two ingests entry by entry (mtime and mode are the usual culprits; the fixtures set mtimes explicitly on the paths whose mtime must match, everything else is freshly written on both sides but compared through the tree that Apply reproduces, so mtimes come from the entries).

- [x] **Step 5: Commit**

```bash
git add worktree && git commit -m "worktree: apply changes to the working directory"
```

(append the attribution lines).

---

### Task 8: `worktree/diff.go` — unified diffs and stat

**Files:**
- Create: `worktree/diff.go`
- Test: `worktree/diff_test.go`
- Modify: `go.mod` (`go get github.com/aymanbagabas/go-udiff@v0.4.1`)

**Interfaces:**
- Consumes: `Change`, `Kind`, `IsDir` (Task 4); `Getter` (Task 3).
- Produces:
  - `type Source interface { Content(p string, e *fstree.Entry) ([]byte, error) }` — a regular file's bytes or a symlink's target; `ErrTooLarge` for a file over `MaxDiffBytes`; nil for other types.
  - `type TreeSource struct{ Get Getter }`, `type DiskSource struct{ Root string }`
  - `const MaxDiffBytes = 16 << 20`, `var ErrTooLarge error`
  - `func Unified(w io.Writer, changes []Change, old, new Source) error`
  - `func Stat(w io.Writer, changes []Change, old, new Source) error`

- [x] **Step 1: Write the failing tests**

`worktree/diff_test.go`:

```go
package worktree

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func diffFixtures(t *testing.T) (st *packstore.Store, a, b key.Key, bdir string) {
	st = openStore(t)
	adir := t.TempDir()
	writeFile(t, filepath.Join(adir, "edit.txt"), "one\ntwo\nthree\n", 0o644)
	writeFile(t, filepath.Join(adir, "bin"), "a\x00b", 0o644)
	writeFile(t, filepath.Join(adir, "gone.txt"), "bye\n", 0o644)
	writeFile(t, filepath.Join(adir, "run.sh"), "#!/bin/sh\n", 0o644)
	writeFile(t, filepath.Join(adir, "sub", "x"), "x\n", 0o644)
	if err := os.Symlink("t1", filepath.Join(adir, "link")); err != nil {
		t.Fatal(err)
	}
	a = ingestDir(t, st, adir)
	bdir = t.TempDir()
	writeFile(t, filepath.Join(bdir, "edit.txt"), "one\n2\nthree\n", 0o644)
	writeFile(t, filepath.Join(bdir, "bin"), "a\x00c", 0o644)
	writeFile(t, filepath.Join(bdir, "new.txt"), "hi\n", 0o644)
	writeFile(t, filepath.Join(bdir, "run.sh"), "#!/bin/sh\n", 0o755)
	writeFile(t, filepath.Join(bdir, "sub", "x"), "x\n", 0o644)
	if err := os.Chmod(filepath.Join(bdir, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("t2", filepath.Join(bdir, "link")); err != nil {
		t.Fatal(err)
	}
	b = ingestDir(t, st, bdir)
	return st, a, b, bdir
}

func TestUnified_TreeToTree(t *testing.T) {
	st, a, b, _ := diffFixtures(t)
	changes, err := DiffTrees(st.Get, a, b)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := Unified(&buf, changes, TreeSource{st.Get}, TreeSource{st.Get}); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{
		"diff a/edit.txt b/edit.txt\n--- a/edit.txt\n+++ b/edit.txt\n", "-two\n+2\n",
		"Binary files a/bin and b/bin differ\n",
		"--- /dev/null\n+++ b/new.txt\n", "+hi\n",
		"--- a/gone.txt\n+++ /dev/null\n", "-bye\n",
		"diff a/run.sh b/run.sh\nold mode 0644\nnew mode 0755\n",
		"diff a/link b/link\n", "-t1", "+t2",
		"diff a/sub b/sub\nold mode 0755\nnew mode 0700\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "--- a/sub\n") || strings.Contains(out, "--- a/run.sh\n") {
		t.Errorf("mode-only changes must have no body:\n%s", out)
	}
}

func TestUnified_TreeToDisk(t *testing.T) {
	st, a, _, bdir := diffFixtures(t)
	changes, err := Scan(bdir, a, st.Get, time.Now(), 2)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := Unified(&buf, changes, TreeSource{st.Get}, DiskSource{bdir}); err != nil {
		t.Fatal(err)
	}
	if out := buf.String(); !strings.Contains(out, "-two\n+2\n") || !strings.Contains(out, "+t2") {
		t.Errorf("disk side not read:\n%s", out)
	}
}

func TestStat(t *testing.T) {
	st, a, b, _ := diffFixtures(t)
	changes, _ := DiffTrees(st.Get, a, b)
	var buf bytes.Buffer
	if err := Stat(&buf, changes, TreeSource{st.Get}, TreeSource{st.Get}); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{" edit.txt | +1 -1\n", " bin | binary\n", " new.txt | +1 -0\n", " gone.txt | +0 -1\n", " link | +1 -1\n", "files changed"} {
		if !strings.Contains(out, want) {
			t.Errorf("stat lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "sub |") {
		t.Errorf("a directory mode change has no stat line:\n%s", out)
	}
}
```

(add `"github.com/amber-store/core/key"` and `"github.com/amber-store/core/packstore"` to the imports.)

- [x] **Step 2: Add the dependency and run to see it fail**

```bash
go get github.com/aymanbagabas/go-udiff@v0.4.1
go test ./worktree -run 'TestUnified|TestStat'
```

Expected: compile error (undefined: Unified, Stat, TreeSource, DiskSource).

- [x] **Step 3: Implement `worktree/diff.go`**

```go
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
```

- [x] **Step 4: Run the tests and tidy**

Run: `go mod tidy && go test ./worktree -v`
Expected: every `worktree` test passes.

- [x] **Step 5: Commit**

```bash
git add go.mod go.sum worktree && git commit -m "worktree: unified diffs and stat with go-udiff"
```

(append the attribution lines).

---

### Task 9: `worktree/flow.go` — the cluster flows, with end-to-end tests

**Files:**
- Create: `worktree/flow.go`
- Test: `node/worktree_test.go` (package `node_test`, using the harness in `node/cluster_test.go`: `cluster3(t)`, `defer h.close()`, `h.client(t, id)` with a client id byte not used by a node, e.g. 100)

**Interfaces:**
- Consumes: everything from Tasks 3–8; `client.Cluster` (`RefGet`, `PullTree`, `Push`, `View`), `client.ErrUnknownRef`, `client.CASMismatch`, `client.Cond`, `client.Progress`, `client.PullStats`, `client.PushStats`; `ingest.Dir` with `Exclude`.
- Produces:
  - `var ErrNoRemote, ErrRemoteMoved, ErrRemoteDeleted, ErrConflict, ErrRefChanged error`
  - `type FetchResult struct { Exists, UpToDate bool; Key key.Key; Stats client.PullStats }`
  - `type PullResult struct { Fetch FetchResult; UpToDate bool; Applied []Change; Conflicts []Conflict }`
  - `type PushResult struct { Root key.Key; Nothing, Recovered bool; Built packstore.WriteStats; Stats client.PushStats }`
  - `type RemoteState int` (`RemoteUpToDate`, `RemoteMoved`, `RemoteAbsent`); `type Status struct { Changes []Change; MetaOnly int; Remote RemoteState; Incoming []Change }`
  - `func Clone(ctx context.Context, cl *client.Cluster, dir string, cfg Config, prog client.Progress) (*Tree, FetchResult, error)`
  - `func Init(ctx context.Context, cl *client.Cluster, dir string, cfg Config, prog client.Progress) (*Tree, FetchResult, error)`
  - `func (t *Tree) Fetch(ctx, cl, prog) (FetchResult, error)`, `Pull(ctx, cl, force bool, jobs int, prog) (PullResult, error)`, `Push(ctx, cl, user string, force bool, jobs int, prog) (PushResult, error)`, `Status(jobs int) (Status, error)`
  - `func TicketFromView(v *view.View) ticket.Ticket`, `func (t *Tree) RefreshTicket(cl *client.Cluster) error`

- [x] **Step 1: Write the failing end-to-end tests**

`node/worktree_test.go`:

```go
package node_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/amber-store/dstore/worktree"
)

func writeWC(t *testing.T, root, rel, content string) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readWC(t *testing.T, root, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("%s: %v", rel, err)
	}
	return string(b)
}

func wcKinds(cs []worktree.Change) map[string]worktree.Kind {
	m := map[string]worktree.Kind{}
	for _, c := range cs {
		m[c.Path] = c.Kind
	}
	return m
}

func TestWorktreeInitPushCloneEditPull(t *testing.T) {
	h := cluster3(t)
	defer h.close()
	ctx := context.Background()
	ca := h.client(t, 100)
	defer ca.Close()

	// A: an existing directory becomes the working copy of a new name.
	a := t.TempDir()
	writeWC(t, a, "hello.txt", "hello\n")
	writeWC(t, a, "sub/deep.txt", "deep\n")
	ta, fr, err := worktree.Init(ctx, ca, a, worktree.Config{Name: "trees/wc"}, nil)
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	defer ta.Close()
	if fr.Exists {
		t.Fatal("a new name must not exist")
	}
	st, err := ta.Status(2)
	if err != nil {
		t.Fatal(err)
	}
	if k := wcKinds(st.Changes); k["hello.txt"] != worktree.Added || k["sub"] != worktree.Added || k["sub/deep.txt"] != worktree.Added || st.Remote != worktree.RemoteAbsent {
		t.Fatalf("status after init: %+v", st)
	}
	pr, err := ta.Push(ctx, ca, "tester", false, 2, nil)
	if err != nil || pr.Nothing {
		t.Fatalf("push: %v %+v", err, pr)
	}
	if st, _ = ta.Status(2); len(st.Changes) != 0 || st.MetaOnly != 0 || st.Remote != worktree.RemoteUpToDate {
		t.Fatalf("status after push: %+v", st)
	}
	if pr, err = ta.Push(ctx, ca, "tester", false, 2, nil); err != nil || !pr.Nothing {
		t.Fatalf("second push: %v %+v", err, pr)
	}

	// B: a clone sees the push.
	cb := h.client(t, 101)
	defer cb.Close()
	b := filepath.Join(t.TempDir(), "wc")
	tb, _, err := worktree.Clone(ctx, cb, b, worktree.Config{Name: "trees/wc"}, nil)
	if err != nil {
		t.Fatalf("clone: %v", err)
	}
	defer tb.Close()
	if readWC(t, b, "hello.txt") != "hello\n" || readWC(t, b, "sub/deep.txt") != "deep\n" {
		t.Fatal("clone content")
	}
	if st, _ = tb.Status(2); len(st.Changes) != 0 || st.Remote != worktree.RemoteUpToDate {
		t.Fatalf("status after clone: %+v", st)
	}

	// B edits and pushes.
	writeWC(t, b, "hello.txt", "hello world\n")
	writeWC(t, b, "new.txt", "n\n")
	if err := os.Remove(filepath.Join(b, "sub", "deep.txt")); err != nil {
		t.Fatal(err)
	}
	st, _ = tb.Status(2)
	if k := wcKinds(st.Changes); k["hello.txt"] != worktree.Modified || k["new.txt"] != worktree.Added || k["sub/deep.txt"] != worktree.Deleted || len(st.Changes) != 3 {
		t.Fatalf("status after edits: %+v", st)
	}
	if _, err := tb.Push(ctx, cb, "tester", false, 2, nil); err != nil {
		t.Fatalf("push from B: %v", err)
	}

	// A fetches, sees the move, pulls.
	if fr, err = ta.Fetch(ctx, ca, nil); err != nil || fr.UpToDate {
		t.Fatalf("fetch: %v %+v", err, fr)
	}
	st, _ = ta.Status(2)
	if k := wcKinds(st.Incoming); st.Remote != worktree.RemoteMoved || k["hello.txt"] != worktree.Modified || k["new.txt"] != worktree.Added || k["sub/deep.txt"] != worktree.Deleted {
		t.Fatalf("status after fetch: %+v", st)
	}
	plr, err := ta.Pull(ctx, ca, false, 2, nil)
	if err != nil || len(plr.Conflicts) != 0 {
		t.Fatalf("pull: %v %+v", err, plr)
	}
	if readWC(t, a, "hello.txt") != "hello world\n" || readWC(t, a, "new.txt") != "n\n" {
		t.Fatal("pull content")
	}
	if _, err := os.Stat(filepath.Join(a, "sub", "deep.txt")); !os.IsNotExist(err) {
		t.Fatalf("sub/deep.txt should be gone: %v", err)
	}
	if st, _ = ta.Status(2); len(st.Changes) != 0 || st.Remote != worktree.RemoteUpToDate {
		t.Fatalf("status after pull: %+v", st)
	}
	if plr, err = ta.Pull(ctx, ca, false, 2, nil); err != nil || !plr.UpToDate {
		t.Fatalf("second pull: %v %+v", err, plr)
	}
}

// cloneTwo pushes a source tree under name and clones it twice.
func cloneTwo(t *testing.T, h *harness, name string) (ta, tb *worktree.Tree, ca, cb *client.Cluster) {
	t.Helper()
	ctx := context.Background()
	c0 := h.client(t, 102)
	defer c0.Close()
	src := t.TempDir()
	writeWC(t, src, "f.txt", "base\n")
	writeWC(t, src, "other.txt", "other\n")
	t0, _, err := worktree.Init(ctx, c0, src, worktree.Config{Name: name}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := t0.Push(ctx, c0, "tester", false, 2, nil); err != nil {
		t.Fatal(err)
	}
	t0.Close()
	ca, cb = h.client(t, 103), h.client(t, 104)
	if ta, _, err = worktree.Clone(ctx, ca, filepath.Join(t.TempDir(), "a"), worktree.Config{Name: name}, nil); err != nil {
		t.Fatal(err)
	}
	if tb, _, err = worktree.Clone(ctx, cb, filepath.Join(t.TempDir(), "b"), worktree.Config{Name: name}, nil); err != nil {
		t.Fatal(err)
	}
	return ta, tb, ca, cb
}

func TestWorktreeConflict(t *testing.T) {
	h := cluster3(t)
	defer h.close()
	ctx := context.Background()
	ta, tb, ca, cb := cloneTwo(t, h, "trees/conflict")
	defer ta.Close()
	defer tb.Close()
	defer ca.Close()
	defer cb.Close()

	writeWC(t, ta.Root, "f.txt", "A\n")
	writeWC(t, tb.Root, "f.txt", "B\n")
	writeWC(t, tb.Root, "b.txt", "b\n")
	if _, err := ta.Push(ctx, ca, "a", false, 2, nil); err != nil {
		t.Fatalf("push A: %v", err)
	}
	// B has not fetched: the cluster refuses its push.
	if _, err := tb.Push(ctx, cb, "b", false, 2, nil); !errors.Is(err, worktree.ErrRefChanged) {
		t.Fatalf("push B: err = %v, want ErrRefChanged", err)
	}
	plr, err := tb.Pull(ctx, cb, false, 2, nil)
	if !errors.Is(err, worktree.ErrConflict) || len(plr.Conflicts) != 1 || plr.Conflicts[0].Path != "f.txt" {
		t.Fatalf("pull B: %v %+v", err, plr)
	}
	if readWC(t, tb.Root, "f.txt") != "B\n" {
		t.Fatal("a refused pull must not touch the directory")
	}
	if plr, err = tb.Pull(ctx, cb, true, 2, nil); err != nil {
		t.Fatalf("forced pull: %v", err)
	}
	if readWC(t, tb.Root, "f.txt") != "A\n" || readWC(t, tb.Root, "b.txt") != "b\n" {
		t.Fatal("forced pull: remote side on the conflict, local addition kept")
	}
	if _, err := tb.Push(ctx, cb, "b", false, 2, nil); err != nil {
		t.Fatalf("push B after pull: %v", err)
	}
	if _, err := ta.Pull(ctx, ca, false, 2, nil); err != nil {
		t.Fatalf("pull A: %v", err)
	}
	if readWC(t, ta.Root, "f.txt") != "A\n" || readWC(t, ta.Root, "b.txt") != "b\n" {
		t.Fatal("A after pull")
	}
}

func TestWorktreePushRecoversAfterLostState(t *testing.T) {
	h := cluster3(t)
	defer h.close()
	ctx := context.Background()
	c := h.client(t, 105)
	defer c.Close()
	dir := t.TempDir()
	writeWC(t, dir, "f.txt", "1\n")
	tr, _, err := worktree.Init(ctx, c, dir, worktree.Config{Name: "trees/recover"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	if _, err := tr.Push(ctx, c, "tester", false, 2, nil); err != nil {
		t.Fatal(err)
	}
	writeWC(t, dir, "f.txt", "2\n")
	before := tr.State
	pr, err := tr.Push(ctx, c, "tester", false, 2, nil)
	if err != nil || pr.Recovered {
		t.Fatalf("push: %v %+v", err, pr)
	}
	// The reference write landed but the state write was lost.
	tr.State = before
	if err := tr.SaveState(); err != nil {
		t.Fatal(err)
	}
	pr, err = tr.Push(ctx, c, "tester", false, 2, nil)
	if err != nil || !pr.Recovered {
		t.Fatalf("retried push: %v %+v", err, pr)
	}
	if st, _ := tr.Status(2); len(st.Changes) != 0 || st.Remote != worktree.RemoteUpToDate {
		t.Fatalf("status after recovery: %+v", st)
	}
}
```

(add `"github.com/amber-store/dstore/client"` to the imports.)

- [x] **Step 2: Run to see it fail**

Run: `go test ./node -run TestWorktree`
Expected: compile errors (undefined: worktree.Init, Clone, …).

- [x] **Step 3: Implement `worktree/flow.go`**

```go
package worktree

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"

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
	Exists   bool // the reference exists on the cluster
	UpToDate bool // its tree was already the stored remote
	Key      key.Key
	Stats    client.PullStats
}

// fetch reads the reference and pulls its tree into the packstore, updating
// the state in memory only.
func (t *Tree) fetch(ctx context.Context, cl *client.Cluster, prog client.Progress) (FetchResult, error) {
	var r FetchResult
	ref, err := cl.RefGet(ctx, t.Config.Name)
	if errors.Is(err, client.ErrUnknownRef) {
		t.State.HasRemote, t.State.Remote, t.State.RemoteVersion = false, key.Key{}, nil
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
	if t.State.HasRemote && t.State.Remote == k {
		r.UpToDate = true
	} else if err := cl.PullTree(ctx, t.Store, k, &r.Stats, prog); err != nil {
		return r, err
	}
	t.State.HasRemote, t.State.Remote, t.State.RemoteVersion = true, k, ref.Version
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
	Nothing   bool // the tree equals base and base is what the cluster holds
	Recovered bool // the cluster already held this tree from an interrupted push
	Built     packstore.WriteStats
	Stats     client.PushStats
}

// Push builds the working directory's tree, uploads it and writes the
// reference under compare-and-swap on the stored remote version. It refuses
// when base and remote differ (a fetch showed the cluster moved) unless
// force, which replaces the reference unconditionally.
func (t *Tree) Push(ctx context.Context, cl *client.Cluster, user string, force bool, jobs int, prog client.Progress) (PushResult, error) {
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
	cond := client.Cond{Force: force}
	if !force {
		cond.Versioned = true
		cond.ExpectedVersion = t.State.RemoteVersion // nil: the name must be new
	}
	ps, err := cl.Push(ctx, t.Store, root, t.Config.Name, user, cond, prog)
	if err != nil {
		var cm *client.CASMismatch
		if !errors.As(err, &cm) {
			return r, err
		}
		cur, perr := key.Parse(cm.Current)
		if !cm.HasCurrent || perr != nil || cur != root {
			return r, fmt.Errorf("%w (%v)", ErrRefChanged, err)
		}
		r.Recovered = true
		t.State.RemoteVersion = cm.Version
	} else {
		r.Stats = ps
		t.State.RemoteVersion = ps.Version
	}
	t.State.Base, t.State.Remote, t.State.HasRemote, t.State.SyncedAt = root, root, true, time.Now()
	return r, t.SaveState()
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
```

- [x] **Step 4: Run the tests**

Run: `go test ./node -run TestWorktree -v` (a three-node in-memory cluster takes a minute or two per test), then `go vet ./...`.
Expected: the three tests PASS.

- [x] **Step 5: Commit**

```bash
git add worktree node/worktree_test.go && git commit -m "worktree: clone, init, fetch, pull, push and status over a cluster"
```

(append the attribution lines).

---

### Task 10: the CLI — `dstore store push|pull` and the working-copy commands

**Files:**
- Modify: `cmd/dstore/client.go` (`storeCmd`, `dialTicket`, `relayModeOf`)
- Modify: `cmd/dstore/main.go` (command list, `localTicket` via `worktree.TicketFromView`)
- Create: `cmd/dstore/wc.go`
- Test: `cmd/dstore/wc_test.go`

**Interfaces:**
- Consumes: everything exported from `worktree`; `runTransfer`, `noTUIFlag`, `signalCtx`, `logger` (existing).
- Produces: commands `clone`, `init`, `fetch`, `pull`, `push`, `status`, `diff`, `store`; `func resolveTicket(flag, stored, env string) (string, error)`; `type netOpts struct { Relay string; NoRelay, NoDiscovery bool }`; `func dialTicket(ctx context.Context, t ticket.Ticket, n netOpts, log *slog.Logger) (*client.Cluster, error)`.

- [x] **Step 1: Write the failing test for ticket precedence**

`cmd/dstore/wc_test.go`:

```go
package main

import "testing"

func TestResolveTicket(t *testing.T) {
	cases := []struct{ flag, stored, env, want string }{
		{"f", "s", "e", "f"},
		{"", "s", "e", "s"},
		{"", "", "e", "e"},
	}
	for _, c := range cases {
		got, err := resolveTicket(c.flag, c.stored, c.env)
		if err != nil || got != c.want {
			t.Errorf("resolveTicket(%q,%q,%q) = %q, %v; want %q", c.flag, c.stored, c.env, got, err, c.want)
		}
	}
	if _, err := resolveTicket("", "", ""); err == nil {
		t.Error("no ticket anywhere must be an error")
	}
}
```

- [x] **Step 2: Run to see it fail**

Run: `go test ./cmd/dstore -run TestResolveTicket`
Expected: compile error (undefined: resolveTicket).

- [x] **Step 3: Refactor dialing in `cmd/dstore/client.go`**

Replace `dialClusterLog` and `relayMode` usage with:

```go
// netOpts are the connection options of a client command.
type netOpts struct {
	Relay       string
	NoRelay     bool
	NoDiscovery bool
}

func netOptsOf(c *cli.Context) netOpts {
	return netOpts{Relay: c.String("relay"), NoRelay: c.Bool("no-relay"), NoDiscovery: c.Bool("no-discovery")}
}

// dialClusterLog is dialCluster with the client logging to log.
func dialClusterLog(ctx context.Context, c *cli.Context, log *slog.Logger) (*client.Cluster, error) {
	var t ticket.Ticket
	var err error
	if s := c.String("ticket"); s != "" {
		t, err = ticket.Parse(s)
		if err != nil {
			return nil, err
		}
	} else if dir := c.String("store"); dir != "" {
		t, err = localTicket(dir)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("no cluster: set --ticket or $DSTORE_TICKET")
	}
	return dialTicket(ctx, t, netOptsOf(c), log)
}

// dialTicket connects to the cluster of t with an ephemeral identity.
func dialTicket(ctx context.Context, t ticket.Ticket, n netOpts, log *slog.Logger) (*client.Cluster, error) {
	sk, err := irohkey.GenerateSecretKey()
	if err != nil {
		return nil, err
	}
	rm, err := relayModeOf(n.Relay, n.NoRelay)
	if err != nil {
		return nil, err
	}
	ep, err := transport.BindIroh(ctx, transport.IrohConfig{SecretKey: sk, RelayMode: rm, Discover: !n.NoDiscovery, Logger: log})
	if err != nil {
		return nil, err
	}
	cl, err := client.Dial(ctx, client.Config{Endpoint: ep, Ticket: t, Logger: log, GCInterval: 4 * time.Hour})
	if err != nil {
		ep.Close()
		return nil, err
	}
	return cl, nil
}
```

In `main.go`, change `relayMode(c)` to a wrapper and add `relayModeOf`:

```go
func relayMode(c *cli.Context) (*relay.Mode, error) {
	return relayModeOf(c.String("relay"), c.Bool("no-relay"))
}

func relayModeOf(url string, noRelay bool) (*relay.Mode, error) {
	if noRelay {
		return nil, nil
	}
	if url != "" {
		ru, err := netaddr.ParseRelayURL(url)
		if err != nil {
			return nil, err
		}
		m := relay.ModeCustomURLs(ru)
		return &m, nil
	}
	m := relay.ModeDefault()
	return &m, nil
}
```

and make `localTicket` build its ticket with `worktree.TicketFromView(v)` (replacing the loop over `v.Nodes`; import `github.com/amber-store/dstore/worktree`).

Then, still in `client.go`, rename `pushCmd`/`pullCmd` to `storePushCmd`/`storePullCmd`, and add:

```go
// storeCmd holds the commands over a standalone local store (the amber
// layout <dir>/packstore, <dir>/refs), as opposed to a working copy.
func storeCmd() *cli.Command {
	return &cli.Command{
		Name:        "store",
		Usage:       "push and pull between a standalone local store and the cluster",
		Subcommands: []*cli.Command{storePushCmd(), storePullCmd()},
	}
}
```

In `main.go`'s command list replace `pushCmd(), pullCmd(),` with `storeCmd(), cloneCmd(), initCmd(), fetchCmd(), pullCmd(), pushCmd(), statusCmd(), diffCmd(),` (the new `pullCmd`/`pushCmd` are the working-copy ones defined next).

- [x] **Step 4: Write `cmd/dstore/wc.go`**

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/amber-store/core/reference"
	"github.com/amber-store/dstore/client"
	"github.com/amber-store/dstore/ticket"
	"github.com/amber-store/dstore/worktree"
	"github.com/urfave/cli/v2"
)

// ---- working-copy commands (architecture/dstore.md §11.7, §13) ----

// wcFlags are the connection flags of the working-copy commands. They bind
// no environment variables: the stored config comes before $DSTORE_TICKET.
func wcFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{Name: "ticket", Usage: "cluster ticket (dstore1…) or comma-separated node ids; overrides the stored one for this run"},
		&cli.StringFlag{Name: "relay", Usage: "relay URL for the fallback path (default: the built-in relay map)"},
		&cli.BoolFlag{Name: "no-relay", Usage: "direct addresses only, no relay"},
		&cli.BoolFlag{Name: "no-discovery", Usage: "neither announce this endpoint nor resolve node ids by discovery"},
	}
}

func jobsFlag() cli.Flag { return &cli.IntFlag{Name: "jobs", Usage: "parallelism (0 = cores)"} }

// resolveTicket applies the precedence: the flag, the stored ticket, the
// environment.
func resolveTicket(flag, stored, env string) (string, error) {
	for _, s := range []string{flag, stored, env} {
		if s != "" {
			return s, nil
		}
	}
	return "", errors.New("no cluster: set --ticket or $DSTORE_TICKET")
}

// wcConfig builds the connection config for a command: the stored config
// (nil for clone and init), overridden by explicit flags, with the
// environment as the last resort.
func wcConfig(c *cli.Context, stored *worktree.Config) (worktree.Config, error) {
	var cfg worktree.Config
	if stored != nil {
		cfg = *stored
	}
	var err error
	if cfg.Ticket, err = resolveTicket(c.String("ticket"), cfg.Ticket, os.Getenv("DSTORE_TICKET")); err != nil {
		return cfg, err
	}
	if c.IsSet("relay") {
		cfg.Relay = c.String("relay")
	}
	if c.IsSet("no-relay") {
		cfg.NoRelay = c.Bool("no-relay")
	}
	if c.IsSet("no-discovery") {
		cfg.NoDiscovery = c.Bool("no-discovery")
	} else if stored == nil {
		if v, err := strconv.ParseBool(os.Getenv("DSTORE_NO_DISCOVERY")); err == nil {
			cfg.NoDiscovery = v
		}
	}
	if c.IsSet("user") {
		cfg.User = c.String("user")
	}
	return cfg, nil
}

func dialConfig(ctx context.Context, cfg worktree.Config, log *slog.Logger) (*client.Cluster, error) {
	t, err := ticket.Parse(cfg.Ticket)
	if err != nil {
		return nil, err
	}
	return dialTicket(ctx, t, netOpts{Relay: cfg.Relay, NoRelay: cfg.NoRelay, NoDiscovery: cfg.NoDiscovery}, log)
}

// openWC opens the working copy containing the current directory.
func openWC() (*worktree.Tree, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	return worktree.Open(wd)
}

// pushUser is the user recorded in the reference: --user, the config, the
// OS user.
func pushUser(c *cli.Context, cfg worktree.Config) (string, error) {
	u := c.String("user")
	if u == "" {
		u = cfg.User
	}
	if u == "" {
		if cu, err := user.Current(); err == nil {
			u = cu.Username
		}
	}
	if err := reference.ValidateUser(u); err != nil {
		return "", fmt.Errorf("user: %w", err)
	}
	return u, nil
}

func cloneCmd() *cli.Command {
	return &cli.Command{
		Name:      "clone",
		Usage:     "clone the tree under NAME into DIR (default: the last segment of NAME) as a working copy",
		ArgsUsage: "NAME [DIR]",
		Flags:     append(wcFlags(), &cli.StringFlag{Name: "user", Usage: "user identity stored for pushes"}, jobsFlag(), noTUIFlag()),
		Action: func(c *cli.Context) error {
			name := c.Args().First()
			if name == "" {
				return errors.New("clone NAME [DIR]")
			}
			if err := reference.ValidateName(name); err != nil {
				return err
			}
			dir := c.Args().Get(1)
			if dir == "" {
				dir = path.Base(name)
			}
			cfg, err := wcConfig(c, nil)
			if err != nil {
				return err
			}
			cfg.Name = name
			ctx, cancel := signalCtx()
			defer cancel()
			var tr *worktree.Tree
			var fr worktree.FetchResult
			err = runTransfer(ctx, c, "clone "+name, func(ctx context.Context, log *slog.Logger, prog client.Progress) error {
				cl, err := dialConfig(ctx, cfg, log)
				if err != nil {
					return err
				}
				defer cl.Close()
				cfg.Ticket = worktree.TicketFromView(cl.View()).Encode()
				tr, fr, err = worktree.Clone(ctx, cl, dir, cfg, prog)
				return err
			})
			if err != nil {
				return err
			}
			defer tr.Close()
			fmt.Printf("cloned %s into %s: root %s, %d objects fetched (%d bytes)\n", name, dir, fr.Key.String()[:16], fr.Stats.Fetched, fr.Stats.Bytes)
			return nil
		},
	}
}

func initCmd() *cli.Command {
	return &cli.Command{
		Name:  "init",
		Usage: "make the current directory a working copy of NAME, with nothing synced yet",
		ArgsUsage: "NAME",
		Flags: append(wcFlags(), &cli.StringFlag{Name: "user", Usage: "user identity stored for pushes"}, noTUIFlag()),
		Action: func(c *cli.Context) error {
			name := c.Args().First()
			if name == "" {
				return errors.New("init NAME")
			}
			if err := reference.ValidateName(name); err != nil {
				return err
			}
			cfg, err := wcConfig(c, nil)
			if err != nil {
				return err
			}
			cfg.Name = name
			wd, err := os.Getwd()
			if err != nil {
				return err
			}
			ctx, cancel := signalCtx()
			defer cancel()
			var tr *worktree.Tree
			var fr worktree.FetchResult
			err = runTransfer(ctx, c, "init "+name, func(ctx context.Context, log *slog.Logger, prog client.Progress) error {
				cl, err := dialConfig(ctx, cfg, log)
				if err != nil {
					return err
				}
				defer cl.Close()
				cfg.Ticket = worktree.TicketFromView(cl.View()).Encode()
				tr, fr, err = worktree.Init(ctx, cl, wd, cfg, prog)
				return err
			})
			if err != nil {
				return err
			}
			defer tr.Close()
			if fr.Exists {
				fmt.Printf("initialised working copy of %s; the reference exists (root %s): status shows everything as new, pull merges\n", name, fr.Key.String()[:16])
			} else {
				fmt.Printf("initialised working copy of %s; the reference does not exist yet: push creates it\n", name)
			}
			return nil
		},
	}
}

// withCluster opens the working copy, dials with its config and runs fn
// under the progress display, refreshing the stored ticket afterwards.
func withCluster(c *cli.Context, title string, fn func(ctx context.Context, tr *worktree.Tree, cl *client.Cluster, prog client.Progress) error) error {
	tr, err := openWC()
	if err != nil {
		return err
	}
	defer tr.Close()
	cfg, err := wcConfig(c, &tr.Config)
	if err != nil {
		return err
	}
	ctx, cancel := signalCtx()
	defer cancel()
	return runTransfer(ctx, c, title+" "+tr.Config.Name, func(ctx context.Context, log *slog.Logger, prog client.Progress) error {
		cl, err := dialConfig(ctx, cfg, log)
		if err != nil {
			return err
		}
		defer cl.Close()
		if err := fn(ctx, tr, cl, prog); err != nil {
			return err
		}
		return tr.RefreshTicket(cl)
	})
}

func fetchCmd() *cli.Command {
	return &cli.Command{
		Name:  "fetch",
		Usage: "record the reference's current tree as the remote and fetch its objects",
		Flags: append(wcFlags(), noTUIFlag()),
		Action: func(c *cli.Context) error {
			var fr worktree.FetchResult
			var name string
			err := withCluster(c, "fetch", func(ctx context.Context, tr *worktree.Tree, cl *client.Cluster, prog client.Progress) (err error) {
				name = tr.Config.Name
				fr, err = tr.Fetch(ctx, cl, prog)
				return err
			})
			if err != nil {
				return err
			}
			switch {
			case !fr.Exists:
				fmt.Printf("%s does not exist on the cluster\n", name)
			case fr.UpToDate:
				fmt.Printf("%s: up to date (%s)\n", name, fr.Key.String()[:16])
			default:
				fmt.Printf("fetched %s: root %s, %d objects fetched (%d bytes)\n", name, fr.Key.String()[:16], fr.Stats.Fetched, fr.Stats.Bytes)
			}
			return nil
		},
	}
}

func pullCmd() *cli.Command {
	return &cli.Command{
		Name:  "pull",
		Usage: "fetch and apply the cluster's changes over the working directory",
		Flags: append(wcFlags(), &cli.BoolFlag{Name: "force", Usage: "take the cluster's side on conflicting paths"}, jobsFlag(), noTUIFlag()),
		Action: func(c *cli.Context) error {
			var r worktree.PullResult
			err := withCluster(c, "pull", func(ctx context.Context, tr *worktree.Tree, cl *client.Cluster, prog client.Progress) (err error) {
				r, err = tr.Pull(ctx, cl, c.Bool("force"), c.Int("jobs"), prog)
				return err
			})
			if errors.Is(err, worktree.ErrConflict) {
				fmt.Fprintln(os.Stderr, "conflicts:")
				for _, cf := range r.Conflicts {
					fmt.Fprintf(os.Stderr, "  %s (local: %s, cluster: %s)\n", cf.Path, cf.Local.Kind, cf.Incoming.Kind)
				}
			}
			if err != nil {
				return err
			}
			if r.UpToDate {
				fmt.Println("already up to date")
				return nil
			}
			fmt.Printf("pulled: %d paths updated", len(r.Applied))
			if len(r.Conflicts) > 0 {
				fmt.Printf(", %d conflicts taken from the cluster", len(r.Conflicts))
			}
			fmt.Println()
			return nil
		},
	}
}

func pushCmd() *cli.Command {
	return &cli.Command{
		Name:  "push",
		Usage: "build the working directory's tree, upload it and write the reference",
		Flags: append(wcFlags(),
			&cli.StringFlag{Name: "user", Usage: "user identity recorded in the reference (default: the stored one, then the OS user)"},
			&cli.BoolFlag{Name: "force", Usage: "replace the reference unconditionally"},
			jobsFlag(), noTUIFlag()),
		Action: func(c *cli.Context) error {
			var r worktree.PushResult
			var name string
			err := withCluster(c, "push", func(ctx context.Context, tr *worktree.Tree, cl *client.Cluster, prog client.Progress) error {
				name = tr.Config.Name
				u, err := pushUser(c, tr.Config)
				if err != nil {
					return err
				}
				r, err = tr.Push(ctx, cl, u, c.Bool("force"), c.Int("jobs"), prog)
				return err
			})
			if err != nil {
				return err
			}
			switch {
			case r.Nothing:
				fmt.Println("nothing to push")
			case r.Recovered:
				fmt.Printf("%s already holds %s (an earlier push completed); state updated\n", name, r.Root.String()[:16])
			default:
				fmt.Printf("pushed %s: root %s, %d objects, %d uploaded, version %x\n", name, r.Root.String()[:16], r.Stats.Keys, r.Stats.Uploaded, r.Stats.Version)
			}
			return nil
		},
	}
}

func statusCmd() *cli.Command {
	return &cli.Command{
		Name:  "status",
		Usage: "list the working directory's changes since the last sync, and whether the cluster moved",
		Flags: []cli.Flag{jobsFlag()},
		Action: func(c *cli.Context) error {
			tr, err := openWC()
			if err != nil {
				return err
			}
			defer tr.Close()
			st, err := tr.Status(c.Int("jobs"))
			if err != nil {
				return err
			}
			fmt.Printf("reference %s, synced to %s\n", tr.Config.Name, tr.State.Base.String()[:16])
			switch st.Remote {
			case worktree.RemoteUpToDate:
				fmt.Println("remote: up to date")
			case worktree.RemoteAbsent:
				fmt.Println("remote: the reference does not exist on the cluster")
			case worktree.RemoteMoved:
				a, m, d := 0, 0, 0
				for _, ch := range st.Incoming {
					switch ch.Kind {
					case worktree.Added:
						a++
					case worktree.Deleted:
						d++
					default:
						m++
					}
				}
				fmt.Printf("remote: moved since your last fetch (+%d ~%d -%d; run pull)\n", a, m, d)
			}
			if len(st.Changes) > 0 {
				fmt.Println("changes:")
				for _, ch := range st.Changes {
					fmt.Printf("  %-9s %s\n", ch.Kind, describeChange(ch))
				}
			}
			if st.MetaOnly > 0 {
				fmt.Printf("%d paths differ only in mtime, ownership or xattrs\n", st.MetaOnly)
			}
			if len(st.Changes) == 0 && st.MetaOnly == 0 {
				fmt.Println("nothing to push")
			}
			return nil
		},
	}
}

// describeChange renders a status line's path with its detail: a trailing
// slash for directories, the old and new type or mode.
func describeChange(ch worktree.Change) string {
	p := ch.Path
	if worktree.IsDir(ch.New) || (ch.New == nil && worktree.IsDir(ch.Old)) {
		p += "/"
	}
	switch ch.Kind {
	case worktree.TypeChanged:
		return fmt.Sprintf("%s (%s → %s)", p, worktree.TypeName(ch.Old.Mode), worktree.TypeName(ch.New.Mode))
	case worktree.ModeChanged:
		return fmt.Sprintf("%s (%04o → %04o)", p, ch.Old.Mode&0o7777, ch.New.Mode&0o7777)
	}
	return p
}

func diffCmd() *cli.Command {
	return &cli.Command{
		Name:      "diff",
		Usage:     "unified diffs of the working directory against the last synced tree",
		ArgsUsage: "[PATH...]",
		Flags: []cli.Flag{
			&cli.BoolFlag{Name: "remote", Usage: "against the tree last fetched from the cluster"},
			&cli.BoolFlag{Name: "incoming", Usage: "the last synced tree against the fetched one (what pull would apply)"},
			&cli.BoolFlag{Name: "stat", Usage: "one line per changed path with line counts"},
			jobsFlag(),
		},
		Action: func(c *cli.Context) error {
			if c.Bool("remote") && c.Bool("incoming") {
				return errors.New("--remote and --incoming exclude each other")
			}
			tr, err := openWC()
			if err != nil {
				return err
			}
			defer tr.Close()
			var changes []worktree.Change
			var old, new worktree.Source
			treeSrc := worktree.TreeSource{Get: tr.Get}
			switch {
			case c.Bool("incoming"):
				if !tr.State.HasRemote {
					return worktree.ErrNoRemote
				}
				changes, err = worktree.DiffTrees(tr.Get, tr.State.Base, tr.State.Remote)
				old, new = treeSrc, treeSrc
			case c.Bool("remote"):
				if !tr.State.HasRemote {
					return worktree.ErrNoRemote
				}
				changes, err = worktree.Scan(tr.Root, tr.State.Remote, tr.Get, time.Now(), c.Int("jobs"))
				old, new = treeSrc, worktree.DiskSource{Root: tr.Root}
			default:
				changes, err = worktree.Scan(tr.Root, tr.State.Base, tr.Get, tr.State.SyncedAt, c.Int("jobs"))
				old, new = treeSrc, worktree.DiskSource{Root: tr.Root}
			}
			if err != nil {
				return err
			}
			if c.NArg() > 0 {
				changes, err = filterPaths(tr.Root, changes, c.Args().Slice())
				if err != nil {
					return err
				}
			}
			if c.Bool("stat") {
				return worktree.Stat(os.Stdout, changes, old, new)
			}
			return worktree.Unified(os.Stdout, changes, old, new)
		},
	}
}

// filterPaths keeps the changes at or below the given paths, which are
// relative to the current directory.
func filterPaths(root string, changes []worktree.Change, args []string) ([]worktree.Change, error) {
	var prefixes []string
	for _, a := range args {
		abs, err := filepath.Abs(a)
		if err != nil {
			return nil, err
		}
		rel, err := filepath.Rel(root, abs)
		if err != nil || rel == ".." || strings.HasPrefix(rel, "../") {
			return nil, fmt.Errorf("%s is outside the working copy", a)
		}
		prefixes = append(prefixes, filepath.ToSlash(rel))
	}
	var out []worktree.Change
	for _, ch := range changes {
		for _, p := range prefixes {
			if p == "." || ch.Path == p || strings.HasPrefix(ch.Path, p+"/") {
				out = append(out, ch)
				break
			}
		}
	}
	return out, nil
}
```

- [x] **Step 5: Build, vet, run the CLI tests**

Run: `go build ./... && go vet ./... && go test ./cmd/dstore`
Expected: PASS. `TestResolveTicket` passes; existing CLI tests unchanged.

- [x] **Step 6: Smoke test by hand over loopback**

Build a binary into the scratchpad, start a one-node cluster with `--allow-unsafe` there, and walk through the commands; delete the binary and the store afterwards:

```bash
S=/private/tmp/claude-502/-Users-dragan-amber-store-dstore/74679f52-14bc-42e3-a2c9-2281dc122297/scratchpad
go build -o $S/dstore ./cmd/dstore
$S/dstore cluster init --store $S/n1 --replicas 1 --min-replicas 1 --allow-unsafe --weight 1 --no-relay --loopback --no-discovery
( $S/dstore serve --store $S/n1 --no-relay --loopback --no-discovery & echo $! > $S/serve.pid )
T=$($S/dstore cluster ticket --store $S/n1)
mkdir -p $S/src && echo hello > $S/src/hello.txt
( cd $S/src && $S/dstore init --ticket "$T" --no-relay --no-discovery --no-tui trees/smoke && $S/dstore status && $S/dstore push --no-tui )
( cd $S && $S/dstore clone --ticket "$T" --no-relay --no-discovery --no-tui trees/smoke copy && cd copy && echo world >> hello.txt && $S/dstore status && $S/dstore diff && $S/dstore push --no-tui )
( cd $S/src && $S/dstore fetch --no-tui && $S/dstore status && $S/dstore diff --incoming && $S/dstore pull --no-tui && cat hello.txt )
kill $(cat $S/serve.pid); rm -rf $S/dstore $S/n1 $S/src $S/copy $S/serve.pid
```

Expected: `status` after init lists `new hello.txt`; the clone's status shows `modified hello.txt` and the diff `+world`; after the pull, `src/hello.txt` holds both lines and status says `nothing to push`.

- [x] **Step 7: Commit**

```bash
git add cmd/dstore && git commit -m "dstore clone, init, fetch, pull, push, status, diff; store push/pull"
```

(append the attribution lines).

---

### Task 11: docs

**Files:**
- Modify: `README.md` (running section: the client commands block; a new "Working copies" section after it; the layout table gets a `worktree` row)
- Modify: `architecture/dstore.md` (§11: a new §11.7 before §12; §13: the command list)

- [x] **Step 1: README**

In the layout table, after the `client` row add:

```
| `worktree` | working copies: a directory holding one reference's tree with a local packstore and state in `.dstore/`; scan, tree diff, three-way merge, applier, unified diffs, and the clone/fetch/pull/push flows (§11.7) |
```

In the "Running a cluster" block, replace the two `dstore push`/`dstore pull` lines with:

```
dstore store push --ticket dstore1… --local ~/.amber ./tree trees/demo   # from a standalone local store
dstore store pull --ticket dstore1… --local ~/.amber trees/demo
```

After that block's paragraph on `push` and `pull` progress, add a section:

```markdown
## Working copies

A reference can be worked on like a git branch without history:

```
dstore clone --ticket dstore1… trees/demo [DIR]   # DIR defaults to "demo"
cd demo
dstore status                    # new, modified, deleted, type and mode changes since the last sync
dstore diff [--stat] [PATH…]     # unified diffs against the last synced tree
dstore fetch                     # learn the cluster's current tree
dstore diff --incoming           # what pull would apply; --remote: against the fetched tree
dstore pull [--force]            # apply the cluster's changes, keeping local ones
dstore push [--force] [--user U] # build, upload, write the reference under CAS
dstore init --ticket dstore1… trees/new   # make an existing directory a working copy; push creates the reference
```

The directory's `.dstore/` holds a packstore with every tree fetched or
pushed, the config (ticket, name, connection flags, user) and the state:
the tree the directory was last synced to and the tree last fetched, with
its version. `status` and `diff` work offline against those. A stored
ticket takes precedence over `$DSTORE_TICKET`; `--ticket` overrides it for
one run; the stored ticket is refreshed from the cluster view after every
successful connection. `push` refuses when the fetched tree moved away
from the synced one and when the reference changed on the cluster since
the last fetch, so nothing is overwritten unseen; `pull` merges per path
and refuses on a path changed on both sides unless `--force`. Paths that
`.amberignore` hides are invisible to `status` and never pushed. A
`touch` shows up only in a count of metadata-only differences, but is
recorded by the next push. `.dstore/packstore` grows with every fetch and
push; there is no local compaction yet.
```

- [x] **Step 2: architecture/dstore.md**

Before `## 12. Failure catalogue`, add:

```markdown
### 11.7 Working copies

A *working copy* is a directory holding one reference's tree with a
`.dstore/` beside it: a packstore (every tree fetched or pushed, and the
copy's lock), a config (ticket, name, connection flags, user) and a
state file (*base*, the tree the directory was last synced to; *remote*,
the tree last fetched with its version). `clone` fetches a reference's
tree and writes it out; `init` starts from the empty tree in an existing
directory. `fetch` records the reference's tree as remote; `status` and
`diff` compare the directory with base offline — a file whose size and
mtime match the base entry is taken as unchanged unless the base mtime
lies within 2 s of the sync (the racily-clean rule), otherwise it is
hashed with the single-file ingest; `pull` merges base→remote over
base→directory per path, keeps local changes, and refuses on a path
changed on both sides unless forced; `push` ingests the directory
(`.dstore` excluded), uploads with §11.4 and writes the reference under
compare-and-swap on the fetched version, refusing when base and remote
differ. Metadata-only differences (mtime, ownership, xattrs) are counted,
not listed, and recorded by the next push. Design:
`docs/superpowers/specs/2026-09-15-working-copy-design.md`.
```

In §13's command block replace the `dstore push/pull/ls/refs/ref …` line with:

```
dstore store push/pull | ls | cat | refs | ref | watch   # client commands over a standalone local store, as the amber CLI
dstore clone NAME [DIR] | init NAME | fetch | pull [--force] | push [--force] | status | diff [--remote|--incoming] [--stat] [PATH…]   # working copies (§11.7)
```

- [x] **Step 3: Commit**

```bash
git add README.md architecture/dstore.md && git commit -m "Document working copies"
```

(append the attribution lines).

---

### Task 12: finish — core PR, the dependency, dstore PR

**Files:**
- Modify: `go.mod`, `go.sum`

- [x] **Step 1: Push the core branch and open its PR**

```bash
cd ~/jobs-build/amber-store-core && git push -u origin ingest-exclude
gh pr create --title "ingest: Opts.Exclude skips root names; ScanWith" --body "..."
```

The body: what Exclude is for (dstore working copies keep `.dstore` inside the tree), root-only and independent of NoIgnore, `ScanWith`, and the attribution footer.

- [x] **Step 2: Point dstore at the branch commit instead of the local path**

```bash
cd ~/amber-store/dstore
SHA=$(cd ~/jobs-build/amber-store-core && git rev-parse HEAD)
go mod edit -dropreplace github.com/amber-store/core
GOFLAGS=-mod=mod go get github.com/amber-store/core@$SHA
go mod tidy
go build ./... && go vet ./... && go test ./worktree ./cmd/dstore
```

Expected: `go.mod` requires a pseudo-version `v0.0.8-0.<date>-<sha>` and everything builds. (`GONOSUMDB`/`GOPRIVATE` are not needed for the public repo; if the proxy has not seen the commit yet, `GOPROXY=direct` for the `go get`.)

- [x] **Step 3: Full test run**

Run: `go test ./...` (about six minutes with the new cluster tests).
Expected: every package ok.

- [x] **Step 4: Commit and push, open the dstore PR**

```bash
git add go.mod go.sum && git commit -m "Depend on core's ingest-exclude branch (v0.0.8 once released)"
git push -u origin working-copy
gh pr create --title "Working copies: clone, init, fetch, pull, push, status, diff" --body "..."
```

The body summarises the spec (commands, the `.dstore` layout, the merge rules, the ticket precedence), notes that the `store` group holds the old push/pull, and that go.mod follows core's branch until v0.0.8 is tagged. End with the attribution footer.

- [ ] **Step 5: After core v0.0.8 is released**

```bash
cd ~/amber-store/dstore && go get github.com/amber-store/core@v0.0.8 && go mod tidy && go build ./... && git commit -am "core v0.0.8"
```

This step waits on the release; the PR is complete without it.
