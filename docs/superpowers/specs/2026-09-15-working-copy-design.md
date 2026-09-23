# Working copies

*2026-09-15. Approved design; implemented on the `working-copy` branch.*

## Goal

The dstore CLI gains git-like operations on one reference, without
commit history. A reference is cloned into a directory; the directory
holds its own `.dstore/` with a local content-addressed store and a
little metadata. From there `fetch`, `pull` and `push` move the current
state between the directory and the cluster (there is no staging area),
`status` lists which paths are new, changed or deleted, and `diff`
shows the changes against the tree the directory was last synced to,
against the tree last fetched from the cluster, or between those two.

## Model

A working copy is a directory whose `.dstore/` holds:

| file | what it is |
|---|---|
| `packstore/` | a core packstore, the local content-addressed store. Every tree fetched or pushed lands here, deduplicated. Its single-owner lock is the working copy's lock: two commands cannot run on one working copy at once. (Since core v0.0.10 a packstore has no single-owner lock; `.dstore/lock`, held from open to close, is the working copy's lock: `worktree.ErrInUse`.) |
| `config` | JSON: `ticket` (the full `dstore1…` form), `name` (the reference), `relay`, `no_relay`, `no_discovery`, `user`. Written by clone and init; read by every later command. |
| `state` | JSON, written to a temporary file and renamed: `base`, `remote`, `remote_version`, `synced_at`. |

The state mirrors git's HEAD and origin:

- **base** — the key of the tree the working directory was last synced
  to. Set by clone, pull and push. After `init` it is the empty
  directory object (an empty `DirLeaf`), which is stored in the
  packstore so that every tree read works the same way.
- **remote** — the key of the reference's tree as of the last fetch,
  with its cluster **version** (the CASPaxos ballot `ref-get` returns),
  which is the compare-and-swap token for push. An empty `remote` means
  the reference does not exist on the cluster.
- **synced_at** — when base was last set, for the racy-mtime rule
  below.

Commands find the working copy by walking up from the current directory
to the first `.dstore/`, so they work from subdirectories. `.dstore` is
excluded from the tree only at the root; a working copy nested inside
another is ingested by the outer one as data, so `clone` and `init`
refuse to run inside an existing working copy. A `.dstore/` without a
`state` file is an interrupted clone: every command says so and asks for
the directory to be deleted and cloned again.

## Ticket precedence

The working-copy commands bind `--ticket` without the environment
variable. For `fetch`, `pull` and `push` the ticket is, in order, an
explicit `--ticket` on the command line (for that run only), the ticket
stored in `config`, and finally `$DSTORE_TICKET`. For `clone` and
`init`, which have no config yet, it is the flag and then the
environment, and the result is what gets stored. `--relay`,
`--no-relay` and `--no-discovery` follow the same order.

After every successful connection, `fetch`, `pull` and `push` rewrite
the stored ticket from the cluster view (up to four members, as a node
derives its own ticket), so a working copy does not go stale when the
members named in its original ticket are replaced.

## Commands

```
dstore clone --ticket T NAME [DIR]   # DIR defaults to the last segment of NAME
dstore init  --ticket T NAME         # the current directory; base = empty tree
dstore fetch
dstore pull  [--force]
dstore push  [--force] [--user U]
dstore status
dstore diff  [--remote | --incoming] [--stat] [PATH...]
dstore store push PATH NAME --local DIR   # today's push, moved
dstore store pull NAME --local DIR        # today's pull, moved
```

Today's `push` and `pull`, which operate on a standalone local store,
move under `store` unchanged. The transfers in `clone`, `fetch`, `pull`
and `push` run under the existing progress display (`--no-tui` and
`--jobs` apply).

**clone.** DIR must not exist or must be empty, and must not lie inside
a working copy. Clone creates `DIR/.dstore/` and its packstore, reads
the reference (`ref-get`; an unknown name is an error and the directory
is removed again if clone created it), pulls the tree into the packstore
with `PullTree`, writes the tree to disk with the applier below (from
the empty tree to the fetched one), then writes `config` and `state`
with base and remote both at that tree. `state` is written last, so an
interrupted clone is recognisable.

**init.** The current directory must not be inside a working copy. Init
creates `.dstore/`, stores the empty directory object, sets base to it,
then runs a fetch so that remote records whether the name already
exists (and holds its tree). A failed fetch fails init and removes
`.dstore/`.

**fetch.** `ref-get`; if the key equals the stored remote, only the
version is refreshed and fetch reports "up to date". Otherwise
`PullTree` brings the objects the packstore lacks and the state records
the new remote and version. An unknown name records remote as empty and
says the reference does not exist on the cluster.

**pull.** A fetch, then: nothing to pull when remote is empty (an
error), "already up to date" when remote equals base. Otherwise two
change lists against base are computed — *local* (the working
directory, by the scan) and *incoming* (remote, by the tree diff) —
and merged (rules below). Any conflict makes pull print every conflict
and exit non-zero before touching the directory, unless `--force`,
which takes the remote side on conflicting paths. The surviving
incoming changes are applied, then base becomes remote and `synced_at`
now. Pull is restartable: a path an interrupted pull already wrote now
matches the remote, which the merge treats as no conflict.

**push.** Base must equal remote, otherwise push refuses ("the
cluster's tree moved since your last sync: pull first, or --force").
Push then builds the tree with `ingest.Dir` over the working directory
into the packstore, with `.dstore` excluded and `.amberignore` honoured,
and stops with "nothing to push" when the root equals base and remote
equals base. Otherwise it uploads with `Cluster.Push` and writes the
reference under compare-and-swap on the stored remote version (an empty
remote means "the name must be new"); `--force` replaces
unconditionally. A `cas-mismatch` whose current key already equals the
new root is the trace of an earlier interrupted push and is treated as
success, adopting the current version; any other mismatch says
"reference changed on the cluster since your last fetch: pull first, or
--force". On success base and remote become the new tree, with the new
version, and `synced_at` now. The user recorded in the reference is
`--user`, else the config's `user`, else the OS user name.

**status.** Prints the reference name and the first 16 hex digits of
the base key, then one
line about the remote: `up to date`, `moved since your last fetch (+A
~M -D; run pull)` with counts of added, modified and deleted paths from
the tree diff base→remote, or `deleted on the cluster`. Then the local
changes, one per line, sorted bytewise by path:

```
  new       docs/new.txt
  modified  src/main.go
  deleted   old.txt
  type      bin/tool (file → symlink)
  mode      run.sh (0644 → 0755)
```

Directories are printed with a trailing `/`. Paths that differ only in
mtime, ownership or xattrs are not listed; their number is given in one
line, `3 paths differ only in mtime, ownership or xattrs`. With no
change of either kind the last line is `nothing to push`. Types are
named `file`, `directory`, `symlink`, `fifo`, `socket`, `char device`,
`block device`.

**diff.** Unified diffs of the working directory against base; with
`--remote` against the fetched tree (the diff to the cluster's state);
with `--incoming` base against the fetched tree (what pull would
apply). The output is git-style: a `diff a/PATH b/PATH` line, then
`old mode NNNN` / `new mode NNNN` lines for a permission change (a
directory's own change consists of these alone), then `--- a/PATH` and
`+++ b/PATH` headers (`/dev/null` for an added or deleted file) and
hunks from `go-udiff`. A symlink's target is
its content, so a retargeted link is a one-line diff. Content that holds
a NUL byte in its first 8 KiB, and files over 16 MiB on either side,
are summarised: `Binary files a/PATH and b/PATH differ`. Metadata-only
changes are not shown. `--stat` prints one line per changed path with
the added and removed line counts (or `binary`) and a total. PATH
arguments are relative to the current directory and restrict the output
to those paths and the subtrees below them.

## Change detection

A **change** is one path's difference between two sides: a kind, the
old entry and the new entry (`fstree.Entry`, nil for a side where the
path is absent). Kinds:

| kind | when |
|---|---|
| added | absent on the old side |
| deleted | absent on the new side |
| type changed | the `S_IFMT` bits differ |
| modified | same type; for a file the content key differs, for a symlink the target, for a device the numbers |
| mode changed | same type and content; the permission bits (`mode & 07777`) differ |
| metadata only | same type, content and mode; uid, gid, mtime or xattrs differ |

**Tree against tree** walks two directory trees entry by entry in name
order, skipping any pair of subdirectories whose keys are equal, and
expands an added or deleted directory into a change for the directory
itself followed by one for each path below it. A type change is
followed the same way: by *deleted* changes for the contents of a
directory that became something else, and by *added* changes for the
contents of a path that became a directory. It is used for base→remote
(status, pull, `diff --incoming`); the scan below reports type changes
identically.

**Working directory against base** (the scan) walks the disk under
`.amberignore` rules with the root's `.dstore` skipped, so that what
status reports is what push would ingest; a path that base holds but
that is now ignored is therefore reported as deleted. For each disk
entry, `lstat` gives type, mode, uid, gid and mtime; the base entry
gives the same plus, for a file, the size (the content key's length
field). A file whose size and mtime both equal the base entry's is
taken as unchanged without reading it — unless the base entry's mtime
is later than `synced_at` minus two seconds, in which case it is hashed
anyway (git's racily-clean rule: a file edited within the timestamp
granularity of the push that recorded it would otherwise be invisible).
A suspect file is hashed by running the single-file `ingest.Objects`,
which uses the same chunker as the directory ingest and so yields the
same content key, without storing anything. Xattrs are read the way
ingest reads them (`listxattr`/`getxattr`, `ENOTSUP` meaning none) and
compared by re-encoding: inline when the encoding fits
`ingest.DefaultXattrInlineMax`, otherwise as an `XattrSet` key.

`diff --remote` runs the same scan with the fetched tree in place of
base, so the working directory is compared to the cluster's state
directly; the racy-mtime rule then uses the current time.

## Merge (pull)

Inputs: the local list (base→working directory) and the incoming list
(base→remote), indexed by path. For each incoming change:

- no local change at the path, and no local deletion or type change of
  an ancestor directory: **apply**;
- local change of kind *metadata only*: **apply** (a `touch` never
  conflicts);
- both sides deleted: nothing to do;
- both sides changed and the results are equivalent (same type, same
  content key or link target or device numbers, same permission bits):
  **apply** (the remote's metadata wins; no conflict);
- a local deletion or type change of an ancestor directory while the
  remote adds or modifies below it: **conflict**;
- an incoming type change of a directory while the local side changed
  anything below it: **conflict**;
- anything else: **conflict**.

Local-only changes are kept. A file added locally inside a directory the
remote deleted survives, because the applier removes a directory only
when the deletions leave it empty. With `--force`, every conflict is
resolved by applying the incoming change.

## Applier

Applies a change list to the working directory: deletions first, deepest
paths first, then additions and modifications in path order, creating
missing parent directories; directory permission bits and mtimes are
applied last, after their children, as core's tar extractor does.
Regular files are streamed with `fstree.WriteContent` into a temporary
file in the same directory and renamed over the target. Symlinks are
replaced. Fifos and devices are created with `mknod`; a failure names
the path and fails the operation. Mode, mtime (nanoseconds) and, when
running as root, ownership are set; xattrs are set best-effort
(`ENOTSUP` is ignored). Entry names containing `/`, `.` or `..` are
refused, and no path is written through a symlinked ancestor. A
collision on a case-insensitive filesystem surfaces as an error from the
filesystem and is documented, not solved.

## Packages

**core** (one change, released as v0.0.8): `ingest.Opts.Exclude
[]string` — names directly under the root that are never ingested,
whatever `NoIgnore` says; honoured by `Objects` and `Dir` at the root
directory only. `ScanWith(dir string, opts Opts)` honours `Exclude`,
`NoIgnore` and `Jobs` for progress totals; `Scan` keeps its signature.

**dstore `worktree`** (new):

- `Find(dir)` walks up to the root; `Open(dir)` loads config and state
  and opens the packstore; `Create(dir, config)` makes a fresh
  `.dstore/` with the empty tree as base; `Close`.
- `Change`, `Kind`, `DiffTrees(get, a, b)`, `Scan(root, base, get,
  syncedAt, jobs)`, `Merge(local, incoming) (apply, conflicts)`,
  `Apply(root, changes, get)`.
- `Unified(w, changes, old, new Source)` and `Stat(w, changes, old,
  new Source)`, where a `Source` reads a path's content either from a
  tree or from disk.
- The flows over a connected `*client.Cluster`: `Clone`, `Init`,
  `(*Tree).Fetch`, `Pull`, `Push`, `Status`, each returning a result
  struct the CLI prints. The CLI stays thin and the flows run against
  the in-memory cluster in tests.

**CLI**: `cmd/dstore/wc.go` holds the commands, the ticket precedence
and the config refresh; `client.go` keeps the store commands under
`store`.

## Tests

- `worktree` unit tests: the tree diff for every kind and for subtree
  pruning; the scan against a base built by ingest — clean, edited,
  touched, chmod, xattr, new, deleted, ignored, and the racy-mtime rule;
  the merge for each rule, including a remote change under a locally
  deleted directory and equivalent edits on both sides; the applier for
  files, symlinks, directories with deferred mode, deletions that empty
  directories, unsafe names and symlinked ancestors; the diff renderer
  for text, binary, added, deleted, symlink and mode change, and
  `--stat`.
- core: `Exclude` skips only root names and ignores `NoIgnore`;
  `ScanWith` matches.
- End-to-end over the in-memory three-node cluster
  (`node/worktree_test.go`): clone, edit, status, push; a second clone
  sees the push; two working copies edit concurrently — the second push
  is refused, fetch shows the remote moved, pull reports the conflict,
  `--force` resolves it; init and push of a new name; a push whose
  reference write succeeded but whose state write was lost recovers on
  retry.

## Docs

README: a "Working copies" section with the commands and the store
commands' new names. `architecture/dstore.md`: the commands in §13 and
a short §11.7 on working copies.

## Later work

`.dstore/packstore` grows with every fetch and push; a local compaction
keeping the base and remote trees reachable is not part of this
version. Nor is an incremental push that rebuilds only the directories
on changed paths; the full ingest is bounded by the local disk's read
speed and dedups before any upload.
