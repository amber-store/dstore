# Reference watching

*2026-09-15. Approved design; implemented on the `ref-watch` branch.*

## Goal

A client can watch the references of a dstore cluster that match a glob
pattern. It sends the pattern together with the names and keys it
currently holds; the cluster answers with the difference and then pushes
every later change until the client closes the stream. When the client's
connection dies it reconnects to a live node and re-establishes the watch
from the state it has, so that nothing is missed and nothing already
seen is repeated needlessly.

## Semantics

A watch converges the client to the current keys of the references
matching a pattern. The server first sends the difference between the
client's list and the cluster's state — names whose key differs, names
the client did not list, names it listed that no longer exist — then a
`synced` marker, then each later change as it happens. Intermediate
states during a disconnect may be collapsed; the final state is always
delivered. Every change carries the reference's version (its commit
ballot), so a client can order what it sees.

## How a node learns of a change

**Coordinator hint plus periodic reconcile.** The node that commits a
`ref-put` or `ref-delete` broadcasts `ref-changed {name, record?, version}`
to every member on the cluster ALPN — the same fire-and-forget broadcast
as `view-changed` — and applies it to its own watchers. Each watch
stream also re-scans its pattern's prefix every `WatchReconcile`
(default 30 s) and sends whatever the hints missed. Hints are a latency
optimisation: nothing is correct only because a hint arrived. A lost
hint (a member unreachable or restarting at that moment) is repaired
within one reconcile interval. Any node serves watches, voter or not.

Rejected: an acceptor hook (voters only; an accept may be a minority
accept that is never chosen, so the node would have to wait and confirm
with a read that can fence an in-flight write) and pure polling (latency
equals the interval).

## Glob

Package `refglob`: `Compile(pattern) (*Pattern, error)`, with
`Match(name) bool` and `Prefix() string` — the literal prefix before the
first metacharacter, used to bound the catalog scan. Path-style:

- `*` and `?` match within one `/`-separated segment;
- `**` as a whole segment matches zero or more segments (`trees/**`
  matches `trees/a` and `trees/a/b`; `**/x` matches `x` and `a/b/x`);
  elsewhere `**` is just `*`;
- `[...]` is a character class with ranges and `!`/`^` negation, not
  crossing `/`;
- `\` escapes the next character;
- a pattern without metacharacters matches exactly itself.

The pattern is translated to an RE2 regexp, so matching is linear time.
An invalid pattern (unterminated class, trailing backslash) is refused
with `bad-request`.

## Wire

Client ALPN `amber-dstore/1`:

| op | request | reply |
|---|---|---|
| `ref-watch` | `{pattern, known: [{name, key}]}` | a stream of `ref-changes {refs: [{name, key, version, created_at, user}], deleted: [names]}` frames, paged at 4 MiB, and `ref-synced` after the initial difference and after every reconcile scan (a heartbeat). The client ends the watch by closing the stream. |

Cluster ALPN `amber-dstore-cluster/1`:

| op | purpose |
|---|---|
| `ref-changed {name, record?, version}` → `ack` | a reference changed; record absent for a deletion (hint) |

Frame numbers: `ref-watch` 42, `ref-changes` 59, `ref-synced` 60,
`ref-changed` 108. New `Msg` fields: `pattern` (60), `deleted` (61). The
known list reuses `refs` (`RefInfo` with `name` and `key`; `version`
optional and ignored). A known list must fit one frame (16 MiB, about
200k references); larger watches are out of scope.

## Node

`node/watch.go`:

- A watcher registry on the node: `subscribe(pattern)` returns a buffered
  hint channel and an unsubscribe function; `hint(name, key, version)`
  delivers to every subscriber whose pattern matches. A full channel
  sets the subscriber's `rescan` flag instead of blocking; the next
  reconcile catches up.
- `handleRefWatch` runs under the node's context, not the two-hour
  per-stream timeout. It compiles the pattern (`bad-request` on error),
  builds `known` from the request, subscribes, scans the prefix and
  sends the difference, sends `ref-synced`, then selects over hints, the
  reconcile ticker, node shutdown, and a goroutine that reads the stream
  until the client closes it. Any write error ends the watch.
- Versions are ballots. A hint whose version is not above the known
  version is ignored; a scan row whose version is below the known
  version is ignored (a hint got ahead of a lagging voter's row). A scan
  compares keys for names the client sent without a version.
- Every path that commits a reference write calls `refChanged(name,
  record, version)`: `handleRefPut`, `handleRefDelete` (when a tombstone
  was written), `RefPutLocal` (which the hourly catalog backup uses). It
  applies the hint locally and broadcasts `ref-changed`. The catalog
  restore runs on an offline node; the reconcile covers it. The `ref-changed` handler on the
  cluster ALPN applies the hint and answers `ack`.
- `Config.WatchReconcile`, default 30 s.

## Client

`Cluster.WatchRefs(ctx, pattern, known map[string][]byte) iter.Seq2[RefChange, error]`.

- `RefChange{Name, Key, Version, CreatedAt, User, Deleted, Synced, Node}`.
  `Synced` is yielded once after each connection's initial difference,
  with `Node` naming the serving node.
- The iterator copies `known` and keeps it current from the changes it
  yields.
- It picks a node in the usual preference order, opens a stream, writes
  the stamped request with its current known map, and reads frames. On a
  read error, a connection close, or no frame for `WatchIdle` (default
  2 min), it drops the connection, records the failure for backoff,
  picks the next node, refreshes the view when every node failed, waits
  a jittered delay (1 s doubling to 30 s while nothing answers), and
  re-sends the watch. `stale-view` adopts the view and retries the same
  node.
- It ends when the context is cancelled (the sequence just stops) or on a
  terminal remote error (`bad-request`, `unauthorized`), which is yielded
  as the error.

## CLI

`dstore watch PATTERN` prints one line per change to stdout —
`name<TAB>key<TAB>created<TAB>user` or `name<TAB>deleted` — and runs
until Ctrl+C. Reconnects are logged to stderr.

## Tests

- `refglob`: a table test of patterns, matches and prefixes.
- In-process three-node cluster (`node/watch_test.go`): the initial
  difference against a known list with a stale key and a vanished name;
  a change written through a different node arriving via the hint, well
  under the reconcile interval; a deletion; a hint lost to a partition
  repaired within the reconcile interval; a watch that survives its node
  going down by reconnecting to another node and receiving what it
  missed.

## Docs

`architecture/dstore.md`: a watching paragraph in §7, rows in §10.1 and
§10.2, a note in §11. README: the command and a status line.
