# dstore

A distributed [amber](https://github.com/amber-store/core) store: a
cluster of nodes that together behave like one content-addressed store
for filesystem trees, reachable over [iroh](https://iroh.computer).

- objects are placed on `R` nodes by **weighted rendezvous hashing** over
  a consensus-agreed **view** of the cluster, so clients find owners
  without asking anyone and try them in a deterministic order;
- the view and the **references** (names for tree roots) are CASPaxos
  registers replicated on the nodes themselves;
- nodes join and leave under moderate churn: a view change **rebalances
  before it is published**, and a cluster-wide **mark-and-sweep** with an
  exact per-node mark reclaims what no reference reaches.

> **Status:** first implementation. The architecture is specified in
> [`architecture/dstore.md`](architecture/dstore.md); this repository
> implements it in Go over [go-iroh](https://github.com/tmc/go-iroh)
> v0.2.0. See [Implementation status](#implementation-status) for what
> is in and what is not.

## Layout

| package | what it is |
|---|---|
| `placement` | weighted rendezvous hashing per 2^20 hash slot, integer-only, with golden vectors (§4) |
| `view` | the cluster view, its CBOR encoding, and the placement functions over it (§3) |
| `wire` | the frame format and message types of both ALPNs; reuses transport-iroh's pack framing (§10) |
| `paxos` | CASPaxos: a Pebble-backed acceptor with epoch gating, amnesia and purge floors; a proposer with CAS, fast reads, identity transitions and majority scans (§5.3) |
| `catalog` | typed registers over the proposer: view, references with versions and tombstones, lease, GC state, join tokens (§5.2) |
| `transport` | the stream abstraction the node and client are written against, with an iroh implementation and an in-memory network for one-process clusters (§16) |
| `meta` | a node's bookkeeping database (§6.1) |
| `node` | the storage node: both ALPNs, the data path with primary forwarding, reference coordination, the lease-driven coordinator, voter changes with sync, transitions with the reconcile pass, garbage collection (§6–§9) |
| `client` | the cluster handle: view cache, owner preference by path, `Missing`/`Put`/`Get`, tree push and pull (§11) |
| `ticket` | the `dstore1…` bootstrap ticket (§5.5) |
| `cmd/dstore` | the CLI (§13) |

## Running a cluster

```
# first node: creates the cluster and prints its ticket
dstore cluster init --store /srv/n1 --replicas 3 --weight auto
dstore serve --store /srv/n1

# more nodes: a single-use token per node, then join (the node keeps serving)
dstore token create --ticket dstore1…
dstore node join --store /srv/n2 --seed dstore1… --token <hex> --weight auto

# clients
dstore push --ticket dstore1… --local ~/.amber ./tree trees/demo
dstore pull --ticket dstore1… --local ~/.amber trees/demo
dstore refs --ticket dstore1…
dstore ls  --ticket dstore1… trees/demo sub
dstore cat --ticket dstore1… trees/demo hello.txt

# operations
dstore cluster status --ticket dstore1…
dstore node remove ID | drain ID | weight ID GiB | zone ID Z | repair ID
dstore voter add ID | remove ID
dstore transition status | abort | refreeze | pause | resume
dstore gc run | status | why KEY | hold | release
dstore catalog backup | backups | restore KEY|FILE
```

A container image is published as `ghcr.io/amber-store/dstore:<tag>` by
the release workflow (`Dockerfile`, entrypoint in `docker/entrypoint.sh`):
with `DSTORE_ROLE=init` a fresh store creates a cluster and serves, with
`DSTORE_ROLE=join` plus `DSTORE_SEED` and `DSTORE_TOKEN` it joins one, and
a store that already belongs to a cluster just serves. `DSTORE_PORT`,
`DSTORE_ADVERTISE`, `DSTORE_PAXOS_DIR`, `DSTORE_GC_INTERVAL` and
`DSTORE_EXTRA_ARGS` map onto the matching flags. Relays follow the CLI
default (the built-in relay map) so that the ticket carries a relay URL
and clients outside the network can reach the nodes; `DSTORE_RELAY=<url>`
selects a relay and `DSTORE_NO_RELAY=1` disables relays. Note that a
relay-reachable cluster is writable by anyone who learns its ticket
until the client allowlist is set (see the gaps below). The
[dstore-operator](https://github.com/amber-store/dstore-operator) runs
it on Kubernetes.

`--no-relay --loopback` run everything on one machine without relays;
`DSTORE_TICKET` and `DSTORE_STORE` stand in for the flags. Every node
keeps its UDP port in `<store>/port` so that tickets stay valid across
restarts. A join is a voter change plus a transition and takes a minute
or two with the default timers (`cluster status` shows its progress);
the second node's vote is deferred until a third node joins, when the
catalog goes from one voter to three in one step (§5.4).

A push spreads its batches over every owner at the same distance from
the client (round-trip times compare in coarse classes, so the nodes of
a LAN cluster share a client's writes by rank), keeps four batches of
16 MiB in flight per primary, and a primary appends and replicates
records as they arrive; a pull fetches keys as it discovers them, over
several streams per node, and writes to the local store while fetching.

`push` and `pull` show a progress display when stderr is a terminal: a
bar, throughput and time left, a per-node table (path, batches in
flight, bytes, rate) and the client's last events; Ctrl+C cancels the
transfer cleanly. `--no-tui` (or `DSTORE_NO_TUI=1`) prints plain log
lines and a status line every five seconds instead, which is also what a
non-terminal stderr gets.

## Tests

```
go test ./...                       # unit tests and in-process clusters (~3 min)
DSTORE_TEST_LOG=1 go test ./node    # with node logs
scripts/e2e-loopback.sh             # three real nodes over iroh on loopback
```

`node/cluster_test.go` drives three-node clusters over the in-memory
transport: joins from one voter to three, push and pull, a GC cycle that
reaps a deleted tree and keeps a live one, a node removal with its data
moved and its vote removed, and a write with a node down that the first
audit heals.

## Implementation status

Implemented, against the spec's milestones:

- **M1** — placement with golden vectors, CASPaxos over iroh with Pebble
  acceptors, `view`/`missing`/`get`/`put`/`ref-*`/`status` on both
  ALPNs, the client library with ranked reads and primary-forwarded
  writes, tree push and pull, the in-process cluster tests.
- **M2** — the maintenance lease, voter add/remove with the sync
  procedure (including the one-to-three step out of a single voter),
  transitions with adoption, freeze, the primary-forwarder rule,
  re-freeze and commit, join/remove/drain/weight/zone, weight ramps,
  ex-member hand-back, first audits, the weekly scrub.
- **M3** — barriers with the barrier counter, pins with the two-barrier
  rule, the owner-partitioned mark with termination accounting, sweeps
  in waves with both clauses of the live predicate, coordinator
  failure handling.
- **M4 (partly)** — the hourly catalog backup as an object plus the
  reserved reference, `catalog restore`.

Deviations forced by the current `core` (its §15 items are not there
yet), to be revisited when they land:

- **No key index in packstore.** The mark keeps an exact set of the
  keys this node holds in sweepable packs instead of a per-pack bitmap
  located through the index; delivered batches carry full keys, not
  8-byte tails. Memory is 32 bytes per record held rather than one bit.
- **No `Seal()`.** A transition's adoption seals the active segment by
  a no-op `Compact`; the first audit covers the active segment through
  the keys the node remembers writing (`recent`), not through an index
  enumeration.
- **`Compact` is core's.** A sweep runs one `Compact` under the sweep
  lock exclusive, so pins wait for the whole sweep rather than for a
  batch tail, and the copy holds the append lock. `MaxCopyBytes`,
  victim lists and unlink-first are not available.
- **Segment ids are not persisted monotone by core.** The node does not
  yet refuse to open a store whose next id fell below its recorded
  `eligible_below`.

Not implemented in this version:

- gateway mode (`amber-store-iroh/1`), which needs transport-iroh's
  `wantsync` over an interface;
- `cluster recover` (majority-loss recovery with fencing) and
  `catalog salvage`;
- gossip for view propagation (nodes learn epochs from replies and from
  a 30 s fast read);
- extra data endpoints per node (§3, §11.3);
- the client allowlist is honoured when set in the view but there is no
  command to set it yet.
