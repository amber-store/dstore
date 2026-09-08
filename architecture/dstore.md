# dstore — a distributed amber store

**Status:** design, pre-implementation. This document is the source of
truth for the first implementation; where it is silent the sibling
projects' conventions apply ([amber-store/core](https://github.com/amber-store/core),
[amber-store/transport-iroh](https://github.com/amber-store/transport-iroh)).

dstore is a cluster of storage nodes that together behave like one amber
store: a content-addressed bag of objects (the CAS objects of
[core's `types.md`](https://github.com/amber-store/core/blob/main/architecture/types.md))
plus a set of references naming tree roots. Every node owns an ordinary
core `packstore`; the cluster adds three things on top:

- **placement** — every object lives on `R` nodes chosen by weighted
  rendezvous hashing over the current *view* of the cluster, so any party
  that knows the view and a key knows, without asking anyone, where the
  object is and in what order to try;
- **a consensus-backed catalog** — the view and the references are
  CASPaxos registers replicated on the nodes themselves: every node is
  an acceptor unless it opted out at join;
- **maintenance that never loses data** — view changes rebalance before
  they are published, and a cluster-wide mark-and-sweep reclaims objects
  no reference reaches.

All communication is [iroh](https://iroh.computer) QUIC through
[go-iroh](https://github.com/tmc/go-iroh): a node's identity *is* its
iroh endpoint ID (a 32-byte ed25519 public key), every connection is
mutually authenticated by the QUIC handshake, and reachability (relays,
hole punching, discovery) is iroh's problem, not ours.

Contents:

1. [Shape](#1-shape)
2. [Identities, ALPNs, trust](#2-identities-alpns-trust)
3. [The view](#3-the-view)
4. [Placement: weighted rendezvous hashing](#4-placement-weighted-rendezvous-hashing)
5. [The catalog: CASPaxos registers](#5-the-catalog-caspaxos-registers)
6. [The data path](#6-the-data-path)
7. [References](#7-references)
8. [Membership change and rebalancing](#8-membership-change-and-rebalancing)
9. [Garbage collection](#9-garbage-collection)
10. [Wire protocol](#10-wire-protocol)
11. [The client library](#11-the-client-library)
12. [Failure catalogue](#12-failure-catalogue)
13. [Operations](#13-operations)
14. [Sizing](#14-sizing)
15. [What core needs](#15-what-core-needs)
16. [Verification plan](#16-verification-plan)
17. [Non-goals and later work](#17-non-goals-and-later-work)
18. [Decisions to confirm](#18-decisions-to-confirm)

---

## 1. Shape

```
 clients (amber CLI, jobs-iroh runners, gateways)
   │  amber-dstore/1: view, missing, get, put, ref-*, status
   ▼
 ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐
 │ node A   │   │ node B   │   │ node C   │   │ node D   │   … N nodes
 │ packstore│   │ packstore│   │ packstore│   │ packstore│
 │ meta     │   │ meta     │   │ meta     │   │ meta     │
 │ paxos ●  │   │ paxos ●  │   │ paxos ●  │   │ paxos ●  │   ● = voter (every node, unless --no-vote)
 └──────────┘   └──────────┘   └──────────┘   └──────────┘
        amber-dstore-cluster/1: paxos, gc, transitions, join
```

**One process per node, one node per store directory.** A node is a
single Go binary (`dstore`) that opens `<store>/packstore` (core), a
`<store>/meta` Pebble DB for its own bookkeeping, and `<store>/paxos`
for acceptor state. Nodes are symmetric: any node answers
any client request, any node can coordinate a reference write or a
maintenance activity, and there is no leader in the data path.

**Two roles, both per node:** a *data* node holds objects and takes part
in placement; a *voter* is an acceptor for the catalog. Every node is a
data node and, by default, a voter: the cluster this is designed for has
three to eight nodes, and at that size a quorum is two to five acceptors,
a reference write costs one fsync on each, and the catalog tolerates
`⌊(n−1)/2⌋` failures anywhere — never fewer than the data does. Joining
and leaving therefore include the acceptor-set change of §5.4, one node
at a time; a join runs it before the node's data ramp, a removal after
(or first, for a dead node, so the remaining quorum is among live
nodes). A node can opt out at join (`--no-vote`) — a relay-only remote
box, a slow archive machine — and `dstore voter add|remove` change that
later. An odd count is still better (four voters tolerate no more
failures than three), so a two-node cluster has no catalog fault
tolerance, which `cluster status` says. (Rejected: a small explicit
voter subset, which pays off only when the cluster is far larger than a
sensible quorum and data-node churn is frequent — at dozens of nodes;
here it would be a second membership to operate for no gain.)

**Three planes:**

| plane | carries | consistency | mechanism |
|---|---|---|---|
| data | objects | content-addressed, idempotent, verified at every hop | packstore + placement + primary-forwarded replication |
| catalog | view, references, leases, GC state | linearizable per register (CAS) | CASPaxos on the voters |
| maintenance | rebalancing, repair, GC | eventually complete, resumable, never destructive before it is safe | per-pack reconcile pass; epoch-numbered GC cycles |

**Who computes what.** Clients build trees (chunk, hash, encode) exactly as
the amber CLI does today and talk to owners directly: they fetch the view,
compute owners per key, and send each record once, to one of its owners,
which stores it and replicates it to the others (§6.2). Nodes verify
every object against its key before storing it, replicate what they
receive, serve reads from their packstore, run the paxos acceptor, and
run maintenance. A
reference write is the one operation a node coordinates on the client's
behalf: its completeness walk is many small round trips to owners, which
belong on the LAN next to the stores (§7), and a client behind a relay
could not afford them.

## 2. Identities, ALPNs, trust

- **Node ID** = iroh endpoint ID, generated on first start into
  `<store>/identity` (as transport-iroh's `server.key`). It names the node
  in the view, keys the placement hash (§4), and is what peers
  authenticate.
- **ALPN `amber-dstore/1`** — the client protocol (§10). Open to any peer
  by default, as transport-iroh is; an optional allowlist of client
  endpoint IDs can be kept in a catalog register (`acl`) and enforced at
  accept time. Reference records carry signatures opaquely, exactly as
  core and transport-iroh do; the signer-owns-the-name rule of the HTTP
  server is an optional policy on `ref-put`, off by default.
- **ALPN `amber-dstore-cluster/1`** — inter-node traffic: paxos, GC,
  reconcile, join. A connection is accepted only if `Conn.RemoteID()` is
  the identity of a member of the view (current or pending) or one of
  its data endpoints (§3), a recently removed member on its `former`
  list (for handing data back only, §8.4), or the stream's first frame is
  a `join` carrying a valid join token; anything else is closed with
  `not-member`. On the client ALPN, when the `acl` allowlist is set, a
  peer not on it is refused with `unauthorized`, and `ref-delete` is
  accepted only from peers the allowlist marks `admin` — the HTTP
  server's rule; with no allowlist (the default) anyone may delete.
- **Join tokens** — `dstore token create` (on any node) writes a register
  `join/<blake3(token)>` = `{created_at, weight?}` and prints the random
  32-byte token; the operator hands it to the joining node out of band.
  Presenting it authorizes exactly one thing, once: proposing oneself as a
  data node. The node's entry in the view records the token id it joined
  with, the join CAS refuses a token id already present in any entry
  (`nodes`, `pending`, `former`), and the token register is deleted right
  after that CAS. Two backstops cover a coordinator that crashes in
  between: an abort deletes the token *before* clearing `pending`, and
  the daily catalog pass (§8.1) deletes every `join/*` older than a week
  as well as any whose id appears in an entry — so one token cannot admit
  two nodes.
- **Gateway ALPN `amber-store-iroh/1`** — optional compatibility surface
  (§11.6): a node speaks the existing transport-iroh protocol and routes
  through the cluster.

Nothing else is signed: iroh's handshake authenticates both ends of every
stream, and objects authenticate themselves through their keys.

## 3. The view

The view is the cluster's membership and placement configuration. It is
one CASPaxos register (`view`), so every change to it is a linearizable
compare-and-swap, and every node and client caches the latest one it has
seen.

```
View {
  cluster_id   [16]byte random at `cluster init`; every message carries it
  incarnation  u64      bumped only by majority-loss recovery or a restore
                        from backup (§13); views order by (incarnation,
                        epoch); the cluster ticket (§5.5) and every
                        request carry it
  epoch        u64      placement/voter epoch: bumps when nodes, pending,
                        participants, replicas or voters change (§3 rules)
  version      u64      bumps on every write to the register
  placement_epoch u64   the id of the transition that last committed
                        `nodes`/`replicas` (= its `pending.id`); what
                        settled pack stamps refer to (§8.4)
  replicas     u8       R: owners per object
  min_replicas u8       owners that must hold an object before a client
                        proceeds / a reference is accepted (default
                        max(R−1, 2); 1 only with --allow-unsafe)
  voters       [{id, since}]  acceptors of every register — every node
                        that did not opt out — and for each the epoch of
                        the view that added it (§5.3)
  voter_sync   enum     done | pending, and the sync's cursor (§5.4)
  nodes        [Node]   the current placement set
  pending      Pending? the target placement set while a transition runs
  former       [{id, until}]  recently removed members that may still hand
                        data back over the cluster ALPN (§8.4)
  fenced       [NodeID] voters declared lost by a recovery (§13)
  recovered_from {incarnation, epoch}?   set by a recovery (§13)
  rebalance_pause bool, rate_cap u64     operator knobs read by the
                        reconcile pass (§8.4)
}
Node {
  id        NodeID    32-byte endpoint ID
  weight    u32       capacity in GiB (placement weight, §4); 0 = holds
                      nothing (a draining node, §8.1)
  addrs     [string]  advisory dial candidates (relay URL, ip:port); iroh
                      discovery is the fallback
  data      [{id, addrs}]  extra data endpoints for sharded transfers
                      (§11.3), each with its own key generated at first
                      start into `<store>/identity.data/<n>`, as
                      transport-iroh advertises; admitted on the cluster
                      ALPN as their node (§2)
  token     [32]byte  id of the join token this entry was admitted with (§2)
  zone      string    failure domain; default the node id, set at join
                      (`--zone`) or by `dstore node zone ID Z`, which is a
                      transition. `rank` never picks two owners from one
                      zone, so a host with several volumes runs one member
                      per volume with `zone = host` and never holds two
                      copies of a key; racks later
  incarnation u64     a counter the node bumps when it starts on a store
                      whose `store_id` (§6.1) it has not registered
                      before — a wiped or replaced disk (§8.4)
  writable  bool      cleared by the node itself below its free-space
                      reserve (§12); writers skip it, readers do not
}
Pending {
  nodes        [Node]   the target placement set (replicas may differ too)
  replicas     u8
  id           u64      transition id: the epoch of the view that set this
                        pending (the CAS bumps epoch; id is the new value)
  participants_ack [NodeID]  nodes that have adopted it (§8.2)
  participants [NodeID]? frozen forwarders (§8.3); absent until frozen
  round        u32      bumped by every re-freeze (§8.3)
  primary_done [NodeID] participants that offered every key they are
                        primary for (§8.4)
  done         [NodeID] participants that finished their pass, this round
}
```

Rules:

- **Placement uses `nodes`** (and, while `pending` is set, also
  `pending.nodes`). A node is in `nodes` exactly when it is an owner today.
  Being unreachable does not remove a node from `nodes`; only a committed
  transition does (§8). This is deliberate: placement must be a pure
  function of the view, never of anyone's opinion of who is up. The
  "active nodes" the brief asks for are therefore the *members*; who is
  reachable is advisory — every `view` reply carries the answering node's
  own list of members it cannot reach, and `cluster status` shows it —
  and acting on it (removing a dead node) is the operator's call (§17).
- **Every request carries the epoch the sender computed placement under.**
  For writes and reference operations a node that knows a newer epoch
  refuses with `stale-view` and the newer view, before reading any data;
  the sender recomputes and retries. Reads are always served — holding the
  object is what matters — and every reply carries the node's epoch, so a
  stale reader learns without a failed request. A node that knows an
  *older* epoch than a request fetches the view first (`need-view` on the
  cluster ALPN; a fast read of `view` otherwise) and then serves.
  Senders only ever move to a higher epoch.
- **Weights are capacity, not free space.** Free space fluctuates; using
  it would move data continuously. An operator changes a weight through a
  transition; a full disk is a `no-space` error and an alert (§12). A
  weight of 0 keeps the node a member that owns nothing — the state of a
  node being drained.
- **A view may not drop `R` or more nodes at once** (nor drop `R−1` while
  another is absent): every key whose owners were all among them would be
  lost. The proposal is refused unless forced.
- **Knobs that must reach every node quickly** without being placement —
  a rebalance pause, the reconcile rate cap — are fields of the view too
  (`version` bump only).
- **Epoch versus version.** `version` bumps on every CAS of the register;
  `epoch` bumps only when something a placement or quorum computation
  depends on changes: `nodes`, `pending` (set, cleared, or its
  `participants` frozen), `replicas`, `voters`. Progress fields
  (`participants_ack`, `done`, `voter_sync`) bump only `version`, so a
  transition's bookkeeping never makes clients re-fetch the view.
  `stale-view` compares `(incarnation, epoch)`; every client request
  carries both.

## 4. Placement: weighted rendezvous hashing

For an object key `k` and a placement set `S` (a list of `Node` with
`weight > 0`), the owners are the `R` highest-ranking nodes; the order of
the whole ranking is the order in which readers try them and writers
prefer them.

Rendezvous hashing with weights is the exponential race: node `n` draws
`u_n ∈ (0,1]` from a hash of `(n, k)` and its score is
`w_n / (−ln u_n)`; the largest score wins, and the probability that `n`
wins is `w_n / Σw`. Ranking all nodes by score gives weighted sampling
without replacement, which is exactly what "the next-best owner" should
mean. Adding, removing, or re-weighting one node changes only the keys that
node wins or loses — with one qualification for the zone rule below: a
node that rises in a ranking can push a same-zone member out of the top
`R` and let a lower-ranked member of another zone in, so a key can also
move between two nodes neither of which changed; that happens only among
same-zone members and is rare when zones are few.

**Slots.** The ranking is computed per *slot*, not per key:
`slot(k) = BE64(k[24:32]) >> 44`, the top 20 bits of the key's hash tail
(the same tail packstore's filters use; every key has hash bytes there,
core's `keys.md`). Keys in the same slot share owners. With 2^20 slots
a node holding 1 % of the weight still owns ~10^4 slots per rank, so
the load variance from the coarseness is about 1 % (the relative spread
of a count of ~10^4), and placement for a key
becomes a table lookup: a party computes `rank(slot, S)` lazily on first
use and caches it for the view's epoch (top-`R` for every slot is 3 MB;
the full ranking is computed only for a slot whose owners all failed).
The maintenance scans (§8, §9), which rank every key a node holds, cost
one lookup per key instead of a hash per (key, node).

**The score must be bit-exact across implementations and CPUs** (Go on
amd64 and arm64, the Rust port, and any client). Floating point is
disqualified: Go may fuse multiply-adds on arm64 and libm's `ln` differs
between platforms. The specification is therefore integer-only:

```
salt_n = BE64(BLAKE3("amber-dstore/placement/1" ‖ n.id)[0:8])          // once per node
h      = fmix64(slot ⊕ salt_n)                                          // murmur3 64-bit finalizer
L      = 64·2^32 − log2fix(h + 1)                                       // −log2(u), u = (h+1)/2^64, in Q7.32
score(n, slot) ≡ n.weight / L      (never materialised)
n ranks above m  ⇔  n.weight·L_m > m.weight·L_n   (128-bit products, bits.Mul64)
                    ties (equal products, or both L = 0): smaller id first
if h = 2^64 − 1 then L = 0 and n ranks first.
```

`fmix64(x)`: `x ^= x>>33; x *= 0xff51afd7ed558ccd; x ^= x>>33;
x *= 0xc4ceb9fe1a85ec53; x ^= x>>33` (wrapping). `log2fix(x)` for
`x ∈ [1, 2^64)` returns `⌊log2(x)·2^32⌋` by the classic bit-by-bit
squaring algorithm, deterministic because every step is an integer
multiply and a truncation:

```
i = bitlen(x) − 1                      // integer part of log2(x)
m = x << (63 − i)                      // mantissa in Q1.63: value in [1, 2), bit 63 set
f = 0
for j = 1 .. 32:
    hi, lo = mul64x64(m, m)            // m² in Q2.126, spread over two words
    if hi & (1 << 63) != 0:            // m² ≥ 2 : take the bit, halve
        m = hi                         // (hi is m²/2 in Q1.63)
        f |= 1 << (32 − j)
    else:                              // m² < 2 : keep
        m = hi << 1 | lo >> 63         // (m² in Q1.63)
return i << 32 | f
```

Thirty-two iterations of one 64×64→128 multiply (`bits.Mul64` in Go)
cost ~35 ns; a slot's full ranking over 50 nodes ~3 µs; the whole table
~3 s of one core if ever computed eagerly, which nothing does. Weights are
GiB (`u32`) and `L < 2^39`, so `weight·L < 2^71` fits the 128-bit product.
Test vectors for `salt`, `h`, `L`, and rankings over a fixed view ship
with the placement package, and the Rust port must reproduce them byte for
byte. (Rejected: keyed BLAKE3 per `(slot, node)` — 50× the cost for no
statistical benefit over a bijective mixer with a per-node salt; virtual
nodes proportional to weight — O(Σw/unit) hashes per lookup; per-key
rather than per-slot ranking — the same math at 10 µs per key, which is
what the maintenance scans cannot afford at 10^9 keys.)

`owners(k, S)` = the first `R` nodes of `rank(slot(k), S)` whose `zone`
has not already been taken by an earlier pick. For the *read order* the
ranking is followed by the members with `weight = 0` in id order: a
draining node owns nothing but may still hold what it has not handed
over (§8.5). During a transition the
*write set* is `owners(k, nodes) ∪ owners(k, pending.nodes)` and the *read
order* is `rank(k, nodes)` followed by `rank(k, pending.nodes)` with
duplicates removed.

## 5. The catalog: CASPaxos registers

### 5.1 Why CASPaxos

The catalog is a set of independent small values that change by
compare-and-swap: the view, one record per reference (whose only
operations are "set if the current version is X", "delete if the current
version is X"), a few leases, and the GC epoch. There is no need for a total order
across values, and there must not be a leader in a peer-to-peer cluster
whose nodes come and go.

[CASPaxos](https://arxiv.org/abs/1802.07000) fits this exactly: one Paxos
instance per register, no log, no leader, no snapshotting, and the change
function is the CAS itself. The trade is that we implement it ourselves
and that changing the acceptor set is a procedure with a proof obligation
(§5.4), which is why joins and removals run it one node at a time. (Rejected:
hashicorp/raft or etcd/raft over an iroh stream layer — mature, but
leader-based, log+snapshot for what is a KV of tiny independent registers,
and their membership change is no simpler for us to operate.)

### 5.2 Registers

| register | value | writers |
|---|---|---|
| `view` | the View (§3) | operator commands via any node; the maintenance coordinator |
| `ref/<name>` | `{record}` — reference record bytes (canonical CBOR, core's `reference`) — or a tombstone `{deleted_at}`; its *version* is the ballot the value was committed at (§5.3) | reference PUT coordinators (§7) |
| `lease/maintenance` | `{holder NodeID, expires u64 ns}` | any node wanting to run a transition or a GC cycle |
| `gc` | `{epoch, phase, barrier_at, snapshot_at, placement, acked, mark_done, sweep_wave, sweep_done, hold, last}` (§9) | the maintenance coordinator; nodes CAS their own acks |
| `keep/<name>` | a keep-forever flag on a reference, set by the gateway's `TPin` (§11.6) for a consumer's retention policy to read | gateway |
| `acl` | optional client allowlist | operator |
| `join/<blake3(token)>` | `{created_at, weight?}` — a single-use join token (§2) | operator creates; the join proposal deletes |

Values are deterministic CBOR with integer keys, like every other record
in the project. Register names are bytes; `ref/` + the reference name
(names are ≤ 1024 B UTF-8 with no `@`, as core defines).

### 5.3 The protocol

Standard CASPaxos with one addition: every message carries the sender's
view epoch, and acceptors only talk to proposers at their own epoch.

**Acceptor state** (per register, in `<store>/paxos`, Pebble, synced
writes): `promised` ballot, `accepted` ballot, `value`; plus one
acceptor-wide `purge_floor` ballot (below). A ballot is
`(counter u64, proposer NodeID)`, ordered lexicographically; a proposer
persists its counter in `meta` *before* using it and never reuses one,
and an acceptor refuses a `prepare` whose ballot is not strictly above
its promise, so two values can never share a ballot even across a
proposer's restart from a backup. The view records for each voter
the epoch of the view that added it (`voters[].since`, §3), and
`<store>/paxos` carries the same marker, written by the sync that filled
the acceptor (§5.4). An acceptor whose marker is missing or differs from
what the installed view says about it — a fresh store under the same
identity, a wiped catalog disk, a paxos directory that never finished a
sync, a voter that was removed and re-added while it was down — is
**amnesiac**: it refuses everything that reports its state (`prepare`,
`scan`, and the fast read below) with `amnesiac`, honours what only adds
information (`accept`, `install`, `purge`), and raises an alert. Readers
and listers count an `amnesiac` reply as no reply. An empty acceptor
with a known identity that answered would let a committed value be
overwritten or a listing miss it (a majority containing it sees
nothing). The way back is `voter remove` then `voter add`, whose sync
refills it and writes a fresh marker (§5.4). A paxos directory restored
from a backup carries a valid marker but stale state and is not
detected: never restore one; remove and re-add the voter instead.

A reference value carries its **version** inside it: the CAS that
writes a value stamps it with the proposer's own ballot — an opaque,
totally ordered `(counter, proposer)` pair that never repeats and,
thanks to `purge_floor` below, only grows for a name even across delete
and recreate. Identity transitions and the voter sync re-accept a value
*verbatim*, version included, so a version changes only when the record
does. It is what `ref-get` returns and what a version-based CAS compares
(§7).

**Messages** (cluster ALPN, one stream per round):

```
prepare  {reg, ballot, epoch}                      → promise{accepted, value} | conflict{ballot} | stale-view{view} | need-view | amnesiac
accept   {reg, ballot, value, epoch, not_after?}   → accepted | conflict{ballot} | stale-view{view} | need-view | expired
read     {reg, epoch}                              → {accepted, value} | amnesiac        (the fast read, no promise)
scan     {prefix, after, limit, epoch}             → {rows: [{reg, promised, accepted, value}], next} | amnesiac   (paged)
install  {view}                                    → installed{epoch}
purge    {reg, ballot}                             → ok                                  (§ Tombstones)
```

An acceptor at a *newer* epoch than the message answers `stale-view` with
its installed view; one at an *older* epoch answers `need-view`, the
proposer sends `install {view}` (the committed view it holds; the acceptor
fsyncs it and applies its own membership consequence, §5.4), and retries.
Only replies tagged with the round's epoch count towards its majority.
`not_after` is a wall-clock deadline (ns) an acceptor enforces on
`accept`: past it, the accept is refused with `expired` and the proposal
fails. Reference commits carry one (§7); nothing else does.

**Proposer** (any node): pick ballot `b` > any it has used; `prepare` to
all voters *in parallel*; on a majority of promises take the value with
the highest accepted ballot (or absent); compute `f(value)` — for a CAS,
either the new value or a `cas-mismatch{current}` failure returned to the
caller without an accept round; `accept` to all voters; success on a
majority. A `conflict` at either phase means a higher ballot exists: back
off with jitter and retry with a higher counter, until the caller's
deadline (a hot name contended by hundreds of clients must not fail after
a fixed number of tries). A `stale-view` means the proposer's epoch is
behind: adopt the returned view, recompute the voter set and quorum,
retry.

One slow voter must not slow every write: each acceptor request has its
own deadline (2 s), stream opens fail fast when a connection's stream
budget is exhausted, a proposer caps its in-flight requests per acceptor
(256) and skips that acceptor for the round beyond it, and a round
completes on the first majority of replies without waiting for the rest.
`dstore cluster status` shows per-voter p99 and in-flight counts.

**Reads.** A *fast read* (`read`) asks a majority of voters for their
`(accepted, value)` without touching promises; if they all report the
same accepted ballot, that value is decided and is returned with no
persistence — the common case, one round trip, no fsync. Otherwise the
reader falls back to the *identity transition* (prepare at a higher
ballot, then accept of the value it found on a majority), which decides
the register one way or the other and fences every lower ballot.
`ref-get` and the nodes' periodic view refresh use the fast read;
anything that must *settle* a register — the resolution of an unknown
commit outcome (§7), the snapshot's read rounds below, the voter sync
(§5.4) — uses the identity transition directly, because the fast read
fences nothing. A proposer's `ok` likewise means "my accept round
carried this value to a majority": a CAS that finds, in its prepare
phase, that its own value is already there (a retry after a lost reply)
or that the name is already deleted re-accepts what it found at its own
ballot before replying, since the prepare phase alone may be looking at
a minority-accepted value. A *listing* (`ref-list`, the GC
roots snapshot) is a paged `scan` of a majority of voters merged by
highest accepted ballot per register, tombstones dropped: pages are
budgeted in bytes (an acceptor stops at 4 MiB; `limit` is an upper
bound), the merge cut is the smallest last-name among the replies that
have more (a reply with no more contributes nothing to the cut), and a
page is empty only when the listing is exhausted. Acceptors reply with
values from one designated voter and hashes from the rest; a node runs
at most two concurrent scans and rate-limits `ref-list` per client.

A listing cannot miss a committed value: a committed value was accepted
by a majority, any majority intersects it, and the intersecting acceptor
holds either that value or one with a higher ballot. What a plain merge
*can* do is show an undecided proposal — a value, or a tombstone,
accepted by a minority whose proposer failed midway — and for the GC
roots snapshot an undecided tombstone is dangerous: it would hide a root
whose old value a later prepare may still adopt. The snapshot therefore
**decides what it reads**: for every register where the scanned voters
disagree on the accepted ballot, or where any of them holds a promise
above its accepted ballot (the trace a proposal in flight must leave on
any majority it obtained promises from), the coordinator runs the
identity transition and uses its result. After that every root in the
snapshot is a decided value, and the losing side of any in-flight
proposal can never be chosen — the read's accept at a higher ballot on a
majority fences it. A user-facing `ref-list` skips the read rounds and
may show an undecided value; it never shows a value that a concurrent
read would contradict for long.

**Quorum** = ⌊|voters|/2⌋ + 1 of the voters listed in the epoch the
message carries. With one voter the protocol degenerates to a local
durable write, which is how a single-node cluster runs.

**Tombstones.** Deleting a reference writes a tombstone value; it stays
in the register until *every* voter holds it: after the delete's accept
round the proposer waits briefly for stragglers and, once all voters have
accepted the tombstone at ballot `b`, sends `purge {reg, b}`. A purge
touches only a row whose accepted ballot is exactly `b` — any other row
holds a later value and the purge is stale, a no-op. Such a row is
deleted, and the acceptor's `purge_floor` raised to `b`, iff its promise
is `b` too; a row whose promise is above `b` (a proposal on the name is
in flight) drops `accepted`/`value` but keeps the promise as a stub that
`scan` still reports — the trace the snapshot's read rounds look for —
and is purged by a later pass once that proposal has settled. A missing row behaves as
`{promised: purge_floor}`: a prepare below the floor is refused with the
floor, so a proposer that recreates the name climbs above every purged
tombstone and every promise those rows ever carried — without the floor, a proposer with a smaller
ballot space could commit a new value on the purged acceptors while a
straggler still holding the tombstone at `b` outranks it and reverts the
create. With a majority-only rule a voter that missed the delete could
later win a prepare quorum together with empty acceptors and resurrect
the old value; with all voters holding it, no current acceptor has an
older ballot, and acceptors that left and returned are wiped by the
membership rules (§5.4). A daily pass retries `purge` for tombstones
older than a day, so an unreachable voter only delays reclamation.

**Optimisation, not required:** a proposer that has just committed may
skip the prepare phase for its next proposal on the same register by
reusing its ballot's counter + 1, as the paper describes. Reference writes
are rare enough that the first version does two rounds.

### 5.4 Changing the voters

Everything in the catalog depends on one invariant:

> **V:** every committed register value has been accepted by a majority of
> the *current* epoch's voters (at its ballot, or superseded by a newer
> committed value with the same property).

A change of the voter set is the only thing that can break V, so it is a
three-step procedure that changes the set by **one node at a time** and
re-establishes V before the next change:

1. **Commit the new voter set.** The coordinator CASes `view` with
   `voters' = voters ± x` and `voter_sync = pending` (a new epoch). The
   round runs at the old epoch on the old voters. Acceptors gate on the
   epoch they have installed, so proposals succeed only among acceptors at
   the same epoch — mixed-epoch quorums never form.
2. **Retire the old configuration.** `install {view}` to every old and new
   voter; wait until a *majority of the old voters* has installed the new
   epoch. From then on no *new* accept at the old epoch can land on an old
   majority (every old majority contains an installed acceptor, which
   refuses); a commit whose accepts had all landed before is visible to
   every new-epoch prepare by the intersection argument below. So nothing
   the sync could miss is committed under the old set while it runs.
3. **Sync every register** at the new epoch — every register an
   empty-prefix majority `scan` returns, whatever its namespace (`view`,
   `ref/*`, `lease/*`, `gc`, `acl`, `join/*`, `pin/*`, …), so no future
   prefix can be forgotten — by an identity transition each. Resumable — progress (`voter_sync_cursor`) lives in the view register
   (a `version` bump every few hundred registers), so a successor
   coordinator continues from it; each transition is idempotent. This is
   O(registers) small round trips: 10^6 references take minutes with
   parallel batches, and a freshly added voter can be pre-seeded with a
   bulk copy of another acceptor's rows (`scan`), applied as a
   highest-ballot merge and never as an overwrite, so the sync mostly
   confirms by hash instead of moving bytes — the copy carries no safety
   weight, the sync does. The cursor advances only past registers whose
   identity transition reached a majority; a register refused by a higher
   ballot (a concurrent writer) is retried before the cursor passes it,
   so a successor coordinator cannot skip one.
4. **Commit `voter_sync = done`.** For an add, the sync also writes the
   marker `since = the epoch of step 1's view` into the new voter's paxos
   directory, and step 4 waits for that write to be acknowledged — so a
   `voter add` of a voter that is down cannot complete, and a voter that
   was removed and re-added while down comes back amnesiac (its marker
   names the old add) until a sync that reaches it. Only now may another
   voter change begin. A removed voter that installs a view without itself retires:
   it marks itself `retired` in `meta`, moves `<store>/paxos` aside
   (kept a week, for salvage — §13), and refuses every proposal until a
   later view re-adds it and the add's sync refills it — so an acceptor
   that left at one epoch cannot come back two epochs later voting with a
   state that missed a sync.

Replacing a dead node is join-then-remove: a 3-node cluster passes
through 4 voters with a 3-quorum (the three live ones), never through
2-of-2. The add refuses a node that is already a voter (a stray `add`
must not touch a serving acceptor) and refuses to proceed unless a
majority of the *new* voter set answers a ping within 5 s; a removal
refuses to leave fewer than 3 voters unless the cluster itself has
fewer than 3 nodes or `--allow-unsafe` is given. The one special case
is the first step out of a single voter: with one acceptor its database
*is* the committed state, so the cluster goes from one voter to three in
one step — the second node to join is admitted as a data node with its
voter add deferred, and when the third joins the single acceptor stops
promising, its rows are copied to both, and one CAS commits the
three-voter view — rather than through a 2-of-2 configuration that has
no fault tolerance and wedges if the second voter dies mid-sync. (A
cluster that stays at two nodes runs at 2-of-2 by necessity.) Every node
should keep `<store>/paxos` on a device of its own (`--paxos-dir`): a
Pebble sync that queues behind packstore's ingest fsyncs makes every
reference write pay the ingest tail, and a full data disk must not be
able to stop the catalog.

*Why one step is safe:* let the old set have `A` voters and the new set
`A±1`; the quorums are majorities of each, so
`⌊A/2⌋+1 + ⌊(A+1)/2⌋+1 = A+2 > A+1 = |old ∪ new|` and
`⌊A/2⌋+1 + ⌊(A−1)/2⌋+1 = A+1 > A`. Any prepare quorum of the new epoch
therefore intersects any accept quorum of the old epoch, so a value
committed under the old set is seen by every proposal under the new set.
*Why two steps are not:* from `{a,b,c}` to `{a,b,c,d,x}` a value accepted by
`{a,b}` is invisible to the new prepare quorum `{c,d,x}`; from
`{a,b,c,d,x}` to `{a,d,x}` a value accepted by `{a,b,c}` is invisible to
`{d,x}`. The sync step re-accepts every value on a majority of the new set,
which is V; step 2 guarantees no value is committed under the old set
*after* the sync read it; and the `voter_sync` flag forbids the next
change before both are done. Together: by induction over epochs, every
prepare majority sees the latest committed value of every register.

A voter that was down through a change and a sync holds stale states; it
is harmless: the sync re-accepted every value at a newer ballot on a
majority of the current voters, so any quorum contains an acceptor that
outranks the stale one, and it cannot serve proposals until it has
installed the current view. The only thing it could resurrect is a
deleted value, which is why a tombstone is purged only once all voters
hold it (§5.3).

### 5.5 Learning the view

- **Nodes** hold the view in `meta`. They learn a newer epoch from any
  `stale-view` reply, from any cluster-ALPN message carrying a higher
  epoch, from a periodic fast read of `view` (every 30 s; no ballots, so
  fifty nodes polling never conflict with a real CAS), and from a gossip
  topic (iroh-gossip, topic `blake3("amber-dstore-view")`) on which the
  writer of a new epoch broadcasts `{epoch}`. Gossip is a latency
  optimisation: nothing is correct only because gossip delivered.
- **Clients** bootstrap from a **cluster ticket** — `dstore cluster
  ticket` prints `dstore1` followed by base32 CBOR of `{cluster_id,
  incarnation, addrs of a few members}` — dial any of the members, ask
  `view`, cache it with `(incarnation, epoch)`, and refresh on
  `stale-view`. They never need the catalog protocol. A node
  that sees a request claiming an epoch above its own refreshes through a
  single-flight read at most every 100 ms, and answers `bad-request` if
  the catalog does not confirm such an epoch — one misconfigured client
  must not turn every request into a quorum read.
- **A node's own entry** (`addrs`, `writable`) is CASed by the node itself
  when it changes, rate-limited to once a minute, `version` bump only.
- **A joining node** gets the view in the `join` reply (§8.1).

## 6. The data path

### 6.1 Per-node state

```
<store>/identity          iroh secret key
<store>/packstore/        core packstore (segment size by capacity, §13; synced writes)
<store>/paxos/            acceptor state (every voter, i.e. every node by default)
<store>/meta/             Pebble:
   view                    last adopted View
   ballot                  proposer counter
   store_id                random at store creation; a node that finds one it
                            has not registered bumps its incarnation (§3, §8.4)
   corrupt/<key>           records found bad on this node (§8.4)
   pack/<store_id>/<id>    {for u64 (the placement change the pack was last
                            reconciled for: a pending.id or placement_epoch),
                            replicated bool, checked u64 ns, verified u64 ns}
   pin/<tail>              barrier counter at pin time (§9.5)
   gc                      {barrier counter → (epoch, eligible_below) history,
                            overflowed counts, last marked count}
   xfer/<id>/<round>       transition pass progress (last pack done, deferred packs)
<store>/gc/                this node's mark for the current epoch: one bitmap per
                            pack, plus the placement it was marked against
```

### 6.2 Writes

A client sends each record **once**, to a *primary*: the owner of the
key, under `nodes` in its cached view, with the best path from the
client — a direct connection before a relayed one, then the lowest
measured round-trip time, then rank order (§11.1). The primary stores
the record and replicates it to the key's other owners. Clients
never fan out — a build runner or a remote user uploads each byte once,
and the LAN does the rest. For a set of keys the client library (or a
gateway node on a legacy client's behalf):

1. computes each key's owners under its cached view (§4), picks the
   primary among them by its path measurements (§11.1), and groups keys
   by primary;
2. sends each primary `missing {epoch, keys, pin}` (32 B per key). The
   reply names the keys the primary itself lacks and, for the keys it
   holds, which owners hold each (`short [{key, holders[]}]` for those
   below `R`) — the primary negotiates them with the other owners before
   answering, key lists only, bounded by `(R−1) × 32 B` per key;
3. uploads those records to the primary as byte-balanced amberpack
   batches (target 60 MiB, ≤ 8192 keys) over `put {epoch}`, in parallel
   across primaries — a tree's keys have primaries spread over the whole
   cluster, so a LAN client still talks to many nodes at once — and, for
   throughput, across sharded connections (§11.3); records travel
   verbatim from the sender's packstore (`GetRecord`), no re-encoding;
4. reads each batch's reply: per key, the owners that now hold it
   (`holders[]`) and any owner that did not (`failed [{node, reason,
   retry_after?}]`, a replica's `busy` or `stale-view` relayed as such),
   plus `rejected[]` (hash mismatch, malformed — the node verifies each
   record itself and skips a bad one rather than aborting the batch),
   `no-space`, or `busy` with a jittered retry hint (a node admits a
   bounded number of concurrent write streams, default 2× cores, and a
   byte budget, with a share reserved for cluster-ALPN writes — forwards
   and reconcile — so client streams cannot starve replication; excess
   streams are simply not read until a slot frees, QUIC flow control
   holding the sender back);
5. applies the ack policy below to the holders. Any owner — primary or
   replica — that keeps answering `busy` past `busy_deadline` (default
   5 min) counts as unreachable; a key short at a replica is re-put to
   the primary (which re-forwards; dedup makes it cheap) or, if the
   primary's forwards keep failing, sent by the client to the lacking
   owner directly — any owner accepts a client `put` for a key it owns,
   the primary is a preference, not a role. The push fails naming the
   owners that did not confirm only when no owner will take a key.

**What the primary does.** On a client-ALPN `missing`, besides
answering what it lacks, it negotiates the keys it *holds* with the
key's other owners under `nodes ∪ pending.nodes` — synchronously, key
lists only, `pin` set if the client set it, so dedup hits are pinned at
every owner just as if the client had asked, and the reply reports which
owners confirmed each key — and *queues* its own copy for forwarding
wherever one is lacking: the reconcile pass's first audit (§8.4) run on
demand, asynchronous, deduplicated per key and under `--rate`, so
"present at the primary" becomes "present at every owner" within
seconds without the client's involvement and without the reply waiting
for bytes. Only the client ALPN triggers this; a `missing` on the
cluster ALPN — the reference gate's, the reconcile pass's — is a plain
presence check, so audits never cascade node to node, and a client is
rate-limited in the audits it can trigger as it is in `ref-list`. On `put`, it accepts a record only if its epoch matches and
the key is one it owns under `nodes ∪ pending.nodes` (else `not-owner`,
with the view) and it has space; it verifies the payload against the key
(`WriteParallel`, the same gate core's daemon uses), appends it durably,
forwards it to the other owners over the cluster ALPN (`missing` then
`put` there; a replica applies the same verification and pins dedup hits
the same way), and answers the batch with each key's holders once its own
append is synced and every forward has been confirmed or failed.
Forwarding is synchronous and bounded: per-target queues are small, the
forward timeout (seconds) is far below the client's batch deadline so
one slow replica cannot convoy every primary's batches, a replica that
does not answer in time fails the key *for this batch* and the reply
says which and why (`busy` with its hint, `stale-view` — after which
the primary adopts the newer view and relays it to the client); the
primary keeps no state about in-flight replication, and a record that
reached it but not every replica is in the same position as after any
partial write — healed by the reconcile pass (§8.4), which the primary
also schedules at once for the keys the batch left short. Records arrive at replicas in young segments and pin
nothing; a dedup hit at a replica pins there (§9.5).

Every node is a primary for some keys, so the extra hop costs one LAN
round trip per batch and a second copy of every byte on the LAN, not a
bottleneck; the owner nearest to every client takes at most `R` times
its share of first-hop traffic, and the forwards still spread the bytes.
In a cluster spanning sites this keeps a client's uploads at its own
site, with the cross-site copies made by the primary. (Rejected: clients writing to every owner themselves —
`R`-fold upload and `R`-fold connections from every client, including
relayed ones, for the sake of per-owner acks that the primary's per-key
counts give anyway. Rejected: a designated per-node ingest proxy — the
primary is chosen per key, so no node becomes a funnel.)

**Ack policy.** A key is *placed* when its reported holders include ≥
`min_replicas` owners of `nodes` and, during a transition, also ≥
`min_replicas` owners of `pending.nodes` — the holders come from the
`put` reply for keys the client uploaded and from the `missing` reply's
`short` list for keys it did not, so a dedup hit is covered too. A client
whose key comes back short retries as step 5 says and then fails the
operation naming the owners that did not confirm; it does not silently
accept fewer replicas. Missing replicas of a placed key are healed by the reconcile
pass (§8.4) within its first-audit window (minutes), which is why
`min_replicas = R−1` is the default — the object is on `R−1` nodes now
and on `R` shortly — but never below 2: at `R = 2` the default is 2, and
`min_replicas = 1` (a reference accepted over a single copy) needs
`--allow-unsafe`.

### 6.3 Reads

`get {epoch, keys}` asks one node for a batch of objects; it answers
`absent[]` first (existence checked before streaming, the project
convention) and then the present records as an amberpack. Readers group
keys by their preferred owner (the ranking as a hint, refined by the
client's path measurements, §11.1), fetch, verify each payload against its key
(peers are not trusted with content — a corrupt disk is enough), and
re-ask the next-ranked owner for what came back absent or failed, down
the read order of §4. A key is *not found* only when every owner in the read order — under
`nodes` and under `pending.nodes` — has said so, the reader has then
refreshed its view (a miss at every owner is the signature of a stale
view) and retried any new owners, and finally the rest of the ranking
has said so too: a former owner keeps a record until it has handed it to
every current owner (§8.5), so after a commit a key can legitimately
live only at a lower-ranked node for a while. The full walk costs up to
`N` round trips and only ever happens for a key that is really gone. Per-node backoff (a node that
timed out is deprioritised for a few seconds, not excluded — placement is
deterministic, the *order* is a hint) keeps a dead node from stalling
every batch.

`missing` is the same primitive without the bytes and serves both the
writer's negotiation (client ALPN, where it also reports the other
owners' holdings, §6.2) and the reference gate (cluster ALPN, a plain
check); `pin: true` makes it an atomic has-and-pin (§9.5). A key the node holds only as a record it has
found corrupt (§8.4) is answered *missing*, so a good copy is sent and
not deduplicated away. It answers *present* only for records that
are durable: a hit in the active segment beyond its last synced offset
makes the node sync once for the whole batch before replying (one fsync
per ≤ 8192 keys), so a writer that prunes a key as present, or a
reference gate that counts it, can never be undone by a power loss on
that node.

## 7. References

A reference is exactly core's record (name, key, user, created_at,
optional signature and public key). Semantics follow the HTTP server and
transport-iroh: **a reference never dangles** (the tree under it is
complete in the cluster when it is accepted), updates are **CAS** with an
opt-out, deletion is a CAS too, and listing is a scan.

`ref-put {record, expected_version? | expected_old? | force}` may be sent
to any node; that node coordinates:

1. **Fix the deadline** `not_after = now + put_ttl` (default 1 h). The
   PUT aborts itself with `timeout` past the deadline, and the commit in
   step 4 carries `not_after` so the acceptors refuse the PUT's own
   accept past it too (§5.3): a coordinator that stalls — a paused VM, a
   partition — cannot land its accept after the pins it relies on may
   have expired (§9.5).
2. **Validate the record**: canonical CBOR, bounds, name rules; if the
   signed-reference policy is on, the signature and the signer-owns-name
   rule against the current record.
3. **Walk for completeness.** Top-down from the root: fetch tree objects
   (`DirNode`, `DirLeaf`, `FileNode`) from their owners with ranked
   retries and expand `fstree.ChildKeys`; leaves are never read. Batch
   *every* reachable key — interior objects just fetched as much as
   leaves — into `missing {pin: true}` per owner: fetching a tree object
   does not pin it, the negotiation does. A key is complete when ≥ `min_replicas` owners of `nodes` (and,
   during a transition, of `pending.nodes`) report it present. The first
   incomplete key aborts with `incomplete {sample of missing keys, shortfall}`
   — the client's cue to re-upload and retry, exactly the 404 of the HTTP
   server.
4. **CAS the register** `ref/<name>` with `not_after`: the change
   function compares the current value's version (its commit ballot,
   §5.3) with `expected_version` (absent = "must not exist") when given,
   or — for clients that only know keys, the transport-iroh shape — the
   current record's key with `expected_old`; with neither it replaces
   unconditionally (`--force`). Versions are what make a retry after a
   lost reply safe: a key-based CAS retried on another node could
   re-apply over an intervening writer's change (ABA), whereas the retry
   of a version-based CAS either finds its own key at a version above
   the expected one (it had succeeded) or a genuine mismatch; a delete
   retry that finds the name absent has succeeded. `cas-mismatch{current,
   version}` is returned as a normal reply; `expired` becomes `timeout`.
5. Reply `ok {key, version}`. If the accept round's outcome is *unknown*
   — a timeout, a lost connection — the coordinator settles it with an
   identity transition (§5.3) before replying: the read either commits
   the PUT's value (then the reply is `ok`) or commits the previous value
   at a higher ballot, which fences the PUT's ballot on a majority so it
   can never be chosen later. The fast read would not do: it fences
   nothing, and a minority-accepted value it does not see could be
   completed by any later read. So the client always learns a definite
   result while its coordinator lives; if the coordinator crashes
   mid-round, the register stays undecided until the next read settles
   it — a later `ref-get`, or the GC snapshot (§9.2), which is why that
   snapshot settles every undecided register before it marks.

A coordinator caches "the subtree under interior key K was verified
complete and pinned at time T" for `put_ttl/2` and does not descend below
an unexpired entry: a PUT that relies on the entry within `put_ttl/2` of
T lands its own accept before `T + 1.5·put_ttl`, while the snapshot that
first stops honouring the pins placed at T is later than
`T + 2·put_ttl − barrier_timeout` (§9.5), so it sees that PUT settled. A build farm
re-publishing a large tree with small deltas thus walks only the delta.

`put` pins as well: every key a node finds already present in an old
pack (a dedup hit) is pinned, and what it stores is protected by the
exemption of young records (§9.5), so an upload is protected from the
moment it lands, not only once its reference is being checked — this is
core's grey capture of dedup hits, in pin form. The walk fetches O(tree objects)
records and negotiates O(all keys) key
bytes — for a 10^6-file tree roughly 2·10^6 keys, ~64 MB of key lists per
owner set and a few tens of seconds. It is the authoritative gate and runs
even when the client has just negotiated the same keys, as the HTTP
server does, and it is never skipped for a key the client says it just
uploaded: the pin argument of §9.5 is anchored at the gate's own visit.

`ref-get {name}` is the fast read of §5.3 with its fallback — a decided
value from a majority, one round trip, and an identity transition only
when the voters disagree. `ref-list` is a majority scan.
`ref-delete {name, expected_version? | expected_old? | force}` writes a
tombstone; no bookkeeping — the next GC snapshot simply stops marking
from it. When an allowlist is set, deletion needs an `admin` peer (§2).

## 8. Membership change and rebalancing

A change to the placement set (a node joins, leaves, is declared dead,
changes weight; `R` changes) is a **transition**: the target set is
published as `pending`, data is copied until every object is where the
target says, and only then does the target become `nodes`. Placement under
`nodes` never changes without the data having moved first. This is how
"publish the new view only after rebalancing" is read here: what the
world *places by* is `nodes`; `pending` is visible too, but only as an
instruction to write to both sets, so that the copy has a finite job. The
replica count is restored before the commit for every owner that was
reachable during the pass; an owner that was down throughout is filled
by the same pass as soon as it returns (§8.4) — the commit does not wait
for it, since otherwise a dead node could hold every other change
hostage.

### 8.1 Proposing

- **Join.** `dstore node join --seed TICKET --token T --weight 4096
  [--zone Z]` binds the node's endpoint and sends
  `join {token, weight, zone, addrs}` to a member named in the ticket
  over the cluster ALPN. That member reads `join/<blake3(token)>` (a
  weight in the token overrides the joiner's), hands the proposal to the
  maintenance lease holder (or takes the lease itself), which CASes
  `view` with
  `pending = {nodes + new at the first ramp step's weight, id: epoch}`,
  the new entry recording the token id; the token register is deleted
  afterwards. The reply carries the view; the joiner adopts it and is
  from then on a member for the cluster ALPN. `--weight auto` takes the
  packstore filesystem's total size.
- **Remove / drain.** `dstore node remove ID` (through any node) proposes
  `pending = {nodes − ID}`. A live node being removed keeps serving and
  takes part in forwarding until the commit. `dstore node drain ID` is the
  gentler form: a transition to `weight = 0` moves everything off the
  node while it stays a member (serving reads, forwarding, voting); the
  later `remove` then moves nothing. A dead node is removed the same way;
  it simply does not participate (§8.3). Rotating a node's identity key is
  `remove` + `join`: its old data becomes an ex-member's held data (§8.4).
- **Weight, replication factor, zone.** `dstore node weight ID N`,
  `dstore node zone ID Z`, `dstore cluster replicas R'` propose the
  corresponding `pending`. Raising `R` copies one more replica of every
  key — `1/R` of the whole store, spread over every node — and lowering
  it lets the sweep drop one; both are ordinary transitions, but
  `replicas` is the one change whose cost is the size of the cluster, so
  the command prints the estimate and asks.
- **Voters** change as part of join and remove: a join runs the voter
  add and its sync (§5.4) before the placement ramp; a removal runs the
  placement transition and then the voter remove — for a dead node the
  voter remove first, so the remaining quorum is among live nodes. A node
  joined with `--no-vote` skips the voter steps; `dstore voter add|remove
  ID` change a node's vote later.

Cluster activities are driven by the holder of `lease/maintenance` (a
CAS-updated `{holder, expires}`; renewed every lease/3, default 60 s;
any node may hold it, voters contend first and the others after a longer
jitter). The holder runs at most one transition and one GC cycle at a
time, and they interleave (§9.7); only a voter change (§5.4) excludes
*starting* anything else, because only it changes quorums — a sweep
already running finishes on its own. The holder also runs the
housekeeping the design mentions elsewhere: the hourly catalog backup
and the **daily catalog pass** (purge retries, join-token cleanup,
§5.3, §2).

**Clocks.** Stated once: the design assumes every node's wall clock is
within a few minutes of every other's. Three things rest on it — lease
expiry (judged by the holder's clock against the stored expiry, so a
skew beyond the lease length gives two holders for a moment, which CAS
makes harmless), `not_after` on reference commits (a proposer's deadline
enforced by acceptors, §5.3), and the spacing of GC barriers against
`put_ttl` (§9.1). Nothing else reads a wall clock for correctness.

**Ramps.** A transition that would take hours (a join, a drain, a big
weight change) must not hold up repair or GC for that long, so joins and
drains ramp: the coordinator runs a sequence of transitions stepping the
node's weight `w/8, w/4, w/2, w` (or down), each committing in minutes.
Weighted rendezvous hashing with a monotone weight change moves keys
only onto (or only off) the ramping node (§4, up to the zone rule), so a
ramp costs no extra movement, and any other proposal — a dead node's
removal above all — slots in between steps.

### 8.2 Adopting

A data node that learns an epoch with a new `pending` (or a new `nodes`):

1. atomically with respect to appends (packstore's own append lock, µs):
   stores the view, starts rejecting writes carrying an older epoch, asks
   packstore to **seal** the active segment (`Seal()`, §15) and records
   `xfer/<id>.seal = the highest sealed segment id afterwards` (an empty
   active segment is not sealed, and the next record lands in a segment
   above `seal` either way);
2. CASes itself into `pending.participants_ack` — simply "I have adopted
   `id`"; the field is a set of node IDs.

From this instant every object the node accepts arrives with the new
epoch and is replicated by its primary under the union write set (§6.2).
Everything it accepted before — including writes accepted at the old
epoch from clients that had not yet seen the change — sits in packs with
id ≤ `seal`, and those are what the pass below forwards first; §8.5 says
how the packs sealed afterwards are covered before the node reports
`done`.

### 8.3 Freezing the participants

The coordinator waits until every node in `nodes ∪ pending.nodes` has
acked, or `adopt_timeout` (default 5 min) passes; then CASes
`pending.participants = the acked set`. A node that did not ack is
*absent* for this transition: it has no forwarding duty and its objects
are forwarded by the other current owners. Freezing makes forwarding duty
a pure function of `(key, view, participants)`, so no two nodes can each
believe the other will forward a key, and no node's late arrival changes
anyone's duty. An absent node that comes back mid-transition adopts the
view, sees it is not a participant, and does nothing except keep its data
(§8.5).

If a participant crashes after the freeze, its pass resumes from its
per-pack progress when it restarts; the transition waits, and the
rank-phased fallback in §8.4 means the other holders cover its keys
after `Δ` anyway, so what it blocks is only its own `done`. After
`participant_timeout` (default 1 h) without progress from it, the
coordinator re-freezes without it (also `dstore transition refreeze`):
one CAS that rewrites `participants`, bumps `pending.round` and clears
`done`. Progress is keyed by `(id, round)`, so every participant's pass
restarts — cheap, since negotiation finds little missing — and the
commit CAS checks the round it observed, so a `done` from the old round
cannot satisfy the new one. `dstore transition abort` — a CAS clearing `pending` — is
always safe because nothing has been dropped yet (§8.5).

### 8.4 The reconcile pass

One mechanism moves data for every reason: rebalancing under a transition,
re-replication after a node was lost, healing the gaps left by a down
owner at write time, and periodic anti-entropy. It is per node, per sealed
pack, restartable, and idempotent.

For a sealed pack `p` on node `n`, under the current placement set `cur`
(= `nodes`), the target set `tgt` (= `pending.nodes` during a transition,
else `cur`), and — during a transition — the frozen participant set `P`:

```
for each key k in ScanIndex(p):
    if in a transition:
        j       = n's position among the participating owners of k under cur (1 = primary)
        ready(k) = n ∉ owners(k, cur)                      // a holder that is not an owner offers everything, now
                   or j = 1                                // the primary forwarder, now
                   or the participants ranked above n for k have reported primary_done
                   or (j−1)·Δ has passed since the freeze
        if not ready(k): defer k                           // the pack waits for it (below)
    targets(k) = owners(k, tgt) − {n}
                 − (owners(k, cur) if in a transition and p is settled for cur)   // audited owners hold what they own
    group k under each target
for each target t: send missing {epoch, keys} in batches of ≤ 8192;
    for the keys t lacks: put the records (GetRecord, verbatim) to t
if no key of p is deferred:
    stamp p in meta (read-check-write): {inc: view.incarnation,
                      for: pending.id if in a transition else view.placement_epoch,
                      replicated: every target answered and no batch failed
                                  and no target's incarnation changed since targets were computed
                                  and nobody marked p replicated = false meanwhile}
else: revisit p when its deferred keys become ready; stamp only then
```

A stamp names the *placement change* the pack was reconciled for — a
transition's `pending.id`, or `placement_epoch` (§3) for the background
pass — under the cluster incarnation of the time. A commit sets
`placement_epoch = pending.id`, so a pack whose stamp says
`inc = view.incarnation`, `for = placement_epoch` and `replicated = true`
is **settled** — the node has offered every record in it that was not
proved dead (below) to every owner under the placement in force and each
owner confirmed. A recovery (§13) starts a new incarnation and may reuse
epoch numbers, which is why the stamp carries the incarnation: a node
that adopts a view with a new incarnation finds all its stamps stale and
re-audits everything, the right price after a disaster. Stamps written
under an aborted transition never match anything, a re-proposed
transition has a new id, and a change that leaves the node set
identical (a weight or zone change, an abort-wipe-rejoin) still gets a
new id. (Rejected: stamping with a hash of the node set — two distinct
changes can produce the same set, and a stamp from the first would
count for the second.)

- **Primary forwarder rule (transitions only).** The highest-ranked
  *participating* current owner forwards a key at once, so data crosses
  the wire once per new replica rather than `R` times. Lower-ranked
  participating owners take the key up only after the participants above
  them have reported `done` — then their negotiation finds nothing
  missing — or after `(j−1)·Δ` (`Δ` default 10 min), which covers a
  primary that crashed or lost the record without any failure detection.
  A deferred key is never skipped: its pack is not stamped and the node
  does not report `done` until the key has been offered — a copy that
  only the second-ranked owner holds is thus handed over before anything
  is dropped. `primary_done` is what makes the shortcut usable: a
  participant CASes it as soon as it has offered every key for which it
  is primary or a non-owner, before its own deferred keys are due, so two
  nodes that are each other's secondaries do not wait `Δ` on each other.
- **Owners are offered to as well** unless the pack was already settled
  for the current placement — only an audit proves that a co-owner holds
  what it owns, and `min_replicas` (§6.2) means a freshly written pack
  may have a co-owner that never received a key.
  A holder that is not a current owner at all — an ex-member that
  rejoined, a node absent from an earlier transition — offers everything
  it holds at once: it cannot know what the owners lost. `missing`
  negotiation makes any duplicate cost key bytes, never object bytes.
- **Outside transitions every holder forwards.** The pass then verifies
  that every owner has every key `n` holds and fills gaps — repair,
  hand-off of leftovers by ex-owners, and anti-entropy in one loop. Key
  lists are cheap (32 B per key per target; a weekly audit of 10^8 keys at
  `R = 3` moves ~20 GB of key lists cluster-wide) and only missing
  records move.
- **Scheduling.** A transition runs the pass over every pack with
  id ≤ `seal` in id order, progress in `xfer/<id>`. Outside transitions a
  background worker cycles through packs that are not settled, packs with
  `replicated = false` (an owner was down or a batch failed), packs never
  checked since they sealed (a **first audit** `first_audit` after
  sealing, default 5 min — this is what heals a write whose owner was down
  at the time), and packs whose `checked` is older than `audit_interval`
  (default 7 days) — one pack at a time per node, rate-limited
  (`--rate`), so it never competes with client traffic; a primary that
  left a key short, or found one under-replicated while negotiating for
  a client (§6.2), schedules that key's audit at once. The first audit
  covers the **active segment** too — its index is in RAM and packstore
  can enumerate it (§15) — so a record written at `min_replicas` is
  offered to its missing owner within `first_audit` of landing, however
  slowly the segment fills; a non-empty active segment is still sealed
  after `seal_after` (default 1 h) so packs do not stay open for days —
  a timer, never a reaction to a failed forward, which during a long
  outage would seal a tiny pack per failure. The first audit and the
  `replicated = false` retries run during transitions as well, over
  packs above `seal`.
- **A wiped target.** A node that starts on a store it has not registered
  before bumps its own `incarnation` in the view; every holder that sees
  a member's incarnation change marks every pack `replicated = false` for
  that target and refills it — through the view, so holders whose packs
  were all settled learn too, within the view's propagation time rather
  than at the next weekly audit. `dstore node repair ID` does the same by
  hand. Pack stamps are keyed by the store's own id, so a wiped store
  cannot inherit stamps for pack ids it reuses. Node-to-node transfers
  dial the target's data endpoints from the sender's own (§3, §11.3), so
  a refill is not capped at one socket's throughput; the cluster ALPN
  resolves a data-endpoint identity to its node for admission.
- **Throttling.** The pass reads a pack's missing records in offset
  order (sequentially when most of the pack is wanted), sends at most
  `--rate` bytes/s per node (default half the node's measured ingest
  rate), and a receiver admits a bounded number of inbound reconcile
  streams (4), answering `busy` beyond. Return-triggered work — a node
  came back, an incarnation changed — starts at a random offset within
  `Δ` on each holder so twenty holders do not open eighty streams at
  once. The view carries a cluster-wide `rebalance_pause` flag and rate
  cap an operator can set without restarts.
- **Garbage is not forwarded.** A key that is *proved dead* is skipped:
  the node would delete it at its next sweep anyway. "Proved dead" means
  clause 1 of §9.6 evaluated at that moment with exactly the state the
  node's next sweep batch would use — the newest complete mark and the
  placement it was marked against (the node owned the key under it), the
  current pin set, the current eligibility bound, and no overflow on any
  count that sweep honours; if any honoured count is overflowed, nothing
  is proved dead and everything is offered. Never an older mark: a pin
  that the current mark's epoch has not yet superseded is still in the
  set and protects the key. A key the node holds without owning is never
  proved dead (its mark says nothing about it, §9.6) and is always
  offered. Without this
  rule a rebalance copies every byte of garbage (a quarter of the store
  in core's benchmark workload) to its new owners. What closes the rule:
  a skipped key can become live again only through a dedup hit or a
  reference gate finding it here, both of which **pin** it (§9.5); a pin
  on a record the node does not own under every placement in force, or
  in a pack already stamped, marks that pack `replicated = false` and
  queues the key for an offer within `first_audit`, and clause 2 of the
  sweep honours pins (§9.6) — so the record is offered long before its
  protection lapses.
- **Bytes are checked, not only key lists.** On a pack's weekly visit the
  worker also verifies its records (CRC and payload rehash — CPU only, the
  pack is open anyway), marks a bad one `corrupt` and its pack
  `replicated = false` so the other holders refill it, and records the
  time; `status` reports the oldest unverified pack as the scrub age.
  This is the content scrub: packstore's own `Verify` is not scheduled
  by dstore.
- **A joining node** runs the pass too (it holds little); a node with an
  empty packstore finishes instantly.
- **What the pass believes.** A pack with no stamp — a crash between
  packstore's seal and the stamp write, a store opened by a new
  incarnation — is treated as never checked and audited first; stamps are
  derived bookkeeping, never the source of truth about what exists. When
  a target rejects a record as failing verification, the holder
  re-verifies its own copy: if it is corrupt locally, the record is
  logged, marked `corrupt` in `meta` and treated as absent — `missing`
  reports it missing (§6.3), the refill lands through core's
  `PutVerified`, which repairs the record in place and keeps its pack
  and footer position (so mark bitmaps stay aligned), and the sweep
  treats a corrupt record as dead so a bad copy never wedges a
  compaction (§9.6); if it is good, the target is at fault and is
  counted as failed for the pack.
  One bad node can thus neither block every transition nor talk the
  cluster out of a good record.
- **Ex-members hand data back.** A node that learns a committed view it
  is not a member of — removed while partitioned, or absent for a
  transition that finished without it — keeps its data, runs the pass
  against that view (every key it holds is "not mine"; every current
  owner is a target), and only then lets its packs be dropped. The view
  keeps a `former` list (removed ids, kept 30 days) that is admitted on
  the cluster ALPN for `missing` and `put` only, so writes that a
  partitioned minority accepted are not lost when the majority removes
  it; everything else answers `not-member`.

### 8.5 Completing and committing

A participant that has finished the pass over every pack ≤ `seal` seals
its active segment once more, runs the pass over `(seal, seal′]`, and
CASes itself into `pending.done` (for the current `round`, §8.3). One
cover round is enough: everything appended after adoption is either
dual-placed by its writer (a client write at the new epoch), delivered
by another participant's pass (this node is its target), or a survivor
a concurrent sweep copied forward — and a sweep during a transition
takes no victim `≤ seal` that the pass has not stamped yet (§9.6), so a
survivor comes from a pack already offered. Old-epoch streams are cut at
adoption (§8.2). `done` thus means: every record this node held before
its `done` has been reconciled for the transition, and the loop does not
depend on the node ever being idle. When `done ⊇ participants` for the
current round, the coordinator (or any node observing it — the CAS is
idempotent) commits: `nodes = pending.nodes; replicas = pending.replicas;
placement_epoch = pending.id; pending = null`, new epoch.
**Rebalancing is achieved** means: every object that existed on a
participant before it adopted the transition has been offered to every
target owner, and every object written since was placed on the target
owners by its primary, as far as they confirmed. What can still be missing after the commit are the
same gaps normal operation leaves — an object a writer could not place on
a down owner — and the background pass closes them.

**Forward before drop.** An object a node no longer owns is not deleted by
the commit. It becomes *droppable* — dead to `Compact` (§9.6) — only when
the pack holding it is settled, i.e. the node has reconciled that pack
for the committed placement and every target answered. Packs the node
seals after its `done` have no such stamp and are reconciled by the
background worker first. An absent node that returns after the commit finds all its packs
stale-stamped and reconciles them before dropping anything, so a copy that
only it holds (every other owner of the key died) survives. This is the
invariant the transition rests on:

> **F:** a node deletes a record only if the record is garbage — proved
> dead by a sweep and not pinned since (§9) — or the node has, since the
> last placement change, offered it to every current owner and each of
> them confirmed holding it.

### 8.6 Cost

At 10^8 objects, 20 nodes, R = 3, adding a 21st node of average weight:
about 1/21 of all replicas (≈ 1.4·10^7 objects, ≈ 1.8 TiB at 128 KiB) move,
each once — or, with the default ramp, in four steps of 1/8, 1/8, 1/4
and 1/2 of that. Each node scans its 1.5·10^7 index entries (~1 GB of
footer data, mmapped) and looks each up in the slot table (§4, a few
seconds), sends tens of MB of key lists (only keys the newcomer owns
are offered; co-owners of settled packs are not re-asked), and copies its
share of the
1.8 TiB at whatever the target's append+fsync ceiling allows — the same
~0.7–1 GiB/s core measures for ingest. The transition takes about as long
as the largest per-node copy, typically minutes to an hour per step.

## 9. Garbage collection

An object is *live* if some reference's tree reaches it. The cluster
recomputes liveness from scratch in numbered **epochs**: a barrier
snapshots the roots consistently with in-flight reference writes, a
distributed mark walks every root — each node expanding the tree objects
it owns and streaming every live key to that key's owners — and every
node marks the keys it was sent into a **bitmap over its own records**,
one bit per record, and sweeps its own packs with `Compact` using the
bitmap as the live predicate. Between epochs a node keeps only its last
mark with the placement it was marked against — what a restart resumes a
sweep with, and what the reconcile pass consults to skip proven garbage
(§8.4) — and its last live count, for `status`.

The mark is exact: a bit is set for a record if and only if some
reference reached it in the snapshot's roots, so nothing live is ever
reaped and no garbage is ever retained by a false positive. (The brief
suggested a probabilistic set; §9.4 says why the exact bitmap is both
smaller and simpler here.) What must also be exact is everything that
decides *what is reachable*: the roots snapshot, the walk, and the
protection of objects that become reachable while the epoch runs.

### 9.1 The epoch

```
idle ──► barrier ──► mark ──► sweep ──► idle
          (all nodes ack   (owners expand,   (each node
           or time out)     keys → owners)    Compacts)
```

The coordinator is the holder of `lease/maintenance`; it records progress
in the `gc` register — `{epoch, phase, barrier_at, snapshot_at,
placement{nodes, pending}, acked, mark_done, sweep_done, last{live per
node, stats}}`, node sets as bitmaps over the view's node list so the
value stays small at 50 nodes — so a successor can tell what happened. A cycle is started by the periodic loop
(`gc_interval`, default 4 h) or by `dstore gc run`. Two constraints tie
the epoch to the reference path:

- **A PUT's own accept lands within `put_ttl`** (default 1 h) of the
  PUT's start — hence within `put_ttl` of its gate's has-and-pin visit to
  every owner it counts, each of which happens after the start: the
  coordinator aborts the PUT past `not_after`, and the
  acceptors refuse its accept past `not_after` (§5.3, §7), so the bound
  holds even for a coordinator that stalls. This bounds when the PUT's
  *own* proposal can land; a value its accept left on a minority can
  still be completed later by any identity transition, which is what the
  next point is for.
- **Every roots snapshot settles every reference register** (§9.2): a
  register some voter holds an in-flight trace for is decided by an
  identity transition before the mark starts, so at the snapshot every
  reference either is a root or can never commit.
- **Consecutive barriers are at least `gc_interval` apart**
  (`gc.barrier_at`), whether or not the epoch reached a snapshot, and
  whoever starts them: the periodic loop, `dstore gc run` (which waits
  for `barrier_at + gc_interval`, printing until when, or is refused
  `too-soon`; `gc run --garbage F` re-runs only the sweep with the
  marks the nodes already hold and needs no barrier), a catalog
  restore or a recovery (both write `barrier_at = now`). As defence in
  depth, a node refuses to ack a barrier less than `gc_interval − skew`
  after its previous ack by its own clock — exclusion is always safe.
  The constants: `gc_interval ≥ 2·put_ttl`, and
  `put_ttl ≥ 2·(barrier_timeout + lease + skew)` — an ack can trail
  `barrier_at` by `barrier_timeout` plus a lease (a coordinator that
  died after posting the barrier keeps the phase open until its
  successor abandons it), and the completeness cache of §7 needs the
  factor two.
  The constants rest on the clock assumption of §8.1 (clocks within
  minutes of each other, against a `put_ttl` of an hour).

Together they give the timing fact the pin rule (§9.5) rests on: a
snapshot taken `gc_interval` or more after a pin was placed sees the
PUT's proposal settled one way or the other.

### 9.2 Barrier

The coordinator CASes `gc = {epoch: g, phase: barrier, barrier_at}` and
sends `gc-barrier {g}` to every data node. A node handles it under its
**sweep lock** exclusive (microseconds: this only orders the barrier
against pins and Compact batch tails): it records in `meta.gc`
`epoch = g` and `eligible_below = the packstore's current active segment
id` (records appended from now on land in segments ≥ that id), writes
`(counter + 1, g, eligible_below)` durably, then CASes itself into
`gc.acked` (a CAS that fails once the phase has moved on), and on
success marks that count *confirmed* — pins stamp, and sweeps use, the
last confirmed count, and a `gc-mark` for an epoch whose count is not
confirmed is refused (excluding oneself is safe). That is the
**barrier counter** of §9.5. Reference PUTs the node is coordinating are
not waited for: they are covered by their pins (§9.5), whichever side of
the barrier they commit on.

After every node acked, or `barrier_timeout` (default 1 min) passed, the
coordinator CASes `phase = mark` — which freezes `acked` — and takes the
**roots snapshot**: a majority `scan` of `ref/` with an identity
transition for every register the scan finds undecided (§5.3). Every
reference committed before this point is in it, every root in it is a
decided value, and every proposal in flight at this point is either
completed into a root or fenced forever — an in-flight delete cannot
hide a root that a later read would bring back, and an in-flight create
cannot appear after the sweep. Nodes that did not ack are *excluded*
from epoch `g`: they are not workers, receive no keys, and run no sweep
for it. That is safe for them (nothing is deleted without a mark) and,
because of the pin rule (§9.5), safe for everyone else; a node is
excluded only if it does not answer within a minute — down, unreachable,
or stalled — never because it is coordinating work.

### 9.3 The sweep lock

Each node has one RWMutex, the sweep lock; its holders never wait on the
network, so it is never held for long, and reference-PUT coordination
never touches it.

| holder | mode | held for |
|---|---|---|
| `missing {pin: true}` and `put` handling (§9.5) | shared | the presence check + pin insert |
| `gc-barrier` handling (§9.2) | exclusive | µs |
| the tail of one `Compact` batch (§9.6): delta re-check, fsync, unlink | exclusive | milliseconds |

Compact exclusive against pins is what makes has-and-pin meaningful: a
key confirmed present is pinned before any sweep batch on that node can
decide its fate. The snapshot's consistency comes from the catalog (it
settles every register, §9.2), and a PUT's objects are protected by the
pin rule (§9.5) on whichever side of a barrier the PUT falls — the
barrier itself need not order anything but pins against sweeps.

### 9.4 Mark

The mark is a distributed traversal partitioned by ownership: the
**worker** for a key is the first node in `rank(slot(k), nodes)` that
acked the barrier, so in the common case the node that expands a tree
object already holds it. Workers are all acked data nodes.

The coordinator starts every worker with `gc-mark {g, nonce, params}` —
`params` being the epoch's placement snapshot —
and then delivers the roots as ordinary key batches: an interior root
goes to its worker as `gc-keys {expand: true}`, a leaf root to its owners
as `gc-keys {expand: false}`, ≤ 8192 keys per batch — the coordinator is
one more sender in the termination accounting below. (A single message
carrying 10^6 roots would not fit a frame.) Each worker keeps a queue of keys to process, an exact
**visited set** of the interior keys it has expanded (full 32-byte keys;
never leaves, which need no pruning, and never the mark bitmap: a bit
says a key is live, not that its subtree has been expanded — expansion is
the worker's job, and its visited set is what prunes), and an outbound
batch per other node. Processing `k`:

```
deliver(k): append tail(k) to the batch for every acked owner of k   // for their marks
if k is a Blob or XattrSet:      deliver(k); done               // leaves are never read
elif worker(k) ≠ self:           deliver(k); append k to the batch for worker(k) ("expand")
elif k ∈ visited:                done                            // exact prune
else: visited += k; deliver(k)
      data = local Get(k), else Get from k's other owners (ranked retries)
      if absent everywhere: missing += k; done
      enqueue fstree.ChildKeys(k, data)
```

Batches (`gc-keys {g, nonce, seq, keys[], expand: bool}`, ≤ 8192 keys,
flushed every 50 ms) are accepted only from acked nodes at epoch `g`;
`seq` numbers a sender's batches to one receiver, so a batch re-sent
after a lost ack is recognised, re-acked and counted once. A delivery
batch carries 8-byte key tails — all a receiver needs to find its own
record, and a quarter of the bytes; a tail collision marks an extra
record live, the safe direction — while an `expand` batch carries full
keys, since the worker must fetch the objects. A receiver **marks** each
tail: it looks the tail up in its key index (§15), which yields *every*
record with that tail — the index is a multimap: two keys can share a
tail (and then share a slot and owners, since the slot is the tail's top
bits), and a record the node holds twice has two locations — and sets
the bit of each of them; a tail it does not hold is dropped (the record
is not this node's to keep), and a location at or above `eligible_below`
is skipped (young records are exempt, §9.5). Marking every location is
what makes a clear bit mean "dead": a mark that chose one copy would
let the sweep delete the other. For `expand` batches the receiver also
enqueues the keys. The coordinator sends no root until every worker has acked its
`gc-mark`, so no forward can reach a worker that has not started. The
processing loop never blocks on a send: outbound batches queue per
destination and spill to a local file past a threshold (64 MB), inbound
is bounded by QUIC flow control alone, and an exact per-destination
*forwarded* set of interior keys suppresses re-forwarding the same key
(a directory leaf shared by a thousand references is forwarded once, not
a thousand times); since an interior key has exactly one worker, that
set holds at most one entry per interior key the node encounters. Every interior object is read once cluster-wide,
locally except when its worker is not one of its owners (an excluded
owner, replica lag); shared subtrees cost once because the visited set is
per worker and a subtree has one worker per interior key.

**Termination.** Every sender — each worker and the coordinator — counts
batches *created* (at enqueue, so a batch waiting to be retried still
counts) and batches received, and reports `{sent, received, idle,
missing[], marked}` to `gc-status` polls every second. *Idle* means:
nothing queued, nothing being expanded, no outbound batch unacked, and
the bitmaps as they stand **fsynced** to `<store>/gc/mark-g/` with the
epoch, the node's barrier count, `eligible_below` and the placement —
re-persisted whenever a batch has arrived since the last report — so
that the mark the coordinator declares complete exists on disk on every
node before the phase changes. The
coordinator starts polling only after its own root batches have all been
acked, and declares the mark complete when two consecutive polls show
every worker idle, no counter changed, and `Σsent = Σreceived` over all
senders (a batch in flight or awaiting retry is a count mismatch); a
mark that has not completed within `mark_timeout` (default 1 h) is
abandoned like any other failed phase (§9.8); it CASes
`mark_done = true` with each node's `marked` count and
`phase = sweep`. A `gc-keys` batch that arrives after that
is refused (`mark-frozen`) and reported by its sender in `gc-status`; a
coordinator that sees any refusal aborts the epoch rather than sweep with
a mark that missed a key — a late batch can only mean the termination
check was wrong, and silent loss is the one thing this design must not
do. A
missing object — *absent* at every owner, each of which answered —
aborts the epoch loudly with the reference name and the key: it is both
a GC failure and an integrity alert (`dstore gc status` shows it until a
cycle succeeds); nothing is swept. An object whose owners did not all
answer is *unreachable*, not missing, and always aborts the epoch: a
subtree hidden behind an unreachable owner is intact and must not be
reaped. `dstore gc run --tolerate-missing` lets a cycle complete with
the report attached for a cluster that has already lost data and must
reclaim space — it tolerates missing leaves; a missing interior object
still aborts unless the operator names that exact key, because
tolerating it reaps the whole subtree below it. Every `gc-mark`,
`gc-keys` and `gc-status` carries a per-epoch nonce; a node that has no
mark state for that `(g, nonce)` — it restarted mid-mark — answers an
error, and any such answer aborts the epoch rather than let a worker
that lost its queue pass the termination check with empty counters.

**The mark bitmap.** A node's mark for epoch `g` is one bitmap per
sealed pack below `eligible_below`, one bit per record in the pack's
footer order — the shape core's `packstore.MarkSet` keeps for the
single-node collector (a bitmap per sealed segment, sized from the
footer's record count), once it locates through the key index rather
than by probing pack filters and holds segment ids rather than cloned
footers (§15). At 1.5·10^7 records per node the bitmaps are under 2 MB;
a 6·10^7-record node marks in 8 MB. Marking a delivered tail costs one
index lookup, ~1 µs, so a node marks its whole share in seconds of CPU,
spread over the mark.
The bitmaps live under `<store>/gc/mark-g/`, one file per pack, with
the placement the mark ran against and the node's barrier count, synced
before every idle report (above), so a restart before or during the
sweep loses nothing; a node that finds no complete `mark-g/` at restart
while `gc` says epoch `g` is sweeping CASes `sweep_done` for `g` with
`no_mark` and does not sweep it — and a sweep takes its pin horizon and
`eligible_below` from the mark file's own count, never from the
register's epoch. A pack that is unlinked takes its bitmap with it. A mark tolerates the previous
epoch's sweep still running: it never holds a segment's mapping, only
its id, and a record that sweep copies forward lands above
`eligible_below`, exempt, while the copy left behind in the victim is
unlinked with it.

(Rejected: a probabilistic filter over the delivered keys. At a 0.1 %
false-positive rate a Bloom filter costs 16 bits per live key, a cuckoo
filter ~13.5, a binary fuse filter — already a core dependency, in the
pack footers — ~11.3 but only from a complete key set, so the mark would
have to buffer every delivered key first. All of them need a size guess
from the previous epoch and a story for overfilling, and all retain a
little garbage per epoch by false positives. The bitmap is one bit per
record the node holds, exact, and needs the key index the design
requires anyway; the only thing it cannot express is liveness of a
record the node does not hold, which no sweep needs. Also rejected:
root-partitioned workers fetching interior objects over the network —
every interior object crosses the wire once per worker that reaches it,
~500 GB at 10^9 objects, and shared subtrees are walked once per
worker.)

### 9.5 Pins

A record that is, or is about to become, reachable through a reference
the snapshot did not see must survive until a snapshot has seen that
reference. Two mechanisms give it exactly that, one for records that are
being written and one for records that already exist.

**Young records are exempt.** Each node keeps a **barrier counter** `c`
in `meta.gc`, incremented every time one of its barrier acks is accepted
into `gc.acked` (§9.2), and remembers `eligible_below` and the epoch for
every count. A sweep for the epoch acked at count `c` only touches packs
with id `< eligible_below(c − 1)`: everything appended since the node's
*previous* ack is out of reach for one more epoch. An upload therefore
needs no bookkeeping at all — a record that lands after ack `c − 1` is
untouchable by the sweeps at `c` and `c + 1`.

**Old records are pinned.** A **pin** protects a record that already sits
in the sweepable region — a dedup hit, or a key the reference gate finds
present in an old pack. Pins are created by `missing {pin: true}` — the
RPC writers use to negotiate uploads and the reference gate uses to check
completeness (§7) — and by `put` for keys it finds already present, on
the owner, under its sweep lock shared, atomically with the presence
check: a key found present in a pack below the current `eligible_below`
is inserted into `meta.pin/<tail>` (the key's 8-byte tail; a tail
collision pins an extra object, which is the safe direction) stamped
with the node's barrier counter, and the pin is synced *before* the
reply that reports the key present — a reply is a promise, and a crash
between the two would keep the record but lose the promise. Keys found
absent are not pinned: the requester will upload them into the exempt
region. Pins therefore grow with the dedup-hit rate, not the ingest
rate.

> **P:** a sweep for the epoch acked at count `c` honours every pin
> stamped `≥ c − 1` (and, by the exemption, leaves every record appended
> since ack `c − 1` alone). A pin stamped `c` is dropped when the node
> starts sweeping with a mark whose epoch it acked at count `c + 2` or
> later.

*Why.* The argument is anchored at the reference gate's visit, never at
an upload. Let the gate of PUT `P` visit owner `o` at `t_g` and count
`k` as present, when `o`'s counter is `c`: either `k` sits below the
current `eligible_below` and is pinned with stamp `c`, or it is a young
record, appended after `o`'s ack at `c` and exempt on that account. The
snapshot of the epoch acked at `c` may predate `t_g`, so it may miss the
reference: honoured (pin) or exempt (young). The barrier `o` acks next
(count `c + 1`) is acked after `t_g`, and its snapshot after that — but
possibly before the reference commits: still honoured or exempt. Any
later barrier is at least `gc_interval` after that one, and that one
began no more than `barrier_timeout + lease` before the ack (§9.1), so
the snapshot of the epoch acked at `c + 2` is more than `put_ttl` after
`t_g`. By then `P`'s own accept has landed or been refused (`not_after`,
fixed at `P`'s start, which precedes `t_g`), and the snapshot settles
the register (§9.2): either the reference is decided — it is a root,
`k`'s bit is set in `o`'s mark, no protection is needed — or `P`'s proposal is
fenced and can never become a reference. Nothing here depends on which
epoch `o` or the coordinator was at, on whether either was excluded from
some barrier, or on cycles abandoned before their snapshot (barriers,
not snapshots, are spaced): it uses only "acks precede snapshots",
"barriers are `gc_interval ≥ 2·put_ttl` apart", "a PUT's own accept
lands within `put_ttl` of its gate's visits", and "a snapshot settles
every register". A node excluded from several epochs and then acking
again simply sees a jump in epochs at `c + 1`, which the argument
covers. The completeness cache of §7 rests on the same fact, which is
why the gate's has-and-pin is never skipped for a key the client says it
just uploaded.

Before the gate, an uploaded record is protected for two barriers by
the exemption alone; a reaped one is not a safety problem — the gate
finds it absent and answers `incomplete` — only a wasted upload. An
upload that runs longer than two `gc_interval`s therefore refreshes:
the client library re-negotiates every uploaded key with
`missing {pin: true}` at least once per `gc_interval/2` (key lists
only), which pins the keys that have meanwhile left the exempt region at
the primary and, through its relay, at the replicas — and the reply says
which owners confirmed, so a replica the relay could not reach is pinned
by the client directly (§11.4). Records from uploads that never get a reference (a client
crashed) become garbage after two more acked barriers unless something
else reaches them.

Pins are batched Pebble writes (8 B key + 8 B value), so a node that
restarts mid-epoch keeps them; a restart never widens the window. A node
keeps at most `max_pins` (default 10^8, 1.6 GB on disk, sized at least
to the node's object count); if the set fills, the node does not refuse
writes — it marks the current count *overflowed* (a flag kept and synced
with the pin set, before the reply that would have carried the pin),
keeps answering, and raises an alert. The flag is part of the sweep's
state: the live predicate reads "some count this sweep honours is
overflowed" at every decision, which switches clause 1 of §9.6 off for
every sweep in progress or to come whose honoured range includes that
count — the node reclaims by ownership (clause 2) only until the flag
ages out with the count; and a batch whose overflow was raised after it
began treats every owned victim record as live in its tail rather than
unlink on a stale decision. Over-retention is the safe direction; an
unbounded stall is not.

### 9.6 Sweep

When `gc.phase = sweep`, each acked node — in waves of at most a quarter
of the nodes at a time, which the coordinator releases through
`gc.sweep_wave` so that no more than a quarter of the cluster's append
bandwidth is ever spent on copying — scores its sealed packs once
against its mark, its pins and the mark placement into an ordered
victim list (dead ratio descending), then runs `Compact(live, opts)`
over it in batches of at most `MaxCopyBytes` (default 1 GiB) of
victims, over the packs with id below the *previous* ack's
`eligible_below` (§9.5) and seal time older than `grace` (default 1 h,
the same policy knob as core). Each batch's victims are unlinked as soon as
its copies are durable, so a sweep never needs more transient space than
one batch. From here on the node's sweep depends on nothing the
coordinator holds — only its own mark, the placement it was marked
against, its pins and the current view — so a coordinator change or an
aborted next epoch cannot invalidate it. A batch copies its survivors taking the
packstore's append lock per record, so uploads interleave with the copy;
only its tail — a re-check of the victims against pins placed since the
batch began, the copy of that delta, the fsync and the unlink — runs
under the sweep lock exclusive, for milliseconds. Between batches the
node idles for at least the batch's own duration (or as `--rate`
dictates), so a sweep uses at most half of a node's append bandwidth. A
record `k` in pack `p` is **dead** when either clause holds, and live
otherwise:

1. *garbage:* the node **owned** `k` under the placement the mark ran
   against (`gc.placement`, kept next to the bitmaps — `nodes` and
   `pending.nodes` as they were at `phase = mark`), and the record's bit
   is clear, and `k` is not pinned (§9.5). A mark says nothing about keys
   its node did not own when the mark ran: the mark delivers a key to its
   owners only (§9.4), so for a record the node merely holds, a clear bit
   is no evidence at all, and only clause 2 can ever declare it dead;
2. *not mine:* `n ∉ owners(k, nodes)`, and if `pending` is set also
   `n ∉ owners(k, pending.nodes)`, `k` is not pinned, and `p` is settled
   (§8.4) — the node has offered `k` to every current owner and each
   confirmed it (F); a pin on an unowned record means a reference gate
   or a dedup hit found it here, and the offer it triggered (§8.4) must
   land before the record goes. During a transition a victim list skips
   packs `≤ seal` the transition pass has not stamped yet (§8.5). An
   unowned record in a pack that is *not* settled (an owner has been
   down for days) is live: it is copied forward into the active segment
   like any survivor, and the new pack is reconciled and settled in turn.
   No pack is ever ineligible for a sweep because of a slow target —
   otherwise a single multi-day outage would freeze reclamation of
   ordinary garbage on every node.

A record marked `corrupt` (§8.4) is dead under either clause: it cannot
be read, its refill arrives through `PutVerified`, and a compaction must
not stall on it. And a record the node holds twice is copied forward
unless a copy survives outside *this sweep's whole victim list* — never
merely outside the current batch — so two victims in different batches
cannot each leave the copy to the other (§15). Clause 1 is garbage
collection; clause 2 is the drop half of rebalancing and needs no mark —
a node may run an *ownership-only* compaction (only clause 2) at any
time. `Compact` then does what it does in core: rewrites
the packs whose dead fraction is over the line (policy 0.5; 0.1 under
min-free pressure), verifying every live record while copying, fsyncing,
unlinking victims last. Between batches pins and PUT walks proceed; a
pin placed between two batches is honoured by the next (the live
predicate consults the pin set at decision time). When done the node
CASes `gc.sweep_done += self` with its stats; the coordinator marks the
epoch idle when every acked node is done or `sweep_timeout` (default 6 h)
passes.

A sweep is not cut by the next barrier: the node acks the barrier between
two batches and keeps sweeping with the mark it has — the pin rule is
stated per sweep epoch, not per current count — until the next epoch's
mark is complete, then switches mark, `eligible_below` and victim list
between batches. A node that restarts resumes with its persisted mark. Victims with no live record at all are unlinked first without
copying anything, and a node keeps a headroom of `MaxCopyBytes` plus one
segment below which it refuses uploads (`no-space`, retryable) but still
sweeps: a full disk must always have a way out. packstore's own `Verify` is
never scheduled by dstore (the reconcile worker scrubs, §8.4); pack
removal today waits on a global gate that every index scan and record
read takes, which is why §15 asks for per-segment pins before the first
version — the reconcile pass reads packs all the time, and a batch tail
must not wait on it. Space reclamation after a join is lazy by the same
policy line core uses: a node that lost 5 % of its keys to a newcomer
keeps them until ordinary garbage pushes each pack over the line, or
min-free pressure lowers it.

### 9.7 GC and transitions

Reachability is independent of placement, so a cycle and a transition
may run at the same time; a rebalance that takes days must not switch GC
off, and a dead node's removal must not wait an hour for a sweep. The
rules that make it safe:

- the coordinator records in `gc` the placement it marks against —
  `nodes` and `pending.nodes` at the moment `phase = mark` — and every
  worker computes worker assignment and key delivery from that snapshot,
  never from a view adopted later;
- keys are delivered to their owners under both sets; a node that is not
  an owner under either at mark time gets no mark and is excluded from
  the epoch's sweep — in particular a node that joins during a mark waits
  for the next epoch to sweep anything — and a node that owns some keys
  and merely holds others judges only the former by its mark (§9.6);
- a sweep during a transition may copy survivors from a pack the
  transition pass has not reached yet into a new segment; §8.5's
  seal-and-cover loop before `done` is what still offers them;
- the ownership clause of `live` (§9.6) reads the view current at each
  Compact batch; it only ever drops what is settled (F), so a commit
  landing mid-sweep is harmless;
- a voter change (§5.4) is the one activity that waits for a running
  cycle to leave `barrier`/`mark` and blocks new cycles, since it
  changes the quorum the `gc` register itself is written with.

### 9.8 Coordinator failure

Every phase is idempotent and its state is in `gc`. When the lease
expires, the next holder reads `gc`: an epoch in `barrier` or `mark` is
abandoned (`phase = aborted`; the next epoch is `g+1`, and its barrier is
scheduled `gc_interval` after `g`'s `barrier_at`); one in `sweep` is
simply observed to completion — the marks are on the nodes, so nothing
of the old coordinator's is needed. Nodes that were mid-mark drop their
queues when they see the abort; nodes mid-sweep finish their batch and
continue, since a sweep depends on nothing the coordinator held (§9.6).

### 9.9 Cost

| | 10^8 objects, 20 nodes, R=3 | 10^9 objects, 50 nodes, R=3 |
|---|---|---|
| interior objects expanded (≈2 %), read locally | 2·10^6 | 2·10^7 |
| key traffic (R × 8 B tails per live key, plus full-key expand batches) | ~2.5 GB total, 130 MB/node | ~25 GB total, 500 MB/node |
| mark wall time (local reads + LAN key streams) | tens of seconds | minutes |
| per-node visited set (32 B × interior keys it expands) | ~3 MB | ~13 MB |
| per-node mark (1 bit per record held) | 2 MB | 8 MB |
| per-node sweep: index scan + Compact victims | seconds + copy time | same shape |

Compare core: a single-node mark of 5·10^5 objects takes 0.5 s. The
cluster mark is bounded by local metadata reads and LAN bandwidth, not by
the walk.

## 10. Wire protocol

Framing is transport-iroh's: one bidirectional QUIC stream per
operation; each frame is a 4-byte big-endian length and a deterministic
CBOR map with integer keys; amberpack payloads ride in `data` frames of
1 MiB terminated by `data-end` (`protocol.SendPackRecords` /
`NewPackReader` are reused verbatim). The initiator writes first; the
responder FIN-closes after its last frame; both close streams best-effort
(a peer's `CancelRead` makes `Close` return an error — ignore it).
Every request carries the sender's `epoch`.

### 10.1 Client ALPN `amber-dstore/1`

| op | request | reply |
|---|---|---|
| `view` | — | `view {View, unreachable[]}` (members this node cannot currently reach) |
| `missing` | `{epoch, keys[], pin bool}` | `missing {lacking[], short[{key, holders[]}]}` — `short` only on the client ALPN (§6.2) |
| `get` | `{epoch, keys[]}` | `absent {keys[]}` then `data…data-end` (verbatim records) |
| `put` | `{epoch}` then `data…data-end` | `put-result {rejected[{key, reason}], holders[{key, nodes[]}], failed[{key, node, reason, retry_after?}]}` — on the client ALPN the receiver replicates to the other owners; on the cluster ALPN it stores only |
| `ref-get` | `{name}` | `ref {record, version}` \| `unknown-ref` |
| `ref-put` | `{record, expected_version? \| expected_old?}` | `ok {key, version}` \| `cas-mismatch {current?, version}` \| `incomplete {keys[], shortfall}` |
| `ref-delete` | `{name, expected_version? \| expected_old?}` | `ok` \| `cas-mismatch {current?, version}` |
| `ref-list` | `{prefix?, after?}` | `refs {[{name, key, version, created_at, user}], next?}` (pages of ≤ 4 MiB) |
| `status` | — | `status {node stats, view epoch, gc, transition}` |

Every request carries `cluster_id`, `incarnation` and `epoch`; every
reply carries the node's `(incarnation, epoch)`. Errors are a single `err {code, text, view?}`
frame. Codes: `stale-view`
(with `view`), `not-owner` (with `view`), `no-space`, `busy` (with a
jittered `retry_after`), `bad-request`, `unauthorized`, `unknown-ref`,
`cas-mismatch`, `incomplete`, `unavailable` (no quorum for a catalog
operation), `timeout`, `internal`; on the cluster ALPN also `not-member`,
`need-view`, `expired`, `amnesiac`, `mark-frozen`. A `put`
batch is applied object by object: `rejected` names the bad ones, the rest
are durable — the store is a bag, as in every amber transport.

Limits: a frame ≤ 16 MiB; a `missing`/`get` request ≤ 8192 keys; a `put`
batch ≤ 64 MiB of records. Existence is checked before any bytes stream,
so errors are proper replies, not truncated bodies.

### 10.2 Cluster ALPN `amber-dstore-cluster/1`

Accepted only from members (§2). The same `missing`/`get`/`put`
operations are available here for the reconcile pass (a node is a
privileged client), plus:

| op | purpose |
|---|---|
| `join {token, weight, zone, addrs}` | propose a transition adding the sender; reply `view` |
| `prepare`, `accept`, `scan`, `install`, `purge` | catalog (§5.3) |
| `gc-barrier {g}` → `ok` | the ack itself is the node's CAS into `gc.acked` (§9.2) |
| `gc-mark {g, nonce, params}` | start a worker (§9.4) |
| `gc-keys {g, nonce, seq, keys[], expand}` → `ack` | live key tails for the receiver's mark; `expand` batches carry full keys and are also traversed; roots arrive this way too (§9.4) |
| `gc-status {g, nonce}` → `{sent, received, idle, marked, missing[]}` | mark termination polling (§9.4) |
| `view-changed {epoch}` (gossip payload) | latency hint (§5.5) |

Transition progress (`participants_ack`, `participants`, `done`) and GC
progress (`acked`, `mark_done`, `sweep_done`) are CASes on the registers,
not messages: the register is the truth a successor coordinator reads.

## 11. The client library

`github.com/amber-store/dstore/client` is what the `dstore` CLI, gateways,
and consumers such as jobs-iroh embed. Its unit of work is a *cluster
handle*: a view cache, a connection pool per node, and the operations
below. Everything is idempotent and resumable: re-running after an
interruption transfers only what is still missing.

### 11.1 View and ranking

`Dial(ticket…)` connects to any bootstrap node, fetches the view, and
keeps it; every operation stamps requests with the cached epoch and on
`stale-view` adopts the reply's view and retries the failed request (at
most `R` times per key before surfacing the error). `Owners(key)`,
`WriteSet(key)`, `ReadOrder(key)` are the §4 functions over the cached
view.

**Owner preference by probing.** Placement says *which* nodes hold a
key; the client decides which of them to talk to first by measuring its
paths. For every node it has a connection to, the pool records the
connection's current path — direct or relayed, which go-iroh reports
and updates as hole punching lands — and its smoothed round-trip time
from QUIC's own estimate; a node the client has never dialed is dialed
when rank order first puts it ahead of a measured one, so measurements
fill in on first use and no probing traffic is spent on nodes the client
never needs. `Primary(key)` orders a key's owners under `nodes` by: a
direct path before a relayed one, then lowest round-trip time, then rank
— so the write goes to the nearest owner and the LAN, not the client's
uplink, carries the replication. Reads use the same preference to choose
which owner to ask first (§6.3 treats the ranking as a hint), and fall
down the ranking exactly as before. Measurements age out after a minute
without traffic and are re-taken; a path that changes (a punch lands, a
relay takes over) re-orders the next batch, never one in flight.

### 11.2 Objects

- `Missing(keys) map[NodeID][]Key` and `Put(objects) (per-key replica
  count, per-node errors)` implement §6.2: keys grouped by primary,
  byte-balanced batches per primary, parallel workers per node (`Jobs`,
  default GOMAXPROCS), each record sent once.
- `Get(keys) iter` implements §6.3: group by the preferred owner (§11.1), verify,
  re-ask down the read order, per-node backoff (`5 s` after a failure,
  exponential to `60 s`).

### 11.3 Throughput

One iroh endpoint tops out well below a fast link: measured on the
reference Mac over loopback with both ends on the machine (2026-09-08),
a single endpoint receives about 300 MB/s with Rust iroh 1.1.0 and
350–380 MB/s with go-iroh 0.2.0, whatever the number of streams sharing
the connection or of client sockets feeding it, and a second receiving
endpoint adds roughly half again before the machine saturates. So bulk
transfers open extra connections to a node's data endpoints (§3), each
its own socket, exactly as transport-iroh's sharded transfers do, but
without tokens; and every endpoint, node or client, configures its QUIC
flow-control windows for the bandwidth-delay product of a WAN path
(§16), since the library defaults cap a single stream at a few tens of
MB/s across 40 ms: every `put`/`get` stream is
independent, so the pool simply holds `Conns` (default 4) connections per
node and deals batches across them. Per-node pools grow under load and
shrink after ~90 s idle, as jobs-iroh's `amberclient` does; the total
cap is at least one control connection per node, so a 50-node cluster is
never served through evictions. First dials try direct addresses with a
2 s timeout and race the relay only on retry, and a `view` reply carries
the answering node's own list of members it cannot currently reach, so a
client skips a dead node on the first attempt instead of paying a dial
timeout per process.

### 11.4 Tree push

`Push(root, name, expectedVersion | expectedOld | force)`: walk the local packstore's tree
(`fstree.ReachableKeys`), `Missing` per primary with `pin: true`, `Put`
what is missing to the primaries, check every key placed (§6.2 ack
policy), then `ref-put` through any node. A push that runs longer than
`gc_interval/2` re-negotiates every key uploaded so far with
`Missing {pin: true}` at that interval (key lists only, a few MB per
million keys), pinning any owner the reply shows unconfirmed directly,
so that a multi-hour upload never outlives the protection of its early
objects (§9.5). A key that cannot be placed fails the push
with the unreachable owners named; a `cas-mismatch` surfaces with the
current key (the CLI suggests `pull` or `--force`); an `incomplete` —
possible if GC reaped a dedup-hit object between the negotiation and the
gate — re-runs the whole negotiation (the cheap part; its `short` list
now names every owner that lacks a key), sends what is missing anywhere
to an owner that lacks it, and retries; it never trusts the sample of
keys the error names to be the whole gap, and after two such rounds it
fails naming the owners still short rather than loop.

### 11.5 Tree pull

`Pull(name)`: `ref-get`; then a top-down frontier walk: keys the local
store already holds *and* whose subtree `fstree.CheckComplete` confirms
are pruned (transport-iroh's rule — an interrupted pull leaves parents
above missing children); the rest are fetched with `Get`, verified,
written to the local packstore, and tree objects are parsed to extend the
frontier. A final completeness walk is the gate before the local reference
is written.

### 11.6 Gateway mode

`dstore serve --gateway` additionally registers the transport-iroh ALPN
`amber-store-iroh/1` and serves its push/pull/ref-list/pin operations by
routing through the client library: the server-driven want loop computes
wants with cluster-wide `Missing`, received objects go to their
primaries with `Put`, and the final commit is a `ref-put`. Existing clients — jobs-iroh's
`amberclient`, the `amber` CLI — then work unchanged against a cluster.
Records fetched for a pull round are staged in a small local packstore
(`<store>/gwcache`, size-capped, entries dropped after an hour) so the
want loop can stream them in transport-iroh's order. This needs
transport-iroh's `wantsync` to accept an interface (`Has/Missing/Get/Put`)
instead of a concrete `*packstore.Store`, and a paged `ref-list` (§15);
until the latter lands the legacy single-frame listing is served up to
~80 k references and refused beyond. `TPin` maps to a `keep/<name>`
register (§5.2) the operator's retention policy can read; dstore itself
never expires references. Every node can be a gateway, and clients should be
given several tickets, so legacy traffic is not funnelled through one
node.

## 12. Failure catalogue

| scenario | what happens | what heals it |
|---|---|---|
| node crashes and restarts, no view change | reads fall through to the next owner; writers take their next-preferred owner as primary, which places on the others (`min_replicas`) and names the owner it could not reach | the node's own and its peers' background reconcile (`replicated = false` packs) |
| node lost for good | as above until the operator removes it | transition: surviving owners forward its share to the new owners (§8) |
| node joins | transition; clients dual-place until commit | — |
| client crashes mid-upload | orphan objects, exempt from two sweeps (§9.5) | GC |
| client crashes between upload and `ref-put` | as above | GC; a retry re-negotiates (mostly dedup) and pins again at every owner the reply confirms |
| reference PUT races a sweep | pins (§9.5) or a clean `incomplete` | client re-negotiates and re-uploads what is missing |
| two clients CAS the same reference | one wins; the other gets `cas-mismatch{current}` | — |
| network partition | minority side: catalog operations `unavailable`; objects still readable/writable where owners are reachable; placement unchanged | reconnect; objects are idempotent, catalog needs nothing; nodes the majority removed meanwhile hand their data back as ex-members (§8.4) |
| coordinator dies mid-transition | participants keep their per-pack progress; the lease expires; the next holder resumes from the registers | — |
| coordinator dies mid-GC | barrier/mark abandoned, sweep unaffected (§9.8) | — |
| a PUT coordinator stalls for hours (paused VM) | its own accept is refused by the acceptors (`not_after`); the client retries | — |
| a PUT coordinator crashes mid-commit | the register may stay undecided; the next read that finds the voters disagreeing, or the GC snapshot, settles it (§7, §9.2); the client sees `timeout` and retries with CAS | — |
| an upload runs for many hours | the client re-pins every `gc_interval/2` (§11.4); an old client that does not would find its early objects reaped and get `incomplete` | re-run with a current client |
| disk nearly full on an owner | the node clears its own `writable` flag below a reserve (5 % or 100 GiB); writers skip it and report; alert at 80/90 % | operator lowers the weight or adds nodes → transition |
| disk full on an owner | `no-space` on uploads; sweeps keep running inside the headroom (§9.6); the catalog is unaffected on a separate device | as above |
| a majority of voters lost (disks intact) | catalog unavailable; objects still served | `dstore cluster recover` (§13): new incarnation, re-seat the catalog on the survivors, fence the lost voters, hold GC until the salvage is reviewed |
| a voter's catalog disk lost, node restarted with the same identity | the acceptor answers `amnesiac`, alert | `voter remove` + `voter add` re-syncs it (§5.3) |
| all voters' catalogs destroyed | references gone, objects intact but unnamed | `dstore catalog restore` from the hourly backup object (§13) |
| a voter is down | catalog works with a majority; tombstone purge pauses | node returns, or the operator removes it (§8.1) |
| clock skew on a coordinator | a lease may be judged expired early → two coordinators for a moment; both CAS the same registers, one loses every CAS | keep clocks within seconds; lease is 60 s |
| corrupt record on disk | CRC/hash fails on read; that owner reports absent; reader tries the next | background reconcile re-copies from a good owner (`replicated=false` after a verify failure) |
| missing object under a reference | mark aborts with the name and key; nothing swept | operator restores from a backup/pull; `dstore gc why KEY` (§13) |

## 13. Operations

```
dstore cluster init  --store DIR --replicas 3 [--weight GiB]   # first node; prints its ticket
dstore serve         --store DIR [--gateway] [--relay URL] [--paxos-dir DIR] [--rate B/s]
dstore token create                     # a single-use join token
dstore node join     --store DIR --seed TICKET --token T --weight GiB|auto [--no-ramp] [--no-vote]
dstore node remove ID [--dead] | drain ID | weight ID GiB | zone ID Z | repair ID
dstore voter add ID | remove ID [--allow-unsafe]      # change a node's vote after join
dstore cluster replicas R
dstore transition status | abort | refreeze | pause | resume
dstore gc run [--tolerate-missing] | run --garbage F | status | why KEY   # run waits for the barrier spacing; --garbage re-sweeps only; why: the references that reach KEY
dstore cluster status | ticket          # status: view, epoch, reachability, disk, transition, gc, voters; ticket: the bootstrap ticket (§5.5)
dstore catalog backup | restore KEY|FILE | salvage DIR   # the reference set as an object (below)
dstore cluster recover --from ID --lost ID,… --lost-destroyed   # majority-loss recovery (below)
dstore push/pull/ls/refs/ref …          # client commands, as the amber CLI
```

**Disaster recovery.** The reference set is the only state that is not
content-addressed, so it is backed up and recoverable:

- `dstore catalog backup` (run hourly by the maintenance coordinator)
  writes a snapshot of every `ref/*` register as an object into the data
  plane, names it with a reserved reference (`dstore/catalog-backup`,
  keeping the last 24) and — because that name lives in the catalog being
  backed up — also records the last 24 snapshot keys in every node's
  `meta` over the cluster ALPN and in the log; `restore KEY|FILE`
  force-writes such a snapshot into a fresh voter set. 10^6 references
  are ~300 MB.
- `dstore cluster recover --from A --lost B,C --lost-destroyed` re-seats
  the catalog on the surviving voter(s) after a majority loss. It
  refuses if any listed voter is reachable and requires the operator to
  attest that the lost voters are gone, then writes a view with a new
  **incarnation**, an epoch above the highest the survivors have seen,
  voters = the survivors, the lost ids **fenced**, and
  `recovered_from = {incarnation, epoch}`. Every catalog message carries
  the incarnation and an acceptor refuses any other, so two incarnations
  can never merge by version. What this cannot prevent is a wrong-side
  recovery: run on the minority side of a partition, it creates a second
  live catalog, and the majority side keeps committing references its
  clients are acked for until the partition heals — which is why the
  attestation flag exists. At heal, a member that hears from a fenced id
  tells it to retire, sending along a digest of the live catalog's
  `(name, version)` pairs; a fenced voter that holds any accepted value
  the live catalog lacks refuses to retire and raises
  `recovery-vs-live-majority` instead — under `--lost-destroyed` its very
  existence contradicts the attestation — so a wrong-side recovery
  surfaces loudly; one that holds nothing the live catalog lacks moves
  `<store>/paxos` aside (`paxos.retired-<epoch>`, kept 7 days) rather
  than deleting it, so it can still be inspected
  (`dstore catalog salvage DIR` diffs it against the live set), and it
  is re-admitted only through an explicit wipe and `voter add`. Recovery
  sets `gc.hold`; no cycle runs until an operator clears it after
  reviewing the salvage, because a reference lost in recovery is a tree
  GC would otherwise reap.

**Node size.** packstore keeps every sealed segment mmapped and, on a
lookup, probes segments' filters newest-first until it finds the key —
so an *absent* key, which is what most of a push negotiation and every
reconcile offer consists of, costs one probe per pack: ~0.5 ms per key
on a 10 TiB node, seconds per 8192-key `missing`. dstore therefore
requires packstore's node-level key index (§15) from the first version,
chooses the segment size from capacity (256 MiB below 4 TiB, 1 GiB
below 16 TiB, 4 GiB above, keeping packs under ~25 k per node), raises
`vm.max_map_count` in the operations guide, and refuses to start a node
whose pack count is within headroom of that limit rather than fail its
next seal. Mapping whole segments has a second cost: the kernel keeps
one 8-byte page-table entry per 4 KiB page ever touched through a
mapping until it is unmapped — about 2 GiB of unreclaimable memory per
TiB of pack bytes read (a sweep or a big reconcile reads most of a
pack), and a 100 TiB store approaches the process address space on
x86-64 with 4-level paging. Until packstore reads record bodies with
`pread` and maps only footers (§15), the honest node-size ceiling is
10–20 TiB. Below the free-space reserve a node runs an ownership-only
compaction at line 0 over unowned bytes (a weight cut must actually free
space), refuses uploads, and keeps accepting reconcile traffic.

Node configuration is flags/env only (`--store`, `--jobs`, `--rate` for
the reconcile copier, `--min-free`); everything cluster-wide is in the
view. Logs are structured (`slog`), one line per completed operation with
peer ID, bytes, duration. `status` exposes counters for a scraper: objects,
bytes, garbage after the last epoch, *foreign* bytes (records the node
holds but does not own — reclaimed only lazily, §9.6, so growth in many
small steps leaves them behind; add capacity in larger steps), packs
pending reconcile, pins, catalog round latencies, per-peer reachability.
Every stall is visible: a transition or an epoch that is waiting names
the node it waits for.

**Milestones.** M1 — a static cluster formed at `cluster init` from a
fixed list of nodes, all voters: the key index in core, the placement
package with its golden vectors (shared with core-rs), CASPaxos
registers over iroh with Pebble acceptors, nodes serving
`view`/`missing`/`get`/`put`/`ref-*`, the client library with ranked
retries, primary-forwarded push and pull, an in-process five-node test.
M2 — the maintenance lease, the voter change with its sync, transitions
with the reconcile pass, join/remove/drain/weight. M3 — garbage
collection: barriers, pins, the owner-partitioned mark, sweeps. M4 —
recovery and backup, gateway mode, the benchmark. Simulation and the Quint models
(§16) start with M1 and grow with each milestone.

## 14. Sizing

| thing | size |
|---|---|
| view | ~100 B per node; a 50-node view is ~5 KB |
| catalog | 10^6 references × ~300 B ≈ 300 MB per voter, plus tombstones until purged |
| pins | 16 B per dedup-hit key negotiated or found present in an old pack within two `gc_interval`s; fresh uploads pin nothing |
| per-pack meta | ~40 B per pack; 1024 packs per TiB at 1 GiB segments |
| node size | 10–20 TiB per node while packstore maps whole segments (§13); beyond that needs bodies read by `pread` |
| page tables | ~2 GiB per TiB of pack bytes read through mmap, freed only at unmap (§13) |
| reconcile key lists | 32 B per key per target owner |
| mark bitmap | 1 bit per record the node holds; the delivered tails are not stored |
| visited set | 32 B per interior key the node expands (≈2 % of its share) |

Ranking a slot is the only O(N) cost (~3 µs at 50 nodes, once per slot
per epoch, lazily); per key, placement is a table lookup. Nothing else
grows with the node count except the view itself and the number of
`missing` batches a pass sends.

## 15. What core needs

Small, additive changes in `github.com/amber-store/core`:

1. `packstore.Store.Seal()` — force-seal the active segment (Compact does
   it internally today); needed so a transition's pass has a finite set of
   packs (§8.2) and for `seal_after` (§8.4). GC does not need it.
2. `packstore.Store.Count()` or `SegmentInfo.Keys` exposed for the
   barrier ack and for sizing each pack's mark bitmap (§9.4).
3. `Compact` reshaped for a long-running sweep (§9.6): its
   `live func(key.Key) bool` stays the predicate; add
   `CompactOpts.Victims []uint64` (score once, pass an ordered list; today
   every call re-scores every segment — this also bounds candidates to
   ids below `eligible_below`), a `live` callback that receives the
   segment id as well as the key (the settled test is per pack),
   `MaxCopyBytes` per call, unlink-first for victims with no live record,
   and a copy loop that takes the append lock per record with only the
   final delta re-check, fsync and unlink under the exclusive section —
   today `Compact` holds the append lock for the whole call, which would
   stop a node's ingest for the duration of its sweep. Three more
   details matter once a sweep is batched: a survivor is a copy outside
   the *whole* sweep's victim list, not outside the batch (today's
   `survivorHas` is per call, so two batches could each leave a
   duplicate to the other); a record that fails verification while
   copying is skipped and reported, not fatal to the batch; `Compact`
   does not seal the active segment per call, and `Victims` tolerates an
   id a previous sweep already unlinked. A failed seal or
   append (ENOSPC) must not poison the store permanently: today
   `setFailed` is sticky until restart, and a full node has to be able
   to sweep its way out and resume.
4. `ScanIndex` with a resume position (`ScanIndexFrom(id, pos)`), so a
   reconcile cursor resumes in O(1), and an enumeration of the *active*
   segment's in-RAM index, so the first audit can cover records before
   they are sealed (§8.4).
5. Segment ids **monotone across restarts**: today `nextID` is
   recomputed at open as the highest id on disk plus one, so after a
   compaction unlinked the highest packs and the active segment was
   sealed, a restart reuses ids — and every dstore rule keyed on ids
   (`eligible_below`, `seal`, the stamps) would silently break. packstore
   should persist a high-water id (one fsynced file written before each
   new active segment; open takes the maximum) and expose the next id;
   dstore refuses to open a store whose next id is below its recorded
   `eligible_below` or `seal`, and deletes a pack's stamp when the pack
   is unlinked.
6. Per-segment scrub pins instead of the global scrub gate, which
   `ScanIndex` and `Record` take too — the reconcile pass reads packs
   continuously, so pack removal must wait only on readers of *that*
   pack. Required from the first version.
7. A node-level key index in packstore — a **multimap**
   `tail → [(segment id, footer position)]`, since two keys may share a
   tail and a record may be held twice; rebuilt from the footers at
   open, maintained at seal and by Compact; Pebble or a compact in-RAM
   table — consulted by `Has`, `Missing`, `Get` (comparing full keys
   across all entries) before any per-pack filter probe, and by the GC
   mark through a `MarkTail(tail)` that sets every matching bit (§9.4).
   `MarkSet` should then hold segment ids and bitmaps only — today it
   clones every footer's index at construction, ~44 bytes per key, and
   locates newest-first through the pack filters — so a sweep may unlink
   a segment while a mark is in progress. Also close the dedup race at a
   segment boundary: `Put` checks `Has` and then appends under a
   different lock, so two concurrent writes of one key can land in two
   segments; re-check under the append lock. Required from the first
   version: without it every absent-key lookup is
   O(packs), and negotiation, reconcile and the mark are quadratic in
   store size (core's own note on `MarkSet.locate` at 599 packs is this
   effect).
8. A `WriteParallel` mode that skips its final fsync and returns a
   durability waiter, so a node can group-commit many small inbound
   streams (reconcile, forwards) under one fsync instead of one per
   stream — which on disks with slow fsync caps replication at the
   fsync rate.
9. Record bodies read with `pread` (mapping only the footer's index and
   filter), or cold segments unmapped and remapped on demand, so that
   page-table memory does not grow with every byte ever read and nodes
   can pass 20 TiB (§13). Not needed for the first version.
10. transport-iroh: `wantsync.Send` over a small store interface and the
   token/gather helpers exported, for the gateway (§11.6), and a paged
   `ref-list` (today's single 16 MiB frame caps a listing near 80 k
   references; jobs-iroh's `Refs()` must page too).

Everything else — the key format, amberpack records, the reference record,
`fstree` walks, `WriteParallel`'s verification — is used as is.

## 16. Verification plan

- **Models.** Three Quint specs in `specs/` (the core repo's convention):
  `paxos.qnt` — CASPaxos with epoch-gated acceptors and the one-at-a-time
  voter change with sync, checking invariant V; `gc.qnt` — barriers (including
  cycles abandoned before a snapshot), PUT accepts bounded by `not_after`,
  minority-accepted values completed by later identity transitions, the
  snapshot's settle step, the exempt region and pins with the two-barrier
  rule (P), and `gc_interval ≥ 2·put_ttl`, checking "no accepted
  reference reaches a swept object" and "no unowned live record is
  reaped"; `xfer.qnt` —
  transitions with absent/crashing participants, rounds, and the pack
  stamps, checking F and "at commit, every object with a live holder has
  been offered to every owner under the new view, and is held by every
  such owner that was reachable during the pass".
- **Deterministic simulation.** The node, client and coordinator code are
  written against a `transport` interface; a simulated transport with
  partitions, delays, reordering, and crash/restart drives a whole cluster
  in one process with a seeded schedule, checking the same invariants plus
  "no dangling reference" and "every stored byte verifies" after every
  step. This is where most bugs will be found.
- **End-to-end** over real iroh endpoints connected by direct address (no
  public discovery), as transport-iroh's tests do: join/remove with data,
  a GC cycle under concurrent pushes, gateway mode with the existing
  `amber` CLI.
- **Benchmark**: an extension of core's `cmd/amber-bench` that drives a
  local multi-node cluster through ingest → delete → transition → GC and
  reports the same tables.
- **Two measurements before the protocol is built**, because the cost
  model depends on them. First, iroh's per-endpoint throughput on the
  target Linux hardware over a 10 GbE link: on the reference Mac over
  loopback (2026-09-08, both ends on one machine, macOS without UDP
  segmentation offload) one endpoint receives ~300 MB/s with Rust iroh
  1.1.0 and ~350–380 MB/s with go-iroh 0.2.0, independent of stream and
  socket count on the sending side, with a second receiving endpoint
  adding about half; Linux with GSO/GRO should do better, and the number
  of data endpoints per node (§3) is sized from that measurement — the
  transport stays iroh. Across a WAN (same day, a Hetzner box with a
  1 Gbit NIC against a Linux node on a home LAN, 43 ms RTT, direct
  addresses) both stacks reach the NIC's line rate, ~107 MB/s on one
  stream — go-iroh out of the box and Rust iroh only once its QUIC
  windows are raised (the quinn defaults, a 1.25 MB stream window, cap
  one stream at ~27 MB/s at that RTT), so a dstore node and client set
  `stream_receive_window`, `receive_window` and `send_window` from the
  bandwidth-delay product they are meant to carry, tens of MiB for a
  1 Gbit WAN. Second, `missing` of 32 k absent keys against a node with
  25 k packs after the key index lands (target: under a second).

## 17. Non-goals and later work

- Authentication beyond iroh identities; signed-reference ownership stays
  an optional policy. Encryption at rest.
- Automatic dead-node removal. A partition would otherwise trigger a mass
  rebalance; the operator decides, with `cluster status` showing how long
  a node has been unreachable. An `auto-remove-after` knob can come later.
- Erasure coding; rack awareness beyond the per-node `zone` (§3).
- Nodes beyond ~20 TiB: the brief's upper range needs packstore to stop
  mapping whole segments (§15.8), which the first version does not do.
- Sloppy writes (a client writing to the next-ranked non-owner when an
  owner is down, Dynamo-style): the reconcile pass would heal them into
  the owners like any other held data, so they fit the model, but
  `min_replicas` already covers one owner down and the extra movement is
  not worth it until measured.
- Reference retention/expiry (jobs-iroh's `reftrack` policy) — a consumer
  layer over `ref-list` + `ref-delete`, as today.
- Multi-cluster replication.

## 18. Decisions to confirm

Choices made here that the reader may want to change:

1. **Every node is a voter by default** (§1): at three to eight nodes a
   separate voter role is a second membership for no gain; `--no-vote`
   exists for a node that should hold no catalog.
2. **Writes are replicated internally**: a client sends each record once
   to a primary owner, which forwards it to the others (§6.2). Clients
   never fan out; the price is one extra LAN hop per record. The primary
   is the owner with the best measured path from the client — direct
   before relayed, then lowest round-trip time (§11.1).
3. **`min_replicas` defaults to `max(R−1, 2)`**: writes and reference
   PUTs proceed with one owner down at `R ≥ 3`; at `R = 2` they wait for
   both owners unless the operator allows a single copy (§6.2).
4. **Dead-node removal is an operator action** (§17).
5. **Reference PUTs are coordinated by a node**: the completeness walk
   is many small owner round trips, best done on the LAN; a client could
   run it itself at WAN cost with no change to the GC argument (§7).
6. **The mark is owner-partitioned** and each node marks streamed key
   tails into an exact bitmap over its own records (§9.4), one bit per
   record — the brief's probabilistic set was rejected as both larger
   (16 bits per key) and less precise; the simpler root-partitioned mark
   with remote fetches would do for small clusters but not at 10^9.
7. **Client access is open by default**, like transport-iroh (§2) — which
   makes a cluster whose nodes are reachable through relays writable by
   anyone who learns a ticket; deployments outside a trusted network
   should set the allowlist.
8. **Placement is per 2^20 hash slot**, a protocol constant; keys in a
   slot share owners (§4).
9. **Reference records carry a version** for CAS; key-based CAS is kept
   for transport-iroh-shaped clients (§7).
10. **The view lists members, not live nodes**; reachability is
    advisory and removal is manual (§3, §17).
11. Module path `github.com/amber-store/dstore`, one binary `dstore`.
