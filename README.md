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
  exact per-node mark bitmap reclaims what no reference reaches.

> **Status:** design. The architecture is specified in
> [`architecture/dstore.md`](architecture/dstore.md); there is no code yet.
