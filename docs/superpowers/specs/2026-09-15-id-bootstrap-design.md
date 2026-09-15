# Bootstrapping from node ids

*2026-09-15. Approved design; implemented on the `id-bootstrap` branch.*

## Goal

A client (or a joining node) can reach a cluster knowing only the ids of
one or more members, instead of the long `dstore1…` ticket that carries
their addresses. The endpoint finds a member's addresses by racing mDNS
on the local link against number0's DNS service, and everything else
comes from the view once one member answers. The ticket stays as the
form that needs no discovery infrastructure.

## Ticket forms

`ticket.Parse` accepts either:

- the existing `dstore1…` ticket (cluster id, incarnation, members with
  addresses), or
- a list of node ids separated by commas or whitespace, each 64 hex
  characters (what the CLI prints) or iroh's 52-character RFC 4648
  base32 form. The result is a ticket whose members carry no addresses
  and no cluster id.

`Ticket.IDs()` returns the members' ids, comma-separated hex: the short
form to hand out. `cluster init` prints `node ids:` beside the ticket;
`cluster ticket --ids` prints the ids of every member. `--ticket`,
`DSTORE_TICKET` and `node join --seed` (so `DSTORE_SEED` in the
container) take either form.

## Transport

`IrohConfig` gains `Discover`, `Announce` and `Logger`.

- **Discover** registers go-iroh address-lookup resolvers: mDNS (lookup
  timeout 3 s) and, when relays are enabled, number0's DNS lookup. A
  dial by id alone resolves through them; go-iroh runs every resolver
  concurrently and takes the first usable answer. A dial whose given
  addresses all fail falls back to a discovery dial before giving up.
- **Announce** publishes this endpoint's addresses: mDNS announcements
  (direct addresses) and, when relays are enabled, a pkarr record at
  number0's relay carrying the relay URL and the direct addresses (an
  identity address filter; the go-iroh default would publish the relay
  only). The endpoint publishes after bind, once its relay URL is known,
  and republishes whenever its address set changes, checked every 30 s.
  The pkarr publisher republishes on its own interval as well.
- `--no-relay` keeps mDNS but skips the number0 services: it means "no
  external infrastructure".
- The mDNS listener runs in a goroutine for the endpoint's life and is
  stopped by `Close`, which also closes the pkarr publisher. A listener
  that cannot bind logs a warning; the endpoint still works with the
  addresses it is given.
- The in-memory transport needs nothing: ids are addresses there.

## Who does what

Nodes discover and announce. Clients only discover, so an ephemeral
client identity is never published. A client with id-only members dials
them without addresses; after the first member answers, the view
supplies every member's addresses as today, and discovery remains the
fallback for a member whose listed addresses fail.

## CLI

Discovery is on by default for nodes and clients. `--no-discovery`
(`DSTORE_NO_DISCOVERY`) turns off both announcing and resolving. The
`--ticket` and `--seed` help text names both forms. The container
entrypoint needs no change.

## Docs and tests

Spec §5.5 describes the id-only bootstrap and what nodes publish; the
README shows an id-only connection. Tests: ticket parsing of both forms
and rejects, and `IDs()`; a real-iroh transport test where a server
announces over mDNS on loopback and a client dials it by id alone,
skipped when the machine cannot open an mDNS listener; the loopback e2e
script connects a client with ids only.
