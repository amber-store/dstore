package client

import (
	"slices"
	"testing"
	"time"

	"github.com/amber-store/dstore/transport"
	"github.com/amber-store/dstore/view"
)

func ownerIDs(n int) []view.NodeID {
	out := make([]view.NodeID, n)
	for i := range out {
		out[i][0] = byte(i + 1)
	}
	return out
}

func noPenalty(view.NodeID) int { return 0 }

func pathsOf(m map[view.NodeID]transport.PathInfo) func(view.NodeID) (transport.PathInfo, bool) {
	return func(id view.NodeID) (transport.PathInfo, bool) {
		p, ok := m[id]
		return p, ok
	}
}

func TestRankOwnersKeepsRankAmongUnmeasuredOwners(t *testing.T) {
	ids := ownerIDs(3)
	got := rankOwners(ids, noPenalty, pathsOf(nil))
	if !slices.Equal(got, ids) {
		t.Fatalf("got %v, want rank order %v", got, ids)
	}
}

func TestRankOwnersDoesNotDemoteUnmeasuredOwnersBehindAMeasuredOne(t *testing.T) {
	// Only the third-ranked owner has been dialed (a LAN path). Rank order
	// must still put the first two ahead, so that a push spreads over all
	// owners and dials them on first use (§11.1).
	ids := ownerIDs(3)
	paths := map[view.NodeID]transport.PathInfo{ids[2]: {Direct: true, RTT: time.Millisecond}}
	got := rankOwners(ids, noPenalty, pathsOf(paths))
	if !slices.Equal(got, ids) {
		t.Fatalf("got %v, want rank order %v", got, ids)
	}
}

func TestRankOwnersPrefersDirectOverRelayed(t *testing.T) {
	ids := ownerIDs(2)
	paths := map[view.NodeID]transport.PathInfo{
		ids[0]: {Direct: false, RTT: time.Millisecond},
		ids[1]: {Direct: true, RTT: 2 * time.Millisecond},
	}
	got := rankOwners(ids, noPenalty, pathsOf(paths))
	if got[0] != ids[1] {
		t.Fatalf("got %v, want the direct owner first", got)
	}
}

func TestRankOwnersPrefersUnmeasuredOverRelayed(t *testing.T) {
	ids := ownerIDs(2)
	paths := map[view.NodeID]transport.PathInfo{ids[0]: {Direct: false, RTT: time.Millisecond}}
	got := rankOwners(ids, noPenalty, pathsOf(paths))
	if got[0] != ids[1] {
		t.Fatalf("got %v, want the undialed owner ahead of the relayed one", got)
	}
}

func TestRankOwnersTiesNearRoundTripsByRank(t *testing.T) {
	// Two owners on the same LAN differ by a fraction of a millisecond;
	// rank order, not the smaller number, decides so that both share the
	// client's batches.
	ids := ownerIDs(2)
	paths := map[view.NodeID]transport.PathInfo{
		ids[0]: {Direct: true, RTT: 900 * time.Microsecond},
		ids[1]: {Direct: true, RTT: 400 * time.Microsecond},
	}
	got := rankOwners(ids, noPenalty, pathsOf(paths))
	if !slices.Equal(got, ids) {
		t.Fatalf("got %v, want rank order %v", got, ids)
	}
}

func TestRankOwnersPrefersAMuchNearerOwner(t *testing.T) {
	ids := ownerIDs(2)
	paths := map[view.NodeID]transport.PathInfo{
		ids[0]: {Direct: true, RTT: 60 * time.Millisecond},
		ids[1]: {Direct: true, RTT: 2 * time.Millisecond},
	}
	got := rankOwners(ids, noPenalty, pathsOf(paths))
	if got[0] != ids[1] {
		t.Fatalf("got %v, want the near owner first", got)
	}
}

func TestRankOwnersPutsPenalisedOwnersLast(t *testing.T) {
	ids := ownerIDs(3)
	pen := func(id view.NodeID) int {
		if id == ids[0] {
			return 2
		}
		return 0
	}
	got := rankOwners(ids, pen, pathsOf(nil))
	want := []view.NodeID{ids[1], ids[2], ids[0]}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
