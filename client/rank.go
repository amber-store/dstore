package client

import (
	"sort"
	"time"

	"github.com/amber-store/dstore/transport"
	"github.com/amber-store/dstore/view"
)

// rttClass buckets a round-trip time so that owners at the same distance
// compare equal and rank order spreads a client's batches over them: a
// LAN cluster is served by every owner, not by whichever answered a
// fraction of a millisecond sooner. An owner the client has not dialed
// yet counts as near, so that rank order puts it in play and it is
// dialed and measured on first use (§11.1).
func rttClass(rtt time.Duration) int {
	switch {
	case rtt < 5*time.Millisecond:
		return 0
	case rtt < 25*time.Millisecond:
		return 1
	case rtt < 100*time.Millisecond:
		return 2
	default:
		return 3
	}
}

// rankOwners orders a key's owners by path preference (§11.1): penalised
// owners last, a direct path before a relayed one, then the round-trip
// class, then the placement's rank order. Unmeasured owners rank as
// direct and near.
func rankOwners(ids []view.NodeID, penalty func(view.NodeID) int, path func(view.NodeID) (transport.PathInfo, bool)) []view.NodeID {
	type scored struct {
		id    view.NodeID
		pen   int
		relay int
		class int
		pos   int
	}
	out := make([]scored, len(ids))
	for i, id := range ids {
		s := scored{id: id, pos: i, pen: penalty(id)}
		if p, ok := path(id); ok {
			if !p.Direct {
				s.relay = 1
			}
			s.class = rttClass(p.RTT)
		}
		out[i] = s
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.pen != b.pen {
			return a.pen < b.pen
		}
		if a.relay != b.relay {
			return a.relay < b.relay
		}
		if a.class != b.class {
			return a.class < b.class
		}
		return a.pos < b.pos
	})
	ranked := make([]view.NodeID, len(out))
	for i, s := range out {
		ranked[i] = s.id
	}
	return ranked
}
