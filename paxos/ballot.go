// Package paxos implements the CASPaxos registers of
// architecture/dstore.md §5: one Paxos instance per register, no log, no
// leader; acceptors gate on the view epoch, and the proposer runs
// compare-and-swap, fast reads, identity transitions and majority scans.
package paxos

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/amber-store/dstore/view"
)

// Ballot is (counter, proposer), ordered lexicographically. The zero ballot
// means "none".
type Ballot struct {
	Counter  uint64
	Proposer view.NodeID
}

// BallotSize is the encoded size of a ballot.
const BallotSize = 40

// IsZero reports whether b is the empty ballot.
func (b Ballot) IsZero() bool { return b.Counter == 0 && b.Proposer == (view.NodeID{}) }

// Compare orders ballots.
func (b Ballot) Compare(o Ballot) int {
	if b.Counter != o.Counter {
		if b.Counter < o.Counter {
			return -1
		}
		return 1
	}
	return bytes.Compare(b.Proposer[:], o.Proposer[:])
}

// Less reports b < o.
func (b Ballot) Less(o Ballot) bool { return b.Compare(o) < 0 }

// Bytes encodes the ballot: 8-byte big-endian counter then the proposer id.
// The encoding orders like the ballot.
func (b Ballot) Bytes() []byte {
	if b.IsZero() {
		return nil
	}
	out := make([]byte, BallotSize)
	binary.BigEndian.PutUint64(out[:8], b.Counter)
	copy(out[8:], b.Proposer[:])
	return out
}

// ParseBallot decodes a ballot; nil or empty decodes to the zero ballot.
func ParseBallot(b []byte) (Ballot, error) {
	if len(b) == 0 {
		return Ballot{}, nil
	}
	if len(b) != BallotSize {
		return Ballot{}, fmt.Errorf("paxos: ballot of %d bytes", len(b))
	}
	var out Ballot
	out.Counter = binary.BigEndian.Uint64(b[:8])
	copy(out.Proposer[:], b[8:])
	return out, nil
}

func (b Ballot) String() string {
	if b.IsZero() {
		return "-"
	}
	return fmt.Sprintf("%d@%s", b.Counter, view.ShortID(b.Proposer))
}
