// Package ticket encodes the cluster ticket clients bootstrap from
// (architecture/dstore.md §5.5): "dstore1" followed by base32 CBOR of the
// cluster id, its incarnation and the addresses of a few members.
package ticket

import (
	"encoding/base32"
	"errors"
	"fmt"
	"strings"

	"github.com/amber-store/dstore/codec"
)

// Prefix is the ticket's textual prefix.
const Prefix = "dstore1"

// Member is one bootstrap node.
type Member struct {
	ID    []byte   `cbor:"0,keyasint"`
	Addrs []string `cbor:"1,keyasint,omitempty"`
}

// Ticket is the bootstrap information.
type Ticket struct {
	ClusterID   []byte   `cbor:"0,keyasint"`
	Incarnation uint64   `cbor:"1,keyasint"`
	Members     []Member `cbor:"2,keyasint"`
}

var enc = base32.StdEncoding.WithPadding(base32.NoPadding)

// Encode returns the textual ticket.
func (t Ticket) Encode() string {
	return Prefix + strings.ToLower(enc.EncodeToString(codec.MustMarshal(t)))
}

func (t Ticket) String() string { return t.Encode() }

// Parse decodes a ticket.
func Parse(s string) (Ticket, error) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(strings.ToLower(s), Prefix) {
		return Ticket{}, errors.New("ticket: missing dstore1 prefix")
	}
	b, err := enc.DecodeString(strings.ToUpper(s[len(Prefix):]))
	if err != nil {
		return Ticket{}, fmt.Errorf("ticket: %w", err)
	}
	var t Ticket
	if err := codec.Unmarshal(b, &t); err != nil {
		return Ticket{}, fmt.Errorf("ticket: %w", err)
	}
	if len(t.Members) == 0 {
		return Ticket{}, errors.New("ticket: no members")
	}
	return t, nil
}
