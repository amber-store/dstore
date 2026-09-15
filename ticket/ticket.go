// Package ticket encodes what clients bootstrap from (architecture/dstore.md
// §5.5): the cluster ticket, "dstore1" followed by base32 CBOR of the
// cluster id, its incarnation and the addresses of a few members, or the
// short form, a list of member ids whose addresses discovery finds.
package ticket

import (
	"encoding/base32"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/amber-store/dstore/codec"
	irohkey "github.com/tmc/go-iroh/key"
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

// IDs returns the short form: the members' ids in hex, comma-separated.
// It parses back to a ticket whose members have no addresses.
func (t Ticket) IDs() string {
	ids := make([]string, 0, len(t.Members))
	for _, m := range t.Members {
		if len(m.ID) == 32 {
			ids = append(ids, hex.EncodeToString(m.ID))
		}
	}
	return strings.Join(ids, ",")
}

// Parse decodes a ticket: the dstore1… form, or a list of node ids
// separated by commas or whitespace, each 64 hex characters or iroh's
// 52-character base32 form, naming members to find by discovery.
func Parse(s string) (Ticket, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Ticket{}, errors.New("ticket: empty")
	}
	if !strings.HasPrefix(strings.ToLower(s), Prefix) {
		return parseIDs(s)
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

func parseIDs(s string) (Ticket, error) {
	var t Ticket
	for _, f := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' || r == '\n' || r == '\r' }) {
		id, err := irohkey.ParseEndpointID(strings.ToLower(f))
		if err != nil {
			return Ticket{}, fmt.Errorf("ticket: %q is neither a dstore1 ticket nor a node id: %w", f, err)
		}
		b := id.Bytes()
		t.Members = append(t.Members, Member{ID: b[:]})
	}
	if len(t.Members) == 0 {
		return Ticket{}, errors.New("ticket: no members")
	}
	return t, nil
}
