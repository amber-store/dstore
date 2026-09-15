package ticket

import (
	"bytes"
	"encoding/base32"
	"encoding/hex"
	"strings"
	"testing"

	irohkey "github.com/tmc/go-iroh/key"
)

func TestRoundTrip(t *testing.T) {
	id := make([]byte, 32)
	id[0] = 7
	in := Ticket{ClusterID: []byte("0123456789abcdef"), Incarnation: 3, Members: []Member{{ID: id, Addrs: []string{"ip:127.0.0.1:4433", "relay:https://relay.example/"}}}}
	s := in.Encode()
	if !strings.HasPrefix(s, Prefix) {
		t.Fatalf("prefix: %s", s)
	}
	out, err := Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	if out.Incarnation != 3 || string(out.ClusterID) != "0123456789abcdef" || len(out.Members) != 1 || out.Members[0].Addrs[1] != "relay:https://relay.example/" {
		t.Fatalf("round trip: %+v", out)
	}
	if _, err := Parse("nope"); err == nil {
		t.Fatal("expected a prefix error")
	}
	if _, err := Parse(strings.ToUpper(s)); err != nil {
		t.Fatalf("case-insensitive parse: %v", err)
	}
}

func TestParseIDs(t *testing.T) {
	newID := func() []byte {
		sk, err := irohkey.GenerateSecretKey()
		if err != nil {
			t.Fatal(err)
		}
		b := sk.Public().EndpointID().Bytes()
		return b[:]
	}
	id1, id2 := newID(), newID()
	hex1, hex2 := hex.EncodeToString(id1), hex.EncodeToString(id2)
	// iroh's base32 form (RFC 4648, no padding, lower case).
	b32 := strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(id2))
	for _, s := range []string{hex1, hex1 + "," + hex2, hex1 + " " + hex2, " " + hex1 + ",\n" + b32 + " ", strings.ToUpper(hex1)} {
		tk, err := Parse(s)
		if err != nil {
			t.Fatalf("Parse(%q): %v", s, err)
		}
		if len(tk.ClusterID) != 0 || tk.Incarnation != 0 {
			t.Fatalf("Parse(%q): cluster fields set: %+v", s, tk)
		}
		if !bytes.Equal(tk.Members[0].ID, id1) || tk.Members[0].Addrs != nil {
			t.Fatalf("Parse(%q): member 0 %+v", s, tk.Members[0])
		}
		if strings.Contains(s, hex2) || strings.Contains(s, b32) {
			if len(tk.Members) != 2 || !bytes.Equal(tk.Members[1].ID, id2) {
				t.Fatalf("Parse(%q): members %+v", s, tk.Members)
			}
		} else if len(tk.Members) != 1 {
			t.Fatalf("Parse(%q): members %+v", s, tk.Members)
		}
	}
	for _, s := range []string{"", "  ", hex1[:63], hex1 + ",zz", strings.Repeat("ab", 32), "dstore1", "nope"} {
		if _, err := Parse(s); err == nil {
			t.Fatalf("Parse(%q) succeeded", s)
		}
	}
	tk, _ := Parse(hex1 + "," + hex2)
	if got := tk.IDs(); got != hex1+","+hex2 {
		t.Fatalf("IDs() = %q", got)
	}
	if _, err := Parse(tk.IDs()); err != nil {
		t.Fatalf("IDs() does not parse back: %v", err)
	}
}
