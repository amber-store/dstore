package ticket

import (
	"strings"
	"testing"
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
