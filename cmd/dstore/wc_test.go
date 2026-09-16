package main

import "testing"

func TestResolveTicket(t *testing.T) {
	cases := []struct{ flag, stored, env, want string }{
		{"f", "s", "e", "f"},
		{"", "s", "e", "s"},
		{"", "", "e", "e"},
	}
	for _, c := range cases {
		got, err := resolveTicket(c.flag, c.stored, c.env)
		if err != nil || got != c.want {
			t.Errorf("resolveTicket(%q,%q,%q) = %q, %v; want %q", c.flag, c.stored, c.env, got, err, c.want)
		}
	}
	if _, err := resolveTicket("", "", ""); err == nil {
		t.Error("no ticket anywhere must be an error")
	}
}
