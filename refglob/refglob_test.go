package refglob

import "testing"

func TestMatch(t *testing.T) {
	cases := []struct {
		pattern string
		name    string
		want    bool
	}{
		{"trees/a", "trees/a", true},
		{"trees/a", "trees/ab", false},
		{"trees/*", "trees/a", true},
		{"trees/*", "trees/a/b", false},
		{"trees/*", "trees/", true},
		{"trees/*", "trees", false},
		{"trees/**", "trees/a", true},
		{"trees/**", "trees/a/b", true},
		{"trees/**", "trees/", true},
		{"trees/**", "trees", false},
		{"**", "a", true},
		{"**", "a/b/c", true},
		{"**", "", true},
		{"**/x", "x", true},
		{"**/x", "a/x", true},
		{"**/x", "a/b/x", true},
		{"**/x", "ax", false},
		{"a/**/x", "a/x", true},
		{"a/**/x", "a/b/c/x", true},
		{"a/**/x", "ab/x", false},
		{"a**b", "axxb", true},
		{"a**b", "ax/xb", false},
		{"t?ees/a", "trees/a", true},
		{"t?ees/a", "t/ees/a", false},
		{"trees/[ab]", "trees/a", true},
		{"trees/[ab]", "trees/c", false},
		{"trees/[!ab]", "trees/c", true},
		{"trees/[^ab]", "trees/a", false},
		{"trees/[a-c]x", "trees/bx", true},
		{"trees/[/]", "trees//", false},
		{`trees/\*`, "trees/*", true},
		{`trees/\*`, "trees/a", false},
		{`trees/\\`, `trees/\`, true},
		{"a.b", "a.b", true},
		{"a.b", "axb", false},
		{"ünï/*", "ünï/cödé", true},
		{"*", "abc", true},
		{"*", "a/b", false},
	}
	for _, c := range cases {
		p, err := Compile(c.pattern)
		if err != nil {
			t.Fatalf("Compile(%q): %v", c.pattern, err)
		}
		if got := p.Match(c.name); got != c.want {
			t.Errorf("%q.Match(%q) = %v, want %v", c.pattern, c.name, got, c.want)
		}
	}
}

func TestPrefix(t *testing.T) {
	cases := []struct{ pattern, prefix string }{
		{"trees/a", "trees/a"},
		{"trees/*", "trees/"},
		{"trees/**", "trees/"},
		{"**", ""},
		{"tr?es/a", "tr"},
		{"trees/[ab]", "trees/"},
		{`trees/\*x`, "trees/*x"},
		{`a\\*`, `a\`},
	}
	for _, c := range cases {
		p, err := Compile(c.pattern)
		if err != nil {
			t.Fatalf("Compile(%q): %v", c.pattern, err)
		}
		if got := p.Prefix(); got != c.prefix {
			t.Errorf("%q.Prefix() = %q, want %q", c.pattern, got, c.prefix)
		}
		if p.String() != c.pattern {
			t.Errorf("%q.String() = %q", c.pattern, p.String())
		}
	}
}

func TestInvalid(t *testing.T) {
	for _, pattern := range []string{"", "trees/[ab", `trees/\`, "a[]b", "\x00", "[z-a]"} {
		if _, err := Compile(pattern); err == nil {
			t.Errorf("Compile(%q) succeeded", pattern)
		}
	}
}
