// Package refglob matches reference names against path-style glob
// patterns (architecture/dstore.md §7, Watching): `*` and `?` match
// within one `/`-separated segment, `**` as a whole segment matches zero
// or more segments, `[...]` is a character class, `\` escapes the next
// character. A pattern is compiled to an RE2 regexp, so matching is
// linear in the name.
package refglob

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// MaxLen bounds a pattern's length in bytes.
const MaxLen = 4096

// Pattern is a compiled glob.
type Pattern struct {
	src    string
	prefix string
	re     *regexp.Regexp
}

// Compile parses pattern.
func Compile(pattern string) (*Pattern, error) {
	if pattern == "" {
		return nil, errors.New("refglob: empty pattern")
	}
	if len(pattern) > MaxLen {
		return nil, fmt.Errorf("refglob: pattern exceeds %d bytes", MaxLen)
	}
	if !utf8.ValidString(pattern) {
		return nil, errors.New("refglob: pattern is not valid UTF-8")
	}
	for _, r := range pattern {
		if r < 0x20 || r == 0x7f {
			return nil, errors.New("refglob: pattern contains a control character")
		}
	}
	rs := []rune(pattern)
	var re, prefix strings.Builder
	re.WriteString(`\A`)
	literal := true // still inside the leading literal run
	segStart := true
	for i := 0; i < len(rs); {
		r := rs[i]
		switch r {
		case '\\':
			if i+1 >= len(rs) {
				return nil, errors.New("refglob: trailing backslash")
			}
			i++
			re.WriteString(regexp.QuoteMeta(string(rs[i])))
			if literal {
				prefix.WriteRune(rs[i])
			}
			segStart = rs[i] == '/'
			i++
		case '*':
			literal = false
			double := i+1 < len(rs) && rs[i+1] == '*'
			if double && segStart && (i+2 == len(rs) || rs[i+2] == '/') {
				if i+2 == len(rs) {
					re.WriteString(`.*`)
					i += 2
				} else {
					re.WriteString(`(?:[^/]*/)*`)
					i += 3
				}
				segStart = true
				continue
			}
			re.WriteString(`[^/]*`)
			segStart = false
			i++
		case '?':
			literal = false
			re.WriteString(`[^/]`)
			segStart = false
			i++
		case '[':
			literal = false
			class, n, err := parseClass(rs[i:])
			if err != nil {
				return nil, err
			}
			re.WriteString(class)
			segStart = false
			i += n
		default:
			re.WriteString(regexp.QuoteMeta(string(r)))
			if literal {
				prefix.WriteRune(r)
			}
			segStart = r == '/'
			i++
		}
	}
	re.WriteString(`\z`)
	compiled, err := regexp.Compile(re.String())
	if err != nil {
		return nil, fmt.Errorf("refglob: %w", err)
	}
	return &Pattern{src: pattern, prefix: prefix.String(), re: compiled}, nil
}

// parseClass parses a `[...]` class at the start of rs and returns its RE2
// form and the number of runes consumed. A class never matches `/`.
func parseClass(rs []rune) (string, int, error) {
	i := 1
	negate := false
	if i < len(rs) && (rs[i] == '!' || rs[i] == '^') {
		negate = true
		i++
	}
	type item struct{ lo, hi rune }
	var items []item
	for {
		if i >= len(rs) {
			return "", 0, errors.New("refglob: unterminated character class")
		}
		if rs[i] == ']' {
			break
		}
		lo, n, err := classRune(rs[i:])
		if err != nil {
			return "", 0, err
		}
		i += n
		hi := lo
		if i+1 < len(rs) && rs[i] == '-' && rs[i+1] != ']' {
			i++
			hi, n, err = classRune(rs[i:])
			if err != nil {
				return "", 0, err
			}
			i += n
			if hi < lo {
				return "", 0, fmt.Errorf("refglob: bad range %q-%q", lo, hi)
			}
		}
		items = append(items, item{lo, hi})
	}
	if len(items) == 0 {
		return "", 0, errors.New("refglob: empty character class")
	}
	var b strings.Builder
	b.WriteByte('[')
	if negate {
		b.WriteString("^/")
	}
	for _, it := range items {
		ranges := [][2]rune{{it.lo, it.hi}}
		if !negate && it.lo <= '/' && '/' <= it.hi {
			// A positive class never matches '/': carve it out.
			ranges = nil
			if it.lo < '/' {
				ranges = append(ranges, [2]rune{it.lo, '/' - 1})
			}
			if it.hi > '/' {
				ranges = append(ranges, [2]rune{'/' + 1, it.hi})
			}
		}
		for _, rg := range ranges {
			b.WriteString(classQuote(rg[0]))
			if rg[1] != rg[0] {
				b.WriteByte('-')
				b.WriteString(classQuote(rg[1]))
			}
		}
	}
	b.WriteByte(']')
	if !negate && b.Len() == 2 {
		return "[^\\x00-\\x{10FFFF}]", i + 1, nil // only '/' was listed: matches nothing
	}
	return b.String(), i + 1, nil
}

// classRune reads one (possibly escaped) rune of a class.
func classRune(rs []rune) (rune, int, error) {
	if rs[0] == '\\' {
		if len(rs) < 2 {
			return 0, 0, errors.New("refglob: trailing backslash in character class")
		}
		return rs[1], 2, nil
	}
	return rs[0], 1, nil
}

// classQuote escapes a rune for use inside an RE2 character class.
func classQuote(r rune) string {
	switch r {
	case '\\', ']', '[', '^', '-':
		return `\` + string(r)
	}
	return string(r)
}

// Match reports whether name matches the pattern.
func (p *Pattern) Match(name string) bool { return p.re.MatchString(name) }

// Prefix returns the literal prefix every matching name starts with.
func (p *Pattern) Prefix() string { return p.prefix }

// String returns the pattern's source.
func (p *Pattern) String() string { return p.src }
