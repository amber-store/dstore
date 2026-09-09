package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// parseSize parses a byte count such as "1048576", "512Mi" or "2Gi". The
// suffixes K, M, G and T are binary (1024-based) whether or not they carry
// an "i", and a trailing "B" is ignored, so "2Gi", "2GiB", "2G" and "2GB"
// all mean 2 GiB.
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("bad size %q: want a number with an optional Ki/Mi/Gi/Ti suffix", s)
	}
	n, err := strconv.ParseInt(s[:i], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad size %q: %v", s, err)
	}
	unit := strings.TrimSuffix(strings.ToLower(s[i:]), "b")
	var shift uint
	switch unit {
	case "":
	case "k", "ki":
		shift = 10
	case "m", "mi":
		shift = 20
	case "g", "gi":
		shift = 30
	case "t", "ti":
		shift = 40
	default:
		return 0, fmt.Errorf("bad size %q: unknown unit %q (want Ki, Mi, Gi or Ti)", s, s[i:])
	}
	if n > math.MaxInt64>>shift {
		return 0, fmt.Errorf("bad size %q: too large", s)
	}
	return n << shift, nil
}
