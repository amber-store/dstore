package main

import (
	"testing"

	"github.com/urfave/cli/v2"
)

func TestParseSize(t *testing.T) {
	good := map[string]int64{
		"0":     0,
		"1024":  1024,
		"512Ki": 512 << 10,
		"256Mi": 256 << 20,
		"2Gi":   2 << 30,
		"1Ti":   1 << 40,
		"2gi":   2 << 30,
		"2GiB":  2 << 30,
		"2G":    2 << 30,
		"2GB":   2 << 30,
		" 2Gi ": 2 << 30,
	}
	for in, want := range good {
		got, err := parseSize(in)
		if err != nil {
			t.Errorf("parseSize(%q): %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("parseSize(%q) = %d, want %d", in, got, want)
		}
	}
	for _, in := range []string{"", "Gi", "2X", "2.5Gi", "-1", "1e3", "9999999999Ti"} {
		if got, err := parseSize(in); err == nil {
			t.Errorf("parseSize(%q) = %d, want error", in, got)
		}
	}
}

func TestPackSizeFlag(t *testing.T) {
	t.Setenv("DSTORE_PACK_SIZE", "")
	run := func(args ...string) (int64, error) {
		var got int64
		app := &cli.App{Flags: nodeFlags(), Action: func(c *cli.Context) error {
			var err error
			got, err = packSize(c)
			return err
		}}
		err := app.Run(append([]string{"dstore"}, args...))
		return got, err
	}
	if got, err := run(); err != nil || got != 2<<30 {
		t.Errorf("default: got %d, %v; want 2 GiB", got, err)
	}
	if got, err := run("--pack-size", "512Mi"); err != nil || got != 512<<20 {
		t.Errorf("--pack-size 512Mi: got %d, %v", got, err)
	}
	t.Setenv("DSTORE_PACK_SIZE", "1Gi")
	if got, err := run(); err != nil || got != 1<<30 {
		t.Errorf("DSTORE_PACK_SIZE=1Gi: got %d, %v", got, err)
	}
	t.Setenv("DSTORE_PACK_SIZE", "")
	for _, bad := range []string{"0", "x", "1.5Gi"} {
		if got, err := run("--pack-size", bad); err == nil {
			t.Errorf("--pack-size %s: got %d, want error", bad, got)
		}
	}
}
