package node

import "testing"

func TestConfigDefaultsSegmentSize(t *testing.T) {
	var c Config
	c.defaults()
	if c.SegmentSize != DefaultSegmentSize {
		t.Fatalf("SegmentSize = %d, want %d", c.SegmentSize, DefaultSegmentSize)
	}
	if DefaultSegmentSize != 2<<30 {
		t.Fatalf("DefaultSegmentSize = %d, want 2 GiB", DefaultSegmentSize)
	}
	c = Config{SegmentSize: 256 << 10}
	c.defaults()
	if c.SegmentSize != 256<<10 {
		t.Fatalf("an explicit SegmentSize was overridden: %d", c.SegmentSize)
	}
}
