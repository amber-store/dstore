package wire

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/amber-store/core/amberpack"
	"github.com/amber-store/core/key"
)

func TestFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	in := &Msg{Type: TMissing, Epoch: 9, Keys: [][]byte{make([]byte, 32)}, Pin: true, Holders: []KeyHolders{{Key: []byte{1}, Holders: [][]byte{{2}}}}}
	if err := WriteMsg(&buf, in); err != nil {
		t.Fatal(err)
	}
	out, err := ReadMsg(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if out.Type != TMissing || out.Epoch != 9 || !out.Pin || len(out.Keys) != 1 || len(out.Holders) != 1 {
		t.Fatalf("round trip: %+v", out)
	}
	if _, err := ReadMsg(&buf); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestErrorFrames(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMsg(&buf, &Msg{Type: TErr, Code: CodeStaleView, Text: "behind", View: []byte{1, 2}}); err != nil {
		t.Fatal(err)
	}
	m, err := Expect(&buf, TOK)
	if !IsCode(err, CodeStaleView) || m == nil {
		t.Fatalf("expected stale-view, got %v", err)
	}
	we, ok := AsError(err)
	if !ok || string(we.View) != "\x01\x02" {
		t.Fatalf("view not carried: %+v", we)
	}
}

// TestPackFramesInterop checks that transport-iroh's pack helpers produce
// frames this package's Msg decodes: TData and TDataEnd with Data at key 8.
func TestPackFramesInterop(t *testing.T) {
	data := []byte("some blob bytes")
	k, err := key.New(key.Blob, uint64(len(data)), data)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := amberpack.EncodeRecord(k, data)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := SendPackRecords(&buf, func(yield func([]byte, error) bool) { yield(rec, nil) }); err != nil {
		t.Fatal(err)
	}
	// Peek the first frame as a Msg.
	peek := bytes.NewReader(buf.Bytes())
	m, err := ReadMsg(peek)
	if err != nil || m.Type != TData || len(m.Data) == 0 {
		t.Fatalf("first frame: %+v %v", m, err)
	}
	// Read the pack back through the reader.
	pr := NewPackReader(&buf)
	r := amberpack.NewReader(pr)
	n := 0
	for raw, err := range r.Records() {
		if err != nil {
			t.Fatal(err)
		}
		if raw.Key != k {
			t.Fatalf("key %s", raw.Key)
		}
		n++
	}
	if n != 1 {
		t.Fatalf("%d records", n)
	}
	if _, err := io.Copy(io.Discard, pr); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadMsg(&buf); !errors.Is(err, io.EOF) {
		t.Fatalf("stream not positioned after TDataEnd: %v", err)
	}
}
