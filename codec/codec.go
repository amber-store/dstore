// Package codec is the deterministic CBOR codec every dstore record and
// frame uses: RFC 8949 core deterministic encoding with integer map keys,
// the convention of every other amber record.
package codec

import "github.com/fxamacker/cbor/v2"

var (
	encMode cbor.EncMode
	decMode cbor.DecMode
)

func init() {
	var err error
	encMode, err = cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		panic(err)
	}
	decMode, err = cbor.DecOptions{}.DecMode()
	if err != nil {
		panic(err)
	}
}

// Marshal encodes v deterministically.
func Marshal(v any) ([]byte, error) { return encMode.Marshal(v) }

// Unmarshal decodes b into v.
func Unmarshal(b []byte, v any) error { return decMode.Unmarshal(b, v) }

// MustMarshal encodes v or panics; for values whose encoding cannot fail.
func MustMarshal(v any) []byte {
	b, err := encMode.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
