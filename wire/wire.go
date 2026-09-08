// Package wire defines the dstore wire protocol (architecture/dstore.md
// §10): one bidirectional QUIC stream per operation, each frame a 4-byte
// big-endian length and a deterministic CBOR map with integer keys.
// amberpack payloads ride in TData frames terminated by TDataEnd; the frame
// numbers and the Data/Code/Text keys coincide with transport-iroh's so that
// its pack helpers are reused verbatim.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"time"

	"github.com/amber-store/dstore/codec"
	"github.com/amber-store/transport-iroh/protocol"
)

// ALPNs.
const (
	ALPNClient  = "amber-dstore/1"
	ALPNCluster = "amber-dstore-cluster/1"
	ALPNGateway = "amber-store-iroh/1"
)

// Limits.
const (
	MaxFrame     = 16 << 20
	MaxKeys      = 8192
	MaxPutBatch  = 64 << 20
	MaxPageBytes = 4 << 20
	ChunkSize    = protocol.ChunkSize
)

// Frame types. TData, TDataEnd and TErr are shared with transport-iroh's
// protocol so that protocol.SendPackRecords/NewPackReader work on these
// streams; everything else lives above 31.
const (
	TData    = protocol.TData    // 7
	TDataEnd = protocol.TDataEnd // 8
	TErr     = protocol.TErr     // 10

	// Client ALPN requests.
	TView      = 32
	TMissing   = 33
	TGet       = 34
	TPut       = 35
	TRefGet    = 36
	TRefPut    = 37
	TRefDelete = 38
	TRefList   = 39
	TStatus    = 40
	TAdmin     = 41

	// Client ALPN replies.
	TViewReply    = 48
	TMissingReply = 49
	TAbsent       = 50
	TPutResult    = 51
	TRef          = 52
	TOK           = 53
	TCASMismatch  = 54
	TIncomplete   = 55
	TRefs         = 56
	TStatusReply  = 57
	TAdminReply   = 58

	// Catalog (cluster ALPN).
	TPrepare = 64
	TAccept  = 65
	TRead    = 66
	TScan    = 67
	TInstall = 68
	TPurge   = 69
	TMarker  = 70 // set the acceptor's sync marker (§5.4)
	TSeed    = 71 // bulk highest-ballot merge of rows into an acceptor

	TPromise   = 80
	TConflict  = 81
	TAccepted  = 82
	TReadReply = 83
	TScanReply = 84
	TInstalled = 85

	// Membership and maintenance (cluster ALPN).
	TJoin        = 96
	TGCBarrier   = 97
	TGCMark      = 98
	TGCKeys      = 99
	TGCStatus    = 100
	TAck         = 101
	TViewChanged = 102
	TGCStatusRep = 103
	TGCAbort     = 104
	TPing        = 105
	TPong        = 106
	TBackupNote  = 107 // record a catalog backup key in the receiver's meta
)

// Error codes.
const (
	CodeStaleView    = "stale-view"
	CodeNotOwner     = "not-owner"
	CodeNoSpace      = "no-space"
	CodeBusy         = "busy"
	CodeBadRequest   = "bad-request"
	CodeUnauthorized = "unauthorized"
	CodeUnknownRef   = "unknown-ref"
	CodeCASMismatch  = "cas-mismatch"
	CodeIncomplete   = "incomplete"
	CodeUnavailable  = "unavailable"
	CodeTimeout      = "timeout"
	CodeInternal     = "internal"
	CodeNotMember    = "not-member"
	CodeNeedView     = "need-view"
	CodeExpired      = "expired"
	CodeAmnesiac     = "amnesiac"
	CodeMarkFrozen   = "mark-frozen"
	CodeRetired      = "retired"
	CodeTooSoon      = "too-soon"
	CodeConflict     = "conflict"
	CodeNoMark       = "no-mark"
)

// KeyHolders names the owners that hold a key.
type KeyHolders struct {
	Key     []byte   `cbor:"0,keyasint"`
	Holders [][]byte `cbor:"1,keyasint,omitempty"`
}

// KeyFailure names an owner that did not take a key and why.
type KeyFailure struct {
	Key        []byte `cbor:"0,keyasint"`
	Node       []byte `cbor:"1,keyasint"`
	Reason     string `cbor:"2,keyasint"`
	RetryAfter int64  `cbor:"3,keyasint,omitempty"` // ms
}

// KeyReject names a record the receiver refused.
type KeyReject struct {
	Key    []byte `cbor:"0,keyasint"`
	Reason string `cbor:"1,keyasint"`
}

// RefInfo is one reference in a listing.
type RefInfo struct {
	Name      string `cbor:"0,keyasint"`
	Key       []byte `cbor:"1,keyasint"`
	Version   []byte `cbor:"2,keyasint"`
	CreatedAt int64  `cbor:"3,keyasint"`
	User      string `cbor:"4,keyasint,omitempty"`
}

// ScanRow is one acceptor row of a scan reply.
type ScanRow struct {
	Reg      []byte `cbor:"0,keyasint"`
	Promised []byte `cbor:"1,keyasint,omitempty"`
	Accepted []byte `cbor:"2,keyasint,omitempty"`
	Value    []byte `cbor:"3,keyasint,omitempty"`
	HasValue bool   `cbor:"4,keyasint,omitempty"`
}

// Msg is the single frame payload type.
type Msg struct {
	Type        int    `cbor:"0,keyasint"`
	ClusterID   []byte `cbor:"1,keyasint,omitempty"`
	Incarnation uint64 `cbor:"2,keyasint,omitempty"`
	Epoch       uint64 `cbor:"3,keyasint,omitempty"`

	Keys   [][]byte `cbor:"4,keyasint,omitempty"`
	Pin    bool     `cbor:"5,keyasint,omitempty"`
	Name   string   `cbor:"6,keyasint,omitempty"`
	Record []byte   `cbor:"7,keyasint,omitempty"`
	Data   []byte   `cbor:"8,keyasint,omitempty"`
	Key    []byte   `cbor:"9,keyasint,omitempty"`
	Code   string   `cbor:"10,keyasint,omitempty"`
	Text   string   `cbor:"11,keyasint,omitempty"`
	View   []byte   `cbor:"12,keyasint,omitempty"`

	Version         []byte `cbor:"13,keyasint,omitempty"`
	ExpectedVersion []byte `cbor:"14,keyasint,omitempty"`
	ExpectedOld     []byte `cbor:"15,keyasint,omitempty"`
	Force           bool   `cbor:"16,keyasint,omitempty"`
	HasExpected     bool   `cbor:"17,keyasint,omitempty"` // an Expected* condition is present (nil means "must not exist")
	Prefix          []byte `cbor:"18,keyasint,omitempty"`
	After           []byte `cbor:"19,keyasint,omitempty"`
	Limit           int    `cbor:"20,keyasint,omitempty"`

	Refs        []RefInfo    `cbor:"21,keyasint,omitempty"`
	Next        []byte       `cbor:"22,keyasint,omitempty"`
	Holders     []KeyHolders `cbor:"23,keyasint,omitempty"`
	Failed      []KeyFailure `cbor:"24,keyasint,omitempty"`
	Rejected    []KeyReject  `cbor:"25,keyasint,omitempty"`
	Short       []KeyHolders `cbor:"26,keyasint,omitempty"`
	Unreachable [][]byte     `cbor:"27,keyasint,omitempty"`
	RetryAfter  int64        `cbor:"28,keyasint,omitempty"` // ms
	Current     []byte       `cbor:"29,keyasint,omitempty"`
	Shortfall   int          `cbor:"30,keyasint,omitempty"`
	HasCurrent  bool         `cbor:"31,keyasint,omitempty"`

	Reg      []byte    `cbor:"32,keyasint,omitempty"`
	Ballot   []byte    `cbor:"33,keyasint,omitempty"`
	Value    []byte    `cbor:"34,keyasint,omitempty"`
	HasValue bool      `cbor:"35,keyasint,omitempty"`
	Accepted []byte    `cbor:"36,keyasint,omitempty"`
	Promised []byte    `cbor:"37,keyasint,omitempty"`
	NotAfter int64     `cbor:"38,keyasint,omitempty"`
	Rows     []ScanRow `cbor:"39,keyasint,omitempty"`
	More     bool      `cbor:"40,keyasint,omitempty"`
	Since    uint64    `cbor:"41,keyasint,omitempty"`

	Token  []byte   `cbor:"42,keyasint,omitempty"`
	Weight uint32   `cbor:"43,keyasint,omitempty"`
	Zone   string   `cbor:"44,keyasint,omitempty"`
	Addrs  []string `cbor:"45,keyasint,omitempty"`
	NoVote bool     `cbor:"46,keyasint,omitempty"`

	G        uint64   `cbor:"47,keyasint,omitempty"`
	Nonce    []byte   `cbor:"48,keyasint,omitempty"`
	Seq      uint64   `cbor:"49,keyasint,omitempty"`
	Expand   bool     `cbor:"50,keyasint,omitempty"`
	Params   []byte   `cbor:"51,keyasint,omitempty"`
	Sent     uint64   `cbor:"52,keyasint,omitempty"`
	Received uint64   `cbor:"53,keyasint,omitempty"`
	Idle     bool     `cbor:"54,keyasint,omitempty"`
	Marked   uint64   `cbor:"55,keyasint,omitempty"`
	Missing  [][]byte `cbor:"56,keyasint,omitempty"`
	Status   []byte   `cbor:"57,keyasint,omitempty"`
	Node     []byte   `cbor:"58,keyasint,omitempty"`
	Error    string   `cbor:"59,keyasint,omitempty"`
}

// ErrProtocol reports a frame that is valid CBOR but wrong for the moment
// it arrived in.
var ErrProtocol = errors.New("wire: unexpected frame")

// WriteMsg writes one frame.
func WriteMsg(w io.Writer, m *Msg) error {
	payload, err := codec.Marshal(m)
	if err != nil {
		return fmt.Errorf("wire: encode frame: %w", err)
	}
	if len(payload) > MaxFrame {
		return fmt.Errorf("wire: frame of %d bytes exceeds limit %d", len(payload), MaxFrame)
	}
	buf := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(payload)))
	copy(buf[4:], payload)
	_, err = w.Write(buf)
	return err
}

// ReadMsg reads one frame. A clean end of stream before the header
// surfaces as io.EOF, a cut mid-frame as io.ErrUnexpectedEOF.
func ReadMsg(r io.Reader) (*Msg, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > MaxFrame {
		return nil, fmt.Errorf("wire: frame of %d bytes exceeds limit %d", n, MaxFrame)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("wire: short frame: %w", err)
	}
	var m Msg
	if err := codec.Unmarshal(payload, &m); err != nil {
		return nil, fmt.Errorf("wire: decode frame: %w", err)
	}
	return &m, nil
}

// Error is a TErr frame surfaced as a Go error.
type Error struct {
	Code       string
	Text       string
	View       []byte        // stale-view / not-owner: the responder's view
	RetryAfter time.Duration // busy
}

func (e *Error) Error() string {
	if e.Text == "" {
		return "remote: " + e.Code
	}
	return fmt.Sprintf("remote: %s: %s", e.Code, e.Text)
}

// Is matches by code.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code && (t.Text == "" || t.Text == e.Text)
}

// ErrorFromMsg converts a TErr frame into an *Error.
func ErrorFromMsg(m *Msg) *Error {
	return &Error{Code: m.Code, Text: m.Text, View: m.View, RetryAfter: time.Duration(m.RetryAfter) * time.Millisecond}
}

// ErrMsg builds a TErr frame.
func ErrMsg(code, text string) *Msg {
	return &Msg{Type: TErr, Code: code, Text: text}
}

// WriteErr writes a TErr frame.
func WriteErr(w io.Writer, code, text string) error {
	return WriteMsg(w, ErrMsg(code, text))
}

// IsCode reports whether err is a remote error with the given code.
func IsCode(err error, code string) bool {
	var e *Error
	return errors.As(err, &e) && e.Code == code
}

// AsError extracts the remote error, if any.
func AsError(err error) (*Error, bool) {
	var e *Error
	ok := errors.As(err, &e)
	return e, ok
}

// Expect reads a frame and converts a TErr into an error; it also checks
// the frame type when want is non-zero.
func Expect(r io.Reader, want int) (*Msg, error) {
	m, err := ReadMsg(r)
	if err != nil {
		return nil, err
	}
	if m.Type == TErr {
		return m, ErrorFromMsg(m)
	}
	if want != 0 && m.Type != want {
		return m, fmt.Errorf("%w: type %d, want %d", ErrProtocol, m.Type, want)
	}
	return m, nil
}

// SendPackRecords streams pre-encoded records as TData frames ending in
// TDataEnd (transport-iroh's helper, reused verbatim).
func SendPackRecords(w io.Writer, recs iter.Seq2[[]byte, error]) error {
	return protocol.SendPackRecords(w, recs)
}

// NewPackReader returns a reader over the pack bytes of a TData…TDataEnd
// sequence. Drain it (io.Copy(io.Discard, r)) before reading further frames.
func NewPackReader(r io.Reader) io.Reader {
	return protocol.NewPackReader(r)
}

// Keys32 converts raw keys to fixed arrays, rejecting bad lengths.
func Keys32(raw [][]byte) ([][32]byte, error) {
	out := make([][32]byte, len(raw))
	for i, b := range raw {
		if len(b) != 32 {
			return nil, fmt.Errorf("wire: key %d has %d bytes", i, len(b))
		}
		copy(out[i][:], b)
	}
	return out, nil
}

// RawKeys converts fixed keys to raw byte slices.
func RawKeys(keys [][32]byte) [][]byte {
	out := make([][]byte, len(keys))
	for i := range keys {
		out[i] = keys[i][:]
	}
	return out
}

// RawIDs converts fixed ids to raw byte slices.
func RawIDs(ids [][32]byte) [][]byte { return RawKeys(ids) }

// CloseStream ends a one-request stream: Close finishes the send side and
// CancelRead completes the receive half so the stream fully retires.
func CloseStream(s io.Closer) {
	_ = s.Close()
	if cr, ok := s.(interface{ CancelRead(code uint64) }); ok {
		cr.CancelRead(0)
	}
}
