// Package catalog is the typed layer over the CASPaxos registers
// (architecture/dstore.md §5.2): the view, references, leases, the GC
// state and join tokens.
package catalog

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/amber-store/dstore/codec"
	"github.com/amber-store/dstore/paxos"
	"github.com/amber-store/dstore/view"
	"github.com/zeebo/blake3"
)

// Register names.
const (
	RegView    = "view"
	RegLease   = "lease/maintenance"
	RegGC      = "gc"
	RegACL     = "acl"
	PrefixRef  = "ref/"
	PrefixJoin = "join/"
	PrefixKeep = "keep/"
)

// Errors.
var (
	ErrUnknownRef = errors.New("catalog: unknown reference")
	ErrNoView     = errors.New("catalog: no view")
)

// CASMismatch reports a reference CAS that found another value.
type CASMismatch struct {
	Current    *RefValue // nil when the name is absent
	Version    []byte
	HasCurrent bool
}

func (e *CASMismatch) Error() string { return "catalog: cas mismatch" }

// Catalog runs typed operations over a proposer.
type Catalog struct {
	P     *paxos.Proposer
	Views paxos.Views
}

// New returns a catalog over p reading the current view from views.
func New(p *paxos.Proposer, views paxos.Views) *Catalog { return &Catalog{P: p, Views: views} }

// ---- view ----

// ReadView fast-reads the view register.
func (c *Catalog) ReadView(ctx context.Context) (*view.View, error) {
	row, err := c.P.Read(ctx, []byte(RegView))
	if err != nil {
		return nil, err
	}
	if !row.HasValue {
		return nil, ErrNoView
	}
	return view.Decode(row.Value)
}

// ErrAbort is returned by a view change function to end the CAS without
// writing; the CAS returns ErrAbort and the current view.
var ErrAbort = errors.New("catalog: aborted")

// CASView applies fn to a clone of the current view and commits it with
// its version bumped. fn returns ErrAbort to leave the register alone;
// bumpEpoch selects whether the epoch bumps as well (§3 rules).
func (c *Catalog) CASView(ctx context.Context, fn func(v *view.View) (bumpEpoch bool, err error)) (*view.View, error) {
	var out *view.View
	res, err := c.P.Propose(ctx, []byte(RegView), func(cur paxos.Row, _ paxos.Ballot) ([]byte, bool, error) {
		if !cur.HasValue {
			return nil, false, ErrNoView
		}
		v, err := view.Decode(cur.Value)
		if err != nil {
			return nil, false, err
		}
		nv := v.Clone()
		bump, err := fn(nv)
		if err != nil {
			out = v
			return nil, false, err
		}
		nv.Version = v.Version + 1
		if bump {
			nv.Epoch = v.Epoch + 1
		}
		enc, err := nv.Encode()
		if err != nil {
			return nil, false, err
		}
		out = nv
		return enc, true, nil
	}, 0)
	if err != nil {
		if errors.Is(err, paxos.ErrUnknown) {
			// Settle to learn the outcome.
			row, serr := c.P.Settle(ctx, []byte(RegView))
			if serr == nil && row.HasValue {
				v, derr := view.Decode(row.Value)
				if derr == nil {
					if v.Version == out.Version {
						return v, nil
					}
					return v, fmt.Errorf("catalog: view CAS lost: %w", err)
				}
			}
		}
		return out, err
	}
	v, err := view.Decode(res.Value)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// InitView writes the very first view; it fails if one exists.
func (c *Catalog) InitView(ctx context.Context, v *view.View) error {
	enc, err := v.Encode()
	if err != nil {
		return err
	}
	_, err = c.P.Propose(ctx, []byte(RegView), func(cur paxos.Row, _ paxos.Ballot) ([]byte, bool, error) {
		if cur.HasValue {
			return nil, false, errors.New("catalog: view already exists")
		}
		return enc, true, nil
	}, 0)
	return err
}

// ---- references ----

// RefValue is the value of a ref/<name> register.
type RefValue struct {
	Record    []byte `cbor:"0,keyasint,omitempty"`
	DeletedAt int64  `cbor:"1,keyasint,omitempty"`
	Version   []byte `cbor:"2,keyasint"` // the ballot the writing CAS committed at
}

// IsTombstone reports whether the value is a delete marker.
func (r RefValue) IsTombstone() bool { return len(r.Record) == 0 }

// DecodeRef parses a register value.
func DecodeRef(b []byte) (RefValue, error) {
	var r RefValue
	if err := codec.Unmarshal(b, &r); err != nil {
		return r, err
	}
	return r, nil
}

// RefReg returns the register name of a reference.
func RefReg(name string) []byte { return []byte(PrefixRef + name) }

// RefGet fast-reads a reference; a tombstone or absent register is
// ErrUnknownRef.
func (c *Catalog) RefGet(ctx context.Context, name string) (RefValue, error) {
	row, err := c.P.Read(ctx, RefReg(name))
	if err != nil {
		return RefValue{}, err
	}
	if !row.HasValue {
		return RefValue{}, ErrUnknownRef
	}
	rv, err := DecodeRef(row.Value)
	if err != nil {
		return RefValue{}, err
	}
	if rv.IsTombstone() {
		return rv, ErrUnknownRef
	}
	return rv, nil
}

// Cond is a reference CAS precondition.
type Cond struct {
	// ExpectedVersion, when Versioned, must equal the current version; nil
	// with Versioned means the name must not exist.
	ExpectedVersion []byte
	Versioned       bool
	// ExpectedOld, when Keyed, must equal the current record's key; nil with
	// Keyed means the name must not exist.
	ExpectedOld []byte
	Keyed       bool
	// Force replaces unconditionally.
	Force bool
}

// check evaluates the condition against the current value.
func (cond Cond) check(cur RefValue, present bool, curKey []byte) error {
	mismatch := func() error {
		m := &CASMismatch{}
		if present {
			cp := cur
			m.Current = &cp
			m.Version = cur.Version
			m.HasCurrent = true
		}
		return m
	}
	switch {
	case cond.Force:
		return nil
	case cond.Versioned:
		if cond.ExpectedVersion == nil {
			if present {
				return mismatch()
			}
			return nil
		}
		if !present || !bytes.Equal(cond.ExpectedVersion, cur.Version) {
			return mismatch()
		}
		return nil
	case cond.Keyed:
		if cond.ExpectedOld == nil {
			if present {
				return mismatch()
			}
			return nil
		}
		if !present || !bytes.Equal(cond.ExpectedOld, curKey) {
			return mismatch()
		}
		return nil
	}
	return nil
}

// KeyOf extracts the key from a reference record without full validation;
// the caller validates records it stores.
type KeyOf func(record []byte) []byte

// RefPut commits record under name subject to cond, with the acceptors
// enforcing notAfter. It returns the committed version. A retry that finds
// its own record already committed at a later version reports success.
func (c *Catalog) RefPut(ctx context.Context, name string, record []byte, cond Cond, notAfter int64, keyOf KeyOf) ([]byte, error) {
	var version []byte
	_, err := c.P.Propose(ctx, RefReg(name), func(cur paxos.Row, b paxos.Ballot) ([]byte, bool, error) {
		var rv RefValue
		present := false
		if cur.HasValue {
			var err error
			rv, err = DecodeRef(cur.Value)
			if err != nil {
				return nil, false, err
			}
			present = !rv.IsTombstone()
		}
		var curKey []byte
		if present {
			curKey = keyOf(rv.Record)
		}
		if err := cond.check(rv, present, curKey); err != nil {
			// A retry after a lost reply: our record is already there.
			if present && bytes.Equal(rv.Record, record) {
				version = rv.Version
				return nil, false, errAlreadyDone
			}
			return nil, false, err
		}
		nv := RefValue{Record: record, Version: b.Bytes()}
		version = nv.Version
		return codec.MustMarshal(nv), true, nil
	}, notAfter)
	if errors.Is(err, errAlreadyDone) {
		return version, nil
	}
	if errors.Is(err, paxos.ErrUnknown) {
		// Settle before answering (§7 step 5).
		row, serr := c.P.Settle(ctx, RefReg(name))
		if serr != nil {
			return nil, err
		}
		if row.HasValue {
			rv, derr := DecodeRef(row.Value)
			if derr == nil && bytes.Equal(rv.Record, record) {
				return rv.Version, nil
			}
		}
		return nil, fmt.Errorf("catalog: reference write lost: %w", err)
	}
	return version, err
}

var errAlreadyDone = errors.New("already done")

// RefDelete writes a tombstone subject to cond and returns the tombstone's
// ballot so that the caller can purge it once every voter holds it.
func (c *Catalog) RefDelete(ctx context.Context, name string, cond Cond, keyOf KeyOf) (paxos.Ballot, error) {
	res, err := c.P.Propose(ctx, RefReg(name), func(cur paxos.Row, b paxos.Ballot) ([]byte, bool, error) {
		var rv RefValue
		present := false
		if cur.HasValue {
			var err error
			rv, err = DecodeRef(cur.Value)
			if err != nil {
				return nil, false, err
			}
			present = !rv.IsTombstone()
		}
		if !present && (cond.Force || (cond.Versioned && cond.ExpectedVersion != nil) || (cond.Keyed && cond.ExpectedOld != nil)) {
			// Deleting an absent name: a retry that already succeeded.
			return nil, false, errAlreadyDone
		}
		var curKey []byte
		if present {
			curKey = keyOf(rv.Record)
		}
		if err := cond.check(rv, present, curKey); err != nil {
			return nil, false, err
		}
		if !present {
			return nil, false, errAlreadyDone
		}
		return codec.MustMarshal(RefValue{DeletedAt: time.Now().UnixNano(), Version: b.Bytes()}), true, nil
	}, 0)
	if errors.Is(err, errAlreadyDone) {
		return paxos.Ballot{}, nil
	}
	if errors.Is(err, paxos.ErrUnknown) {
		row, serr := c.P.Settle(ctx, RefReg(name))
		if serr == nil && row.HasValue {
			if rv, derr := DecodeRef(row.Value); derr == nil && rv.IsTombstone() {
				return row.Accepted, nil
			}
		}
		return paxos.Ballot{}, err
	}
	return res.Ballot, err
}

// TryPurge purges the tombstone of name at ballot b if every voter holds
// it. It reports whether the purge was sent.
func (c *Catalog) TryPurge(ctx context.Context, name string, b paxos.Ballot) bool {
	rows := c.P.ReadAll(ctx, RefReg(name))
	cur := c.Views.View()
	if cur == nil || len(rows) < len(cur.Voters) {
		return false
	}
	for _, r := range rows {
		if r.Accepted.Compare(b) != 0 {
			return false
		}
	}
	c.P.Purge(ctx, RefReg(name), b)
	return true
}

// RefEntry is one reference of a listing.
type RefEntry struct {
	Name      string
	Value     RefValue
	Undecided bool
}

// RefList is a paged majority listing: tombstones dropped, undecided values
// shown as they are.
func (c *Catalog) RefList(ctx context.Context, prefix, after string, limit int) ([]RefEntry, string, error) {
	var afterReg []byte
	if after != "" {
		afterReg = RefReg(after)
	}
	rows, next, err := c.P.Scan(ctx, []byte(PrefixRef+prefix), afterReg, limit)
	if err != nil {
		return nil, "", err
	}
	out := make([]RefEntry, 0, len(rows))
	for _, r := range rows {
		if !r.Row.HasValue {
			continue
		}
		rv, err := DecodeRef(r.Row.Value)
		if err != nil || rv.IsTombstone() {
			continue
		}
		out = append(out, RefEntry{Name: string(r.Reg[len(PrefixRef):]), Value: rv, Undecided: r.Undecided})
	}
	nextName := ""
	if next != nil {
		nextName = string(next[len(PrefixRef):])
	}
	return out, nextName, nil
}

// RefsSnapshot lists every reference, settling every undecided register
// with an identity transition first (§9.2) so that every root returned is
// a decided value.
func (c *Catalog) RefsSnapshot(ctx context.Context) ([]RefEntry, error) {
	var out []RefEntry
	var after []byte
	for {
		rows, next, err := c.P.Scan(ctx, []byte(PrefixRef), after, 10000)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			row := r.Row
			if r.Undecided {
				settled, err := c.P.Settle(ctx, r.Reg)
				if err != nil {
					return nil, fmt.Errorf("settle %s: %w", r.Reg, err)
				}
				row = settled
			}
			if !row.HasValue {
				continue
			}
			rv, err := DecodeRef(row.Value)
			if err != nil || rv.IsTombstone() {
				continue
			}
			out = append(out, RefEntry{Name: string(r.Reg[len(PrefixRef):]), Value: rv})
		}
		if next == nil {
			break
		}
		after = next
	}
	return out, nil
}

// ---- lease ----

// Lease is the maintenance lease.
type Lease struct {
	Holder  []byte `cbor:"0,keyasint"`
	Expires int64  `cbor:"1,keyasint"` // ns
}

// AcquireLease takes or renews the lease for self. It returns whether self
// holds it afterwards and the current lease.
func (c *Catalog) AcquireLease(ctx context.Context, self view.NodeID, ttl time.Duration) (bool, Lease, error) {
	now := time.Now().UnixNano()
	var cur Lease
	held := false
	_, err := c.P.Propose(ctx, []byte(RegLease), func(row paxos.Row, _ paxos.Ballot) ([]byte, bool, error) {
		cur = Lease{}
		if row.HasValue {
			_ = codec.Unmarshal(row.Value, &cur)
		}
		if len(cur.Holder) > 0 && !bytes.Equal(cur.Holder, self[:]) && cur.Expires > now {
			return nil, false, errAlreadyDone
		}
		cur = Lease{Holder: self[:], Expires: now + int64(ttl)}
		held = true
		return codec.MustMarshal(cur), true, nil
	}, 0)
	if errors.Is(err, errAlreadyDone) {
		return false, cur, nil
	}
	if err != nil {
		return false, cur, err
	}
	return held, cur, nil
}

// ReleaseLease drops the lease if self holds it.
func (c *Catalog) ReleaseLease(ctx context.Context, self view.NodeID) error {
	_, err := c.P.Propose(ctx, []byte(RegLease), func(row paxos.Row, _ paxos.Ballot) ([]byte, bool, error) {
		var cur Lease
		if row.HasValue {
			_ = codec.Unmarshal(row.Value, &cur)
		}
		if !bytes.Equal(cur.Holder, self[:]) {
			return nil, false, errAlreadyDone
		}
		return codec.MustMarshal(Lease{}), true, nil
	}, 0)
	if errors.Is(err, errAlreadyDone) {
		return nil
	}
	return err
}

// ReadLease fast-reads the lease.
func (c *Catalog) ReadLease(ctx context.Context) (Lease, error) {
	row, err := c.P.Read(ctx, []byte(RegLease))
	if err != nil {
		return Lease{}, err
	}
	var l Lease
	if row.HasValue {
		_ = codec.Unmarshal(row.Value, &l)
	}
	return l, nil
}

// ---- join tokens ----

// Token is a join token's register value.
type Token struct {
	CreatedAt int64  `cbor:"0,keyasint"`
	Weight    uint32 `cbor:"1,keyasint,omitempty"`
}

// TokenID returns blake3(token).
func TokenID(token []byte) []byte {
	sum := blake3.Sum256(token)
	return sum[:]
}

// TokenReg returns the register of a token id.
func TokenReg(id []byte) []byte { return append([]byte(PrefixJoin), id...) }

// CreateToken writes a fresh single-use join token and returns it.
func (c *Catalog) CreateToken(ctx context.Context, weight uint32) ([]byte, error) {
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return nil, err
	}
	id := TokenID(token)
	_, err := c.P.Propose(ctx, TokenReg(id), func(row paxos.Row, _ paxos.Ballot) ([]byte, bool, error) {
		if row.HasValue && len(row.Value) > 0 {
			return nil, false, errors.New("token exists")
		}
		return codec.MustMarshal(Token{CreatedAt: time.Now().UnixNano(), Weight: weight}), true, nil
	}, 0)
	if err != nil {
		return nil, err
	}
	return token, nil
}

// ReadToken returns the token register of id, or ErrUnknownRef.
func (c *Catalog) ReadToken(ctx context.Context, id []byte) (Token, error) {
	row, err := c.P.Read(ctx, TokenReg(id))
	if err != nil {
		return Token{}, err
	}
	if !row.HasValue || len(row.Value) == 0 {
		return Token{}, ErrUnknownRef
	}
	var t Token
	if err := codec.Unmarshal(row.Value, &t); err != nil {
		return Token{}, err
	}
	return t, nil
}

// DeleteToken clears a token register (an empty value; purged later).
func (c *Catalog) DeleteToken(ctx context.Context, id []byte) error {
	_, err := c.P.Propose(ctx, TokenReg(id), func(row paxos.Row, _ paxos.Ballot) ([]byte, bool, error) {
		return nil, true, nil
	}, 0)
	return err
}

// ListTokens returns every join token register (id → token, empty value =
// deleted).
func (c *Catalog) ListTokens(ctx context.Context) (map[string]Token, error) {
	rows, _, err := c.P.Scan(ctx, []byte(PrefixJoin), nil, 100000)
	if err != nil {
		return nil, err
	}
	out := map[string]Token{}
	for _, r := range rows {
		if !r.Row.HasValue || len(r.Row.Value) == 0 {
			continue
		}
		var t Token
		if codec.Unmarshal(r.Row.Value, &t) == nil {
			out[string(r.Reg[len(PrefixJoin):])] = t
		}
	}
	return out, nil
}

// ---- gc ----

// GC phases.
const (
	PhaseIdle    = 0
	PhaseBarrier = 1
	PhaseMark    = 2
	PhaseSweep   = 3
	PhaseAborted = 4
)

// PhaseName names a phase.
func PhaseName(p int) string {
	switch p {
	case PhaseIdle:
		return "idle"
	case PhaseBarrier:
		return "barrier"
	case PhaseMark:
		return "mark"
	case PhaseSweep:
		return "sweep"
	case PhaseAborted:
		return "aborted"
	}
	return fmt.Sprintf("phase(%d)", p)
}

// NodeStat is one node's report in the gc register.
type NodeStat struct {
	ID       []byte `cbor:"0,keyasint"`
	Marked   uint64 `cbor:"1,keyasint,omitempty"`
	Live     uint64 `cbor:"2,keyasint,omitempty"`
	Freed    uint64 `cbor:"3,keyasint,omitempty"`
	Packs    int    `cbor:"4,keyasint,omitempty"`
	NoMark   bool   `cbor:"5,keyasint,omitempty"`
	Copied   uint64 `cbor:"6,keyasint,omitempty"`
	Duration int64  `cbor:"7,keyasint,omitempty"`
}

// GCState is the gc register.
type GCState struct {
	Epoch      uint64     `cbor:"0,keyasint"`
	Phase      int        `cbor:"1,keyasint"`
	BarrierAt  int64      `cbor:"2,keyasint,omitempty"`
	SnapshotAt int64      `cbor:"3,keyasint,omitempty"`
	Placement  []byte     `cbor:"4,keyasint,omitempty"` // encoded view at phase = mark
	Acked      [][]byte   `cbor:"5,keyasint,omitempty"`
	MarkDone   bool       `cbor:"6,keyasint,omitempty"`
	SweepWave  int        `cbor:"7,keyasint,omitempty"`
	SweepDone  []NodeStat `cbor:"8,keyasint,omitempty"`
	Hold       bool       `cbor:"9,keyasint,omitempty"`
	Last       []NodeStat `cbor:"10,keyasint,omitempty"`
	Nonce      []byte     `cbor:"11,keyasint,omitempty"`
	Marked     []NodeStat `cbor:"12,keyasint,omitempty"`
	Error      string     `cbor:"13,keyasint,omitempty"` // last abort reason
	Missing    [][]byte   `cbor:"14,keyasint,omitempty"` // keys found missing by the last mark
	Tolerate   bool       `cbor:"15,keyasint,omitempty"`
	LastOK     uint64     `cbor:"16,keyasint,omitempty"` // last epoch that completed
	Garbage    float64    `cbor:"17,keyasint,omitempty"`
}

// ReadGC fast-reads the gc register.
func (c *Catalog) ReadGC(ctx context.Context) (GCState, error) {
	row, err := c.P.Read(ctx, []byte(RegGC))
	if err != nil {
		return GCState{}, err
	}
	var g GCState
	if row.HasValue {
		if err := codec.Unmarshal(row.Value, &g); err != nil {
			return GCState{}, err
		}
	}
	return g, nil
}

// CASGC applies fn to the gc register. fn returns ErrAbort to leave it.
func (c *Catalog) CASGC(ctx context.Context, fn func(g *GCState) error) (GCState, error) {
	var out GCState
	_, err := c.P.Propose(ctx, []byte(RegGC), func(row paxos.Row, _ paxos.Ballot) ([]byte, bool, error) {
		var g GCState
		if row.HasValue {
			if err := codec.Unmarshal(row.Value, &g); err != nil {
				return nil, false, err
			}
		}
		if err := fn(&g); err != nil {
			out = g
			return nil, false, err
		}
		out = g
		return codec.MustMarshal(g), true, nil
	}, 0)
	return out, err
}

// ---- keep flags ----

// SetKeep marks a reference keep-forever (the gateway's TPin).
func (c *Catalog) SetKeep(ctx context.Context, name string) error {
	_, err := c.P.Propose(ctx, []byte(PrefixKeep+name), func(row paxos.Row, _ paxos.Ballot) ([]byte, bool, error) {
		return codec.MustMarshal(map[int]int64{0: time.Now().UnixNano()}), true, nil
	}, 0)
	return err
}
