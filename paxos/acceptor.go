package paxos

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/amber-store/dstore/codec"
	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
	"github.com/cockroachdb/pebble/v2"
)

// Pebble key prefixes of the acceptor database.
const (
	rowPrefix    = "r/"
	metaFloor    = "m/floor"
	metaMarker   = "m/marker"
	metaView     = "m/view"
	metaRetired  = "m/retired"
	metaSeenRows = "m/seen"
)

// row is the persisted state of one register.
type row struct {
	Promised []byte `cbor:"0,keyasint,omitempty"`
	Accepted []byte `cbor:"1,keyasint,omitempty"`
	Value    []byte `cbor:"2,keyasint,omitempty"`
	HasValue bool   `cbor:"3,keyasint,omitempty"`
}

type marker struct {
	Since uint64 `cbor:"0,keyasint"`
}

type discardLogger struct{}

func (discardLogger) Infof(string, ...any)  {}
func (discardLogger) Errorf(string, ...any) {}
func (discardLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

// Acceptor is one node's CASPaxos acceptor over a synced Pebble database.
type Acceptor struct {
	self view.NodeID
	db   *pebble.DB
	wo   *pebble.WriteOptions

	life      sync.RWMutex // Handle holds it shared, Close exclusive
	closed    bool
	mu        sync.Mutex
	installed *view.View
	marker    uint64
	hasMarker bool
	retired   bool
	floor     Ballot
	// OnInstall, when set, is called with every newer view the acceptor
	// installs, outside the lock.
	OnInstall func(v *view.View)
	// Alert, when set, receives amnesiac warnings.
	Alert func(msg string)
}

// OpenAcceptor opens (creating if needed) the acceptor database at dir.
func OpenAcceptor(dir string, self view.NodeID) (*Acceptor, error) {
	db, err := pebble.Open(dir, &pebble.Options{Logger: discardLogger{}})
	if err != nil {
		return nil, fmt.Errorf("paxos: open %s: %w", dir, err)
	}
	a := &Acceptor{self: self, db: db, wo: pebble.Sync}
	if b, ok := a.get(metaView); ok {
		v, err := view.Decode(b)
		if err != nil {
			db.Close()
			return nil, err
		}
		a.installed = v
	}
	if b, ok := a.get(metaMarker); ok {
		var m marker
		if err := codec.Unmarshal(b, &m); err == nil {
			a.marker, a.hasMarker = m.Since, true
		}
	}
	if b, ok := a.get(metaFloor); ok {
		a.floor, _ = ParseBallot(b)
	}
	if _, ok := a.get(metaRetired); ok {
		a.retired = true
	}
	return a, nil
}

// Close closes the database; later calls answer an internal error.
func (a *Acceptor) Close() error {
	a.life.Lock()
	defer a.life.Unlock()
	if a.closed {
		return nil
	}
	a.closed = true
	return a.db.Close()
}

func (a *Acceptor) get(k string) ([]byte, bool) {
	b, closer, err := a.db.Get([]byte(k))
	if err != nil {
		return nil, false
	}
	out := append([]byte{}, b...)
	closer.Close()
	return out, true
}

// Installed returns the installed view, or nil.
func (a *Acceptor) Installed() *view.View {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.installed
}

// Marker returns the sync marker.
func (a *Acceptor) Marker() (since uint64, ok bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.marker, a.hasMarker
}

// SetMarker writes the sync marker (the epoch of the view that added this
// voter, §5.4) and clears the retired flag.
func (a *Acceptor) SetMarker(since uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.db.NewBatch()
	b.Set([]byte(metaMarker), codec.MustMarshal(marker{Since: since}), nil)
	b.Delete([]byte(metaRetired), nil)
	if err := b.Commit(a.wo); err != nil {
		return err
	}
	a.marker, a.hasMarker, a.retired = since, true, false
	return nil
}

// Install adopts v if it is not older than the installed view. A view
// that does not list this node as a voter retires the acceptor (§5.4).
func (a *Acceptor) Install(v *view.View) error {
	a.mu.Lock()
	if cur := a.installed; cur != nil {
		c := cur.Compare(v.Incarnation, v.Epoch)
		if c > 0 || c == 0 && v.Version <= cur.Version {
			a.mu.Unlock()
			return nil
		}
	}
	enc, err := v.Encode()
	if err != nil {
		a.mu.Unlock()
		return err
	}
	b := a.db.NewBatch()
	b.Set([]byte(metaView), enc, nil)
	// Only an acceptor that has served as a voter (it carries a marker)
	// retires when a view drops it; one that never voted has nothing to
	// wipe and is simply not a voter.
	retire := !v.IsVoter(a.self) && !a.retired && a.hasMarker
	if retire {
		b.Set([]byte(metaRetired), []byte{1}, nil)
		// Wipe the rows: an acceptor that left cannot come back voting
		// with state that missed a sync (§5.4).
		it, err := a.db.NewIter(&pebble.IterOptions{LowerBound: []byte(rowPrefix), UpperBound: []byte(rowPrefix + "\xff")})
		if err == nil {
			for it.First(); it.Valid(); it.Next() {
				b.Delete(append([]byte{}, it.Key()...), nil)
			}
			it.Close()
		}
		b.Delete([]byte(metaFloor), nil)
	}
	if err := b.Commit(a.wo); err != nil {
		a.mu.Unlock()
		return err
	}
	a.installed = v
	if retire {
		a.retired = true
		a.floor = Ballot{}
	}
	cb := a.OnInstall
	a.mu.Unlock()
	if cb != nil {
		cb(v)
	}
	return nil
}

// Retired reports whether the acceptor has retired.
func (a *Acceptor) Retired() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.retired
}

// Amnesiac reports whether the acceptor's marker disagrees with what the
// installed view says about it.
func (a *Acceptor) Amnesiac() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.amnesiacLocked()
}

func (a *Acceptor) amnesiacLocked() bool {
	if a.installed == nil {
		return false
	}
	for _, vo := range a.installed.Voters {
		if bytes.Equal(vo.ID, a.self[:]) {
			return !a.hasMarker || a.marker != vo.Since
		}
	}
	return false
}

func (a *Acceptor) readRow(reg []byte) (row, bool, error) {
	b, closer, err := a.db.Get(append([]byte(rowPrefix), reg...))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return row{}, false, nil
		}
		return row{}, false, err
	}
	defer closer.Close()
	var r row
	if err := codec.Unmarshal(b, &r); err != nil {
		return row{}, false, err
	}
	return r, true, nil
}

func (a *Acceptor) writeRow(reg []byte, r row) error {
	return a.db.Set(append([]byte(rowPrefix), reg...), codec.MustMarshal(r), a.wo)
}

// Handle answers one catalog message. The reply always carries the
// acceptor's (incarnation, epoch).
func (a *Acceptor) Handle(m *wire.Msg) *wire.Msg {
	a.life.RLock()
	defer a.life.RUnlock()
	if a.closed {
		return wire.ErrMsg(wire.CodeInternal, "acceptor closed")
	}
	reply := a.handle(m)
	a.mu.Lock()
	if a.installed != nil {
		reply.Incarnation = a.installed.Incarnation
		reply.Epoch = a.installed.Epoch
	}
	a.mu.Unlock()
	return reply
}

func (a *Acceptor) handle(m *wire.Msg) *wire.Msg {
	switch m.Type {
	case wire.TInstall:
		v, err := view.Decode(m.View)
		if err != nil {
			return wire.ErrMsg(wire.CodeBadRequest, err.Error())
		}
		if err := a.Install(v); err != nil {
			return wire.ErrMsg(wire.CodeInternal, err.Error())
		}
		return &wire.Msg{Type: wire.TInstalled}
	case wire.TMarker:
		if err := a.SetMarker(m.Since); err != nil {
			return wire.ErrMsg(wire.CodeInternal, err.Error())
		}
		return &wire.Msg{Type: wire.TOK}
	case wire.TSeed:
		return a.seed(m)
	case wire.TPurge:
		return a.purge(m)
	}

	a.mu.Lock()
	inst := a.installed
	a.mu.Unlock()
	if inst == nil {
		return wire.ErrMsg(wire.CodeNeedView, "no view installed")
	}
	switch c := inst.Compare(m.Incarnation, m.Epoch); {
	case c > 0:
		enc, _ := inst.Encode()
		return &wire.Msg{Type: wire.TErr, Code: wire.CodeStaleView, View: enc}
	case c < 0:
		return wire.ErrMsg(wire.CodeNeedView, "")
	}

	switch m.Type {
	case wire.TPrepare:
		return a.prepare(m)
	case wire.TAccept:
		return a.accept(m)
	case wire.TRead:
		return a.read(m)
	case wire.TScan:
		return a.scan(m)
	}
	return wire.ErrMsg(wire.CodeBadRequest, fmt.Sprintf("unknown catalog op %d", m.Type))
}

func (a *Acceptor) refuseIfForgetful() *wire.Msg {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.retired {
		return wire.ErrMsg(wire.CodeRetired, "acceptor retired")
	}
	if a.amnesiacLocked() {
		if a.Alert != nil {
			a.Alert("acceptor is amnesiac: marker disagrees with the installed view; run voter remove + voter add")
		}
		return wire.ErrMsg(wire.CodeAmnesiac, "acceptor state does not match the view")
	}
	return nil
}

func (a *Acceptor) prepare(m *wire.Msg) *wire.Msg {
	if r := a.refuseIfForgetful(); r != nil {
		return r
	}
	b, err := ParseBallot(m.Ballot)
	if err != nil || b.IsZero() {
		return wire.ErrMsg(wire.CodeBadRequest, "bad ballot")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	r, found, err := a.readRow(m.Reg)
	if err != nil {
		return wire.ErrMsg(wire.CodeInternal, err.Error())
	}
	promised, _ := ParseBallot(r.Promised)
	if !found && a.floor.Compare(promised) > 0 {
		promised = a.floor
	}
	if b.Compare(promised) <= 0 {
		return &wire.Msg{Type: wire.TConflict, Ballot: promised.Bytes()}
	}
	r.Promised = b.Bytes()
	if err := a.writeRow(m.Reg, r); err != nil {
		return wire.ErrMsg(wire.CodeInternal, err.Error())
	}
	return &wire.Msg{Type: wire.TPromise, Accepted: r.Accepted, Value: r.Value, HasValue: r.HasValue}
}

func (a *Acceptor) accept(m *wire.Msg) *wire.Msg {
	a.mu.Lock()
	retired := a.retired
	a.mu.Unlock()
	if retired {
		return wire.ErrMsg(wire.CodeRetired, "acceptor retired")
	}
	b, err := ParseBallot(m.Ballot)
	if err != nil || b.IsZero() {
		return wire.ErrMsg(wire.CodeBadRequest, "bad ballot")
	}
	if m.NotAfter != 0 && time.Now().UnixNano() > m.NotAfter {
		return wire.ErrMsg(wire.CodeExpired, "accept past not_after")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	r, found, err := a.readRow(m.Reg)
	if err != nil {
		return wire.ErrMsg(wire.CodeInternal, err.Error())
	}
	promised, _ := ParseBallot(r.Promised)
	if !found && a.floor.Compare(promised) > 0 {
		promised = a.floor
	}
	if b.Compare(promised) < 0 {
		return &wire.Msg{Type: wire.TConflict, Ballot: promised.Bytes()}
	}
	accepted, _ := ParseBallot(r.Accepted)
	if b.Compare(accepted) < 0 {
		return &wire.Msg{Type: wire.TConflict, Ballot: accepted.Bytes()}
	}
	r.Promised = b.Bytes()
	r.Accepted = b.Bytes()
	r.Value = m.Value
	r.HasValue = m.HasValue
	if err := a.writeRow(m.Reg, r); err != nil {
		return wire.ErrMsg(wire.CodeInternal, err.Error())
	}
	return &wire.Msg{Type: wire.TAccepted}
}

func (a *Acceptor) read(m *wire.Msg) *wire.Msg {
	if r := a.refuseIfForgetful(); r != nil {
		return r
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	r, _, err := a.readRow(m.Reg)
	if err != nil {
		return wire.ErrMsg(wire.CodeInternal, err.Error())
	}
	return &wire.Msg{Type: wire.TReadReply, Accepted: r.Accepted, Promised: r.Promised, Value: r.Value, HasValue: r.HasValue}
}

func (a *Acceptor) scan(m *wire.Msg) *wire.Msg {
	if r := a.refuseIfForgetful(); r != nil {
		return r
	}
	limit := m.Limit
	if limit <= 0 || limit > 100000 {
		limit = 100000
	}
	lower := append([]byte(rowPrefix), m.Prefix...)
	if len(m.After) > 0 {
		lower = append(append([]byte(rowPrefix), m.After...), 0)
	}
	upper := append(append([]byte(rowPrefix), m.Prefix...), 0xff, 0xff, 0xff, 0xff)
	if len(m.Prefix) == 0 {
		upper = []byte(rowPrefix + "\xff")
	}
	it, err := a.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return wire.ErrMsg(wire.CodeInternal, err.Error())
	}
	defer it.Close()
	reply := &wire.Msg{Type: wire.TScanReply}
	size := 0
	for it.First(); it.Valid(); it.Next() {
		if len(reply.Rows) >= limit || size >= wire.MaxPageBytes {
			reply.More = true
			break
		}
		var r row
		if err := codec.Unmarshal(it.Value(), &r); err != nil {
			continue
		}
		reg := append([]byte{}, it.Key()[len(rowPrefix):]...)
		reply.Rows = append(reply.Rows, wire.ScanRow{Reg: reg, Promised: r.Promised, Accepted: r.Accepted, Value: r.Value, HasValue: r.HasValue})
		size += len(reg) + len(r.Value) + 2*BallotSize
	}
	if n := len(reply.Rows); n > 0 {
		reply.Next = reply.Rows[n-1].Reg
	}
	return reply
}

// purge deletes a tombstone row whose accepted ballot is exactly the
// given one (§5.3 Tombstones).
func (a *Acceptor) purge(m *wire.Msg) *wire.Msg {
	b, err := ParseBallot(m.Ballot)
	if err != nil || b.IsZero() {
		return wire.ErrMsg(wire.CodeBadRequest, "bad ballot")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	r, found, err := a.readRow(m.Reg)
	if err != nil {
		return wire.ErrMsg(wire.CodeInternal, err.Error())
	}
	if !found {
		return &wire.Msg{Type: wire.TOK}
	}
	accepted, _ := ParseBallot(r.Accepted)
	if accepted.Compare(b) != 0 {
		return &wire.Msg{Type: wire.TOK} // stale purge, no-op
	}
	promised, _ := ParseBallot(r.Promised)
	batch := a.db.NewBatch()
	if promised.Compare(b) == 0 {
		batch.Delete(append([]byte(rowPrefix), m.Reg...), nil)
		if b.Compare(a.floor) > 0 {
			batch.Set([]byte(metaFloor), b.Bytes(), nil)
		}
	} else {
		// A proposal is in flight: keep the promise as a stub.
		r.Accepted, r.Value, r.HasValue = nil, nil, false
		batch.Set(append([]byte(rowPrefix), m.Reg...), codec.MustMarshal(r), nil)
	}
	if err := batch.Commit(a.wo); err != nil {
		return wire.ErrMsg(wire.CodeInternal, err.Error())
	}
	if promised.Compare(b) == 0 && b.Compare(a.floor) > 0 {
		a.floor = b
	}
	return &wire.Msg{Type: wire.TOK}
}

// seed merges rows by highest accepted ballot, never overwriting a row
// with a higher ballot (§5.4 pre-seeding).
func (a *Acceptor) seed(m *wire.Msg) *wire.Msg {
	a.mu.Lock()
	defer a.mu.Unlock()
	batch := a.db.NewBatch()
	for _, sr := range m.Rows {
		cur, _, err := a.readRow(sr.Reg)
		if err != nil {
			return wire.ErrMsg(wire.CodeInternal, err.Error())
		}
		curAcc, _ := ParseBallot(cur.Accepted)
		newAcc, _ := ParseBallot(sr.Accepted)
		if newAcc.IsZero() || newAcc.Compare(curAcc) <= 0 {
			continue
		}
		cur.Accepted = sr.Accepted
		cur.Value = sr.Value
		cur.HasValue = sr.HasValue
		curProm, _ := ParseBallot(cur.Promised)
		if newAcc.Compare(curProm) > 0 {
			cur.Promised = sr.Accepted
		}
		batch.Set(append([]byte(rowPrefix), sr.Reg...), codec.MustMarshal(cur), nil)
	}
	if err := batch.Commit(a.wo); err != nil {
		return wire.ErrMsg(wire.CodeInternal, err.Error())
	}
	return &wire.Msg{Type: wire.TOK}
}

// LocalRead returns the acceptor's own row for reg, for a single-voter
// cluster's fast path and for diagnostics.
func (a *Acceptor) LocalRead(reg []byte) (accepted Ballot, value []byte, has bool, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	r, _, err := a.readRow(reg)
	if err != nil {
		return Ballot{}, nil, false, err
	}
	accepted, _ = ParseBallot(r.Accepted)
	return accepted, r.Value, r.HasValue, nil
}

// AllRows returns every row, for seeding another acceptor and for backups.
func (a *Acceptor) AllRows() ([]wire.ScanRow, error) {
	it, err := a.db.NewIter(&pebble.IterOptions{LowerBound: []byte(rowPrefix), UpperBound: []byte(rowPrefix + "\xff")})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []wire.ScanRow
	for it.First(); it.Valid(); it.Next() {
		var r row
		if err := codec.Unmarshal(it.Value(), &r); err != nil {
			continue
		}
		out = append(out, wire.ScanRow{Reg: append([]byte{}, it.Key()[len(rowPrefix):]...), Promised: r.Promised, Accepted: r.Accepted, Value: r.Value, HasValue: r.HasValue})
	}
	return out, nil
}
