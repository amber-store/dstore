package paxos

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
)

// Transport carries one catalog request to an acceptor and returns its
// reply. Implementations dial over the cluster ALPN or, in tests, deliver
// in process.
type Transport interface {
	Call(ctx context.Context, to view.NodeID, req *wire.Msg) (*wire.Msg, error)
}

// Views is the proposer's source of the current view and its sink for
// newer ones learned from stale-view replies.
type Views interface {
	View() *view.View
	Adopt(v *view.View)
}

// Ballots hands out proposer counters that are persisted before use and
// never reused.
type Ballots interface {
	NextBallot() (uint64, error)
}

// Row is the decided (or best known) state of a register.
type Row struct {
	Accepted Ballot
	Value    []byte
	HasValue bool
}

// Errors.
var (
	// ErrUnavailable means no quorum answered.
	ErrUnavailable = errors.New("paxos: no quorum")
	// ErrUnknown means the accept round's outcome is unknown.
	ErrUnknown = errors.New("paxos: commit outcome unknown")
	// ErrExpired means an acceptor refused the accept past not_after.
	ErrExpired = errors.New("paxos: accept expired")
)

// Config tunes a proposer.
type Config struct {
	AcceptorTimeout time.Duration // per acceptor request, default 2 s
	MaxInFlight     int           // per acceptor, default 256
}

// Proposer runs CASPaxos rounds against the voters of the current view.
type Proposer struct {
	self    view.NodeID
	tr      Transport
	views   Views
	ballots Ballots
	cfg     Config

	mu       sync.Mutex
	inflight map[view.NodeID]int
	// Stats per voter.
	stats map[view.NodeID]*VoterStats
}

// VoterStats is what cluster status reports per voter.
type VoterStats struct {
	Calls    uint64
	Failures uint64
	P99      time.Duration
	samples  []time.Duration
}

// NewProposer returns a proposer for node self.
func NewProposer(self view.NodeID, tr Transport, views Views, ballots Ballots, cfg Config) *Proposer {
	if cfg.AcceptorTimeout == 0 {
		cfg.AcceptorTimeout = 2 * time.Second
	}
	if cfg.MaxInFlight == 0 {
		cfg.MaxInFlight = 256
	}
	return &Proposer{self: self, tr: tr, views: views, ballots: ballots, cfg: cfg, inflight: map[view.NodeID]int{}, stats: map[view.NodeID]*VoterStats{}}
}

// Stats returns a snapshot of per-voter statistics.
func (p *Proposer) Stats() map[view.NodeID]VoterStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[view.NodeID]VoterStats, len(p.stats))
	for id, s := range p.stats {
		cp := *s
		if n := len(s.samples); n > 0 {
			sorted := append([]time.Duration{}, s.samples...)
			sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
			cp.P99 = sorted[(n*99)/100]
		}
		cp.samples = nil
		out[id] = cp
	}
	return out
}

func (p *Proposer) record(id view.NodeID, d time.Duration, failed bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.stats[id]
	if s == nil {
		s = &VoterStats{}
		p.stats[id] = s
	}
	s.Calls++
	if failed {
		s.Failures++
	}
	s.samples = append(s.samples, d)
	if len(s.samples) > 512 {
		s.samples = s.samples[len(s.samples)-256:]
	}
}

func (p *Proposer) stamp(v *view.View, m *wire.Msg) *wire.Msg {
	m.ClusterID = v.ClusterID
	m.Incarnation = v.Incarnation
	m.Epoch = v.Epoch
	return m
}

// reply is one acceptor's answer to a round.
type reply struct {
	id  view.NodeID
	msg *wire.Msg
	err error
}

// call sends req to one acceptor with the per-request deadline, handling
// need-view by installing the round's view once and retrying.
func (p *Proposer) call(ctx context.Context, v *view.View, id view.NodeID, req *wire.Msg) (*wire.Msg, error) {
	p.mu.Lock()
	if p.inflight[id] >= p.cfg.MaxInFlight {
		p.mu.Unlock()
		return nil, errors.New("paxos: acceptor in-flight cap reached")
	}
	p.inflight[id]++
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.inflight[id]--
		p.mu.Unlock()
	}()
	for attempt := 0; attempt < 2; attempt++ {
		// Stragglers finish even if the caller stopped waiting: an accept
		// that landed is worth its ack, and the deadline caps the wait.
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.cfg.AcceptorTimeout)
		start := time.Now()
		resp, err := p.tr.Call(cctx, id, req)
		cancel()
		p.record(id, time.Since(start), err != nil)
		if err != nil {
			return nil, err
		}
		if resp.Type == wire.TErr && resp.Code == wire.CodeNeedView && attempt == 0 {
			enc, err := v.Encode()
			if err != nil {
				return nil, err
			}
			ictx, icancel := context.WithTimeout(ctx, p.cfg.AcceptorTimeout)
			_, err = p.tr.Call(ictx, id, p.stamp(v, &wire.Msg{Type: wire.TInstall, View: enc}))
			icancel()
			if err != nil {
				return nil, err
			}
			continue
		}
		return resp, nil
	}
	return nil, errors.New("paxos: acceptor still needs the view")
}

// broadcast sends req to every voter of v in parallel and returns replies
// as they arrive on the channel; the channel is closed when all are in.
func (p *Proposer) broadcast(ctx context.Context, v *view.View, req *wire.Msg) <-chan reply {
	voters := v.VoterIDs()
	ch := make(chan reply, len(voters))
	var wg sync.WaitGroup
	for _, id := range voters {
		wg.Add(1)
		go func(id view.NodeID) {
			defer wg.Done()
			r := *req
			resp, err := p.call(ctx, v, id, &r)
			ch <- reply{id: id, msg: resp, err: err}
		}(id)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}

// staleError carries a newer view learned from an acceptor.
type staleError struct{ v *view.View }

func (e *staleError) Error() string { return "paxos: stale view" }

// conflictError carries the higher ballot an acceptor reported.
type conflictError struct{ b Ballot }

func (e *conflictError) Error() string { return "paxos: conflict at " + e.b.String() }

// classify turns an acceptor reply into an outcome for the round.
func classify(v *view.View, r reply) (msg *wire.Msg, err error) {
	if r.err != nil {
		return nil, r.err
	}
	m := r.msg
	if m.Type == wire.TErr {
		switch m.Code {
		case wire.CodeStaleView:
			nv, err := view.Decode(m.View)
			if err != nil {
				return nil, err
			}
			return nil, &staleError{nv}
		case wire.CodeExpired:
			return nil, ErrExpired
		}
		return nil, wire.ErrorFromMsg(m)
	}
	if m.Incarnation != v.Incarnation || m.Epoch != v.Epoch {
		return nil, fmt.Errorf("paxos: reply at epoch %d/%d, round at %d/%d", m.Incarnation, m.Epoch, v.Incarnation, v.Epoch)
	}
	if m.Type == wire.TConflict {
		b, _ := ParseBallot(m.Ballot)
		return nil, &conflictError{b}
	}
	return m, nil
}

// prepare runs the prepare phase and returns the highest accepted row.
func (p *Proposer) prepare(ctx context.Context, v *view.View, reg []byte, b Ballot) (Row, error) {
	req := p.stamp(v, &wire.Msg{Type: wire.TPrepare, Reg: reg, Ballot: b.Bytes()})
	ch := p.broadcast(ctx, v, req)
	quorum := v.Quorum()
	var best Row
	promises := 0
	var firstErr error
	for r := range ch {
		m, err := classify(v, r)
		if err != nil {
			var se *staleError
			var ce *conflictError
			if errors.As(err, &se) || errors.As(err, &ce) {
				return Row{}, err
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if m.Type != wire.TPromise {
			continue
		}
		acc, _ := ParseBallot(m.Accepted)
		if promises == 0 || acc.Compare(best.Accepted) > 0 {
			best = Row{Accepted: acc, Value: m.Value, HasValue: m.HasValue}
		}
		promises++
		if promises >= quorum {
			return best, nil
		}
	}
	if firstErr != nil {
		return Row{}, fmt.Errorf("%w: %v", ErrUnavailable, firstErr)
	}
	return Row{}, ErrUnavailable
}

// accept runs the accept phase.
func (p *Proposer) accept(ctx context.Context, v *view.View, reg []byte, b Ballot, value []byte, hasValue bool, notAfter int64) error {
	req := p.stamp(v, &wire.Msg{Type: wire.TAccept, Reg: reg, Ballot: b.Bytes(), Value: value, HasValue: hasValue, NotAfter: notAfter})
	ch := p.broadcast(ctx, v, req)
	quorum := v.Quorum()
	accepted := 0
	var firstErr error
	for r := range ch {
		m, err := classify(v, r)
		if err != nil {
			var se *staleError
			var ce *conflictError
			if errors.As(err, &se) || errors.As(err, &ce) || errors.Is(err, ErrExpired) {
				return err
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if m.Type == wire.TAccepted {
			accepted++
			if accepted >= quorum {
				return nil
			}
		}
	}
	if accepted > 0 {
		return fmt.Errorf("%w: %d of %d accepted", ErrUnknown, accepted, quorum)
	}
	if firstErr != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, firstErr)
	}
	return ErrUnavailable
}

func (p *Proposer) nextBallot() (Ballot, error) {
	c, err := p.ballots.NextBallot()
	if err != nil {
		return Ballot{}, err
	}
	return Ballot{Counter: c, Proposer: p.self}, nil
}

// ballotAbove returns a fresh ballot above b.
func (p *Proposer) ballotAbove(b Ballot) (Ballot, error) {
	for {
		nb, err := p.nextBallot()
		if err != nil {
			return Ballot{}, err
		}
		if nb.Compare(b) > 0 {
			return nb, nil
		}
		// The counter is monotone; bump until it clears b's counter.
		if nb.Counter <= b.Counter {
			bumper, ok := p.ballots.(interface{ BumpBallot(uint64) error })
			if ok {
				if err := bumper.BumpBallot(b.Counter); err != nil {
					return Ballot{}, err
				}
			}
		}
	}
}

// Result is a committed proposal.
type Result struct {
	Ballot   Ballot
	Value    []byte
	HasValue bool
}

// ChangeFunc computes the new value from the current row. It returns the
// value to commit (present or not) or an error that ends the proposal
// without an accept round — a CAS mismatch, typically.
type ChangeFunc func(cur Row, b Ballot) (value []byte, hasValue bool, err error)

// Identity re-accepts what the prepare phase found.
func Identity(cur Row, _ Ballot) ([]byte, bool, error) { return cur.Value, cur.HasValue, nil }

// Propose runs one CAS on reg with the change function fn. notAfter, if
// non-zero, is enforced by the acceptors on the accept round. It retries
// conflicts with backoff until ctx ends and adopts newer views from
// stale-view replies.
func (p *Proposer) Propose(ctx context.Context, reg []byte, fn ChangeFunc, notAfter int64) (Result, error) {
	var last Ballot
	backoff := 5 * time.Millisecond
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		v := p.views.View()
		if v == nil {
			return Result{}, errors.New("paxos: no view")
		}
		b, err := p.ballotAbove(last)
		if err != nil {
			return Result{}, err
		}
		cur, err := p.prepare(ctx, v, reg, b)
		if err != nil {
			if retry, err2 := p.handleRoundError(ctx, err, &last, &backoff); retry {
				continue
			} else if err2 != nil {
				return Result{}, err2
			}
		}
		value, hasValue, err := fn(cur, b)
		if err != nil {
			// The prepare phase alone may be looking at a minority-accepted
			// value: re-accept what was found at this ballot so the answer
			// given to the caller is a decided one (§5.3).
			if cur.Accepted.IsZero() && !cur.HasValue {
				return Result{}, err
			}
			if aerr := p.accept(ctx, v, reg, b, cur.Value, cur.HasValue, 0); aerr != nil {
				if retry, err2 := p.handleRoundError(ctx, aerr, &last, &backoff); retry {
					continue
				} else if err2 != nil && !errors.Is(aerr, ErrUnknown) {
					return Result{}, err2
				}
			}
			return Result{Ballot: cur.Accepted, Value: cur.Value, HasValue: cur.HasValue}, err
		}
		err = p.accept(ctx, v, reg, b, value, hasValue, notAfter)
		if err != nil {
			if errors.Is(err, ErrUnknown) {
				return Result{Ballot: b, Value: value, HasValue: hasValue}, err
			}
			if retry, err2 := p.handleRoundError(ctx, err, &last, &backoff); retry {
				continue
			} else if err2 != nil {
				return Result{}, err2
			}
		}
		return Result{Ballot: b, Value: value, HasValue: hasValue}, nil
	}
}

// handleRoundError decides whether a round error is retried.
func (p *Proposer) handleRoundError(ctx context.Context, err error, last *Ballot, backoff *time.Duration) (bool, error) {
	var se *staleError
	var ce *conflictError
	switch {
	case errors.As(err, &se):
		p.views.Adopt(se.v)
		return true, nil
	case errors.As(err, &ce):
		if ce.b.Compare(*last) > 0 {
			*last = ce.b
		}
		d := time.Duration(rand.Int64N(int64(*backoff))) + *backoff/2
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return false, ctx.Err()
		}
		if *backoff < 500*time.Millisecond {
			*backoff *= 2
		}
		return true, nil
	}
	return false, err
}

// Settle runs the identity transition on reg: it decides the register one
// way or the other and fences every lower ballot.
func (p *Proposer) Settle(ctx context.Context, reg []byte) (Row, error) {
	res, err := p.Propose(ctx, reg, Identity, 0)
	if err != nil {
		return Row{}, err
	}
	return Row{Accepted: res.Ballot, Value: res.Value, HasValue: res.HasValue}, nil
}

// Read is the fast read: a majority of voters report the same accepted
// ballot and the value is returned with no persistence; otherwise it
// falls back to Settle.
func (p *Proposer) Read(ctx context.Context, reg []byte) (Row, error) {
	v := p.views.View()
	if v == nil {
		return Row{}, errors.New("paxos: no view")
	}
	req := p.stamp(v, &wire.Msg{Type: wire.TRead, Reg: reg})
	ch := p.broadcast(ctx, v, req)
	quorum := v.Quorum()
	var rows []Row
	for r := range ch {
		m, err := classify(v, r)
		if err != nil {
			var se *staleError
			if errors.As(err, &se) {
				p.views.Adopt(se.v)
				return p.Read(ctx, reg)
			}
			continue
		}
		if m.Type != wire.TReadReply {
			continue
		}
		acc, _ := ParseBallot(m.Accepted)
		rows = append(rows, Row{Accepted: acc, Value: m.Value, HasValue: m.HasValue})
		if len(rows) >= quorum {
			break
		}
	}
	if len(rows) < quorum {
		return p.Settle(ctx, reg)
	}
	for _, r := range rows[1:] {
		if r.Accepted.Compare(rows[0].Accepted) != 0 {
			return p.Settle(ctx, reg)
		}
	}
	return rows[0], nil
}

// MergedRow is one register of a majority scan.
type MergedRow struct {
	Reg       []byte
	Row       Row
	Undecided bool // voters disagree, or a promise is above the accepted ballot
}

// Scan is a paged majority scan merged by highest accepted ballot per
// register. It returns the rows of one page and the cursor to continue from
// (nil when exhausted).
func (p *Proposer) Scan(ctx context.Context, prefix, after []byte, limit int) ([]MergedRow, []byte, error) {
	v := p.views.View()
	if v == nil {
		return nil, nil, errors.New("paxos: no view")
	}
	req := p.stamp(v, &wire.Msg{Type: wire.TScan, Prefix: prefix, After: after, Limit: limit})
	ch := p.broadcast(ctx, v, req)
	quorum := v.Quorum()
	var replies []*wire.Msg
	for r := range ch {
		m, err := classify(v, r)
		if err != nil {
			var se *staleError
			if errors.As(err, &se) {
				p.views.Adopt(se.v)
				return p.Scan(ctx, prefix, after, limit)
			}
			continue
		}
		if m.Type == wire.TScanReply {
			replies = append(replies, m)
		}
	}
	if len(replies) < quorum {
		return nil, nil, ErrUnavailable
	}
	// The cut is the smallest last-name among replies that have more.
	var cut []byte
	hasCut := false
	for _, m := range replies {
		if m.More && len(m.Next) > 0 && (!hasCut || bytes.Compare(m.Next, cut) < 0) {
			cut = m.Next
			hasCut = true
		}
	}
	type acc struct {
		row      Row
		promised Ballot
		count    int
		disagree bool
		stub     bool
	}
	merged := map[string]*acc{}
	for _, m := range replies {
		for _, sr := range m.Rows {
			if hasCut && bytes.Compare(sr.Reg, cut) > 0 {
				continue
			}
			a := merged[string(sr.Reg)]
			accepted, _ := ParseBallot(sr.Accepted)
			promised, _ := ParseBallot(sr.Promised)
			if a == nil {
				a = &acc{row: Row{Accepted: accepted, Value: sr.Value, HasValue: sr.HasValue}, promised: promised}
				merged[string(sr.Reg)] = a
			} else {
				if accepted.Compare(a.row.Accepted) != 0 {
					a.disagree = true
				}
				if accepted.Compare(a.row.Accepted) > 0 {
					a.row = Row{Accepted: accepted, Value: sr.Value, HasValue: sr.HasValue}
				}
				if promised.Compare(a.promised) > 0 {
					a.promised = promised
				}
			}
			if accepted.IsZero() {
				a.stub = true
			}
			a.count++
		}
	}
	out := make([]MergedRow, 0, len(merged))
	for reg, a := range merged {
		undecided := a.disagree || a.count < len(replies) || a.promised.Compare(a.row.Accepted) > 0 || a.stub
		out = append(out, MergedRow{Reg: []byte(reg), Row: a.row, Undecided: undecided})
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].Reg, out[j].Reg) < 0 })
	var next []byte
	if hasCut {
		next = cut
	}
	return out, next, nil
}

// Purge asks every voter to drop the tombstone row of reg at ballot b. It
// reports how many voters confirmed.
func (p *Proposer) Purge(ctx context.Context, reg []byte, b Ballot) int {
	v := p.views.View()
	if v == nil {
		return 0
	}
	req := p.stamp(v, &wire.Msg{Type: wire.TPurge, Reg: reg, Ballot: b.Bytes()})
	ch := p.broadcast(ctx, v, req)
	n := 0
	for r := range ch {
		if r.err == nil && r.msg.Type == wire.TOK {
			n++
		}
	}
	return n
}

// Install sends the view to the given acceptors and returns the ids that
// confirmed.
func (p *Proposer) Install(ctx context.Context, v *view.View, to []view.NodeID) []view.NodeID {
	enc, err := v.Encode()
	if err != nil {
		return nil
	}
	var mu sync.Mutex
	var ok []view.NodeID
	var wg sync.WaitGroup
	for _, id := range to {
		wg.Add(1)
		go func(id view.NodeID) {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, p.cfg.AcceptorTimeout)
			defer cancel()
			resp, err := p.tr.Call(cctx, id, p.stamp(v, &wire.Msg{Type: wire.TInstall, View: enc}))
			if err == nil && resp.Type == wire.TInstalled {
				mu.Lock()
				ok = append(ok, id)
				mu.Unlock()
			}
		}(id)
	}
	wg.Wait()
	return ok
}

// ReadAll asks every voter for its row of reg (a fast read that does not
// stop at a majority) and reports each answer; for tombstone purging.
func (p *Proposer) ReadAll(ctx context.Context, reg []byte) map[view.NodeID]Row {
	v := p.views.View()
	if v == nil {
		return nil
	}
	req := p.stamp(v, &wire.Msg{Type: wire.TRead, Reg: reg})
	ch := p.broadcast(ctx, v, req)
	out := map[view.NodeID]Row{}
	for r := range ch {
		m, err := classify(v, r)
		if err != nil || m.Type != wire.TReadReply {
			continue
		}
		acc, _ := ParseBallot(m.Accepted)
		out[r.id] = Row{Accepted: acc, Value: m.Value, HasValue: m.HasValue}
	}
	return out
}

// Self returns the proposer's node id.
func (p *Proposer) Self() view.NodeID { return p.self }

// Transport returns the underlying transport.
func (p *Proposer) Transport() Transport { return p.tr }
