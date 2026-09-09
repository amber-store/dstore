package node

import (
	"context"
	"errors"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/amber-store/dstore/transport"
	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
)

// putTx is one put batch in progress. Records verified as they arrive are
// appended to the store in chunks on a worker and, on the client ALPN,
// streamed to each key's other owners at once, so that a batch's store
// and replication overlap its transfer instead of following it (§6.2).
type putTx struct {
	n      *Node
	ctx    context.Context
	pl     *view.Placement
	client bool

	keys       [][32]byte
	chunk      *putBatch
	chunkBytes int

	storeCh   chan *putBatch
	storeDone chan struct{}
	errMu     sync.Mutex
	err       error

	fwd map[view.NodeID]*forwarder
}

func newPutBatch() *putBatch { return &putBatch{records: map[[32]byte][]byte{}} }

func (n *Node) newPutTx(ctx context.Context, pl *view.Placement, client bool) *putTx {
	tx := &putTx{n: n, ctx: ctx, pl: pl, client: client, chunk: newPutBatch(),
		storeCh: make(chan *putBatch, 1), storeDone: make(chan struct{}), fwd: map[view.NodeID]*forwarder{}}
	go tx.storeWorker()
	return tx
}

// storeWorker appends chunks as they fill; after a failure it drains.
func (tx *putTx) storeWorker() {
	defer close(tx.storeDone)
	for b := range tx.storeCh {
		if tx.failed() != nil {
			continue
		}
		if _, _, err := tx.n.storeBatch(b); err != nil {
			tx.fail(err)
		}
	}
}

func (tx *putTx) fail(err error) {
	tx.errMu.Lock()
	if tx.err == nil {
		tx.err = err
	}
	tx.errMu.Unlock()
}

func (tx *putTx) failed() error {
	tx.errMu.Lock()
	defer tx.errMu.Unlock()
	return tx.err
}

// add takes one verified record this node owns.
func (tx *putTx) add(k [32]byte, rec []byte) error {
	if err := tx.failed(); err != nil {
		return err
	}
	tx.keys = append(tx.keys, k)
	tx.chunk.keys = append(tx.chunk.keys, k)
	tx.chunk.records[k] = rec
	tx.chunkBytes += len(rec)
	if tx.chunkBytes >= tx.n.cfg.PutChunkBytes {
		tx.flushChunk()
	}
	if tx.client {
		for _, o := range tx.pl.WriteSet(k) {
			if o != tx.n.id {
				tx.forwarder(o).send(k, rec)
			}
		}
	}
	return nil
}

func (tx *putTx) flushChunk() {
	if len(tx.chunk.keys) == 0 {
		return
	}
	tx.storeCh <- tx.chunk
	tx.chunk, tx.chunkBytes = newPutBatch(), 0
}

// finish waits for the store and the forwards. It returns the owners now
// holding each key and, per key an owner did not take, why.
func (tx *putTx) finish() (holders map[[32]byte][][]byte, failed []wire.KeyFailure, err error) {
	tx.flushChunk()
	close(tx.storeCh)
	for _, f := range tx.fwd {
		f.close()
	}
	<-tx.storeDone
	holders = make(map[[32]byte][][]byte, len(tx.keys))
	for _, k := range tx.keys {
		holders[k] = [][]byte{tx.n.id[:]}
	}
	for o, f := range tx.fwd {
		ok, errs := f.wait()
		for _, k := range ok {
			holders[k] = append(holders[k], o[:])
		}
		short := make([][32]byte, 0, len(errs))
		for k, reason := range errs {
			failed = append(failed, wire.KeyFailure{Key: append([]byte{}, k[:]...), Node: o[:], Reason: reason, RetryAfter: int64(500 + rand.IntN(1500))})
			short = append(short, k)
		}
		if len(short) > 0 {
			tx.n.rec.scheduleForward(o, short)
		}
	}
	if err := tx.failed(); err != nil {
		return nil, nil, err
	}
	return holders, failed, nil
}

// abort ends the batch after an error: what was received stays stored,
// the forwards are cut.
func (tx *putTx) abort() {
	tx.flushChunk()
	close(tx.storeCh)
	for _, f := range tx.fwd {
		f.giveUp(errors.New("batch aborted"))
		f.close()
	}
	<-tx.storeDone
	for _, f := range tx.fwd {
		<-f.done
	}
}

func (tx *putTx) forwarder(o view.NodeID) *forwarder {
	f := tx.fwd[o]
	if f == nil {
		ctx, cancel := context.WithCancel(tx.ctx)
		f = &forwarder{tx: tx, to: o, ctx: ctx, cancel: cancel, ch: make(chan []byte, 64), done: make(chan struct{})}
		tx.fwd[o] = f
		go f.run()
	}
	return f
}

// forwarder streams a batch's records for one other owner over a put on
// the cluster ALPN as they arrive.
type forwarder struct {
	tx     *putTx
	to     view.NodeID
	ctx    context.Context // cancelled by giveUp, aborting a dial in progress
	cancel context.CancelFunc
	ch     chan []byte
	keys   [][32]byte // handed to it, in order; the handler's goroutine only
	done   chan struct{}

	once sync.Once
	mu   sync.Mutex
	s    transport.Stream
	dead bool
	err  error
	res  *wire.Msg
}

// send hands one record to the forward. A peer that does not keep up for
// ForwardTimeout is given up for the rest of the batch: its keys fail
// and the reconcile pass heals them (§6.2).
func (f *forwarder) send(k [32]byte, rec []byte) {
	f.keys = append(f.keys, k)
	f.mu.Lock()
	dead := f.dead
	f.mu.Unlock()
	if dead {
		return
	}
	select {
	case f.ch <- rec:
		return
	case <-f.done:
		return
	default:
	}
	t := time.NewTimer(f.tx.n.cfg.ForwardTimeout)
	defer t.Stop()
	select {
	case f.ch <- rec:
	case <-f.done:
	case <-t.C:
		f.giveUp(context.DeadlineExceeded)
	}
}

func (f *forwarder) close() { f.once.Do(func() { close(f.ch) }) }

// giveUp cuts the dial or the stream so that a blocked send returns.
func (f *forwarder) giveUp(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dead {
		return
	}
	f.dead = true
	f.err = err
	f.cancel()
	if f.s != nil {
		_ = f.s.Close()
		f.s.CancelRead(0)
	}
}

func (f *forwarder) fail(err error) {
	f.mu.Lock()
	if f.err == nil {
		f.err = err
	}
	f.mu.Unlock()
}

func (f *forwarder) run() {
	defer close(f.done)
	defer f.cancel()
	n := f.tx.n
	// An owner that cannot be reached within the forward timeout fails
	// its keys for this batch; the reconcile pass heals them (§6.2).
	octx, ocancel := context.WithTimeout(f.ctx, n.cfg.ForwardTimeout)
	s, err := n.pool.Open(octx, f.to, wire.ALPNCluster)
	ocancel()
	if err != nil {
		n.markUnreachable(f.to, err)
		f.fail(err)
		return
	}
	defer wire.CloseStream(s)
	f.mu.Lock()
	if f.dead {
		f.mu.Unlock()
		return
	}
	f.s = s
	f.mu.Unlock()
	if err := wire.WriteMsg(s, n.stampReq(&wire.Msg{Type: wire.TPut})); err != nil {
		f.fail(err)
		return
	}
	err = wire.SendPackRecords(s, func(yield func([]byte, error) bool) {
		for rec := range f.ch {
			if !yield(rec, nil) {
				return
			}
		}
	})
	if err != nil {
		f.fail(err)
		return
	}
	_ = s.CloseWrite()
	type result struct {
		m   *wire.Msg
		err error
	}
	reply := make(chan result, 1)
	go func() {
		m, err := wire.Expect(s, wire.TPutResult)
		reply <- result{m, err}
	}()
	t := time.NewTimer(n.cfg.ForwardTimeout)
	defer t.Stop()
	select {
	case r := <-reply:
		if r.err != nil {
			n.adoptStale(r.err)
			f.fail(r.err)
			return
		}
		n.markReachable(f.to)
		f.mu.Lock()
		f.res = r.m
		f.mu.Unlock()
	case <-t.C:
		s.CancelRead(0)
		f.fail(context.DeadlineExceeded)
	case <-f.ctx.Done():
		s.CancelRead(0)
		f.fail(f.ctx.Err())
	}
}

// wait returns the keys the owner confirmed and, per key it did not, the
// reason. It is called after close, from the handler's goroutine.
func (f *forwarder) wait() (ok [][32]byte, failed map[[32]byte]string) {
	<-f.done
	failed = map[[32]byte]string{}
	f.mu.Lock()
	err, res := f.err, f.res
	f.mu.Unlock()
	if err != nil || res == nil {
		if err == nil {
			err = errors.New("no reply")
		}
		for _, k := range f.keys {
			failed[k] = reasonOf(err)
		}
		return nil, failed
	}
	confirmed := map[[32]byte]bool{}
	for _, h := range res.Holders {
		if len(h.Key) == 32 {
			confirmed[[32]byte(h.Key)] = true
		}
	}
	rej := map[[32]byte]string{}
	for _, r := range res.Rejected {
		if len(r.Key) == 32 {
			rej[[32]byte(r.Key)] = r.Reason
		}
	}
	for _, k := range f.keys {
		switch {
		case confirmed[k]:
			ok = append(ok, k)
		case rej[k] != "":
			failed[k] = "rejected: " + rej[k]
			if strings.HasPrefix(rej[k], "verify") {
				f.tx.n.rec.verifyLocal(k)
			}
		default:
			failed[k] = "unconfirmed"
		}
	}
	return ok, failed
}

// adoptStale adopts the view a stale-view reply carries.
func (n *Node) adoptStale(err error) {
	if !wire.IsCode(err, wire.CodeStaleView) {
		return
	}
	if we, ok := wire.AsError(err); ok && len(we.View) > 0 {
		if v, derr := view.Decode(we.View); derr == nil {
			n.adopt(v, "stale-reply")
		}
	}
}
