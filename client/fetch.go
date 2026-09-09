package client

import (
	"context"
	"io"
	"sync"

	"github.com/amber-store/core/amberpack"
	"github.com/amber-store/core/key"
	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
)

// Get batch limits: a batch is at most getBatchKeys keys or getBatchBytes
// by the keys' length fields, a blob's length being its record size and
// a tree object's capped at a chunk.
const (
	getBatchKeys  = 2048
	getBatchBytes = 8 << 20
	getEstMax     = 64 << 10
)

func estSize(k [32]byte) int {
	return amberpack.RecHeaderSize + min(int(key.Key(k).Length()), getEstMax)
}

// fetchKey is a key with its position in the read order.
type fetchKey struct {
	key     [32]byte
	attempt int
}

// fetched is one outcome: rec is nil when no owner has the key.
type fetched struct {
	key [32]byte
	rec []byte
}

type fetchJob struct {
	node view.NodeID
	keys []fetchKey
}

type fetchAcc struct {
	keys  []fetchKey
	bytes int
}

// fetcher fetches records for keys as they are handed to it (§6.3,
// §11.5): keys are batched per preferred owner, batches run on Jobs
// workers over the pooled connections, records stream out as each is
// verified, and a key an owner lacks is re-asked down its read order.
type fetcher struct {
	c         *Cluster
	ctx       context.Context
	cancel    context.CancelFunc
	in        chan [32]byte
	retry     chan fetchKey
	out       chan fetched
	jobs      chan fetchJob
	jobDone   chan struct{}
	done      chan struct{}
	wg        sync.WaitGroup
	refreshed bool
}

func (c *Cluster) newFetcher(ctx context.Context) *fetcher {
	fctx, cancel := context.WithCancel(ctx)
	f := &fetcher{c: c, ctx: fctx, cancel: cancel,
		in: make(chan [32]byte, 1024), retry: make(chan fetchKey, 1024), out: make(chan fetched, 256),
		jobs: make(chan fetchJob), jobDone: make(chan struct{}, c.cfg.Jobs), done: make(chan struct{})}
	for range c.cfg.Jobs {
		f.wg.Add(1)
		go f.worker()
	}
	go f.run()
	return f
}

// input is the channel keys to fetch go to; finish closes it.
func (f *fetcher) input() chan<- [32]byte { return f.in }

// add hands one key to fetch; false once the fetch is stopped.
func (f *fetcher) add(k [32]byte) bool {
	select {
	case f.in <- k:
		return true
	case <-f.ctx.Done():
		return false
	}
}

// finish says no more keys will come; results ends once every key's fate
// is known.
func (f *fetcher) finish() { close(f.in) }

// results yields outcomes; it is closed when the fetch is over.
func (f *fetcher) results() <-chan fetched { return f.out }

// stop cancels the fetch and waits for its goroutines.
func (f *fetcher) stop() {
	f.cancel()
	<-f.done
}

func (f *fetcher) run() {
	defer close(f.done)
	defer func() {
		close(f.jobs)
		f.wg.Wait()
		close(f.out)
	}()
	acc := map[view.NodeID]*fetchAcc{}
	inflight := 0
	inOpen := true
	for {
		// Take everything queued before dispatching, so that batches fill
		// while the workers are busy.
		for inOpen {
			select {
			case k, ok := <-f.in:
				if !ok {
					inOpen = false
					continue
				}
				f.route(acc, fetchKey{key: k})
				continue
			default:
			}
			break
		}
	drained:
		for {
			select {
			case r := <-f.retry:
				f.route(acc, r)
			default:
				break drained
			}
		}
		for inflight < f.c.cfg.Jobs {
			job, ok := pickBatch(acc)
			if !ok {
				break
			}
			inflight++
			select {
			case f.jobs <- job:
			case <-f.ctx.Done():
				return
			}
		}
		if !inOpen && inflight == 0 && len(acc) == 0 {
			return
		}
		var inCh chan [32]byte
		if inOpen {
			inCh = f.in
		}
		select {
		case k, ok := <-inCh:
			if !ok {
				inOpen = false
				continue
			}
			f.route(acc, fetchKey{key: k})
		case r := <-f.retry:
			f.route(acc, r)
		case <-f.jobDone:
			inflight--
		case <-f.ctx.Done():
			return
		}
	}
}

// route queues a key at the owner its attempt names; past the end of the
// read order the view is refreshed once (a miss at every owner is the
// signature of a stale view, §6.3) and then the key is reported missing.
func (f *fetcher) route(acc map[view.NodeID]*fetchAcc, fk fetchKey) {
	order := f.c.ReadOrder(fk.key)
	if fk.attempt >= len(order) && !f.refreshed {
		f.refreshed = true
		_ = f.c.RefreshView(f.ctx)
		order = f.c.ReadOrder(fk.key)
		fk.attempt = 0
	}
	if fk.attempt >= len(order) {
		select {
		case f.out <- fetched{key: fk.key}:
		case <-f.ctx.Done():
		}
		return
	}
	node := order[fk.attempt]
	a := acc[node]
	if a == nil {
		a = &fetchAcc{}
		acc[node] = a
	}
	a.keys = append(a.keys, fk)
	a.bytes += estSize(fk.key)
}

// pickBatch takes a batch from a full accumulator, else from the largest.
func pickBatch(acc map[view.NodeID]*fetchAcc) (fetchJob, bool) {
	var best view.NodeID
	var bestAcc *fetchAcc
	for id, a := range acc {
		if len(a.keys) >= getBatchKeys || a.bytes >= getBatchBytes {
			best, bestAcc = id, a
			break
		}
		if bestAcc == nil || len(a.keys) > len(bestAcc.keys) {
			best, bestAcc = id, a
		}
	}
	if bestAcc == nil {
		return fetchJob{}, false
	}
	n := 0
	bytes := 0
	for n < len(bestAcc.keys) && n < getBatchKeys && (n == 0 || bytes+estSize(bestAcc.keys[n].key) <= getBatchBytes) {
		bytes += estSize(bestAcc.keys[n].key)
		n++
	}
	job := fetchJob{node: best, keys: bestAcc.keys[:n:n]}
	bestAcc.keys = bestAcc.keys[n:]
	bestAcc.bytes -= bytes
	if len(bestAcc.keys) == 0 {
		delete(acc, best)
	}
	return job, true
}

func (f *fetcher) worker() {
	defer f.wg.Done()
	for job := range f.jobs {
		f.fetch(job)
		f.jobDone <- struct{}{}
	}
}

// fetch runs one batch; keys the owner did not return go back for the
// next owner in their read order.
func (f *fetcher) fetch(job fetchJob) {
	keys := make([][32]byte, len(job.keys))
	for i, fk := range job.keys {
		keys[i] = fk.key
	}
	got := make(map[[32]byte]bool, len(keys))
	_ = f.c.getStream(f.ctx, job.node, keys, func(k [32]byte, rec []byte) bool {
		got[k] = true
		select {
		case f.out <- fetched{key: k, rec: rec}:
			return true
		case <-f.ctx.Done():
			return false
		}
	})
	for _, fk := range job.keys {
		if got[fk.key] {
			continue
		}
		select {
		case f.retry <- fetchKey{key: fk.key, attempt: fk.attempt + 1}:
		case <-f.ctx.Done():
			return
		}
	}
}

// getStream fetches a batch from one node, handing over each record as
// it is verified; emit returning false ends the stream.
func (c *Cluster) getStream(ctx context.Context, id view.NodeID, keys [][32]byte, emit func(k [32]byte, rec []byte) bool) error {
	cctx, cancel := context.WithTimeout(ctx, 10*c.cfg.RequestTimeout)
	defer cancel()
	s, err := c.pool.Open(cctx, id, wire.ALPNClient)
	if err != nil {
		c.handleErr(id, err)
		return err
	}
	defer wire.CloseStream(s)
	if err := wire.WriteMsg(s, c.stamp(&wire.Msg{Type: wire.TGet, Keys: wire.RawKeys(keys)})); err != nil {
		return err
	}
	_ = s.CloseWrite()
	if _, err := wire.Expect(s, wire.TAbsent); err != nil {
		c.handleErr(id, err)
		return err
	}
	pr := wire.NewPackReader(s)
	reader := amberpack.NewReader(pr)
	for raw, err := range reader.Records() {
		if err != nil {
			return err
		}
		k, rec, err := VerifyRecord(raw)
		if err != nil {
			continue // a corrupt copy: the next owner is asked
		}
		if !emit(k, rec) {
			return ctx.Err()
		}
	}
	_, _ = io.Copy(io.Discard, pr)
	c.ok(id)
	return nil
}
