package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/amber-store/core/amberpack"
	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/core/reference"
	"github.com/amber-store/dstore/view"
)

// PushStats summarises a push.
type PushStats struct {
	Keys     int
	Uploaded int
	Bytes    int64
	Version  []byte
}

// Push uploads the tree under root from local and writes the reference
// (§11.4). Every record is sent once, to a primary; the ack policy is
// checked per key; keys short at an owner are re-sent and, failing that,
// sent to the lacking owner directly.
func (c *Cluster) Push(ctx context.Context, local *packstore.Store, root key.Key, name, user string, cond Cond, prog Progress) (PushStats, error) {
	var st PushStats
	keys, err := fstree.ReachableKeys(root, local.Get)
	if err != nil {
		return st, fmt.Errorf("walk local tree: %w", err)
	}
	all := make([][32]byte, len(keys))
	for i, k := range keys {
		all[i] = [32]byte(k)
	}
	st.Keys = len(all)
	src := func(k [32]byte) ([]byte, error) { return local.GetRecord(key.Key(k)) }
	size := storedSizer(local)
	tr := newTracker(c, prog)
	obs := tr.observer()

	start := time.Now()
	lastPin := start
	holders := map[[32]byte][]view.NodeID{}
	var uploaded int
	var lastErr error
	c.probeHinted(ctx)
	for round := 0; round < 3; round++ {
		mr, err := c.Missing(ctx, all, true)
		if err != nil {
			return st, err
		}
		for k, h := range mr.Holders {
			holders[k] = h
		}
		for k, err := range mr.Failed {
			return st, fmt.Errorf("negotiate %x at its primary: %w", k[:8], err)
		}
		lacking, lackBytes := countKeys(mr.Lacking, size)
		if round == 0 {
			tr.totals(len(all), len(all)-lacking, lackBytes)
			c.log.Info("negotiated", "objects", len(all), "present", len(all)-lacking, "upload", lacking, "bytes", lackBytes, "primaries", len(mr.Lacking))
		} else {
			tr.more(lackBytes)
			c.log.Info("re-sending objects short at their primaries", "round", round+1, "objects", lacking, "bytes", lackBytes)
		}
		if len(mr.Lacking) > 0 {
			pr := c.Put(ctx, mr.Lacking, src, size, obs)
			n := 0
			for k, h := range pr.Holders {
				if _, had := holders[k]; !had {
					uploaded++
					n++
				}
				holders[k] = h
			}
			tr.objects(n)
			for p, err := range pr.Errors {
				// The keys stay short; the next round negotiates them at
				// another owner, the failed one being penalised.
				lastErr = fmt.Errorf("upload to %s: %w", view.ShortID(p), err)
				c.log.Warn("upload to a primary failed, its objects go to another owner", "node", view.ShortID(p), "err", err)
			}
			for k, reason := range pr.Rejected {
				return st, fmt.Errorf("record %x rejected: %s", k[:8], reason)
			}
		}
		// Ack policy: every key placed?
		var short [][32]byte
		for _, k := range all {
			if !c.Placed(k, holders[k]) {
				short = append(short, k)
			}
		}
		if len(short) == 0 {
			break
		}
		if round == 2 {
			err := c.shortError(short, holders)
			if lastErr != nil {
				err = fmt.Errorf("%w; last upload error: %v", err, lastErr)
			}
			return st, err
		}
		// Re-put short keys to their primaries; on the last round send
		// them to the lacking owners directly.
		if round == 1 {
			if err := c.directFill(ctx, short, holders, src, size, tr); err != nil {
				return st, err
			}
		}
		// Re-pin everything uploaded so far if the push runs long.
		if time.Since(lastPin) > c.cfg.GCInterval/2 {
			_, _ = c.Missing(ctx, all, true)
			lastPin = time.Now()
		}
	}
	st.Uploaded = uploaded
	st.Bytes = tr.bytes()
	took := time.Since(start)
	c.log.Info("upload complete", "uploaded", uploaded, "bytes", st.Bytes, "took", took.Round(time.Millisecond), "rate", Rate(st.Bytes, took))

	rec := reference.Reference{Name: name, Key: root[:], User: user, CreatedAt: time.Now().UnixNano()}
	enc, err := rec.Encode()
	if err != nil {
		return st, err
	}
	for attempt := 0; attempt < 3; attempt++ {
		version, err := c.RefPut(ctx, enc, cond)
		if err == nil {
			st.Version = version
			c.log.Info("reference written", "name", name, "version", fmt.Sprintf("%x", version))
			return st, nil
		}
		var inc *Incomplete
		if !errors.As(err, &inc) || attempt == 2 {
			return st, err
		}
		// GC may have reaped a dedup hit between negotiation and gate:
		// re-run the whole negotiation and send what is missing anywhere.
		c.log.Warn("reference write incomplete, renegotiating", "attempt", attempt+1, "err", err)
		mr, merr := c.Missing(ctx, all, true)
		if merr != nil {
			return st, merr
		}
		if len(mr.Lacking) > 0 {
			_, lackBytes := countKeys(mr.Lacking, size)
			tr.more(lackBytes)
			c.Put(ctx, mr.Lacking, src, size, obs)
		}
		var short [][32]byte
		for _, k := range all {
			if h, ok := mr.Holders[k]; !ok || !c.Placed(k, h) {
				short = append(short, k)
			}
		}
		if len(short) > 0 {
			if err := c.directFill(ctx, short, mr.Holders, src, size, tr); err != nil {
				return st, err
			}
		}
		st.Bytes = tr.bytes()
	}
	return st, errors.New("push: reference write did not complete")
}

// directFill sends short keys straight to the owners that lack them.
func (c *Cluster) directFill(ctx context.Context, short [][32]byte, holders map[[32]byte][]view.NodeID, src RecordSource, size RecordSizer, tr *tracker) error {
	byOwner := map[view.NodeID][][32]byte{}
	for _, k := range short {
		have := map[view.NodeID]bool{}
		for _, h := range holders[k] {
			have[h] = true
		}
		for _, o := range c.WriteSet(k) {
			if !have[o] {
				byOwner[o] = append(byOwner[o], k)
			}
		}
	}
	if len(byOwner) == 0 {
		return nil
	}
	_, bytes := countKeys(byOwner, size)
	tr.more(bytes)
	c.log.Info("sending short objects to their owners directly", "objects", len(short), "owners", len(byOwner), "bytes", bytes)
	pr := c.Put(ctx, byOwner, src, size, tr.observer())
	for k, h := range pr.Holders {
		holders[k] = mergeIDs(holders[k], h)
	}
	return nil
}

// storedSizer sizes records by the local packstore's index, falling back
// to the key's length field for a key the store does not hold.
func storedSizer(local *packstore.Store) RecordSizer {
	return func(k [32]byte) int {
		if n, ok, err := local.StoredSize(key.Key(k)); err == nil && ok {
			return amberpack.RecHeaderSize + int(n)
		}
		return int(key.Key(k).Length())
	}
}

func mergeIDs(a, b []view.NodeID) []view.NodeID {
	seen := map[view.NodeID]bool{}
	var out []view.NodeID
	for _, id := range append(append([]view.NodeID{}, a...), b...) {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

func (c *Cluster) shortError(short [][32]byte, holders map[[32]byte][]view.NodeID) error {
	missing := map[view.NodeID]int{}
	for _, k := range short {
		have := map[view.NodeID]bool{}
		for _, h := range holders[k] {
			have[h] = true
		}
		for _, o := range c.WriteSet(k) {
			if !have[o] {
				missing[o]++
			}
		}
	}
	var names []string
	for id, n := range missing {
		names = append(names, fmt.Sprintf("%s (%d keys)", view.ShortID(id), n))
	}
	return fmt.Errorf("push: %d keys could not be placed; owners not confirming: %v", len(short), names)
}

// PullStats summarises a pull.
type PullStats struct {
	Keys    int
	Fetched int
	Bytes   int64
	Root    key.Key
	Record  []byte
	Version []byte
}

// Pull fetches the tree under the named reference into local (§11.5): a
// top-down frontier walk that prunes subtrees the local store already holds
// complete, then a final completeness check.
func (c *Cluster) Pull(ctx context.Context, local *packstore.Store, name string, prog Progress) (PullStats, error) {
	var st PullStats
	ref, err := c.RefGet(ctx, name)
	if err != nil {
		return st, err
	}
	root, err := key.Parse(ref.Ref.Key)
	if err != nil {
		return st, err
	}
	st.Root, st.Record, st.Version = root, ref.Record, ref.Version
	c.probeHinted(ctx)
	if err := c.PullTree(ctx, local, root, &st, prog); err != nil {
		return st, err
	}
	return st, nil
}

// PullTree fetches the tree under root into local: a work queue that
// fetches keys as they are discovered, prunes subtrees the local store
// already holds complete (an interrupted pull leaves parents above
// missing children), writes records as they arrive, and ends with a
// completeness check.
func (c *Cluster) PullTree(ctx context.Context, local *packstore.Store, root key.Key, st *PullStats, prog Progress) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	f := c.newFetcher(ctx)
	defer f.stop()
	w := newLocalWriter(local, c.cfg.Jobs)
	defer w.stop()

	seen := map[key.Key]bool{}
	var queue [][32]byte
	pending := 0
	// want decides what to do with a key: nothing when held complete,
	// expand locally when held but incomplete below, else fetch.
	var want func(k key.Key) error
	want = func(k key.Key) error {
		if seen[k] {
			return nil
		}
		seen[k] = true
		has, err := local.Has(k)
		if err != nil {
			return err
		}
		if has {
			if k.Type() == key.Blob || k.Type() == key.XattrSet {
				return nil
			}
			if _, err := fstree.CheckComplete(k, local.Get, local.Has, c.cfg.Jobs); err == nil {
				return nil
			}
			if data, err := local.Get(k); err == nil {
				if kids, err := fstree.ChildKeys(k, data); err == nil {
					for _, kid := range kids {
						if err := want(kid); err != nil {
							return err
						}
					}
					return nil
				}
			}
		}
		queue = append(queue, [32]byte(k))
		pending++
		st.Keys++
		return nil
	}
	if err := want(root); err != nil {
		return err
	}
	for pending > 0 {
		var send chan<- [32]byte
		var next [32]byte
		if len(queue) > 0 {
			send, next = f.input(), queue[0]
		}
		select {
		case send <- next:
			queue = queue[1:]
		case r, ok := <-f.results():
			if !ok {
				if err := ctx.Err(); err != nil {
					return err
				}
				return errors.New("pull: fetch ended early")
			}
			if r.rec == nil {
				return fmt.Errorf("pull: object %x not found in the cluster", r.key[:8])
			}
			k := key.Key(r.key)
			if err := w.write(packstore.Object{Key: k, Record: r.rec}); err != nil {
				return err
			}
			st.Fetched++
			st.Bytes += int64(len(r.rec))
			if prog != nil {
				prog(ProgressReport{Objects: st.Fetched, TotalObjects: st.Keys, Bytes: st.Bytes})
			}
			if k.Type() != key.Blob && k.Type() != key.XattrSet {
				rec, err := amberpack.ParseRecord(r.rec)
				if err != nil {
					return err
				}
				data, err := amberpack.DecodePayload(rec.Flags, rec.Ulen, r.rec[amberpack.RecHeaderSize:])
				if err != nil {
					return err
				}
				kids, err := fstree.ChildKeys(k, data)
				if err != nil {
					return err
				}
				for _, kid := range kids {
					if err := want(kid); err != nil {
						return err
					}
				}
			}
			pending--
		case <-w.failed:
			return w.close()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.finish()
	if err := w.close(); err != nil {
		return err
	}
	if _, err := fstree.CheckComplete(root, local.Get, local.Has, c.cfg.Jobs); err != nil {
		return fmt.Errorf("pull: tree incomplete after fetch: %w", err)
	}
	return nil
}

// pullWriteBytes is how many fetched bytes are written to the local store
// at a time, on a goroutine of their own so that writing overlaps fetching.
const pullWriteBytes = 16 << 20

// localWriter writes fetched records to the local store in batches on its
// own goroutine.
type localWriter struct {
	local  *packstore.Store
	jobs   int
	batch  []packstore.Object
	bytes  int
	ch     chan []packstore.Object
	failed chan struct{}
	done   chan struct{}
	err    error
	closed bool
}

func newLocalWriter(local *packstore.Store, jobs int) *localWriter {
	w := &localWriter{local: local, jobs: jobs, ch: make(chan []packstore.Object, 1), failed: make(chan struct{}), done: make(chan struct{})}
	go w.run()
	return w
}

func (w *localWriter) run() {
	defer close(w.done)
	for objs := range w.ch {
		if w.err != nil {
			continue
		}
		_, err := w.local.WriteParallel(func(yield func(packstore.Object, error) bool) {
			for _, o := range objs {
				if !yield(o, nil) {
					return
				}
			}
		}, packstore.WriteOpts{Writers: w.jobs})
		if err != nil {
			w.err = err
			close(w.failed)
		}
	}
}

func (w *localWriter) write(o packstore.Object) error {
	w.batch = append(w.batch, o)
	w.bytes += len(o.Record)
	if w.bytes < pullWriteBytes {
		return nil
	}
	return w.flush()
}

func (w *localWriter) flush() error {
	if len(w.batch) == 0 {
		return nil
	}
	objs := w.batch
	w.batch, w.bytes = nil, 0
	select {
	case w.ch <- objs:
		return nil
	case <-w.failed:
		return w.err
	}
}

// close writes what is left and waits for the writer.
func (w *localWriter) close() error {
	if w.closed {
		return w.err
	}
	w.closed = true
	err := w.flush()
	close(w.ch)
	<-w.done
	if err != nil {
		return err
	}
	return w.err
}

// stop abandons unwritten records and waits for the writer.
func (w *localWriter) stop() {
	if w.closed {
		return
	}
	w.closed = true
	close(w.ch)
	<-w.done
}
