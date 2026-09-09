package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"sync"
	"time"

	"github.com/amber-store/core/amberpack"
	"github.com/amber-store/core/key"
	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
)

// Batch limits (§6.2). Put batches are smaller than the spec's 60 MiB
// target so that a primary holds Conns of them in flight per client
// within one spec-sized batch of memory.
const (
	defaultBatchBytes = 16 << 20
	batchBytes        = 60 << 20 // pull: bytes written to the local store at a time
	batchKeys         = 8192
)

// MissingResult is the outcome of a negotiation.
type MissingResult struct {
	// Lacking maps each primary to the keys it lacks.
	Lacking map[view.NodeID][][32]byte
	// Holders maps each key the primaries hold to the owners that hold it.
	Holders map[[32]byte][]view.NodeID
	// Failed maps each key whose primary could not be asked to the error.
	Failed map[[32]byte]error
}

// Missing groups keys by primary and asks each which it lacks (§6.2 step
// 2). With pin, present keys are pinned at every owner that confirms.
func (c *Cluster) Missing(ctx context.Context, keys [][32]byte, pin bool) (*MissingResult, error) {
	res := &MissingResult{Lacking: map[view.NodeID][][32]byte{}, Holders: map[[32]byte][]view.NodeID{}, Failed: map[[32]byte]error{}}
	byPrimary := map[view.NodeID][][32]byte{}
	for _, k := range keys {
		p, ok := c.Primary(k)
		if !ok {
			res.Failed[k] = errors.New("no owners")
			continue
		}
		byPrimary[p] = append(byPrimary[p], k)
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, c.cfg.Jobs)
	for p, ks := range byPrimary {
		for i := 0; i < len(ks); i += batchKeys {
			end := min(i+batchKeys, len(ks))
			wg.Add(1)
			go func(p view.NodeID, ks [][32]byte) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				resp, err := c.callRetry(ctx, p, &wire.Msg{Type: wire.TMissing, Keys: wire.RawKeys(ks), Pin: pin})
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					for _, k := range ks {
						res.Failed[k] = err
					}
					return
				}
				lacking, _ := wire.Keys32(resp.Keys)
				lack := map[[32]byte]struct{}{}
				for _, k := range lacking {
					lack[k] = struct{}{}
				}
				short := map[[32]byte][]view.NodeID{}
				for _, kh := range resp.Short {
					if len(kh.Key) == 32 {
						short[[32]byte(kh.Key)] = view.IDsOf(kh.Holders)
					}
				}
				for _, k := range ks {
					if _, l := lack[k]; l {
						res.Lacking[p] = append(res.Lacking[p], k)
						continue
					}
					if h, ok := short[k]; ok {
						res.Holders[k] = h
					} else {
						res.Holders[k] = c.WriteSet(k) // held everywhere
					}
				}
			}(p, ks[i:end])
		}
	}
	wg.Wait()
	return res, nil
}

// PutResult is the outcome of an upload.
type PutResult struct {
	Holders  map[[32]byte][]view.NodeID
	Failed   map[[32]byte][]wire.KeyFailure
	Rejected map[[32]byte]string
	Errors   map[view.NodeID]error
}

// RecordSource yields the record bytes of a key.
type RecordSource func(k [32]byte) ([]byte, error)

// Put uploads records to their primaries in byte-balanced batches, in
// parallel across primaries and, per primary, with Conns batches in
// flight (§6.2 steps 3–4, §11.3): while a primary stores and replicates
// one batch the next is already on the wire. The primaries replicate.
func (c *Cluster) Put(ctx context.Context, byPrimary map[view.NodeID][][32]byte, src RecordSource, size RecordSizer, obs PutObserver) *PutResult {
	res := &PutResult{Holders: map[[32]byte][]view.NodeID{}, Failed: map[[32]byte][]wire.KeyFailure{}, Rejected: map[[32]byte]string{}, Errors: map[view.NodeID]error{}}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, c.cfg.Jobs)
	for p, ks := range byPrimary {
		wg.Add(1)
		go func(p view.NodeID, ks [][32]byte) {
			defer wg.Done()
			var pwg sync.WaitGroup
			defer pwg.Wait()
			slots := make(chan struct{}, c.cfg.Conns)
			for _, b := range batches(ks, size, c.cfg.BatchBytes, batchKeys) {
				mu.Lock()
				failed := res.Errors[p] != nil
				mu.Unlock()
				if failed {
					return // the primary's remaining batches are not worth sending
				}
				slots <- struct{}{}
				sem <- struct{}{}
				pwg.Add(1)
				go func(b [][32]byte) {
					defer pwg.Done()
					defer func() { <-sem; <-slots }()
					resp, err := c.putBatch(ctx, p, b, src, size, obs)
					mu.Lock()
					defer mu.Unlock()
					if err != nil {
						if res.Errors[p] == nil {
							res.Errors[p] = err
						}
						return
					}
					res.merge(resp)
				}(b)
			}
		}(p, ks)
	}
	wg.Wait()
	return res
}

// merge folds one batch reply into the result; the caller holds the lock.
func (r *PutResult) merge(resp *wire.Msg) {
	for _, h := range resp.Holders {
		if len(h.Key) == 32 {
			r.Holders[[32]byte(h.Key)] = view.IDsOf(h.Holders)
		}
	}
	for _, f := range resp.Failed {
		if len(f.Key) == 32 {
			r.Failed[[32]byte(f.Key)] = append(r.Failed[[32]byte(f.Key)], f)
		}
	}
	for _, rj := range resp.Rejected {
		if len(rj.Key) == 32 {
			r.Rejected[[32]byte(rj.Key)] = rj.Reason
		}
	}
}

// putBatch streams one batch to a primary.
func (c *Cluster) putBatch(ctx context.Context, p view.NodeID, keys [][32]byte, src RecordSource, size RecordSizer, obs PutObserver) (*wire.Msg, error) {
	var last error
	for attempt := 0; attempt < 4; attempt++ {
		resp, err := c.putOnce(ctx, p, keys, src, size, obs)
		if err == nil {
			return resp, nil
		}
		last = err
		if wire.IsCode(err, wire.CodeStaleView) {
			c.log.Warn("upload retry", "node", view.ShortID(p), "reason", "stale view", "attempt", attempt+1)
			c.handleErr(p, err)
			continue
		}
		if wire.IsCode(err, wire.CodeBusy) {
			wait := time.Second
			if we, ok := wire.AsError(err); ok && we.RetryAfter > 0 {
				wait = we.RetryAfter
			}
			c.log.Warn("upload retry", "node", view.ShortID(p), "reason", "busy", "wait", wait, "attempt", attempt+1)
			time.Sleep(wait)
			continue
		}
		c.log.Warn("upload failed", "node", view.ShortID(p), "objects", len(keys), "err", err)
		return nil, err
	}
	return nil, last
}

func (c *Cluster) putOnce(ctx context.Context, p view.NodeID, keys [][32]byte, src RecordSource, size RecordSizer, obs PutObserver) (*wire.Msg, error) {
	cctx, cancel := context.WithTimeout(ctx, 10*c.cfg.RequestTimeout)
	defer cancel()
	var total int64
	for _, k := range keys {
		total += int64(size(k))
	}
	if obs.Start != nil {
		obs.Start(p)
	}
	flushed := false
	if obs.Done != nil {
		defer func() { obs.Done(p, flushed) }()
	}
	began := time.Now()
	s, err := c.pool.Open(cctx, p, wire.ALPNClient)
	if err != nil {
		c.handleErr(p, err)
		return nil, err
	}
	defer wire.CloseStream(s)
	c.log.Info("uploading", append([]any{"node", view.ShortID(p), "objects", len(keys), "bytes", total}, c.pathAttrs(p)...)...)
	if err := wire.WriteMsg(s, c.stamp(&wire.Msg{Type: wire.TPut})); err != nil {
		return nil, err
	}
	err = wire.SendPackRecords(s, func(yield func([]byte, error) bool) {
		for _, k := range keys {
			rec, err := src(k)
			if err != nil {
				continue
			}
			if !yield(rec, nil) {
				return
			}
			if obs.Sent != nil {
				obs.Sent(p, len(rec))
			}
		}
	})
	if err != nil {
		return nil, err
	}
	_ = s.CloseWrite()
	flushed = true
	if obs.Flushed != nil {
		obs.Flushed(p)
	}
	c.log.Info("batch sent, waiting for the node to store and replicate it", "node", view.ShortID(p), "objects", len(keys), "bytes", total)
	resp, err := wire.Expect(s, wire.TPutResult)
	if err != nil {
		c.handleErr(p, err)
		return nil, err
	}
	c.ok(p)
	took := time.Since(began)
	c.log.Info("uploaded", "node", view.ShortID(p), "objects", len(keys), "bytes", total, "took", took.Round(time.Millisecond), "rate", Rate(total, took))
	return resp, nil
}

// Placed reports whether the holders satisfy the ack policy (§6.2): at
// least min_replicas owners of nodes and, during a transition, of
// pending.nodes.
func (c *Cluster) Placed(k [32]byte, holders []view.NodeID) bool {
	pl := c.Placement()
	v := pl.View()
	minR := int(v.MinReplicas)
	count := func(owners []view.NodeID) int {
		n := 0
		for _, o := range owners {
			for _, h := range holders {
				if h == o {
					n++
					break
				}
			}
		}
		return n
	}
	if count(pl.Owners(k)) < min(minR, len(pl.Owners(k))) {
		return false
	}
	if po := pl.PendingOwners(k); po != nil && count(po) < min(minR, len(po)) {
		return false
	}
	return true
}

// GetResult is one fetched record.
type GetResult struct {
	Key    [32]byte
	Record []byte
}

// Get fetches records, grouping keys by preferred owner and re-asking
// down the read order (§6.3). It yields every record found; keys not
// found anywhere are returned in missing.
func (c *Cluster) Get(ctx context.Context, keys [][32]byte) (iter.Seq2[GetResult, error], func() [][32]byte) {
	var missing [][32]byte
	var mmu sync.Mutex
	seq := func(yield func(GetResult, error) bool) {
		remaining := map[[32]byte]int{} // key → index into its read order
		for _, k := range keys {
			remaining[k] = 0
		}
		refreshed := false
		for len(remaining) > 0 {
			byNode := map[view.NodeID][][32]byte{}
			var exhausted [][32]byte
			for k, idx := range remaining {
				order := c.ReadOrder(k)
				if idx >= len(order) {
					exhausted = append(exhausted, k)
					continue
				}
				byNode[order[idx]] = append(byNode[order[idx]], k)
			}
			if len(exhausted) > 0 {
				if !refreshed {
					// A miss at every owner is the signature of a stale view.
					refreshed = true
					_ = c.RefreshView(ctx)
					for _, k := range exhausted {
						remaining[k] = 0
					}
					continue
				}
				mmu.Lock()
				missing = append(missing, exhausted...)
				mmu.Unlock()
				for _, k := range exhausted {
					delete(remaining, k)
				}
				if len(byNode) == 0 {
					break
				}
			}
			type fetched struct {
				recs map[[32]byte][]byte
				keys [][32]byte
				err  error
			}
			results := make(chan fetched, len(byNode))
			var wg sync.WaitGroup
			sem := make(chan struct{}, c.cfg.Jobs)
			for id, ks := range byNode {
				wg.Add(1)
				go func(id view.NodeID, ks [][32]byte) {
					defer wg.Done()
					sem <- struct{}{}
					defer func() { <-sem }()
					recs, err := c.getFrom(ctx, id, ks)
					results <- fetched{recs, ks, err}
				}(id, ks)
			}
			wg.Wait()
			close(results)
			for f := range results {
				for _, k := range f.keys {
					if rec, ok := f.recs[k]; ok {
						delete(remaining, k)
						if !yield(GetResult{Key: k, Record: rec}, nil) {
							return
						}
					} else {
						remaining[k]++
					}
				}
			}
			if ctx.Err() != nil {
				yield(GetResult{}, ctx.Err())
				return
			}
		}
	}
	return seq, func() [][32]byte { mmu.Lock(); defer mmu.Unlock(); return missing }
}

// getFrom fetches a batch from one node, verifying every record.
func (c *Cluster) getFrom(ctx context.Context, id view.NodeID, keys [][32]byte) (map[[32]byte][]byte, error) {
	out := map[[32]byte][]byte{}
	for i := 0; i < len(keys); i += batchKeys {
		end := min(i+batchKeys, len(keys))
		if err := c.getBatch(ctx, id, keys[i:end], out); err != nil {
			return out, err
		}
	}
	return out, nil
}

func (c *Cluster) getBatch(ctx context.Context, id view.NodeID, keys [][32]byte, out map[[32]byte][]byte) error {
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
		out[k] = rec
	}
	_, _ = io.Copy(io.Discard, pr)
	c.ok(id)
	return nil
}

// VerifyRecord parses and verifies one wire record against its key.
func VerifyRecord(raw amberpack.RawRecord) ([32]byte, []byte, error) {
	k := raw.Key
	if err := k.Validate(); err != nil {
		return [32]byte{}, nil, err
	}
	payload, err := amberpack.DecodePayload(raw.Flags, raw.Ulen, raw.Bytes[amberpack.RecHeaderSize:])
	if err != nil {
		return [32]byte{}, nil, err
	}
	want, err := key.New(k.Type(), k.Length(), payload)
	if err != nil {
		return [32]byte{}, nil, err
	}
	if want != k {
		return [32]byte{}, nil, fmt.Errorf("payload hashes to %s, not %s", want, k)
	}
	return [32]byte(k), append([]byte{}, raw.Bytes...), nil
}
