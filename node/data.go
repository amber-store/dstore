package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/amber-store/core/amberpack"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/dstore/transport"
	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
)

// ---- missing ----

// handleMissing answers which keys this node lacks and, on the client
// ALPN, which owners hold each key it has (§6.2, §6.3). pin makes it an
// atomic has-and-pin (§9.5).
func (n *Node) handleMissing(ctx context.Context, s transport.Stream, m *wire.Msg, client bool) error {
	n.stats.missing.Add(1)
	if len(m.Keys) > wire.MaxKeys {
		return wire.WriteErr(s, wire.CodeBadRequest, "too many keys")
	}
	keys, err := wire.Keys32(m.Keys)
	if err != nil {
		return wire.WriteErr(s, wire.CodeBadRequest, err.Error())
	}
	lacking, present, err := n.localMissing(keys, m.Pin)
	if err != nil {
		return wire.WriteErr(s, wire.CodeInternal, err.Error())
	}
	reply := n.stampReply(&wire.Msg{Type: wire.TMissingReply, Keys: wire.RawKeys(lacking)})
	if client && len(present) > 0 {
		reply.Short = n.negotiateHolders(ctx, present, m.Pin)
	}
	return wire.WriteMsg(s, reply)
}

// localMissing checks presence under the sweep lock shared, pins present
// keys when asked, and syncs once so every reported presence is durable.
func (n *Node) localMissing(keys [][32]byte, pin bool) (lacking, present [][32]byte, err error) {
	n.sweepMu.RLock()
	defer n.sweepMu.RUnlock()
	ks := make([]key.Key, len(keys))
	for i, k := range keys {
		ks[i] = key.Key(k)
	}
	miss, err := n.store.Missing(ks)
	if err != nil {
		return nil, nil, err
	}
	missSet := make(map[[32]byte]struct{}, len(miss))
	for _, k := range miss {
		missSet[[32]byte(k)] = struct{}{}
	}
	for _, k := range keys {
		if _, ok := missSet[k]; ok || n.isCorrupt(k) {
			lacking = append(lacking, k)
		} else {
			present = append(present, k)
		}
	}
	if len(present) > 0 {
		if err := n.store.Sync(); err != nil {
			return nil, nil, err
		}
		if pin {
			if err := n.pinKeys(present); err != nil {
				return nil, nil, err
			}
			n.rec.pinned(present)
		}
	}
	return lacking, present, nil
}

// negotiateHolders asks each key's other owners whether they hold it and
// reports the keys held by fewer than R owners with their holders; keys
// short at an owner are queued for a forward (§6.2).
func (n *Node) negotiateHolders(ctx context.Context, present [][32]byte, pin bool) []wire.KeyHolders {
	pl := n.Placement()
	byOwner := map[view.NodeID][][32]byte{}
	for _, k := range present {
		for _, o := range pl.WriteSet(k) {
			if o != n.id {
				byOwner[o] = append(byOwner[o], k)
			}
		}
	}
	holders := map[[32]byte][]view.NodeID{}
	var mu sync.Mutex
	for _, k := range present {
		holders[k] = []view.NodeID{n.id}
	}
	var wg sync.WaitGroup
	for o, keys := range byOwner {
		wg.Add(1)
		go func(o view.NodeID, keys [][32]byte) {
			defer wg.Done()
			lacking, err := n.remoteMissing(ctx, o, keys, pin)
			if err != nil {
				mu.Lock()
				for _, k := range keys {
					holders[k] = append(holders[k], view.NodeID{}) // placeholder: unknown
				}
				mu.Unlock()
				return
			}
			lack := map[[32]byte]struct{}{}
			for _, k := range lacking {
				lack[k] = struct{}{}
			}
			mu.Lock()
			var short [][32]byte
			for _, k := range keys {
				if _, ok := lack[k]; ok {
					short = append(short, k)
				} else {
					holders[k] = append(holders[k], o)
				}
			}
			mu.Unlock()
			if len(short) > 0 {
				n.rec.scheduleForward(o, short)
			}
		}(o, keys)
	}
	wg.Wait()
	var out []wire.KeyHolders
	for _, k := range present {
		var ids [][]byte
		for _, h := range holders[k] {
			if h != (view.NodeID{}) {
				ids = append(ids, append([]byte{}, h[:]...))
			}
		}
		if len(ids) < len(pl.WriteSet(k)) {
			out = append(out, wire.KeyHolders{Key: k[:], Holders: ids})
		}
	}
	return out
}

// remoteMissing runs a plain missing on a peer over the cluster ALPN.
func (n *Node) remoteMissing(ctx context.Context, to view.NodeID, keys [][32]byte, pin bool) ([][32]byte, error) {
	var lacking [][32]byte
	for i := 0; i < len(keys); i += wire.MaxKeys {
		end := min(i+wire.MaxKeys, len(keys))
		req := n.stampReq(&wire.Msg{Type: wire.TMissing, Keys: wire.RawKeys(keys[i:end]), Pin: pin})
		cctx, cancel := context.WithTimeout(ctx, n.cfg.ForwardTimeout)
		resp, err := n.pool.Call(cctx, to, wire.ALPNCluster, req)
		cancel()
		if err != nil {
			if wire.IsCode(err, wire.CodeStaleView) {
				if we, ok := wire.AsError(err); ok && len(we.View) > 0 {
					if v, derr := view.Decode(we.View); derr == nil {
						n.adopt(v, "stale-reply")
					}
				}
			}
			n.markUnreachable(to, err)
			return nil, err
		}
		n.markReachable(to)
		ks, err := wire.Keys32(resp.Keys)
		if err != nil {
			return nil, err
		}
		lacking = append(lacking, ks...)
	}
	return lacking, nil
}

// stampReq stamps a cluster request with this node's view coordinates.
func (n *Node) stampReq(m *wire.Msg) *wire.Msg {
	if v := n.View(); v != nil {
		m.ClusterID = v.ClusterID
		m.Incarnation = v.Incarnation
		m.Epoch = v.Epoch
	}
	return m
}

// ---- get ----

func (n *Node) handleGet(ctx context.Context, s transport.Stream, m *wire.Msg) error {
	n.stats.gets.Add(1)
	if len(m.Keys) > wire.MaxKeys {
		return wire.WriteErr(s, wire.CodeBadRequest, "too many keys")
	}
	keys, err := wire.Keys32(m.Keys)
	if err != nil {
		return wire.WriteErr(s, wire.CodeBadRequest, err.Error())
	}
	var absent [][]byte
	var present []key.Key
	for _, k := range keys {
		kk := key.Key(k)
		has, err := n.store.Has(kk)
		if err != nil {
			return wire.WriteErr(s, wire.CodeInternal, err.Error())
		}
		if !has || n.isCorrupt(k) {
			absent = append(absent, k[:])
		} else {
			present = append(present, kk)
		}
	}
	if err := wire.WriteMsg(s, n.stampReply(&wire.Msg{Type: wire.TAbsent, Keys: absent})); err != nil {
		return err
	}
	n.store.SortByLocation(present)
	var sent uint64
	err = wire.SendPackRecords(s, func(yield func([]byte, error) bool) {
		for _, k := range present {
			rec, err := n.store.GetRecord(k)
			if err != nil {
				if errors.Is(err, packstore.ErrNotFound) {
					continue
				}
				if errors.Is(err, packstore.ErrCorrupt) || errors.Is(err, packstore.ErrVerify) {
					n.markCorrupt([32]byte(k))
					n.rec.corruptFound([32]byte(k))
					continue
				}
				yield(nil, err)
				return
			}
			sent += uint64(len(rec))
			if !yield(rec, nil) {
				return
			}
		}
	})
	n.stats.bytesOut.Add(sent)
	return err
}

// ---- put ----

// putBatch is one verified put batch held in memory for forwarding.
type putBatch struct {
	keys    [][32]byte
	records map[[32]byte][]byte
}

// verifyRecord parses and verifies one wire record against its key.
func verifyRecord(raw amberpack.RawRecord) ([32]byte, []byte, error) {
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
		return [32]byte{}, nil, fmt.Errorf("payload hashes to %s", want)
	}
	switch k.Type() {
	case key.Blob, key.XattrSet:
		if k.Length() != uint64(len(payload)) {
			return [32]byte{}, nil, errors.New("length field mismatch")
		}
	}
	return [32]byte(k), append([]byte{}, raw.Bytes...), nil
}

// handlePut stores a batch of records it owns and, on the client ALPN,
// replicates them to the key's other owners (§6.2).
func (n *Node) handlePut(ctx context.Context, s transport.Stream, m *wire.Msg, client bool, from view.NodeID) error {
	n.stats.puts.Add(1)
	if err := n.checkEpoch(m); err != nil {
		if errors.Is(err, errStale) {
			return n.writeStale(s)
		}
		return wire.WriteErr(s, wire.CodeBadRequest, err.Error())
	}
	if !n.writable.Load() {
		return wire.WriteErr(s, wire.CodeNoSpace, "node below its free-space reserve")
	}
	// Admission: a bounded number of concurrent write streams; excess is
	// simply not read until a slot frees.
	select {
	case n.writeSlots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-n.writeSlots }()

	pl := n.Placement()
	reader := amberpack.NewReader(wire.NewPackReader(s))
	batch := &putBatch{records: map[[32]byte][]byte{}}
	var rejected []wire.KeyReject
	var total int
	for raw, err := range reader.Records() {
		if err != nil {
			return wire.WriteErr(s, wire.CodeBadRequest, "pack: "+err.Error())
		}
		total += len(raw.Bytes)
		if total > wire.MaxPutBatch {
			return wire.WriteErr(s, wire.CodeBadRequest, "batch over 64 MiB")
		}
		k, rec, err := verifyRecord(raw)
		if err != nil {
			rejected = append(rejected, wire.KeyReject{Key: append([]byte{}, raw.Key[:]...), Reason: "verify: " + err.Error()})
			continue
		}
		if !pl.InWriteSet(k, n.id) && !n.isFormerHoldingBack() {
			rejected = append(rejected, wire.KeyReject{Key: k[:], Reason: wire.CodeNotOwner})
			continue
		}
		if _, dup := batch.records[k]; dup {
			continue
		}
		batch.keys = append(batch.keys, k)
		batch.records[k] = rec
	}
	n.stats.bytesIn.Add(uint64(total))

	stored, dedup, err := n.storeBatch(batch)
	if err != nil {
		if isNoSpace(err) {
			n.writable.Store(false)
			return wire.WriteErr(s, wire.CodeNoSpace, err.Error())
		}
		return wire.WriteErr(s, wire.CodeInternal, err.Error())
	}
	_ = stored
	_ = dedup

	reply := n.stampReply(&wire.Msg{Type: wire.TPutResult, Rejected: rejected})
	holders := map[[32]byte][][]byte{}
	for _, k := range batch.keys {
		holders[k] = [][]byte{n.id[:]}
	}
	if client {
		failed := n.forwardBatch(ctx, pl, batch, holders)
		reply.Failed = failed
		n.stats.forwarded.Add(uint64(len(batch.keys)))
	}
	for _, k := range batch.keys {
		reply.Holders = append(reply.Holders, wire.KeyHolders{Key: k[:], Holders: holders[k]})
	}
	return wire.WriteMsg(s, reply)
}

// isFormerHoldingBack reports whether this node is an ex-member; it then
// accepts nothing it does not own, which is everything.
func (n *Node) isFormerHoldingBack() bool { return false }

func isNoSpace(err error) bool {
	return err != nil && (bytes.Contains([]byte(err.Error()), []byte("no space")) || bytes.Contains([]byte(err.Error()), []byte("ENOSPC")))
}

// storeBatch appends verified records durably, pinning dedup hits found in
// the sweepable region and recording the new keys for their first audit.
func (n *Node) storeBatch(b *putBatch) (stored, dedup int, err error) {
	if len(b.keys) == 0 {
		return 0, 0, nil
	}
	n.sweepMu.RLock()
	defer n.sweepMu.RUnlock()
	var hits [][32]byte
	var fresh [][32]byte
	for _, k := range b.keys {
		has, err := n.store.Has(key.Key(k))
		if err != nil {
			return 0, 0, err
		}
		if has && !n.isCorrupt(k) {
			hits = append(hits, k)
		} else {
			fresh = append(fresh, k)
		}
	}
	if len(fresh) > 0 {
		seq := func(yield func(packstore.Object, error) bool) {
			for _, k := range fresh {
				if !yield(packstore.Object{Key: key.Key(k), Record: b.records[k]}, nil) {
					return
				}
			}
		}
		if _, err := n.store.WriteParallel(seq, packstore.WriteOpts{Writers: 1}); err != nil {
			return 0, 0, err
		}
		n.clearCorruptKeys(fresh)
		n.noteRecent(fresh)
	}
	if len(hits) > 0 {
		if err := n.pinKeys(hits); err != nil {
			return 0, 0, err
		}
		n.rec.pinned(hits)
	}
	return len(fresh), len(hits), nil
}

// noteRecent remembers keys for the first audit (§8.4).
func (n *Node) noteRecent(keys [][32]byte) {
	now := time.Now()
	n.recentMu.Lock()
	for _, k := range keys {
		n.recent[k] = now
	}
	n.recentMu.Unlock()
}

// forwardBatch replicates a batch to each key's other owners and returns
// the failures. holders is extended with every owner that confirmed.
func (n *Node) forwardBatch(ctx context.Context, pl *view.Placement, b *putBatch, holders map[[32]byte][][]byte) []wire.KeyFailure {
	byOwner := map[view.NodeID][][32]byte{}
	for _, k := range b.keys {
		for _, o := range pl.WriteSet(k) {
			if o != n.id {
				byOwner[o] = append(byOwner[o], k)
			}
		}
	}
	var mu sync.Mutex
	var failed []wire.KeyFailure
	var wg sync.WaitGroup
	for o, keys := range byOwner {
		wg.Add(1)
		go func(o view.NodeID, keys [][32]byte) {
			defer wg.Done()
			fctx, cancel := context.WithTimeout(ctx, n.cfg.ForwardTimeout)
			defer cancel()
			ok, errs := n.forwardTo(fctx, o, keys, func(k [32]byte) []byte { return b.records[k] })
			mu.Lock()
			for _, k := range ok {
				holders[k] = append(holders[k], o[:])
			}
			for k, reason := range errs {
				failed = append(failed, wire.KeyFailure{Key: append([]byte{}, k[:]...), Node: o[:], Reason: reason, RetryAfter: int64(500 + rand.IntN(1500))})
			}
			mu.Unlock()
			if len(errs) > 0 {
				short := make([][32]byte, 0, len(errs))
				for k := range errs {
					short = append(short, k)
				}
				n.rec.scheduleForward(o, short)
			}
		}(o, keys)
	}
	wg.Wait()
	return failed
}

// forwardTo negotiates keys with one owner and sends what it lacks. It
// returns the keys the owner confirmed and, per failed key, a reason.
func (n *Node) forwardTo(ctx context.Context, to view.NodeID, keys [][32]byte, record func([32]byte) []byte) (ok [][32]byte, failed map[[32]byte]string) {
	failed = map[[32]byte]string{}
	fail := func(reason string, ks [][32]byte) {
		for _, k := range ks {
			failed[k] = reason
		}
	}
	lacking, err := n.remoteMissing(ctx, to, keys, false)
	if err != nil {
		fail(reasonOf(err), keys)
		return nil, failed
	}
	lack := map[[32]byte]struct{}{}
	for _, k := range lacking {
		lack[k] = struct{}{}
	}
	for _, k := range keys {
		if _, l := lack[k]; !l {
			ok = append(ok, k)
		}
	}
	if len(lacking) == 0 {
		return ok, failed
	}
	res, err := n.putTo(ctx, to, func(yield func([]byte, error) bool) {
		for _, k := range lacking {
			rec := record(k)
			if rec == nil {
				continue
			}
			if !yield(rec, nil) {
				return
			}
		}
	})
	if err != nil {
		fail(reasonOf(err), lacking)
		return ok, failed
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
	for _, k := range lacking {
		switch {
		case confirmed[k]:
			ok = append(ok, k)
		case rej[k] != "":
			failed[k] = "rejected: " + rej[k]
			if bytes.HasPrefix([]byte(rej[k]), []byte("verify")) {
				n.rec.verifyLocal(k)
			}
		default:
			failed[k] = "unconfirmed"
		}
	}
	return ok, failed
}

// putTo streams records to a peer's put over the cluster ALPN.
func (n *Node) putTo(ctx context.Context, to view.NodeID, recs iter.Seq2[[]byte, error]) (*wire.Msg, error) {
	s, err := n.pool.Open(ctx, to, wire.ALPNCluster)
	if err != nil {
		n.markUnreachable(to, err)
		return nil, err
	}
	defer wire.CloseStream(s)
	if err := wire.WriteMsg(s, n.stampReq(&wire.Msg{Type: wire.TPut})); err != nil {
		return nil, err
	}
	if err := wire.SendPackRecords(s, recs); err != nil {
		return nil, err
	}
	_ = s.CloseWrite()
	type result struct {
		m   *wire.Msg
		err error
	}
	done := make(chan result, 1)
	go func() {
		m, err := wire.Expect(s, wire.TPutResult)
		done <- result{m, err}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			if wire.IsCode(r.err, wire.CodeStaleView) {
				if we, ok := wire.AsError(r.err); ok && len(we.View) > 0 {
					if v, derr := view.Decode(we.View); derr == nil {
						n.adopt(v, "stale-reply")
					}
				}
			}
			return nil, r.err
		}
		n.markReachable(to)
		return r.m, nil
	case <-ctx.Done():
		s.CancelRead(0)
		return nil, ctx.Err()
	}
}

func reasonOf(err error) string {
	if we, ok := wire.AsError(err); ok {
		return we.Code
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return wire.CodeTimeout
	}
	return "unreachable"
}

// fetchRecord returns a record from this node or, failing that, from the
// key's owners in read order (cluster ALPN).
func (n *Node) fetchRecord(ctx context.Context, k [32]byte) ([]byte, error) {
	if rec, err := n.store.GetRecord(key.Key(k)); err == nil && !n.isCorrupt(k) {
		return rec, nil
	}
	pl := n.Placement()
	for _, o := range pl.ReadOrder(k) {
		if o == n.id {
			continue
		}
		recs, err := n.getFrom(ctx, o, [][32]byte{k})
		if err != nil {
			continue
		}
		if rec, ok := recs[k]; ok {
			return rec, nil
		}
	}
	return nil, packstore.ErrNotFound
}

// getFrom fetches records from one peer.
func (n *Node) getFrom(ctx context.Context, to view.NodeID, keys [][32]byte) (map[[32]byte][]byte, error) {
	cctx, cancel := context.WithTimeout(ctx, n.cfg.ForwardTimeout)
	defer cancel()
	s, err := n.pool.Open(cctx, to, wire.ALPNCluster)
	if err != nil {
		n.markUnreachable(to, err)
		return nil, err
	}
	defer wire.CloseStream(s)
	if err := wire.WriteMsg(s, n.stampReq(&wire.Msg{Type: wire.TGet, Keys: wire.RawKeys(keys)})); err != nil {
		return nil, err
	}
	_ = s.CloseWrite()
	if _, err := wire.Expect(s, wire.TAbsent); err != nil {
		return nil, err
	}
	out := map[[32]byte][]byte{}
	pr := wire.NewPackReader(s)
	reader := amberpack.NewReader(pr)
	for raw, err := range reader.Records() {
		if err != nil {
			return out, err
		}
		k, rec, err := verifyRecord(raw)
		if err != nil {
			continue
		}
		out[k] = rec
	}
	_, _ = io.Copy(io.Discard, pr)
	n.markReachable(to)
	return out, nil
}

// getData returns an object's decoded payload from anywhere in the cluster.
func (n *Node) getData(ctx context.Context, k [32]byte) ([]byte, error) {
	if data, err := n.store.Get(key.Key(k)); err == nil && !n.isCorrupt(k) {
		return data, nil
	}
	rec, err := n.fetchRecord(ctx, k)
	if err != nil {
		return nil, err
	}
	r, err := amberpack.ParseRecord(rec)
	if err != nil {
		return nil, err
	}
	return amberpack.DecodePayload(r.Flags, r.Ulen, rec[amberpack.RecHeaderSize:])
}
