package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/reference"
	"github.com/amber-store/dstore/catalog"
	"github.com/amber-store/dstore/transport"
	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
)

func keyOfRecord(rec []byte) []byte {
	r, err := reference.Decode(rec)
	if err != nil {
		return nil
	}
	return r.Key
}

func condOf(m *wire.Msg) catalog.Cond {
	c := catalog.Cond{Force: m.Force}
	if m.HasExpected {
		if m.ExpectedVersion != nil || m.ExpectedOld == nil && m.Version == nil {
			c.Versioned = true
			c.ExpectedVersion = m.ExpectedVersion
		}
		if m.ExpectedOld != nil {
			c.Keyed = true
			c.ExpectedOld = m.ExpectedOld
			c.Versioned = false
		}
	}
	return c
}

func (n *Node) writeCASMismatch(s transport.Stream, cm *catalog.CASMismatch) error {
	reply := n.stampReply(&wire.Msg{Type: wire.TCASMismatch, Version: cm.Version, HasCurrent: cm.HasCurrent})
	if cm.Current != nil {
		reply.Record = cm.Current.Record
		reply.Current = keyOfRecord(cm.Current.Record)
	}
	return wire.WriteMsg(s, reply)
}

func (n *Node) catalogErr(s transport.Stream, err error) error {
	var cm *catalog.CASMismatch
	switch {
	case errors.As(err, &cm):
		return n.writeCASMismatch(s, cm)
	case errors.Is(err, catalog.ErrUnknownRef):
		return wire.WriteErr(s, wire.CodeUnknownRef, "no such reference")
	case errors.Is(err, context.DeadlineExceeded):
		return wire.WriteErr(s, wire.CodeTimeout, err.Error())
	}
	if wire.IsCode(err, wire.CodeExpired) {
		return wire.WriteErr(s, wire.CodeTimeout, "reference commit expired")
	}
	return wire.WriteErr(s, wire.CodeUnavailable, err.Error())
}

func (n *Node) handleRefGet(ctx context.Context, s transport.Stream, m *wire.Msg) error {
	if err := reference.ValidateName(m.Name); err != nil {
		return wire.WriteErr(s, wire.CodeBadRequest, err.Error())
	}
	rv, err := n.cat.RefGet(ctx, m.Name)
	if err != nil {
		return n.catalogErr(s, err)
	}
	return wire.WriteMsg(s, n.stampReply(&wire.Msg{Type: wire.TRef, Record: rv.Record, Version: rv.Version}))
}

func (n *Node) handleRefList(ctx context.Context, s transport.Stream, m *wire.Msg) error {
	entries, next, err := n.cat.RefList(ctx, string(m.Prefix), string(m.After), 20000)
	if err != nil {
		return n.catalogErr(s, err)
	}
	reply := n.stampReply(&wire.Msg{Type: wire.TRefs})
	size := 0
	for _, e := range entries {
		r, err := reference.Decode(e.Value.Record)
		if err != nil {
			continue
		}
		reply.Refs = append(reply.Refs, wire.RefInfo{Name: e.Name, Key: r.Key, Version: e.Value.Version, CreatedAt: r.CreatedAt, User: r.User})
		size += len(e.Name) + 32 + 40 + len(r.User) + 16
		if size > wire.MaxPageBytes {
			next = e.Name
			break
		}
	}
	if next != "" {
		reply.Next = []byte(next)
	}
	return wire.WriteMsg(s, reply)
}

func (n *Node) handleRefDelete(ctx context.Context, s transport.Stream, m *wire.Msg) error {
	if err := reference.ValidateName(m.Name); err != nil {
		return wire.WriteErr(s, wire.CodeBadRequest, err.Error())
	}
	b, err := n.cat.RefDelete(ctx, m.Name, condOf(m), keyOfRecord)
	if err != nil {
		return n.catalogErr(s, err)
	}
	if !b.IsZero() {
		go func() {
			pctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
			defer cancel()
			time.Sleep(200 * time.Millisecond)
			n.cat.TryPurge(pctx, m.Name, b)
		}()
	}
	return wire.WriteMsg(s, n.stampReply(&wire.Msg{Type: wire.TOK}))
}

// handleRefPut coordinates a reference write (§7).
func (n *Node) handleRefPut(ctx context.Context, s transport.Stream, m *wire.Msg) error {
	n.stats.refPuts.Add(1)
	start := time.Now()
	notAfter := start.Add(n.cfg.PutTTL).UnixNano()
	pctx, cancel := context.WithDeadline(ctx, start.Add(n.cfg.PutTTL))
	defer cancel()

	rec, err := reference.Decode(m.Record)
	if err != nil {
		return wire.WriteErr(s, wire.CodeBadRequest, "record: "+err.Error())
	}
	if len(rec.Key) != 32 {
		return wire.WriteErr(s, wire.CodeBadRequest, "record key")
	}
	if err := reference.ValidateName(rec.Name); err != nil {
		return wire.WriteErr(s, wire.CodeBadRequest, "name: "+err.Error())
	}
	if m.Name != "" && m.Name != rec.Name {
		return wire.WriteErr(s, wire.CodeBadRequest, "frame name differs from the record's")
	}
	name := rec.Name
	root := [32]byte(rec.Key)
	if _, err := key.Parse(rec.Key); err != nil {
		return wire.WriteErr(s, wire.CodeBadRequest, "root key: "+err.Error())
	}
	if err := n.checkEpoch(m); err != nil && errors.Is(err, errStale) {
		return n.writeStale(s)
	}

	missing, shortfall, err := n.walkComplete(pctx, root)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return wire.WriteErr(s, wire.CodeTimeout, "completeness walk exceeded put_ttl")
		}
		return wire.WriteErr(s, wire.CodeUnavailable, "completeness walk: "+err.Error())
	}
	if len(missing) > 0 {
		sample := missing
		if len(sample) > 64 {
			sample = sample[:64]
		}
		return wire.WriteMsg(s, n.stampReply(&wire.Msg{Type: wire.TIncomplete, Keys: wire.RawKeys(sample), Shortfall: shortfall}))
	}
	version, err := n.cat.RefPut(pctx, name, m.Record, condOf(m), notAfter, keyOfRecord)
	if err != nil {
		return n.catalogErr(s, err)
	}
	n.log.Info("reference written", "name", name, "took", time.Since(start))
	return wire.WriteMsg(s, n.stampReply(&wire.Msg{Type: wire.TOK, Key: rec.Key, Version: version}))
}

// walkComplete walks the tree under root top-down, fetching tree objects
// and has-and-pinning every reachable key at its owners. It returns the
// keys held by fewer than min_replicas owners (a sample) and the total.
func (n *Node) walkComplete(ctx context.Context, root [32]byte) (missing [][32]byte, shortfall int, err error) {
	pl := n.Placement()
	v := pl.View()
	minR := int(v.MinReplicas)

	frontier := [][32]byte{root}
	seen := map[[32]byte]struct{}{}
	pending := map[[32]byte]struct{}{} // keys awaiting negotiation
	var toCheck [][32]byte
	cutoff := time.Now().Add(-n.cfg.PutTTL / 2)

	flush := func() error {
		if len(toCheck) == 0 {
			return nil
		}
		short, err := n.negotiateComplete(ctx, pl, toCheck, minR)
		if err != nil {
			return err
		}
		for _, k := range short {
			if len(missing) < 1024 {
				missing = append(missing, k)
			}
			shortfall++
		}
		toCheck = toCheck[:0]
		return nil
	}

	for len(frontier) > 0 {
		var next [][32]byte
		var interior [][32]byte
		for _, k := range frontier {
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			pending[k] = struct{}{}
			toCheck = append(toCheck, k)
			kk := key.Key(k)
			if kk.Type() == key.Blob || kk.Type() == key.XattrSet {
				continue
			}
			if n.completeSince(k).After(cutoff) {
				continue // verified subtree, unexpired
			}
			interior = append(interior, k)
		}
		// Fetch interior objects in parallel and expand.
		type res struct {
			k    [32]byte
			kids []key.Key
			err  error
		}
		results := make([]res, len(interior))
		var wg sync.WaitGroup
		sem := make(chan struct{}, 16)
		for i, k := range interior {
			wg.Add(1)
			go func(i int, k [32]byte) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				data, err := n.getData(ctx, k)
				if err != nil {
					results[i] = res{k: k, err: err}
					return
				}
				kids, err := fstree.ChildKeys(key.Key(k), data)
				results[i] = res{k: k, kids: kids, err: err}
			}(i, k)
		}
		wg.Wait()
		for _, r := range results {
			if r.err != nil {
				// Absent everywhere: incomplete; the negotiation reports it.
				continue
			}
			for _, c := range r.kids {
				next = append(next, [32]byte(c))
			}
		}
		if len(toCheck) >= wire.MaxKeys {
			if err := flush(); err != nil {
				return nil, 0, err
			}
		}
		frontier = next
	}
	if err := flush(); err != nil {
		return nil, 0, err
	}
	if shortfall == 0 {
		now := time.Now()
		n.completeMu.Lock()
		for k := range seen {
			if key.Key(k).Type() != key.Blob && key.Key(k).Type() != key.XattrSet {
				n.completeCache[k] = now
			}
		}
		if len(n.completeCache) > 1<<20 {
			n.completeCache = map[[32]byte]time.Time{}
		}
		n.completeMu.Unlock()
	}
	return missing, shortfall, nil
}

func (n *Node) completeSince(k [32]byte) time.Time {
	n.completeMu.Lock()
	defer n.completeMu.Unlock()
	return n.completeCache[k]
}

// negotiateComplete has-and-pins keys at every owner and returns the keys
// present at fewer than minR owners of nodes (and of pending.nodes).
func (n *Node) negotiateComplete(ctx context.Context, pl *view.Placement, keys [][32]byte, minR int) ([][32]byte, error) {
	byOwner := map[view.NodeID][][32]byte{}
	for _, k := range keys {
		for _, o := range pl.WriteSet(k) {
			byOwner[o] = append(byOwner[o], k)
		}
	}
	type outcome struct {
		lacking map[[32]byte]struct{}
		err     error
	}
	results := map[view.NodeID]*outcome{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for o, ks := range byOwner {
		wg.Add(1)
		go func(o view.NodeID, ks [][32]byte) {
			defer wg.Done()
			oc := &outcome{lacking: map[[32]byte]struct{}{}}
			var lacking [][32]byte
			var err error
			if o == n.id {
				lacking, _, err = n.localMissing(ks, true)
			} else {
				lacking, err = n.remoteMissing(ctx, o, ks, true)
			}
			if err != nil {
				oc.err = err
			}
			for _, k := range lacking {
				oc.lacking[k] = struct{}{}
			}
			mu.Lock()
			results[o] = oc
			mu.Unlock()
		}(o, ks)
	}
	wg.Wait()
	var short [][32]byte
	for _, k := range keys {
		count := func(owners []view.NodeID) int {
			c := 0
			for _, o := range owners {
				oc := results[o]
				if oc == nil || oc.err != nil {
					continue
				}
				if _, lack := oc.lacking[k]; !lack {
					c++
				}
			}
			return c
		}
		need := min(minR, len(pl.Owners(k)))
		if count(pl.Owners(k)) < need {
			short = append(short, k)
			continue
		}
		if po := pl.PendingOwners(k); po != nil {
			if count(po) < min(minR, len(po)) {
				short = append(short, k)
			}
		}
	}
	return short, nil
}

// refsSnapshot lists every reference root for GC, settling undecided ones.
func (n *Node) refsSnapshot(ctx context.Context) ([]catalog.RefEntry, error) {
	return n.cat.RefsSnapshot(ctx)
}

// Why returns the reference names whose trees reach k (dstore gc why).
func (n *Node) Why(ctx context.Context, k [32]byte) ([]string, error) {
	refs, err := n.cat.RefsSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range refs {
		r, err := reference.Decode(e.Value.Record)
		if err != nil || len(r.Key) != 32 {
			continue
		}
		found := false
		get := func(kk key.Key) ([]byte, error) {
			if [32]byte(kk) == k {
				found = true
			}
			return n.getData(ctx, [32]byte(kk))
		}
		keys, err := fstree.ReachableKeys(key.Key(r.Key), get)
		if err != nil {
			continue
		}
		for _, kk := range keys {
			if [32]byte(kk) == k {
				found = true
			}
		}
		if found {
			out = append(out, e.Name)
		}
	}
	return out, nil
}

// RefPutLocal is the in-process form of ref-put, for the CLI on a node.
func (n *Node) RefPutLocal(ctx context.Context, name string, record []byte, cond catalog.Cond) ([]byte, error) {
	rec, err := reference.Decode(record)
	if err != nil {
		return nil, err
	}
	missing, shortfall, err := n.walkComplete(ctx, [32]byte(rec.Key))
	if err != nil {
		return nil, err
	}
	if shortfall > 0 {
		return nil, fmt.Errorf("incomplete: %d keys short (e.g. %x)", shortfall, missing[0][:8])
	}
	return n.cat.RefPut(ctx, name, record, cond, time.Now().Add(n.cfg.PutTTL).UnixNano(), keyOfRecord)
}
