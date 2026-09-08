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

// Progress receives push/pull progress, if set.
type Progress func(done, total int)

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

	start := time.Now()
	lastPin := start
	holders := map[[32]byte][]view.NodeID{}
	var uploaded int
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
		if len(mr.Lacking) > 0 {
			pr := c.Put(ctx, mr.Lacking, src)
			for k, h := range pr.Holders {
				holders[k] = h
				uploaded++
				st.Bytes += int64(key.Key(k).Length())
				if prog != nil {
					prog(uploaded, len(all))
				}
			}
			for p, err := range pr.Errors {
				return st, fmt.Errorf("upload to %s: %w", view.ShortID(p), err)
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
			return st, c.shortError(short, holders)
		}
		// Re-put short keys to their primaries; on the last round send
		// them to the lacking owners directly.
		if round == 1 {
			if err := c.directFill(ctx, short, holders, src); err != nil {
				return st, err
			}
		}
		all2 := short
		_ = all2
		// Re-pin everything uploaded so far if the push runs long.
		if time.Since(lastPin) > c.cfg.GCInterval/2 {
			_, _ = c.Missing(ctx, all, true)
			lastPin = time.Now()
		}
	}
	st.Uploaded = uploaded

	rec := reference.Reference{Name: name, Key: root[:], User: user, CreatedAt: time.Now().UnixNano()}
	enc, err := rec.Encode()
	if err != nil {
		return st, err
	}
	for attempt := 0; attempt < 3; attempt++ {
		version, err := c.RefPut(ctx, enc, cond)
		if err == nil {
			st.Version = version
			return st, nil
		}
		var inc *Incomplete
		if !errors.As(err, &inc) || attempt == 2 {
			return st, err
		}
		// GC may have reaped a dedup hit between negotiation and gate:
		// re-run the whole negotiation and send what is missing anywhere.
		mr, merr := c.Missing(ctx, all, true)
		if merr != nil {
			return st, merr
		}
		if len(mr.Lacking) > 0 {
			c.Put(ctx, mr.Lacking, src)
		}
		var short [][32]byte
		for _, k := range all {
			if h, ok := mr.Holders[k]; !ok || !c.Placed(k, h) {
				short = append(short, k)
			}
		}
		if len(short) > 0 {
			if err := c.directFill(ctx, short, mr.Holders, src); err != nil {
				return st, err
			}
		}
	}
	return st, errors.New("push: reference write did not complete")
}

// directFill sends short keys straight to the owners that lack them.
func (c *Cluster) directFill(ctx context.Context, short [][32]byte, holders map[[32]byte][]view.NodeID, src RecordSource) error {
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
	pr := c.Put(ctx, byOwner, src)
	for k, h := range pr.Holders {
		holders[k] = mergeIDs(holders[k], h)
	}
	return nil
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
	if err := c.PullTree(ctx, local, root, &st, prog); err != nil {
		return st, err
	}
	return st, nil
}

// PullTree fetches the tree under root into local.
func (c *Cluster) PullTree(ctx context.Context, local *packstore.Store, root key.Key, st *PullStats, prog Progress) error {
	frontier := []key.Key{root}
	seen := map[key.Key]bool{}
	for len(frontier) > 0 {
		var want [][32]byte
		for _, k := range frontier {
			if seen[k] {
				continue
			}
			seen[k] = true
			has, err := local.Has(k)
			if err != nil {
				return err
			}
			if has {
				if k.Type() == key.Blob || k.Type() == key.XattrSet {
					continue
				}
				if _, err := fstree.CheckComplete(k, local.Get, local.Has, c.cfg.Jobs); err == nil {
					continue // complete subtree
				}
				// Present but incomplete below: expand it locally.
				data, err := local.Get(k)
				if err == nil {
					kids, err := fstree.ChildKeys(k, data)
					if err == nil {
						frontier = append(frontier, kids...)
						continue
					}
				}
			}
			want = append(want, [32]byte(k))
		}
		st.Keys += len(want)
		if len(want) == 0 {
			frontier = nil
			continue
		}
		var next []key.Key
		seq, missingFn := c.Get(ctx, want)
		var batch []packstore.Object
		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			objs := batch
			batch = nil
			_, err := local.WriteParallel(func(yield func(packstore.Object, error) bool) {
				for _, o := range objs {
					if !yield(o, nil) {
						return
					}
				}
			}, packstore.WriteOpts{Writers: c.cfg.Jobs})
			return err
		}
		var size int
		for r, err := range seq {
			if err != nil {
				return err
			}
			k := key.Key(r.Key)
			batch = append(batch, packstore.Object{Key: k, Record: r.Record})
			size += len(r.Record)
			st.Fetched++
			st.Bytes += int64(len(r.Record))
			if prog != nil {
				prog(st.Fetched, st.Keys)
			}
			if k.Type() != key.Blob && k.Type() != key.XattrSet {
				rec, err := amberpack.ParseRecord(r.Record)
				if err != nil {
					return err
				}
				data, err := amberpack.DecodePayload(rec.Flags, rec.Ulen, r.Record[amberpack.RecHeaderSize:])
				if err != nil {
					return err
				}
				kids, err := fstree.ChildKeys(k, data)
				if err != nil {
					return err
				}
				next = append(next, kids...)
			}
			if size > batchBytes {
				if err := flush(); err != nil {
					return err
				}
				size = 0
			}
		}
		if err := flush(); err != nil {
			return err
		}
		if missing := missingFn(); len(missing) > 0 {
			return fmt.Errorf("pull: %d objects not found in the cluster (e.g. %x)", len(missing), missing[0][:8])
		}
		frontier = next
	}
	if _, err := fstree.CheckComplete(root, local.Get, local.Has, c.cfg.Jobs); err != nil {
		return fmt.Errorf("pull: tree incomplete after fetch: %w", err)
	}
	return nil
}
