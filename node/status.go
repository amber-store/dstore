package node

import (
	"context"
	"time"

	"github.com/amber-store/dstore/codec"
	"github.com/amber-store/dstore/transport"
	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
)

// VoterStat is one voter's catalog round statistics.
type VoterStat struct {
	ID       []byte `cbor:"0,keyasint"`
	Calls    uint64 `cbor:"1,keyasint"`
	Failures uint64 `cbor:"2,keyasint"`
	P99ms    int64  `cbor:"3,keyasint"`
}

// Status is a node's status report (§10.1, §13).
type Status struct {
	ID            []byte      `cbor:"0,keyasint"`
	Epoch         uint64      `cbor:"1,keyasint"`
	Incarnation   uint64      `cbor:"2,keyasint"`
	Packs         int         `cbor:"3,keyasint"`
	Records       uint64      `cbor:"4,keyasint"`
	Bytes         int64       `cbor:"5,keyasint"`
	Pins          int         `cbor:"6,keyasint"`
	Unreachable   [][]byte    `cbor:"7,keyasint,omitempty"`
	PendingPacks  int         `cbor:"8,keyasint"`
	Transition    string      `cbor:"9,keyasint,omitempty"`
	GC            string      `cbor:"10,keyasint,omitempty"`
	LeaseHolder   []byte      `cbor:"11,keyasint,omitempty"`
	Voters        []VoterStat `cbor:"12,keyasint,omitempty"`
	Writable      bool        `cbor:"13,keyasint"`
	FreeBytes     int64       `cbor:"14,keyasint"`
	TotalBytes    int64       `cbor:"15,keyasint"`
	Puts          uint64      `cbor:"16,keyasint"`
	Gets          uint64      `cbor:"17,keyasint"`
	RefPuts       uint64      `cbor:"18,keyasint"`
	BytesIn       uint64      `cbor:"19,keyasint"`
	BytesOut      uint64      `cbor:"20,keyasint"`
	Amnesiac      bool        `cbor:"21,keyasint,omitempty"`
	Retired       bool        `cbor:"22,keyasint,omitempty"`
	ScrubAgeSec   int64       `cbor:"23,keyasint,omitempty"`
	LastLive      uint64      `cbor:"24,keyasint,omitempty"`
	Corrupt       int         `cbor:"25,keyasint,omitempty"`
	IsHolder      bool        `cbor:"26,keyasint,omitempty"`
	UnauditedKeys int         `cbor:"27,keyasint,omitempty"`
}

// status builds the report.
func (n *Node) status(ctx context.Context) Status {
	st := Status{ID: n.id[:], Writable: n.writable.Load()}
	if v := n.View(); v != nil {
		st.Epoch, st.Incarnation = v.Epoch, v.Incarnation
		st.Transition = n.maint.transitionText(v)
	}
	if segs, err := n.store.Segments(); err == nil {
		st.Packs = len(segs)
		for _, s := range segs {
			st.Records += s.Keys
			st.Bytes += s.Body
		}
		st.ScrubAgeSec = n.rec.scrubAge(segs)
		st.PendingPacks = n.rec.pendingPacks(segs)
	}
	st.Pins = n.meta.Count([]byte(mkPin))
	st.Corrupt = n.meta.Count([]byte(mkCorrupt))
	st.Unreachable = n.Unreachable()
	st.FreeBytes, st.TotalBytes, _ = diskFree(n.cfg.StoreDir)
	st.Puts = n.stats.puts.Load()
	st.Gets = n.stats.gets.Load()
	st.RefPuts = n.stats.refPuts.Load()
	st.BytesIn = n.stats.bytesIn.Load()
	st.BytesOut = n.stats.bytesOut.Load()
	st.Amnesiac = n.acceptor.Amnesiac()
	st.Retired = n.acceptor.Retired()
	st.IsHolder = n.maint.isHolder()
	st.LastLive = n.gc.lastLive()
	n.recentMu.Lock()
	st.UnauditedKeys = len(n.recent)
	n.recentMu.Unlock()
	if ctx != nil {
		lctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		if l, err := n.cat.ReadLease(lctx); err == nil {
			st.LeaseHolder = l.Holder
		}
		if g, err := n.cat.ReadGC(lctx); err == nil {
			st.GC = n.gc.statusText(g)
		}
		cancel()
	}
	for id, vs := range n.prop.Stats() {
		st.Voters = append(st.Voters, VoterStat{ID: append([]byte{}, id[:]...), Calls: vs.Calls, Failures: vs.Failures, P99ms: vs.P99.Milliseconds()})
	}
	return st
}

// Status returns the node's status report.
func (n *Node) Status(ctx context.Context) Status { return n.status(ctx) }

func (n *Node) handleStatus(ctx context.Context, s transport.Stream) error {
	st := n.status(ctx)
	return wire.WriteMsg(s, n.stampReply(&wire.Msg{Type: wire.TStatusReply, Status: codec.MustMarshal(st)}))
}

// DecodeStatus parses a status blob.
func DecodeStatus(b []byte) (Status, error) {
	var st Status
	err := codec.Unmarshal(b, &st)
	return st, err
}

// ShortID formats a raw id.
func ShortID(b []byte) string {
	if len(b) != 32 {
		return "?"
	}
	return view.ShortID(view.NodeID(b))
}
