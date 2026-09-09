package client

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
)

// Progress receives push and pull progress, if set. It is called for every
// record that goes over the wire, under the transfer's lock, so it must be
// cheap: keep the latest report and render it elsewhere.
type Progress func(ProgressReport)

// ProgressReport is a snapshot of a transfer. Bytes count record lengths,
// the same unit as PushStats.Bytes; a record sent twice counts twice, and
// TotalBytes grows with it.
type ProgressReport struct {
	Objects      int // objects done, including ones the cluster already held
	TotalObjects int
	Bytes        int64 // bytes sent or received so far
	TotalBytes   int64 // bytes to transfer, 0 until known
	// Nodes lists the nodes the transfer has talked to, ordered by id.
	Nodes []NodeProgress
}

// NodeProgress is one node's share of a transfer.
type NodeProgress struct {
	ID       view.NodeID
	Direct   bool          // the connection's current path is direct
	RTT      time.Duration // the connection's round-trip time, 0 if unknown
	InFlight int           // batches in progress
	Awaiting int           // of those, fully sent and waiting for the node's reply
	Bytes    int64
}

// PutObserver watches an upload's batches; nil funcs are skipped. Flushed
// reports a batch fully handed to the wire, after which the client waits
// for the node to store and replicate it; Done says whether that point had
// been reached.
type PutObserver struct {
	Start   func(node view.NodeID)
	Sent    func(node view.NodeID, n int)
	Flushed func(node view.NodeID)
	Done    func(node view.NodeID, flushed bool)
}

// tracker accumulates a push's progress and reports it.
type tracker struct {
	c     *Cluster
	prog  Progress
	mu    sync.Mutex
	rep   ProgressReport
	nodes map[view.NodeID]*NodeProgress
}

func newTracker(c *Cluster, prog Progress) *tracker {
	return &tracker{c: c, prog: prog, nodes: map[view.NodeID]*NodeProgress{}}
}

func (t *tracker) observer() PutObserver {
	return PutObserver{
		Start: func(id view.NodeID) { t.update(func() { t.node(id).InFlight++ }) },
		Sent: func(id view.NodeID, n int) {
			t.update(func() {
				t.node(id).Bytes += int64(n)
				t.rep.Bytes += int64(n)
			})
		},
		Flushed: func(id view.NodeID) { t.update(func() { t.node(id).Awaiting++ }) },
		Done: func(id view.NodeID, flushed bool) {
			t.update(func() {
				np := t.node(id)
				np.InFlight--
				if flushed {
					np.Awaiting--
				}
			})
		},
	}
}

// totals sets what the transfer has to move; done counts the objects the
// cluster already held.
func (t *tracker) totals(objects, done int, bytes int64) {
	t.update(func() { t.rep.TotalObjects, t.rep.Objects, t.rep.TotalBytes = objects, done, bytes })
}

// more adds bytes that turned out to need sending (a re-send round).
func (t *tracker) more(bytes int64) { t.update(func() { t.rep.TotalBytes += bytes }) }

// objects adds n finished objects.
func (t *tracker) objects(n int) { t.update(func() { t.rep.Objects += n }) }

func (t *tracker) bytes() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.rep.Bytes
}

func (t *tracker) node(id view.NodeID) *NodeProgress {
	np := t.nodes[id]
	if np == nil {
		np = &NodeProgress{ID: id}
		t.nodes[id] = np
	}
	return np
}

func (t *tracker) update(f func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	f()
	if t.prog != nil {
		t.prog(t.snapshot())
	}
}

func (t *tracker) snapshot() ProgressReport {
	rep := t.rep
	rep.Nodes = make([]NodeProgress, 0, len(t.nodes))
	for _, np := range t.nodes {
		n := *np
		if p, ok := t.c.pool.Path(n.ID, wire.ALPNClient); ok {
			n.Direct, n.RTT = p.Direct, p.RTT
		}
		rep.Nodes = append(rep.Nodes, n)
	}
	sort.Slice(rep.Nodes, func(i, j int) bool { return bytes.Compare(rep.Nodes[i].ID[:], rep.Nodes[j].ID[:]) < 0 })
	return rep
}

// countKeys sums the objects and bytes of a per-node key map.
func countKeys(m map[view.NodeID][][32]byte) (objects int, bytes int64) {
	for _, ks := range m {
		objects += len(ks)
		for _, k := range ks {
			bytes += int64(key.Key(k).Length())
		}
	}
	return objects, bytes
}

// pathAttrs describes the client's connection to id for a log line.
func (c *Cluster) pathAttrs(id view.NodeID) []any {
	p, ok := c.pool.Path(id, wire.ALPNClient)
	if !ok {
		return []any{"path", "none"}
	}
	kind := "direct"
	if !p.Direct {
		kind = "relay"
	}
	return []any{"path", kind, "rtt", p.RTT.Round(time.Millisecond)}
}

// Rate formats bytes over took as a human-readable throughput.
func Rate(bytes int64, took time.Duration) string {
	if took <= 0 {
		return "-"
	}
	return HumanBytes(int64(float64(bytes)/took.Seconds())) + "/s"
}

// HumanBytes formats n in binary units.
func HumanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
