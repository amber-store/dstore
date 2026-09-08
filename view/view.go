// Package view defines the cluster view (architecture/dstore.md §3): the
// membership and placement configuration every node and client places by,
// and the placement functions of §4 over it.
package view

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/amber-store/dstore/codec"
	"github.com/amber-store/dstore/placement"
)

// NodeID is a 32-byte iroh endpoint identity.
type NodeID = placement.NodeID

// ParseNodeID parses a hex node id.
func ParseNodeID(s string) (NodeID, error) {
	var id NodeID
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 32 {
		return id, fmt.Errorf("view: bad node id %q", s)
	}
	copy(id[:], b)
	return id, nil
}

// IDString formats a node id as hex.
func IDString(id NodeID) string { return hex.EncodeToString(id[:]) }

// ShortID formats the first 4 bytes of a node id.
func ShortID(id NodeID) string { return hex.EncodeToString(id[:4]) }

// Voter is one acceptor of the catalog and the epoch that added it.
type Voter struct {
	ID    []byte `cbor:"0,keyasint"`
	Since uint64 `cbor:"1,keyasint"`
}

// Former is a recently removed member that may still hand data back.
type Former struct {
	ID    []byte `cbor:"0,keyasint"`
	Until int64  `cbor:"1,keyasint"` // ns since the Unix epoch
}

// DataEndpoint is an extra endpoint of a node for sharded transfers.
type DataEndpoint struct {
	ID    []byte   `cbor:"0,keyasint"`
	Addrs []string `cbor:"1,keyasint,omitempty"`
}

// Node is one member of a placement set.
type Node struct {
	ID          []byte         `cbor:"0,keyasint"`
	Weight      uint32         `cbor:"1,keyasint"`
	Addrs       []string       `cbor:"2,keyasint,omitempty"`
	Data        []DataEndpoint `cbor:"3,keyasint,omitempty"`
	Token       []byte         `cbor:"4,keyasint,omitempty"`
	Zone        string         `cbor:"5,keyasint,omitempty"`
	Incarnation uint64         `cbor:"6,keyasint,omitempty"`
	Writable    bool           `cbor:"7,keyasint"`
}

// NID returns the node's id as a NodeID.
func (n Node) NID() NodeID {
	var id NodeID
	copy(id[:], n.ID)
	return id
}

// ZoneOrID returns the node's failure domain.
func (n Node) ZoneOrID() string {
	if n.Zone != "" {
		return n.Zone
	}
	return string(n.ID)
}

// Pending is the target placement set while a transition runs.
type Pending struct {
	Nodes           []Node   `cbor:"0,keyasint"`
	Replicas        uint8    `cbor:"1,keyasint"`
	ID              uint64   `cbor:"2,keyasint"`
	ParticipantsAck [][]byte `cbor:"3,keyasint,omitempty"`
	Participants    [][]byte `cbor:"4,keyasint,omitempty"`
	Frozen          bool     `cbor:"5,keyasint,omitempty"`
	Round           uint32   `cbor:"6,keyasint,omitempty"`
	PrimaryDone     [][]byte `cbor:"7,keyasint,omitempty"`
	Done            [][]byte `cbor:"8,keyasint,omitempty"`
	FrozenAt        int64    `cbor:"9,keyasint,omitempty"`  // ns; when participants were frozen
	Reason          string   `cbor:"10,keyasint,omitempty"` // operator-facing description
	Ramp            *Ramp    `cbor:"11,keyasint,omitempty"`
	Since           int64    `cbor:"12,keyasint,omitempty"` // ns; when the pending was set
}

// Ramp records a multi-step weight ramp the coordinator is stepping through.
type Ramp struct {
	Node   []byte `cbor:"0,keyasint"`
	Target uint32 `cbor:"1,keyasint"`
	Step   int    `cbor:"2,keyasint"`
}

// VoterSync is the state of a voter change.
const (
	VoterSyncDone    = 0
	VoterSyncPending = 1
)

// View is the cluster's membership and placement configuration.
type View struct {
	ClusterID       []byte   `cbor:"0,keyasint"`
	Incarnation     uint64   `cbor:"1,keyasint"`
	Epoch           uint64   `cbor:"2,keyasint"`
	Version         uint64   `cbor:"3,keyasint"`
	PlacementEpoch  uint64   `cbor:"4,keyasint"`
	Replicas        uint8    `cbor:"5,keyasint"`
	MinReplicas     uint8    `cbor:"6,keyasint"`
	Voters          []Voter  `cbor:"7,keyasint"`
	VoterSync       int      `cbor:"8,keyasint"`
	VoterSyncCursor []byte   `cbor:"9,keyasint,omitempty"`
	Nodes           []Node   `cbor:"10,keyasint"`
	Pending         *Pending `cbor:"11,keyasint,omitempty"`
	Former          []Former `cbor:"12,keyasint,omitempty"`
	Fenced          [][]byte `cbor:"13,keyasint,omitempty"`
	RecoveredInc    uint64   `cbor:"14,keyasint,omitempty"`
	RecoveredEpoch  uint64   `cbor:"15,keyasint,omitempty"`
	RebalancePause  bool     `cbor:"16,keyasint,omitempty"`
	RateCap         uint64   `cbor:"17,keyasint,omitempty"`
	VoterSyncTarget []byte   `cbor:"18,keyasint,omitempty"` // voter being added/removed (id)
	VoterSyncAdd    bool     `cbor:"19,keyasint,omitempty"`
	DeferredVoters  [][]byte `cbor:"20,keyasint,omitempty"` // nodes admitted with their voter add deferred (§5.4)
	ACL             *ACL     `cbor:"21,keyasint,omitempty"`
	Ramps           []Ramp   `cbor:"22,keyasint,omitempty"` // weight ramps in progress (§8.1)
	RemoveVoters    [][]byte `cbor:"23,keyasint,omitempty"` // voters to remove once their placement transition commits
}

// ACL is the optional client allowlist (§2).
type ACL struct {
	Allowed [][]byte `cbor:"0,keyasint,omitempty"`
	Admins  [][]byte `cbor:"1,keyasint,omitempty"`
}

// ErrNotMember reports an id that is not in the view.
var ErrNotMember = errors.New("view: not a member")

// Encode returns the deterministic CBOR of v.
func (v *View) Encode() ([]byte, error) { return codec.Marshal(v) }

// Decode parses a view.
func Decode(b []byte) (*View, error) {
	var v View
	if err := codec.Unmarshal(b, &v); err != nil {
		return nil, fmt.Errorf("view: decode: %w", err)
	}
	return &v, nil
}

// Clone returns a deep copy.
func (v *View) Clone() *View {
	b, err := v.Encode()
	if err != nil {
		panic(err)
	}
	c, err := Decode(b)
	if err != nil {
		panic(err)
	}
	return c
}

// Compare orders v against (inc, epoch): −1 when v is older, 0 when
// equal, +1 when v is newer.
func (v *View) Compare(inc, epoch uint64) int {
	if v.Incarnation != inc {
		if v.Incarnation < inc {
			return -1
		}
		return 1
	}
	if v.Epoch != epoch {
		if v.Epoch < epoch {
			return -1
		}
		return 1
	}
	return 0
}

// Node returns the member with id under nodes, or pending.nodes.
func (v *View) Node(id NodeID) (Node, bool) {
	for _, n := range v.Nodes {
		if bytes.Equal(n.ID, id[:]) {
			return n, true
		}
	}
	if v.Pending != nil {
		for _, n := range v.Pending.Nodes {
			if bytes.Equal(n.ID, id[:]) {
				return n, true
			}
		}
	}
	return Node{}, false
}

// IsMember reports whether id is in nodes or pending.nodes, or is one of
// a member's data endpoints (§2).
func (v *View) IsMember(id NodeID) bool {
	_, ok := v.Node(id)
	if ok {
		return true
	}
	return v.DataEndpointOwner(id) != nil
}

// DataEndpointOwner resolves a data-endpoint identity to its node id.
func (v *View) DataEndpointOwner(id NodeID) *NodeID {
	check := func(nodes []Node) *NodeID {
		for _, n := range nodes {
			for _, d := range n.Data {
				if bytes.Equal(d.ID, id[:]) {
					nid := n.NID()
					return &nid
				}
			}
		}
		return nil
	}
	if r := check(v.Nodes); r != nil {
		return r
	}
	if v.Pending != nil {
		return check(v.Pending.Nodes)
	}
	return nil
}

// IsFormer reports whether id is on the former list.
func (v *View) IsFormer(id NodeID) bool {
	for _, f := range v.Former {
		if bytes.Equal(f.ID, id[:]) {
			return true
		}
	}
	return false
}

// IsVoter reports whether id is an acceptor.
func (v *View) IsVoter(id NodeID) bool {
	for _, vo := range v.Voters {
		if bytes.Equal(vo.ID, id[:]) {
			return true
		}
	}
	return false
}

// VoterIDs returns the voter ids.
func (v *View) VoterIDs() []NodeID {
	out := make([]NodeID, len(v.Voters))
	for i, vo := range v.Voters {
		copy(out[i][:], vo.ID)
	}
	return out
}

// Quorum returns ⌊|voters|/2⌋ + 1.
func (v *View) Quorum() int { return len(v.Voters)/2 + 1 }

// AllMembers returns the ids of every node in nodes ∪ pending.nodes.
func (v *View) AllMembers() []NodeID {
	seen := map[NodeID]struct{}{}
	var out []NodeID
	add := func(nodes []Node) {
		for _, n := range nodes {
			id := n.NID()
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
	}
	add(v.Nodes)
	if v.Pending != nil {
		add(v.Pending.Nodes)
	}
	return out
}

// Placement is the cached placement tables of a view: one over nodes,
// and one over pending.nodes while a transition runs. It is built once
// per view and shared by every computation of that epoch.
type Placement struct {
	view    *View
	cur     *placement.Table
	pending *placement.Table
	mu      sync.Mutex
}

// NewPlacement builds the placement tables of v.
func NewPlacement(v *View) *Placement {
	p := &Placement{view: v}
	p.cur = placement.NewTable(placement.NewSet(members(v.Nodes)), int(v.Replicas))
	if v.Pending != nil {
		p.pending = placement.NewTable(placement.NewSet(members(v.Pending.Nodes)), int(v.Pending.Replicas))
	}
	return p
}

func members(nodes []Node) []placement.Member {
	out := make([]placement.Member, len(nodes))
	for i, n := range nodes {
		out[i] = placement.Member{ID: n.NID(), Weight: n.Weight, Zone: n.ZoneOrID()}
	}
	return out
}

// View returns the view the placement was built from.
func (p *Placement) View() *View { return p.view }

// Owners returns the owners of key under nodes.
func (p *Placement) Owners(key [32]byte) []NodeID {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cur.OwnerIDs(key)
}

// PendingOwners returns the owners of key under pending.nodes, or nil.
func (p *Placement) PendingOwners(key [32]byte) []NodeID {
	if p.pending == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pending.OwnerIDs(key)
}

// WriteSet returns owners(k, nodes) ∪ owners(k, pending.nodes).
func (p *Placement) WriteSet(key [32]byte) []NodeID {
	out := p.Owners(key)
	if p.pending == nil {
		return out
	}
	seen := make(map[NodeID]struct{}, len(out))
	for _, id := range out {
		seen[id] = struct{}{}
	}
	for _, id := range p.PendingOwners(key) {
		if _, ok := seen[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

// ReadOrder returns rank(k, nodes) then rank(k, pending.nodes), deduplicated.
func (p *Placement) ReadOrder(key [32]byte) []NodeID {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.cur.RankIDs(key)
	if p.pending == nil {
		return out
	}
	seen := make(map[NodeID]struct{}, len(out))
	for _, id := range out {
		seen[id] = struct{}{}
	}
	for _, id := range p.pending.RankIDs(key) {
		if _, ok := seen[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

// IsOwner reports whether id owns key under nodes.
func (p *Placement) IsOwner(key [32]byte, id NodeID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cur.IsOwner(key, id)
}

// IsPendingOwner reports whether id owns key under pending.nodes.
func (p *Placement) IsPendingOwner(key [32]byte, id NodeID) bool {
	if p.pending == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pending.IsOwner(key, id)
}

// InWriteSet reports whether id owns key under nodes or pending.nodes.
func (p *Placement) InWriteSet(key [32]byte, id NodeID) bool {
	return p.IsOwner(key, id) || p.IsPendingOwner(key, id)
}

// Contains reports whether id is in ids.
func Contains(ids [][]byte, id NodeID) bool {
	for _, b := range ids {
		if bytes.Equal(b, id[:]) {
			return true
		}
	}
	return false
}

// AddID appends id to ids if absent, returning the new list.
func AddID(ids [][]byte, id NodeID) [][]byte {
	if Contains(ids, id) {
		return ids
	}
	return append(ids, append([]byte{}, id[:]...))
}

// IDsOf converts raw ids to NodeIDs.
func IDsOf(raw [][]byte) []NodeID {
	out := make([]NodeID, len(raw))
	for i, b := range raw {
		copy(out[i][:], b)
	}
	return out
}

// SortNodes orders nodes by id, the canonical order of a view's node list.
func SortNodes(nodes []Node) {
	sort.Slice(nodes, func(i, j int) bool { return bytes.Compare(nodes[i].ID, nodes[j].ID) < 0 })
}

// DefaultMinReplicas returns max(R−1, 2) capped at R.
func DefaultMinReplicas(r uint8) uint8 {
	m := int(r) - 1
	if m < 2 {
		m = 2
	}
	if m > int(r) {
		m = int(r)
	}
	return uint8(m)
}

// ValidateChange refuses a target node set that drops R or more of the
// current nodes at once (§3), unless forced.
func ValidateChange(cur, target []Node, replicas int, force bool) error {
	if force {
		return nil
	}
	dropped := 0
	for _, n := range cur {
		found := false
		for _, t := range target {
			if bytes.Equal(n.ID, t.ID) && t.Weight > 0 {
				found = true
				break
			}
		}
		if !found && n.Weight > 0 {
			dropped++
		}
	}
	if dropped >= replicas && replicas > 0 {
		return fmt.Errorf("view: change drops %d nodes at once with R=%d; every key owned only by them would be lost (use --force)", dropped, replicas)
	}
	return nil
}
