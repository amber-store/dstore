package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/amber-store/dstore/catalog"
	"github.com/amber-store/dstore/transport"
	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
)

// rampSteps are the fractions of the target weight a join or drain steps
// through (§8.1).
var rampSteps = []int{8, 4, 2, 1}

// firstRampWeight returns the first step of a ramp to w.
func firstRampWeight(w uint32, noRamp bool) uint32 {
	if noRamp || w < 8 {
		return w
	}
	return w / 8
}

// nextRampWeight returns the next step above cur towards target.
func nextRampWeight(cur, target uint32) uint32 {
	for i := len(rampSteps) - 1; i >= 0; i-- {
		w := target / uint32(rampSteps[i])
		if w > cur {
			return w
		}
	}
	return target
}

// handleJoin admits a node (§8.1): validates its token, runs the voter add
// unless it opted out, and proposes the transition that adds it at the
// first ramp step.
func (n *Node) handleJoin(ctx context.Context, remote view.NodeID, s transport.Stream, m *wire.Msg) error {
	if n.View() == nil {
		return wire.WriteErr(s, wire.CodeUnavailable, "no view")
	}
	if len(m.Token) != 32 {
		return wire.WriteErr(s, wire.CodeUnauthorized, "join needs a 32-byte token")
	}
	tokenID := catalog.TokenID(m.Token)
	tok, err := n.cat.ReadToken(ctx, tokenID)
	if err != nil {
		return wire.WriteErr(s, wire.CodeUnauthorized, "unknown or used join token")
	}
	weight := m.Weight
	if tok.Weight > 0 {
		weight = tok.Weight
	}
	joiner := remote
	if len(m.Node) == 32 {
		joiner = view.NodeID(m.Node) // forwarded by another member
	}
	req := joinRequest{id: joiner, tokenID: tokenID, weight: weight, zone: m.Zone, addrs: m.Addrs, noVote: m.NoVote, noRamp: m.Force}
	n.rememberAddrs(joiner, m.Addrs)
	forward := &wire.Msg{Type: wire.TJoin, Token: m.Token, Weight: m.Weight, Zone: m.Zone, Addrs: m.Addrs, NoVote: m.NoVote, Force: m.Force, Node: joiner[:]}
	// A join slots in between other transitions: wait for a running one
	// before taking the holder's turn, so the coordinator keeps driving it.
	for {
		v := n.View()
		if v.Pending == nil && v.VoterSync != view.VoterSyncPending {
			break
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return wire.WriteErr(s, wire.CodeUnavailable, "a transition is in progress; retry later")
		}
	}
	v, err := n.maint.runAsHolder(ctx, forward, func(ctx context.Context) (*view.View, error) {
		return n.admitNode(ctx, req)
	})
	if err != nil {
		if wire.IsCode(err, wire.CodeUnauthorized) || wire.IsCode(err, wire.CodeBadRequest) {
			we, _ := wire.AsError(err)
			return wire.WriteErr(s, we.Code, we.Text)
		}
		return wire.WriteErr(s, wire.CodeUnavailable, err.Error())
	}
	enc, _ := v.Encode()
	return wire.WriteMsg(s, n.stampReply(&wire.Msg{Type: wire.TViewReply, View: enc}))
}

type joinRequest struct {
	id      view.NodeID
	tokenID []byte
	weight  uint32
	zone    string
	addrs   []string
	noVote  bool
	noRamp  bool
}

// admitNode runs on the lease holder.
func (n *Node) admitNode(ctx context.Context, r joinRequest) (*view.View, error) {
	v := n.View()
	if _, ok := v.Node(r.id); ok {
		return v, nil // already a member: an idempotent retry
	}
	if tokenUsed(v, r.tokenID) {
		return nil, &wire.Error{Code: wire.CodeUnauthorized, Text: "join token already used"}
	}
	if v.Pending != nil || v.VoterSync == view.VoterSyncPending {
		return nil, errors.New("a transition is in progress; retry later")
	}
	// Voter add first (§8.1), unless the node opted out.
	if !r.noVote {
		if err := n.maint.addVoter(ctx, r.id); err != nil {
			return nil, fmt.Errorf("voter add: %w", err)
		}
	}
	first := firstRampWeight(r.weight, r.noRamp)
	nv, err := n.maint.proposeTransition(ctx, "join "+view.ShortID(r.id), func(v *view.View, p *view.Pending) error {
		if tokenUsed(v, r.tokenID) {
			return &wire.Error{Code: wire.CodeUnauthorized, Text: "join token already used"}
		}
		p.Nodes = append(p.Nodes, view.Node{ID: r.id[:], Weight: first, Addrs: r.addrs, Token: r.tokenID, Zone: r.zone, Writable: true, Incarnation: 1})
		view.SortNodes(p.Nodes)
		if first < r.weight {
			v.Ramps = append(v.Ramps, view.Ramp{Node: r.id[:], Target: r.weight, Step: 1})
		}
		if r.noVote {
			v.DeferredVoters = removeID(v.DeferredVoters, r.id)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	_ = n.cat.DeleteToken(ctx, r.tokenID)
	return nv, nil
}

// tokenUsed reports whether a token id appears in any entry (§2).
func tokenUsed(v *view.View, id []byte) bool {
	check := func(nodes []view.Node) bool {
		for _, nd := range nodes {
			if bytes.Equal(nd.Token, id) {
				return true
			}
		}
		return false
	}
	if check(v.Nodes) {
		return true
	}
	if v.Pending != nil && check(v.Pending.Nodes) {
		return true
	}
	return false
}

func removeID(ids [][]byte, id view.NodeID) [][]byte {
	out := ids[:0]
	for _, b := range ids {
		if !bytes.Equal(b, id[:]) {
			out = append(out, b)
		}
	}
	return out
}

// Join sends a join request to a seed member and adopts the returned view.
// The node must be started (serving the cluster ALPN) so that the voter
// add's sync can reach it.
func (n *Node) Join(ctx context.Context, seed view.NodeID, seedAddrs []string, token []byte, weight uint32, zone string, noVote, noRamp bool) (*view.View, error) {
	if n.View() != nil {
		return nil, errors.New("node: already a member")
	}
	if noVote {
		_ = n.meta.Set([]byte(mkNoVote), []byte{1})
	}
	c, err := n.ep.Dial(ctx, seed, seedAddrs, wire.ALPNCluster)
	if err != nil {
		return nil, fmt.Errorf("dial seed: %w", err)
	}
	defer c.Close()
	s, err := c.OpenStream(ctx)
	if err != nil {
		return nil, err
	}
	defer wire.CloseStream(s)
	req := &wire.Msg{Type: wire.TJoin, Token: token, Weight: weight, Zone: zone, Addrs: n.ep.Addrs(), NoVote: noVote, Force: noRamp}
	if err := wire.WriteMsg(s, req); err != nil {
		return nil, err
	}
	_ = s.CloseWrite()
	m, err := wire.Expect(s, wire.TViewReply)
	if err != nil {
		return nil, err
	}
	v, err := view.Decode(m.View)
	if err != nil {
		return nil, err
	}
	n.adopt(v, "join")
	_ = n.meta.Set([]byte(mkRegistered+string(n.storeID)), []byte{1})
	n.log.Info("joined cluster", "epoch", v.Epoch)
	return v, nil
}

// waitForView blocks until the node has a view (a joining node).
func (n *Node) waitForView(ctx context.Context) bool {
	for {
		if n.View() != nil {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(200 * time.Millisecond):
		}
	}
}
