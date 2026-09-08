package node

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/amber-store/dstore/catalog"
	"github.com/amber-store/dstore/codec"
	"github.com/amber-store/dstore/ticket"
	"github.com/amber-store/dstore/transport"
	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
)

// AdminRequest is the operator command carried by a TAdmin frame.
type AdminRequest struct {
	Op          string   `cbor:"0,keyasint"`
	Node        []byte   `cbor:"1,keyasint,omitempty"`
	Weight      uint32   `cbor:"2,keyasint,omitempty"`
	Zone        string   `cbor:"3,keyasint,omitempty"`
	Replicas    uint8    `cbor:"4,keyasint,omitempty"`
	Dead        bool     `cbor:"5,keyasint,omitempty"`
	AllowUnsafe bool     `cbor:"6,keyasint,omitempty"`
	Force       bool     `cbor:"7,keyasint,omitempty"`
	Key         []byte   `cbor:"8,keyasint,omitempty"`
	Garbage     float64  `cbor:"9,keyasint,omitempty"`
	Tolerate    bool     `cbor:"10,keyasint,omitempty"`
	Forwarded   bool     `cbor:"11,keyasint,omitempty"`
	Pause       bool     `cbor:"12,keyasint,omitempty"`
	Rate        uint64   `cbor:"13,keyasint,omitempty"`
	Names       []string `cbor:"14,keyasint,omitempty"`
}

// AdminReply is the result of an operator command.
type AdminReply struct {
	Text   string   `cbor:"0,keyasint,omitempty"`
	Token  []byte   `cbor:"1,keyasint,omitempty"`
	View   []byte   `cbor:"2,keyasint,omitempty"`
	Names  []string `cbor:"3,keyasint,omitempty"`
	Key    []byte   `cbor:"4,keyasint,omitempty"`
	Ticket string   `cbor:"5,keyasint,omitempty"`
	GC     []byte   `cbor:"6,keyasint,omitempty"`
}

// handleAdmin runs an operator command on this node.
func (n *Node) handleAdmin(ctx context.Context, s transport.Stream, m *wire.Msg) error {
	var req AdminRequest
	if err := codec.Unmarshal(m.Params, &req); err != nil {
		return wire.WriteErr(s, wire.CodeBadRequest, err.Error())
	}
	reply, err := n.Admin(ctx, req)
	if err != nil {
		if we, ok := wire.AsError(err); ok {
			return wire.WriteErr(s, we.Code, we.Text)
		}
		return wire.WriteErr(s, wire.CodeUnavailable, err.Error())
	}
	return wire.WriteMsg(s, n.stampReply(&wire.Msg{Type: wire.TAdminReply, Status: codec.MustMarshal(reply)}))
}

func nodeIDOf(b []byte) (view.NodeID, error) {
	if len(b) != 32 {
		return view.NodeID{}, errors.New("node id must be 32 bytes")
	}
	return view.NodeID(b), nil
}

// Admin executes an operator command (§13).
func (n *Node) Admin(ctx context.Context, req AdminRequest) (AdminReply, error) {
	v := n.View()
	if v == nil {
		return AdminReply{}, errors.New("no view")
	}
	switch req.Op {
	case "token-create":
		tok, err := n.cat.CreateToken(ctx, req.Weight)
		if err != nil {
			return AdminReply{}, err
		}
		return AdminReply{Token: tok, Text: hex.EncodeToString(tok)}, nil

	case "cluster-ticket":
		t := ticket.Ticket{ClusterID: v.ClusterID, Incarnation: v.Incarnation}
		for i, nd := range v.Nodes {
			if i >= 3 {
				break
			}
			t.Members = append(t.Members, ticket.Member{ID: nd.ID, Addrs: nd.Addrs})
		}
		// Put this node first: the requester reached it.
		me, _ := v.Node(n.id)
		t.Members = append([]ticket.Member{{ID: me.ID, Addrs: n.ep.Addrs()}}, t.Members...)
		return AdminReply{Ticket: t.Encode()}, nil

	case "node-remove", "node-drain", "node-weight", "node-zone", "node-repair":
		id, err := nodeIDOf(req.Node)
		if err != nil {
			return AdminReply{}, err
		}
		if req.Op == "node-repair" {
			n.broadcastCluster(ctx, &wire.Msg{Type: wire.TViewChanged, Node: id[:]})
			return AdminReply{Text: "repair scheduled: holders will refill " + view.ShortID(id)}, nil
		}
		fwd := &wire.Msg{Type: wire.TAdmin, Params: codec.MustMarshal(req)}
		nv, err := n.maint.runAsHolder(ctx, fwd, func(ctx context.Context) (*view.View, error) {
			return n.maint.nodeChange(ctx, req.Op, id, req)
		})
		if err != nil {
			return AdminReply{}, err
		}
		enc, _ := nv.Encode()
		return AdminReply{View: enc, Text: fmt.Sprintf("transition proposed at epoch %d", nv.Epoch)}, nil

	case "voter-add", "voter-remove":
		id, err := nodeIDOf(req.Node)
		if err != nil {
			return AdminReply{}, err
		}
		fwd := &wire.Msg{Type: wire.TAdmin, Params: codec.MustMarshal(req)}
		nv, err := n.maint.runAsHolder(ctx, fwd, func(ctx context.Context) (*view.View, error) {
			if req.Op == "voter-add" {
				if err := n.maint.addVoter(ctx, id); err != nil {
					return nil, err
				}
			} else {
				if err := n.maint.removeVoter(ctx, id, req.AllowUnsafe); err != nil {
					return nil, err
				}
			}
			return n.View(), nil
		})
		if err != nil {
			return AdminReply{}, err
		}
		enc, _ := nv.Encode()
		return AdminReply{View: enc, Text: fmt.Sprintf("voters now %d", len(nv.Voters))}, nil

	case "replicas":
		fwd := &wire.Msg{Type: wire.TAdmin, Params: codec.MustMarshal(req)}
		nv, err := n.maint.runAsHolder(ctx, fwd, func(ctx context.Context) (*view.View, error) {
			return n.maint.proposeTransition(ctx, fmt.Sprintf("replicas %d", req.Replicas), func(v *view.View, p *view.Pending) error {
				if req.Replicas == 0 {
					return errors.New("replicas must be ≥ 1")
				}
				p.Replicas = req.Replicas
				return nil
			})
		})
		if err != nil {
			return AdminReply{}, err
		}
		enc, _ := nv.Encode()
		return AdminReply{View: enc, Text: "transition proposed"}, nil

	case "transition-status":
		enc, _ := v.Encode()
		return AdminReply{View: enc, Text: n.maint.transitionText(v)}, nil

	case "transition-abort":
		nv, err := n.cat.CASView(ctx, func(v *view.View) (bool, error) {
			if v.Pending == nil {
				return false, catalog.ErrAbort
			}
			v.Pending = nil
			return true, nil
		})
		if err != nil && !errors.Is(err, catalog.ErrAbort) {
			return AdminReply{}, err
		}
		if nv != nil {
			n.adopt(nv, "abort")
		}
		return AdminReply{Text: "transition aborted"}, nil

	case "transition-refreeze":
		nv, err := n.maint.refreeze(ctx, true)
		if err != nil {
			return AdminReply{}, err
		}
		enc, _ := nv.Encode()
		return AdminReply{View: enc, Text: "participants re-frozen"}, nil

	case "transition-pause", "transition-resume", "rate-cap":
		nv, err := n.cat.CASView(ctx, func(v *view.View) (bool, error) {
			switch req.Op {
			case "transition-pause":
				v.RebalancePause = true
			case "transition-resume":
				v.RebalancePause = false
			case "rate-cap":
				v.RateCap = req.Rate
			}
			return false, nil
		})
		if err != nil {
			return AdminReply{}, err
		}
		n.adopt(nv, "knob")
		return AdminReply{Text: "ok"}, nil

	case "gc-run":
		fwd := &wire.Msg{Type: wire.TAdmin, Params: codec.MustMarshal(req)}
		_, err := n.maint.runAsHolder(ctx, fwd, func(ctx context.Context) (*view.View, error) {
			if req.Garbage > 0 {
				return n.View(), n.gc.resweep(ctx, req.Garbage)
			}
			return n.View(), n.gc.runCycle(ctx, req.Tolerate, true)
		})
		if err != nil {
			return AdminReply{}, err
		}
		g, _ := n.cat.ReadGC(ctx)
		return AdminReply{GC: codec.MustMarshal(g), Text: n.gc.statusText(g)}, nil

	case "gc-status":
		g, err := n.cat.ReadGC(ctx)
		if err != nil {
			return AdminReply{}, err
		}
		return AdminReply{GC: codec.MustMarshal(g), Text: n.gc.statusText(g)}, nil

	case "gc-hold":
		_, err := n.cat.CASGC(ctx, func(g *catalog.GCState) error { g.Hold = req.Pause; return nil })
		if err != nil {
			return AdminReply{}, err
		}
		return AdminReply{Text: "ok"}, nil

	case "gc-why":
		if len(req.Key) != 32 {
			return AdminReply{}, errors.New("key must be 32 bytes")
		}
		names, err := n.Why(ctx, [32]byte(req.Key))
		if err != nil {
			return AdminReply{}, err
		}
		return AdminReply{Names: names}, nil

	case "catalog-backup":
		k, err := n.maint.backupCatalog(ctx)
		if err != nil {
			return AdminReply{}, err
		}
		return AdminReply{Key: k[:], Text: "backup written"}, nil

	case "catalog-backups":
		return AdminReply{Names: n.backupKeys()}, nil

	case "keep":
		for _, name := range req.Names {
			if err := n.cat.SetKeep(ctx, name); err != nil {
				return AdminReply{}, err
			}
		}
		return AdminReply{Text: "ok"}, nil
	}
	return AdminReply{}, &wire.Error{Code: wire.CodeBadRequest, Text: "unknown admin op " + req.Op}
}

// broadcastCluster sends a message to every member, ignoring failures.
func (n *Node) broadcastCluster(ctx context.Context, m *wire.Msg) {
	v := n.View()
	if v == nil {
		return
	}
	for _, id := range v.AllMembers() {
		if id == n.id {
			continue
		}
		go func(id view.NodeID) {
			cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			_, _ = n.pool.Call(cctx, id, wire.ALPNCluster, n.stampReq(m))
		}(id)
	}
}

// noteBackup records a catalog backup key in meta (§13).
func (n *Node) noteBackup(k []byte) {
	if len(k) != 32 {
		return
	}
	keyName := fmt.Sprintf("%s%020d", mkBackup, time.Now().UnixNano())
	_ = n.meta.Set([]byte(keyName), k)
	// Keep the last 24.
	var keys [][]byte
	_ = n.meta.Scan([]byte(mkBackup), func(k, _ []byte) bool { keys = append(keys, append([]byte{}, k...)); return true })
	for len(keys) > 24 {
		_ = n.meta.Delete(keys[0])
		keys = keys[1:]
	}
}

func (n *Node) backupKeys() []string {
	var out []string
	_ = n.meta.Scan([]byte(mkBackup), func(_, v []byte) bool { out = append(out, hex.EncodeToString(v)); return true })
	return out
}
