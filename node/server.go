package node

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"

	"github.com/amber-store/dstore/transport"
	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
)

// acceptLoop accepts connections on both ALPNs.
func (n *Node) acceptLoop() {
	defer n.wg.Done()
	for {
		c, err := n.ep.Accept(n.ctx)
		if err != nil {
			if n.ctx.Err() != nil {
				return
			}
			if errors.Is(err, transport.ErrClosed) {
				return
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
		n.wg.Add(1)
		go n.serveConn(c)
	}
}

func (n *Node) serveConn(c transport.Conn) {
	defer n.wg.Done()
	defer c.Close()
	alpn := c.ALPN()
	remote := c.RemoteID()
	if alpn != wire.ALPNClient && alpn != wire.ALPNCluster {
		return
	}
	n.markReachable(remote)
	for {
		s, err := c.AcceptStream(n.ctx)
		if err != nil {
			return
		}
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			n.serveStream(alpn, remote, s)
		}()
	}
}

// admission decides what a peer may do on the cluster ALPN (§2).
type admission int

const (
	admitNone admission = iota
	admitMember
	admitFormer
)

func (n *Node) admit(remote view.NodeID) admission {
	v := n.View()
	if v == nil {
		// A joining node holds nothing yet and must accept the voter
		// sync's install, seed and marker before it has a view.
		return admitMember
	}
	if v.IsMember(remote) || v.IsVoter(remote) || view.Contains(v.DeferredVoters, remote) {
		return admitMember
	}
	if v.IsFormer(remote) {
		return admitFormer
	}
	return admitNone
}

// serveStream serves one operation.
func (n *Node) serveStream(alpn string, remote view.NodeID, s transport.Stream) {
	defer wire.CloseStream(s)
	m, err := wire.ReadMsg(s)
	if err != nil {
		return
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(n.ctx, 2*n.cfg.PutTTL)
	defer cancel()
	var herr error
	if alpn == wire.ALPNCluster {
		herr = n.serveCluster(ctx, remote, s, m)
	} else {
		herr = n.serveClient(ctx, remote, s, m)
	}
	if herr != nil && !errors.Is(herr, io.EOF) {
		n.log.Debug("operation failed", "op", m.Type, "peer", view.ShortID(remote), "error", herr, "took", time.Since(start))
	}
}

func (n *Node) serveClient(ctx context.Context, remote view.NodeID, s transport.Stream, m *wire.Msg) error {
	if v := n.View(); v != nil && v.ACL != nil && len(v.ACL.Allowed) > 0 {
		if !view.Contains(v.ACL.Allowed, remote) && !view.Contains(v.ACL.Admins, remote) {
			return wire.WriteErr(s, wire.CodeUnauthorized, "not on the allowlist")
		}
		if m.Type == wire.TRefDelete && !view.Contains(v.ACL.Admins, remote) {
			return wire.WriteErr(s, wire.CodeUnauthorized, "ref-delete needs an admin peer")
		}
	}
	switch m.Type {
	case wire.TView:
		return n.handleView(s)
	case wire.TMissing:
		return n.handleMissing(ctx, s, m, true)
	case wire.TGet:
		return n.handleGet(ctx, s, m)
	case wire.TPut:
		return n.handlePut(ctx, s, m, true, remote)
	case wire.TRefGet:
		return n.handleRefGet(ctx, s, m)
	case wire.TRefPut:
		return n.handleRefPut(ctx, s, m)
	case wire.TRefDelete:
		return n.handleRefDelete(ctx, s, m)
	case wire.TRefList:
		return n.handleRefList(ctx, s, m)
	case wire.TStatus:
		return n.handleStatus(ctx, s)
	case wire.TAdmin:
		return n.handleAdmin(ctx, s, m)
	case wire.TPing:
		return wire.WriteMsg(s, n.stampReply(&wire.Msg{Type: wire.TPong}))
	}
	return wire.WriteErr(s, wire.CodeBadRequest, "unknown operation")
}

func (n *Node) serveCluster(ctx context.Context, remote view.NodeID, s transport.Stream, m *wire.Msg) error {
	adm := n.admit(remote)
	if m.Type == wire.TJoin {
		return n.handleJoin(ctx, remote, s, m)
	}
	switch adm {
	case admitNone:
		return wire.WriteErr(s, wire.CodeNotMember, "not a member of the view")
	case admitFormer:
		if m.Type != wire.TMissing && m.Type != wire.TPut && m.Type != wire.TView {
			return wire.WriteErr(s, wire.CodeNotMember, "former member: missing/put only")
		}
	}
	switch m.Type {
	case wire.TPrepare, wire.TAccept, wire.TRead, wire.TScan, wire.TInstall, wire.TPurge, wire.TMarker, wire.TSeed:
		return wire.WriteMsg(s, n.acceptor.Handle(m))
	case wire.TView:
		return n.handleView(s)
	case wire.TMissing:
		if err := n.checkEpoch(m); err != nil {
			return n.writeStale(s)
		}
		return n.handleMissing(ctx, s, m, false)
	case wire.TGet:
		return n.handleGet(ctx, s, m)
	case wire.TPut:
		return n.handlePut(ctx, s, m, false, remote)
	case wire.TGCBarrier:
		return n.gc.handleBarrier(ctx, s, m)
	case wire.TGCMark:
		return n.gc.handleMark(ctx, s, m)
	case wire.TGCKeys:
		return n.gc.handleKeys(ctx, s, m)
	case wire.TGCStatus:
		return n.gc.handleStatus(ctx, s, m)
	case wire.TGCAbort:
		return n.gc.handleAbort(ctx, s, m)
	case wire.TViewChanged:
		go n.refreshView()
		return wire.WriteMsg(s, &wire.Msg{Type: wire.TAck})
	case wire.TPing:
		return wire.WriteMsg(s, n.stampReply(&wire.Msg{Type: wire.TPong}))
	case wire.TBackupNote:
		n.noteBackup(m.Key)
		return wire.WriteMsg(s, &wire.Msg{Type: wire.TAck})
	case wire.TStatus:
		return n.handleStatus(ctx, s)
	case wire.TAdmin:
		return n.handleAdmin(ctx, s, m)
	}
	return wire.WriteErr(s, wire.CodeBadRequest, "unknown cluster operation")
}

// stampReply adds this node's (incarnation, epoch) to a reply.
func (n *Node) stampReply(m *wire.Msg) *wire.Msg {
	if v := n.View(); v != nil {
		m.Incarnation = v.Incarnation
		m.Epoch = v.Epoch
	}
	return m
}

// errStale reports a request at an older epoch than the node's.
var errStale = errors.New("stale view")

// checkEpoch compares a request's epoch with the node's (§3): older is
// refused with stale-view; newer triggers a single-flight refresh.
func (n *Node) checkEpoch(m *wire.Msg) error {
	v := n.View()
	if v == nil {
		return errStale
	}
	if len(m.ClusterID) > 0 && !bytes.Equal(m.ClusterID, v.ClusterID) {
		return errors.New("wrong cluster")
	}
	switch c := v.Compare(m.Incarnation, m.Epoch); {
	case c > 0:
		return errStale
	case c < 0:
		n.refreshOnce()
		nv := n.View()
		if nv.Compare(m.Incarnation, m.Epoch) < 0 {
			return errors.New("epoch above the catalog's")
		}
		if nv.Compare(m.Incarnation, m.Epoch) > 0 {
			return errStale
		}
	}
	return nil
}

var refreshGate = make(chan struct{}, 1)

// refreshOnce runs one view refresh, at most every 100 ms across callers.
func (n *Node) refreshOnce() {
	select {
	case refreshGate <- struct{}{}:
		n.refreshView()
		time.AfterFunc(100*time.Millisecond, func() { <-refreshGate })
	default:
		time.Sleep(100 * time.Millisecond)
	}
}

func (n *Node) writeStale(s io.Writer) error {
	v := n.View()
	enc, _ := v.Encode()
	return wire.WriteMsg(s, n.stampReply(&wire.Msg{Type: wire.TErr, Code: wire.CodeStaleView, Text: "request epoch is behind", View: enc}))
}

func (n *Node) handleView(s io.Writer) error {
	v := n.View()
	if v == nil {
		return wire.WriteErr(s, wire.CodeUnavailable, "no view")
	}
	enc, err := v.Encode()
	if err != nil {
		return err
	}
	return wire.WriteMsg(s, n.stampReply(&wire.Msg{Type: wire.TViewReply, View: enc, Unreachable: n.Unreachable()}))
}
