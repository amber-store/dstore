package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/amber-store/core/reference"
	"github.com/amber-store/dstore/wire"
)

// Ref is a reference as the cluster returns it.
type Ref struct {
	Name    string
	Record  []byte
	Version []byte
	Ref     reference.Reference
}

// ErrUnknownRef reports an absent reference.
var ErrUnknownRef = errors.New("client: unknown reference")

// CASMismatch reports a reference CAS that found another value.
type CASMismatch struct {
	Current    []byte // current key, nil when absent
	Record     []byte
	Version    []byte
	HasCurrent bool
}

func (e *CASMismatch) Error() string {
	if !e.HasCurrent {
		return "cas mismatch: reference is absent"
	}
	return fmt.Sprintf("cas mismatch: current key %x", e.Current)
}

// Incomplete reports a reference write whose tree is not complete in the
// cluster.
type Incomplete struct {
	Sample    [][32]byte
	Shortfall int
}

func (e *Incomplete) Error() string {
	return fmt.Sprintf("incomplete: %d keys short", e.Shortfall)
}

// Cond is a reference CAS condition.
type Cond struct {
	ExpectedVersion []byte
	Versioned       bool
	ExpectedOld     []byte
	Keyed           bool
	Force           bool
}

func (cond Cond) apply(m *wire.Msg) {
	m.Force = cond.Force
	if cond.Versioned {
		m.HasExpected = true
		m.ExpectedVersion = cond.ExpectedVersion
	}
	if cond.Keyed {
		m.HasExpected = true
		m.ExpectedOld = cond.ExpectedOld
	}
}

func refErr(err error) error {
	if wire.IsCode(err, wire.CodeUnknownRef) {
		return ErrUnknownRef
	}
	return err
}

// RefGet reads a reference through any node.
func (c *Cluster) RefGet(ctx context.Context, name string) (*Ref, error) {
	resp, err := c.anyNode(ctx, &wire.Msg{Type: wire.TRefGet, Name: name})
	if err != nil {
		return nil, refErr(err)
	}
	if resp.Type != wire.TRef {
		return nil, fmt.Errorf("client: unexpected reply %d", resp.Type)
	}
	r, err := reference.Decode(resp.Record)
	if err != nil {
		return nil, err
	}
	return &Ref{Name: name, Record: resp.Record, Version: resp.Version, Ref: r}, nil
}

// RefPut writes a reference record subject to cond; the coordinating node
// checks the tree's completeness first.
func (c *Cluster) RefPut(ctx context.Context, record []byte, cond Cond) (version []byte, err error) {
	m := &wire.Msg{Type: wire.TRefPut, Record: record}
	cond.apply(m)
	resp, err := c.anyNode(ctx, m)
	if err != nil {
		return nil, refErr(err)
	}
	switch resp.Type {
	case wire.TOK:
		return resp.Version, nil
	case wire.TCASMismatch:
		return nil, &CASMismatch{Current: resp.Current, Record: resp.Record, Version: resp.Version, HasCurrent: resp.HasCurrent}
	case wire.TIncomplete:
		sample, _ := wire.Keys32(resp.Keys)
		return nil, &Incomplete{Sample: sample, Shortfall: resp.Shortfall}
	}
	return nil, fmt.Errorf("client: unexpected reply %d", resp.Type)
}

// RefDelete deletes a reference subject to cond.
func (c *Cluster) RefDelete(ctx context.Context, name string, cond Cond) error {
	m := &wire.Msg{Type: wire.TRefDelete, Name: name}
	cond.apply(m)
	resp, err := c.anyNode(ctx, m)
	if err != nil {
		return refErr(err)
	}
	switch resp.Type {
	case wire.TOK:
		return nil
	case wire.TCASMismatch:
		return &CASMismatch{Current: resp.Current, Record: resp.Record, Version: resp.Version, HasCurrent: resp.HasCurrent}
	}
	return fmt.Errorf("client: unexpected reply %d", resp.Type)
}

// RefList lists references with the prefix, following pages.
func (c *Cluster) RefList(ctx context.Context, prefix string) ([]wire.RefInfo, error) {
	var out []wire.RefInfo
	var after []byte
	for {
		resp, err := c.anyNode(ctx, &wire.Msg{Type: wire.TRefList, Prefix: []byte(prefix), After: after})
		if err != nil {
			return nil, err
		}
		if resp.Type != wire.TRefs {
			return nil, fmt.Errorf("client: unexpected reply %d", resp.Type)
		}
		out = append(out, resp.Refs...)
		if len(resp.Next) == 0 || len(resp.Refs) == 0 {
			break
		}
		after = resp.Next
	}
	return out, nil
}
