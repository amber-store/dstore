package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/amber-store/core/amberpack"
	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/core/reference"
	"github.com/amber-store/core/refstore"
	"github.com/amber-store/dstore/client"
	"github.com/amber-store/dstore/codec"
	"github.com/amber-store/dstore/node"
	"github.com/amber-store/dstore/ticket"
	"github.com/amber-store/dstore/transport"
	"github.com/amber-store/dstore/view"
	irohkey "github.com/tmc/go-iroh/key"
	"github.com/urfave/cli/v2"
	"golang.org/x/sys/unix"
)

func diskTotal(dir string) int64 {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0
	}
	return int64(st.Blocks) * int64(st.Bsize)
}

// dialCluster connects with an ephemeral identity using --ticket, or a
// ticket derived from a local store's view.
func dialCluster(ctx context.Context, c *cli.Context) (*client.Cluster, error) {
	return dialClusterLog(ctx, c, logger(c))
}

// dialClusterLog is dialCluster with the client logging to log.
func dialClusterLog(ctx context.Context, c *cli.Context, log *slog.Logger) (*client.Cluster, error) {
	var t ticket.Ticket
	var err error
	if s := c.String("ticket"); s != "" {
		t, err = ticket.Parse(s)
		if err != nil {
			return nil, err
		}
	} else if dir := c.String("store"); dir != "" {
		t, err = localTicket(dir)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("no cluster: set --ticket or $DSTORE_TICKET")
	}
	sk, err := irohkey.GenerateSecretKey()
	if err != nil {
		return nil, err
	}
	rm, err := relayMode(c)
	if err != nil {
		return nil, err
	}
	ep, err := transport.BindIroh(ctx, transport.IrohConfig{SecretKey: sk, RelayMode: rm})
	if err != nil {
		return nil, err
	}
	cl, err := client.Dial(ctx, client.Config{Endpoint: ep, Ticket: t, Logger: log, GCInterval: 4 * time.Hour})
	if err != nil {
		ep.Close()
		return nil, err
	}
	return cl, nil
}

func admin(ctx context.Context, cl *client.Cluster, req node.AdminRequest) (node.AdminReply, error) {
	b, err := cl.Admin(ctx, view.NodeID{}, req)
	if err != nil {
		return node.AdminReply{}, err
	}
	var r node.AdminReply
	if err := codec.Unmarshal(b, &r); err != nil {
		return r, err
	}
	return r, nil
}

func adminAction(c *cli.Context, req node.AdminRequest) error {
	ctx, cancel := signalCtx()
	defer cancel()
	cl, err := dialCluster(ctx, c)
	if err != nil {
		return err
	}
	defer cl.Close()
	r, err := admin(ctx, cl, req)
	if err != nil {
		return err
	}
	if r.Text != "" {
		fmt.Println(r.Text)
	}
	for _, n := range r.Names {
		fmt.Println(n)
	}
	if len(r.Key) == 32 {
		fmt.Printf("key %x\n", r.Key)
	}
	return nil
}

func printStatus(ctx context.Context, cl *client.Cluster) error {
	v := cl.View()
	fmt.Printf("cluster %x incarnation %d epoch %d version %d\n", v.ClusterID[:4], v.Incarnation, v.Epoch, v.Version)
	fmt.Printf("replicas %d min_replicas %d nodes %d voters %d", v.Replicas, v.MinReplicas, len(v.Nodes), len(v.Voters))
	if len(v.Voters) < 3 {
		fmt.Print(" (no catalog fault tolerance)")
	}
	fmt.Println()
	if v.Pending != nil {
		fmt.Printf("transition %d (%s): frozen=%v acked=%d participants=%d done=%d\n", v.Pending.ID, v.Pending.Reason, v.Pending.Frozen, len(v.Pending.ParticipantsAck), len(v.Pending.Participants), len(v.Pending.Done))
	}
	if v.VoterSync == view.VoterSyncPending {
		fmt.Printf("voter change in progress (target %s)\n", node.ShortID(v.VoterSyncTarget))
	}
	for _, nd := range v.Nodes {
		id := nd.NID()
		line := fmt.Sprintf("  %s weight %d zone %q voter=%v writable=%v", view.IDString(id), nd.Weight, nd.Zone, v.IsVoter(id), nd.Writable)
		sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		b, err := cl.Status(sctx, id)
		cancel()
		if err != nil {
			fmt.Println(line, "— unreachable:", err)
			continue
		}
		st, err := node.DecodeStatus(b)
		if err != nil {
			fmt.Println(line, "— bad status")
			continue
		}
		fmt.Println(line)
		fmt.Printf("      epoch %d packs %d records %d bytes %d pins %d pending-packs %d free %d GiB", st.Epoch, st.Packs, st.Records, st.Bytes, st.Pins, st.PendingPacks, st.FreeBytes>>30)
		if st.IsHolder {
			fmt.Print(" [lease holder]")
		}
		if st.Amnesiac {
			fmt.Print(" [AMNESIAC]")
		}
		if st.Retired {
			fmt.Print(" [retired]")
		}
		fmt.Println()
		if len(st.Unreachable) > 0 {
			var ids []string
			for _, u := range st.Unreachable {
				ids = append(ids, node.ShortID(u))
			}
			fmt.Printf("      cannot reach: %s\n", strings.Join(ids, " "))
		}
		if st.GC != "" {
			fmt.Printf("      gc: %s\n", st.GC)
		}
		if st.Transition != "" && st.Transition != "idle" {
			fmt.Printf("      transition: %s\n", st.Transition)
		}
		for _, vs := range st.Voters {
			fmt.Printf("      voter %s: %d calls, %d failures, p99 %d ms\n", node.ShortID(vs.ID), vs.Calls, vs.Failures, vs.P99ms)
		}
	}
	return nil
}

func recordPayload(rec []byte) ([]byte, error) {
	r, err := amberpack.ParseRecord(rec)
	if err != nil {
		return nil, err
	}
	return amberpack.DecodePayload(r.Flags, r.Ulen, rec[amberpack.RecHeaderSize:])
}

// ---- client commands ----

func localStoreFlag() cli.Flag {
	return &cli.StringFlag{Name: "local", Usage: "local store directory (layout: <dir>/packstore, <dir>/refs)", EnvVars: []string{"AMBER_STORE"}, Required: true}
}

func openLocal(c *cli.Context) (*packstore.Store, *refstore.Store, error) {
	dir := c.String("local")
	st, err := packstore.Open(filepath.Join(dir, "packstore"), packstore.WithSync(true))
	if err != nil {
		return nil, nil, err
	}
	refs, err := refstore.Open(filepath.Join(dir, "refs"), true)
	if err != nil {
		st.Close()
		return nil, nil, err
	}
	return st, refs, nil
}

func pushCmd() *cli.Command {
	return &cli.Command{
		Name:      "push",
		Usage:     "build a tree from PATH into the local store and push it under NAME",
		ArgsUsage: "PATH NAME",
		Flags: append(clientFlags(), localStoreFlag(),
			&cli.StringFlag{Name: "user", Usage: "user identity recorded in the reference"},
			&cli.BoolFlag{Name: "force", Usage: "replace unconditionally"},
			&cli.StringFlag{Name: "expected-version", Usage: "CAS: the version ref get printed (hex); omit to require the name to be new"},
			&cli.IntFlag{Name: "jobs"}, noTUIFlag(),
		),
		Action: func(c *cli.Context) error {
			if c.NArg() != 2 {
				return errors.New("push PATH NAME")
			}
			path, name := c.Args().Get(0), c.Args().Get(1)
			if err := reference.ValidateName(name); err != nil {
				return err
			}
			ctx, cancel := signalCtx()
			defer cancel()
			st, refs, err := openLocal(c)
			if err != nil {
				return err
			}
			defer st.Close()
			defer refs.Close()
			root, stats, err := ingest.Dir(st, path, ingest.Opts{Jobs: c.Int("jobs")})
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "built %s: %d new objects\n", root.String()[:16], stats.Stored)
			cond := client.Cond{Force: c.Bool("force")}
			if !cond.Force {
				cond.Versioned = true
				if ev := c.String("expected-version"); ev != "" {
					b, err := hexDecode(ev)
					if err != nil {
						return err
					}
					cond.ExpectedVersion = b
				}
			}
			var ps client.PushStats
			err = runTransfer(ctx, c, "push "+name, func(ctx context.Context, log *slog.Logger, prog client.Progress) error {
				cl, err := dialClusterLog(ctx, c, log)
				if err != nil {
					return err
				}
				defer cl.Close()
				ps, err = cl.Push(ctx, st, root, name, c.String("user"), cond, prog)
				return err
			})
			if err != nil {
				var cm *client.CASMismatch
				if errors.As(err, &cm) {
					return fmt.Errorf("%w (pull first, or --force)", err)
				}
				return err
			}
			rec := reference.Reference{Name: name, Key: root[:], User: c.String("user"), CreatedAt: time.Now().UnixNano()}
			if enc, err := rec.Encode(); err == nil {
				_ = refs.Put(name, enc)
			}
			fmt.Printf("pushed %s: %d objects, %d uploaded, version %x\n", name, ps.Keys, ps.Uploaded, ps.Version)
			return nil
		},
	}
}

func pullCmd() *cli.Command {
	return &cli.Command{
		Name:      "pull",
		Usage:     "pull the tree under NAME into the local store",
		ArgsUsage: "NAME",
		Flags:     append(clientFlags(), localStoreFlag(), &cli.IntFlag{Name: "jobs"}, noTUIFlag()),
		Action: func(c *cli.Context) error {
			name := c.Args().First()
			if name == "" {
				return errors.New("pull NAME")
			}
			ctx, cancel := signalCtx()
			defer cancel()
			st, refs, err := openLocal(c)
			if err != nil {
				return err
			}
			defer st.Close()
			defer refs.Close()
			var ps client.PullStats
			err = runTransfer(ctx, c, "pull "+name, func(ctx context.Context, log *slog.Logger, prog client.Progress) error {
				cl, err := dialClusterLog(ctx, c, log)
				if err != nil {
					return err
				}
				defer cl.Close()
				ps, err = cl.Pull(ctx, st, name, prog)
				return err
			})
			if err != nil {
				return err
			}
			if err := refs.Put(name, ps.Record); err != nil {
				return err
			}
			fmt.Printf("pulled %s: root %s, %d objects fetched (%d bytes)\n", name, ps.Root, ps.Fetched, ps.Bytes)
			return nil
		},
	}
}

func refsCmd() *cli.Command {
	return &cli.Command{
		Name:      "refs",
		Usage:     "list references",
		ArgsUsage: "[PREFIX]",
		Flags:     clientFlags(),
		Action: func(c *cli.Context) error {
			ctx, cancel := signalCtx()
			defer cancel()
			cl, err := dialCluster(ctx, c)
			if err != nil {
				return err
			}
			defer cl.Close()
			refs, err := cl.RefList(ctx, c.Args().First())
			if err != nil {
				return err
			}
			for _, r := range refs {
				fmt.Printf("%s\t%x\t%s\t%s\n", r.Name, r.Key, time.Unix(0, r.CreatedAt).Format(time.RFC3339), r.User)
			}
			return nil
		},
	}
}

func refCmd() *cli.Command {
	return &cli.Command{
		Name:  "ref",
		Usage: "get or delete a reference",
		Subcommands: []*cli.Command{
			{Name: "get", ArgsUsage: "NAME", Flags: clientFlags(),
				Action: func(c *cli.Context) error {
					ctx, cancel := signalCtx()
					defer cancel()
					cl, err := dialCluster(ctx, c)
					if err != nil {
						return err
					}
					defer cl.Close()
					r, err := cl.RefGet(ctx, c.Args().First())
					if err != nil {
						return err
					}
					fmt.Printf("name %s\nkey %x\nversion %x\nuser %s\ncreated %s\n", r.Name, r.Ref.Key, r.Version, r.Ref.User, time.Unix(0, r.Ref.CreatedAt).Format(time.RFC3339))
					return nil
				}},
			{Name: "delete", ArgsUsage: "NAME", Flags: append(clientFlags(), &cli.BoolFlag{Name: "force"}, &cli.StringFlag{Name: "expected-version"}),
				Action: func(c *cli.Context) error {
					ctx, cancel := signalCtx()
					defer cancel()
					cl, err := dialCluster(ctx, c)
					if err != nil {
						return err
					}
					defer cl.Close()
					cond := client.Cond{Force: c.Bool("force")}
					if ev := c.String("expected-version"); ev != "" {
						b, err := hexDecode(ev)
						if err != nil {
							return err
						}
						cond.Versioned, cond.ExpectedVersion = true, b
					} else if !cond.Force {
						cond.Force = true
					}
					return cl.RefDelete(ctx, c.Args().First(), cond)
				}},
		},
	}
}

// clusterGet reads one object's payload from the cluster.
func clusterGet(ctx context.Context, cl *client.Cluster) func(key.Key) ([]byte, error) {
	return func(k key.Key) ([]byte, error) {
		seq, missing := cl.Get(ctx, [][32]byte{[32]byte(k)})
		for r, err := range seq {
			if err != nil {
				return nil, err
			}
			return recordPayload(r.Record)
		}
		if len(missing()) > 0 {
			return nil, fmt.Errorf("object %s not found", k)
		}
		return nil, errors.New("no data")
	}
}

func lsCmd() *cli.Command {
	return &cli.Command{
		Name:      "ls",
		Usage:     "list a directory of a pushed tree",
		ArgsUsage: "NAME [PATH]",
		Flags:     clientFlags(),
		Action: func(c *cli.Context) error {
			ctx, cancel := signalCtx()
			defer cancel()
			cl, err := dialCluster(ctx, c)
			if err != nil {
				return err
			}
			defer cl.Close()
			r, err := cl.RefGet(ctx, c.Args().First())
			if err != nil {
				return err
			}
			root, err := key.Parse(r.Ref.Key)
			if err != nil {
				return err
			}
			get := clusterGet(ctx, cl)
			dir := root
			if p := c.Args().Get(1); p != "" && p != "/" {
				dir, err = fstree.ResolvePath(root, strings.Trim(p, "/"), get)
				if err != nil {
					return err
				}
			}
			entries, err := fstree.CollectEntries(dir, get)
			if err != nil {
				return err
			}
			for _, e := range entries {
				fmt.Println(string(e.Name))
			}
			return nil
		},
	}
}

func catCmd() *cli.Command {
	return &cli.Command{
		Name:      "cat",
		Usage:     "write a file of a pushed tree to stdout",
		ArgsUsage: "NAME PATH",
		Flags:     clientFlags(),
		Action: func(c *cli.Context) error {
			if c.NArg() != 2 {
				return errors.New("cat NAME PATH")
			}
			ctx, cancel := signalCtx()
			defer cancel()
			cl, err := dialCluster(ctx, c)
			if err != nil {
				return err
			}
			defer cl.Close()
			r, err := cl.RefGet(ctx, c.Args().First())
			if err != nil {
				return err
			}
			root, err := key.Parse(r.Ref.Key)
			if err != nil {
				return err
			}
			get := clusterGet(ctx, cl)
			e, err := fstree.ResolveEntry(root, strings.Trim(c.Args().Get(1), "/"), get)
			if err != nil {
				return err
			}
			if len(e.ContentKey) != 32 {
				return errors.New("not a regular file with content")
			}
			k, err := key.Parse(e.ContentKey)
			if err != nil {
				return err
			}
			return fstree.WriteContent(os.Stdout, k, get)
		},
	}
}

func hexDecode(s string) ([]byte, error) {
	b := make([]byte, len(s)/2)
	for i := 0; i < len(b); i++ {
		var v byte
		for j := 0; j < 2; j++ {
			ch := s[2*i+j]
			switch {
			case ch >= '0' && ch <= '9':
				v = v<<4 | (ch - '0')
			case ch >= 'a' && ch <= 'f':
				v = v<<4 | (ch - 'a' + 10)
			case ch >= 'A' && ch <= 'F':
				v = v<<4 | (ch - 'A' + 10)
			default:
				return nil, fmt.Errorf("bad hex %q", s)
			}
		}
		b[i] = v
	}
	return b, nil
}
