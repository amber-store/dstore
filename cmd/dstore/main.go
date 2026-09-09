// Command dstore is the dstore node and client (architecture/dstore.md §13).
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/amber-store/dstore/node"
	"github.com/amber-store/dstore/ticket"
	"github.com/amber-store/dstore/transport"
	"github.com/amber-store/dstore/view"
	"github.com/amber-store/dstore/wire"
	irohkey "github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
	"github.com/urfave/cli/v2"
)

// version is set at build time (-ldflags "-X main.version=...").
var version = "dev"

func main() {
	app := &cli.App{
		Name:    "dstore",
		Usage:   "a distributed amber store: cluster nodes and the client",
		Version: version,
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "log-level", Value: "info", Usage: "debug|info|warn|error (a global flag: give it before the command)", EnvVars: []string{"DSTORE_LOG_LEVEL"}},
		},
		Commands: []*cli.Command{
			clusterCmd(), serveCmd(), tokenCmd(), nodeCmd(), voterCmd(), transitionCmd(), gcCmd(), catalogCmd(),
			pushCmd(), pullCmd(), refsCmd(), refCmd(), lsCmd(), catCmd(),
		},
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "dstore:", err)
		os.Exit(1)
	}
}

func logLevel(c *cli.Context) slog.Level {
	switch strings.ToLower(c.String("log-level")) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	}
	return slog.LevelInfo
}

func logger(c *cli.Context) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel(c)}))
}

// ---- shared flags ----

func storeFlag() cli.Flag {
	return &cli.StringFlag{Name: "store", Usage: "store directory", EnvVars: []string{"DSTORE_STORE"}}
}

func ticketFlag() cli.Flag {
	return &cli.StringFlag{Name: "ticket", Usage: "cluster ticket (dstore1…)", EnvVars: []string{"DSTORE_TICKET"}}
}

// clientFlags are the flags of every command that dials the cluster.
func clientFlags() []cli.Flag {
	return []cli.Flag{
		ticketFlag(),
		&cli.StringFlag{Name: "relay", Usage: "relay URL for the fallback path (default: the built-in relay map)"},
		&cli.BoolFlag{Name: "no-relay", Usage: "direct addresses only, no relay"},
	}
}

func netFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{Name: "relay", Usage: "relay URL for the fallback path (default: the built-in relay map)"},
		&cli.BoolFlag{Name: "no-relay", Usage: "direct addresses only, no relay"},
		&cli.StringSliceFlag{Name: "advertise-addr", Usage: "direct address to advertise, ip or ip:port (repeatable)"},
		&cli.BoolFlag{Name: "loopback", Usage: "advertise 127.0.0.1 only (single-machine tests)"},
		&cli.StringFlag{Name: "bind", Usage: "UDP address to bind, ip:port"},
	}
}

func nodeFlags() []cli.Flag {
	return append([]cli.Flag{
		storeFlag(),
		&cli.StringFlag{Name: "paxos-dir", Usage: "acceptor state directory (default <store>/paxos; put it on its own device)"},
		&cli.Int64Flag{Name: "rate", Usage: "reconcile copy rate in bytes/s (0 = unlimited)"},
		&cli.IntFlag{Name: "jobs", Usage: "parallelism (0 = cores)"},
		&cli.Int64Flag{Name: "min-free", Usage: "free bytes below which uploads are refused (0 = 5% or 100 GiB)"},
		&cli.BoolFlag{Name: "gateway", Usage: "also serve the transport-iroh ALPN (not implemented in this version)"},
		&cli.DurationFlag{Name: "gc-interval", Value: 4 * time.Hour},
		&cli.DurationFlag{Name: "put-ttl", Value: time.Hour},
	}, netFlags()...)
}

func relayMode(c *cli.Context) (*relay.Mode, error) {
	if c.Bool("no-relay") {
		return nil, nil
	}
	if u := c.String("relay"); u != "" {
		ru, err := netaddr.ParseRelayURL(u)
		if err != nil {
			return nil, err
		}
		m := relay.ModeCustomURLs(ru)
		return &m, nil
	}
	m := relay.ModeDefault()
	return &m, nil
}

func loadOrCreateKey(path string) (irohkey.SecretKey, error) {
	b, err := os.ReadFile(path)
	if err == nil {
		return irohkey.ParseSecretKey(strings.TrimSpace(string(b)))
	}
	if !errors.Is(err, os.ErrNotExist) {
		return irohkey.SecretKey{}, err
	}
	sk, err := irohkey.GenerateSecretKey()
	if err != nil {
		return irohkey.SecretKey{}, err
	}
	seed := sk.Bytes()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return irohkey.SecretKey{}, err
	}
	if err := os.WriteFile(path, []byte(hex.EncodeToString(seed[:])+"\n"), 0o600); err != nil {
		return irohkey.SecretKey{}, err
	}
	return sk, nil
}

func bindNodeEndpoint(ctx context.Context, c *cli.Context, dir string) (*transport.IrohEndpoint, error) {
	sk, err := loadOrCreateKey(filepath.Join(dir, "identity"))
	if err != nil {
		return nil, err
	}
	rm, err := relayMode(c)
	if err != nil {
		return nil, err
	}
	cfg := transport.IrohConfig{SecretKey: sk, ALPNs: []string{wire.ALPNClient, wire.ALPNCluster}, RelayMode: rm, Loopback: c.Bool("loopback")}
	if vals := c.StringSlice("advertise-addr"); len(vals) > 0 {
		cfg.Advertise = []netip.AddrPort{}
		for _, v := range vals {
			if ap, err := netip.ParseAddrPort(v); err == nil {
				cfg.Advertise = append(cfg.Advertise, ap)
			} else if ip, err := netip.ParseAddr(v); err == nil {
				cfg.Advertise = append(cfg.Advertise, netip.AddrPortFrom(ip, 0)) // port fixed after bind
			} else {
				return nil, fmt.Errorf("bad --advertise-addr %q", v)
			}
		}
	}
	// A node keeps its UDP port across restarts so that tickets and the
	// addresses in the view stay valid: the first bind picks one, later
	// binds reuse it. --bind overrides.
	portFile := filepath.Join(dir, "port")
	if b := c.String("bind"); b != "" {
		ap, err := netip.ParseAddrPort(b)
		if err != nil {
			return nil, fmt.Errorf("bad --bind %q", b)
		}
		cfg.BindAddr = ap
	} else if pb, err := os.ReadFile(portFile); err == nil {
		if port, err := strconv.ParseUint(strings.TrimSpace(string(pb)), 10, 16); err == nil {
			cfg.BindAddr = netip.AddrPortFrom(netip.IPv4Unspecified(), uint16(port))
		}
	}
	ep, err := transport.BindIroh(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if c.String("bind") == "" {
		_ = os.WriteFile(portFile, []byte(strconv.Itoa(int(ep.Raw().LocalAddr().Port()))+"\n"), 0o644)
	}
	return ep, nil
}

func openNode(ctx context.Context, c *cli.Context) (*node.Node, error) {
	dir := c.String("store")
	if dir == "" {
		return nil, errors.New("no store directory: set --store or $DSTORE_STORE")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	ep, err := bindNodeEndpoint(ctx, c, dir)
	if err != nil {
		return nil, err
	}
	cfg := node.Config{
		StoreDir: dir, PaxosDir: c.String("paxos-dir"), Endpoint: ep, Logger: logger(c),
		Jobs: c.Int("jobs"), Rate: c.Int64("rate"), MinFree: c.Int64("min-free"), Gateway: c.Bool("gateway"),
		GCInterval: c.Duration("gc-interval"), PutTTL: c.Duration("put-ttl"),
	}
	n, err := node.Open(cfg)
	if err != nil {
		ep.Close()
		return nil, err
	}
	return n, nil
}

func signalCtx() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func parseWeight(s string, dir string) (uint32, error) {
	if s == "auto" {
		total := diskTotal(dir)
		if total == 0 {
			return 0, errors.New("--weight auto: cannot stat the store filesystem")
		}
		return uint32(total >> 30), nil
	}
	w, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("bad weight %q (GiB or auto)", s)
	}
	return uint32(w), nil
}

// ---- cluster ----

func clusterCmd() *cli.Command {
	return &cli.Command{
		Name:  "cluster",
		Usage: "init, status, ticket, replicas",
		Subcommands: []*cli.Command{
			{
				Name:  "init",
				Usage: "create a cluster on this store and print its ticket; then run serve",
				Flags: append(nodeFlags(),
					&cli.UintFlag{Name: "replicas", Value: 3, Usage: "R: owners per object"},
					&cli.UintFlag{Name: "min-replicas", Usage: "owners that must hold an object before a write succeeds (default max(R−1, 2))"},
					&cli.StringFlag{Name: "weight", Value: "auto", Usage: "capacity in GiB, or auto"},
					&cli.StringFlag{Name: "zone", Usage: "failure domain (default: the node id)"},
					&cli.BoolFlag{Name: "allow-unsafe", Usage: "allow min-replicas 1"},
				),
				Action: func(c *cli.Context) error {
					ctx, cancel := signalCtx()
					defer cancel()
					n, err := openNode(ctx, c)
					if err != nil {
						return err
					}
					defer n.Close()
					w, err := parseWeight(c.String("weight"), c.String("store"))
					if err != nil {
						return err
					}
					v, err := n.InitCluster(ctx, uint8(c.Uint("replicas")), uint8(c.Uint("min-replicas")), w, c.String("zone"), c.Bool("allow-unsafe"))
					if err != nil {
						return err
					}
					id := n.ID()
					t := ticket.Ticket{ClusterID: v.ClusterID, Incarnation: v.Incarnation, Members: []ticket.Member{{ID: id[:], Addrs: n.Endpoint().Addrs()}}}
					fmt.Println("node id:", view.IDString(id))
					fmt.Println("cluster ticket:", t.Encode())
					fmt.Println("now run: dstore serve --store", c.String("store"))
					return nil
				},
			},
			{
				Name:  "status",
				Usage: "view, epoch, reachability, disk, transition, gc, voters",
				Flags: append(clientFlags(), storeFlag()),
				Action: func(c *cli.Context) error {
					ctx, cancel := signalCtx()
					defer cancel()
					cl, err := dialCluster(ctx, c)
					if err != nil {
						return err
					}
					defer cl.Close()
					return printStatus(ctx, cl)
				},
			},
			{
				Name:  "ticket",
				Usage: "print the bootstrap ticket",
				Flags: append(clientFlags(), storeFlag()),
				Action: func(c *cli.Context) error {
					ctx, cancel := signalCtx()
					defer cancel()
					if c.String("ticket") == "" && c.String("store") != "" {
						t, err := localTicket(c.String("store"))
						if err != nil {
							return err
						}
						fmt.Println(t.Encode())
						return nil
					}
					cl, err := dialCluster(ctx, c)
					if err != nil {
						return err
					}
					defer cl.Close()
					r, err := admin(ctx, cl, node.AdminRequest{Op: "cluster-ticket"})
					if err != nil {
						return err
					}
					fmt.Println(r.Ticket)
					return nil
				},
			},
			{
				Name:      "replicas",
				Usage:     "change R (a transition that copies 1/R of the store)",
				ArgsUsage: "R",
				Flags:     append(clientFlags(), &cli.BoolFlag{Name: "yes", Usage: "do not ask"}),
				Action: func(c *cli.Context) error {
					r, err := strconv.ParseUint(c.Args().First(), 10, 8)
					if err != nil {
						return errors.New("replicas R")
					}
					if !c.Bool("yes") {
						fmt.Printf("changing R to %d moves about 1/%d of every node's data; continue? [y/N] ", r, r)
						var ans string
						fmt.Scanln(&ans)
						if !strings.HasPrefix(strings.ToLower(ans), "y") {
							return errors.New("aborted")
						}
					}
					return adminAction(c, node.AdminRequest{Op: "replicas", Replicas: uint8(r)})
				},
			},
		},
	}
}

// localTicket derives a ticket from a store's persisted view.
func localTicket(dir string) (ticket.Ticket, error) {
	n, err := node.OpenOffline(dir)
	if err != nil {
		return ticket.Ticket{}, err
	}
	defer n.Close()
	v := n.View()
	if v == nil {
		return ticket.Ticket{}, errors.New("this store is not a member of a cluster")
	}
	t := ticket.Ticket{ClusterID: v.ClusterID, Incarnation: v.Incarnation}
	for _, nd := range v.Nodes {
		t.Members = append(t.Members, ticket.Member{ID: nd.ID, Addrs: nd.Addrs})
		if len(t.Members) >= 4 {
			break
		}
	}
	return t, nil
}

// ---- serve / join ----

func serveCmd() *cli.Command {
	return &cli.Command{
		Name:  "serve",
		Usage: "run a node",
		Flags: nodeFlags(),
		Action: func(c *cli.Context) error {
			ctx, cancel := signalCtx()
			defer cancel()
			n, err := openNode(ctx, c)
			if err != nil {
				return err
			}
			if n.View() == nil {
				n.Close()
				return errors.New("this store is not a member of a cluster: run cluster init or node join")
			}
			if err := n.Start(ctx); err != nil {
				n.Close()
				return err
			}
			<-ctx.Done()
			n.Log().Info("shutting down")
			return n.Close()
		},
	}
}

func tokenCmd() *cli.Command {
	return &cli.Command{
		Name:  "token",
		Usage: "join tokens",
		Subcommands: []*cli.Command{{
			Name:  "create",
			Usage: "create a single-use join token",
			Flags: append(clientFlags(), &cli.UintFlag{Name: "weight", Usage: "weight the token imposes on the joiner (GiB)"}),
			Action: func(c *cli.Context) error {
				ctx, cancel := signalCtx()
				defer cancel()
				cl, err := dialCluster(ctx, c)
				if err != nil {
					return err
				}
				defer cl.Close()
				r, err := admin(ctx, cl, node.AdminRequest{Op: "token-create", Weight: uint32(c.Uint("weight"))})
				if err != nil {
					return err
				}
				fmt.Println(r.Text)
				return nil
			},
		}},
	}
}

func nodeCmd() *cli.Command {
	idArg := func(c *cli.Context) ([]byte, error) {
		id, err := view.ParseNodeID(c.Args().First())
		if err != nil {
			return nil, err
		}
		return id[:], nil
	}
	return &cli.Command{
		Name:  "node",
		Usage: "join, remove, drain, weight, zone, repair",
		Subcommands: []*cli.Command{
			{
				Name:  "join",
				Usage: "join a cluster with this store and keep serving",
				Flags: append(nodeFlags(),
					&cli.StringFlag{Name: "seed", Required: true, Usage: "cluster ticket"},
					&cli.StringFlag{Name: "token", Required: true, Usage: "join token (hex)"},
					&cli.StringFlag{Name: "weight", Value: "auto", Usage: "capacity in GiB, or auto"},
					&cli.StringFlag{Name: "zone"},
					&cli.BoolFlag{Name: "no-ramp", Usage: "join at full weight in one step"},
					&cli.BoolFlag{Name: "no-vote", Usage: "hold no catalog (a relay-only or archive box)"},
				),
				Action: func(c *cli.Context) error {
					ctx, cancel := signalCtx()
					defer cancel()
					t, err := ticket.Parse(c.String("seed"))
					if err != nil {
						return err
					}
					tok, err := hex.DecodeString(c.String("token"))
					if err != nil || len(tok) != 32 {
						return errors.New("token must be 32 bytes of hex")
					}
					n, err := openNode(ctx, c)
					if err != nil {
						return err
					}
					w, err := parseWeight(c.String("weight"), c.String("store"))
					if err != nil {
						n.Close()
						return err
					}
					if err := n.Start(ctx); err != nil {
						n.Close()
						return err
					}
					if n.View() == nil {
						var lastErr error
						for _, m := range t.Members {
							if len(m.ID) != 32 {
								continue
							}
							jctx, jcancel := context.WithTimeout(ctx, 10*time.Minute)
							_, err := n.Join(jctx, view.NodeID(m.ID), m.Addrs, tok, w, c.String("zone"), c.Bool("no-vote"), c.Bool("no-ramp"))
							jcancel()
							if err == nil {
								lastErr = nil
								break
							}
							lastErr = err
						}
						if lastErr != nil {
							n.Close()
							return fmt.Errorf("join: %w", lastErr)
						}
					}
					fmt.Println("node id:", view.IDString(n.ID()))
					<-ctx.Done()
					return n.Close()
				},
			},
			{Name: "remove", ArgsUsage: "ID", Flags: append(clientFlags(), &cli.BoolFlag{Name: "dead", Usage: "the node is gone: remove its vote first"}, &cli.BoolFlag{Name: "allow-unsafe"}),
				Action: func(c *cli.Context) error {
					id, err := idArg(c)
					if err != nil {
						return err
					}
					return adminAction(c, node.AdminRequest{Op: "node-remove", Node: id, Dead: c.Bool("dead"), AllowUnsafe: c.Bool("allow-unsafe")})
				}},
			{Name: "drain", ArgsUsage: "ID", Flags: clientFlags(),
				Action: func(c *cli.Context) error {
					id, err := idArg(c)
					if err != nil {
						return err
					}
					return adminAction(c, node.AdminRequest{Op: "node-drain", Node: id})
				}},
			{Name: "weight", ArgsUsage: "ID GiB", Flags: clientFlags(),
				Action: func(c *cli.Context) error {
					id, err := idArg(c)
					if err != nil {
						return err
					}
					w, err := strconv.ParseUint(c.Args().Get(1), 10, 32)
					if err != nil {
						return errors.New("weight ID GiB")
					}
					return adminAction(c, node.AdminRequest{Op: "node-weight", Node: id, Weight: uint32(w)})
				}},
			{Name: "zone", ArgsUsage: "ID ZONE", Flags: clientFlags(),
				Action: func(c *cli.Context) error {
					id, err := idArg(c)
					if err != nil {
						return err
					}
					return adminAction(c, node.AdminRequest{Op: "node-zone", Node: id, Zone: c.Args().Get(1)})
				}},
			{Name: "repair", ArgsUsage: "ID", Flags: clientFlags(),
				Action: func(c *cli.Context) error {
					id, err := idArg(c)
					if err != nil {
						return err
					}
					return adminAction(c, node.AdminRequest{Op: "node-repair", Node: id})
				}},
		},
	}
}

func voterCmd() *cli.Command {
	act := func(op string) cli.ActionFunc {
		return func(c *cli.Context) error {
			id, err := view.ParseNodeID(c.Args().First())
			if err != nil {
				return err
			}
			return adminAction(c, node.AdminRequest{Op: op, Node: id[:], AllowUnsafe: c.Bool("allow-unsafe")})
		}
	}
	return &cli.Command{
		Name:  "voter",
		Usage: "change a node's vote after join",
		Subcommands: []*cli.Command{
			{Name: "add", ArgsUsage: "ID", Flags: clientFlags(), Action: act("voter-add")},
			{Name: "remove", ArgsUsage: "ID", Flags: append(clientFlags(), &cli.BoolFlag{Name: "allow-unsafe"}), Action: act("voter-remove")},
		},
	}
}

func transitionCmd() *cli.Command {
	act := func(op string) cli.ActionFunc {
		return func(c *cli.Context) error { return adminAction(c, node.AdminRequest{Op: op}) }
	}
	return &cli.Command{
		Name:  "transition",
		Usage: "status, abort, refreeze, pause, resume",
		Subcommands: []*cli.Command{
			{Name: "status", Flags: clientFlags(), Action: act("transition-status")},
			{Name: "abort", Flags: clientFlags(), Action: act("transition-abort")},
			{Name: "refreeze", Flags: clientFlags(), Action: act("transition-refreeze")},
			{Name: "pause", Flags: clientFlags(), Action: act("transition-pause")},
			{Name: "resume", Flags: clientFlags(), Action: act("transition-resume")},
		},
	}
}

func gcCmd() *cli.Command {
	return &cli.Command{
		Name:  "gc",
		Usage: "run, status, why, hold, release",
		Subcommands: []*cli.Command{
			{Name: "run", Flags: append(clientFlags(), &cli.BoolFlag{Name: "tolerate-missing"}, &cli.Float64Flag{Name: "garbage", Usage: "re-sweep only, at this dead ratio"}),
				Action: func(c *cli.Context) error {
					return adminAction(c, node.AdminRequest{Op: "gc-run", Tolerate: c.Bool("tolerate-missing"), Garbage: c.Float64("garbage")})
				}},
			{Name: "status", Flags: clientFlags(), Action: func(c *cli.Context) error { return adminAction(c, node.AdminRequest{Op: "gc-status"}) }},
			{Name: "hold", Flags: clientFlags(), Action: func(c *cli.Context) error { return adminAction(c, node.AdminRequest{Op: "gc-hold", Pause: true}) }},
			{Name: "release", Flags: clientFlags(), Action: func(c *cli.Context) error { return adminAction(c, node.AdminRequest{Op: "gc-hold", Pause: false}) }},
			{Name: "why", ArgsUsage: "KEY", Flags: clientFlags(),
				Action: func(c *cli.Context) error {
					k, err := hex.DecodeString(c.Args().First())
					if err != nil || len(k) != 32 {
						return errors.New("why KEY (64 hex chars)")
					}
					return adminAction(c, node.AdminRequest{Op: "gc-why", Key: k})
				}},
		},
	}
}

func catalogCmd() *cli.Command {
	return &cli.Command{
		Name:  "catalog",
		Usage: "backup, restore, backups",
		Subcommands: []*cli.Command{
			{Name: "backup", Flags: clientFlags(), Action: func(c *cli.Context) error { return adminAction(c, node.AdminRequest{Op: "catalog-backup"}) }},
			{Name: "backups", Usage: "list the backup object keys a node remembers", Flags: clientFlags(), Action: func(c *cli.Context) error { return adminAction(c, node.AdminRequest{Op: "catalog-backups"}) }},
			{Name: "restore", ArgsUsage: "KEY|FILE", Usage: "force-write every reference of a backup object", Flags: append(clientFlags(), storeFlag()),
				Action: func(c *cli.Context) error {
					ctx, cancel := signalCtx()
					defer cancel()
					arg := c.Args().First()
					var data []byte
					if b, err := os.ReadFile(arg); err == nil {
						data = b
					} else {
						k, err := hex.DecodeString(arg)
						if err != nil || len(k) != 32 {
							return errors.New("restore KEY|FILE")
						}
						cl, err := dialCluster(ctx, c)
						if err != nil {
							return err
						}
						defer cl.Close()
						seq, _ := cl.Get(ctx, [][32]byte{[32]byte(k)})
						for r, err := range seq {
							if err != nil {
								return err
							}
							data, err = recordPayload(r.Record)
							if err != nil {
								return err
							}
						}
						if data == nil {
							return errors.New("backup object not found in the cluster")
						}
					}
					dir := c.String("store")
					if dir == "" {
						return errors.New("restore runs on a voter: give --store")
					}
					n, err := node.OpenOffline(dir)
					if err != nil {
						return err
					}
					defer n.Close()
					written, err := n.RestoreCatalog(ctx, data)
					fmt.Printf("%d references written\n", written)
					return err
				}},
		},
	}
}
