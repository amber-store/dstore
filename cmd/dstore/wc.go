package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/core/reference"
	"github.com/amber-store/dstore/client"
	"github.com/amber-store/dstore/ticket"
	"github.com/amber-store/dstore/worktree"
	"github.com/urfave/cli/v2"
)

// ---- working-copy commands (architecture/dstore.md §11.7, §13) ----

// wcFlags are the connection flags of the working-copy commands. They bind
// no environment variables: the stored config comes before $DSTORE_TICKET.
func wcFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{Name: "ticket", Usage: "cluster ticket (dstore1…) or comma-separated node ids; overrides the stored one for this run"},
		&cli.StringFlag{Name: "relay", Usage: "relay URL for the fallback path (default: the built-in relay map)"},
		&cli.BoolFlag{Name: "no-relay", Usage: "direct addresses only, no relay"},
		&cli.BoolFlag{Name: "no-discovery", Usage: "neither announce this endpoint nor resolve node ids by discovery"},
	}
}

func jobsFlag() cli.Flag { return &cli.IntFlag{Name: "jobs", Usage: "parallelism (0 = cores)"} }

// resolveTicket applies the precedence: the flag, the stored ticket, the
// environment.
func resolveTicket(flag, stored, env string) (string, error) {
	for _, s := range []string{flag, stored, env} {
		if s != "" {
			return s, nil
		}
	}
	return "", errors.New("no cluster: set --ticket or $DSTORE_TICKET")
}

// wcConfig builds the connection config for a command: the stored config
// (nil for clone and init), overridden by explicit flags, with the
// environment as the last resort.
func wcConfig(c *cli.Context, stored *worktree.Config) (worktree.Config, error) {
	var cfg worktree.Config
	if stored != nil {
		cfg = *stored
	}
	var err error
	if cfg.Ticket, err = resolveTicket(c.String("ticket"), cfg.Ticket, os.Getenv("DSTORE_TICKET")); err != nil {
		return cfg, err
	}
	if c.IsSet("relay") {
		cfg.Relay = c.String("relay")
	}
	if c.IsSet("no-relay") {
		cfg.NoRelay = c.Bool("no-relay")
	}
	if c.IsSet("no-discovery") {
		cfg.NoDiscovery = c.Bool("no-discovery")
	} else if stored == nil {
		if v, err := strconv.ParseBool(os.Getenv("DSTORE_NO_DISCOVERY")); err == nil {
			cfg.NoDiscovery = v
		}
	}
	if c.IsSet("user") {
		cfg.User = c.String("user")
	}
	return cfg, nil
}

func dialConfig(ctx context.Context, cfg worktree.Config, log *slog.Logger) (*client.Cluster, error) {
	t, err := ticket.Parse(cfg.Ticket)
	if err != nil {
		return nil, err
	}
	return dialTicket(ctx, t, netOpts{Relay: cfg.Relay, NoRelay: cfg.NoRelay, NoDiscovery: cfg.NoDiscovery}, log)
}

// openWC opens the working copy containing the current directory.
func openWC() (*worktree.Tree, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	return worktree.Open(wd)
}

// pushUser is the user recorded in the reference: --user, the config, the
// OS user.
func pushUser(c *cli.Context, cfg worktree.Config) (string, error) {
	u := c.String("user")
	if u == "" {
		u = cfg.User
	}
	if u == "" {
		if cu, err := user.Current(); err == nil {
			u = cu.Username
		}
	}
	if err := reference.ValidateUser(u); err != nil {
		return "", fmt.Errorf("user: %w", err)
	}
	return u, nil
}

func cloneCmd() *cli.Command {
	return &cli.Command{
		Name:      "clone",
		Usage:     "clone the tree under NAME into DIR (default: the last segment of NAME) as a working copy",
		ArgsUsage: "NAME [DIR]",
		Flags:     append(wcFlags(), &cli.StringFlag{Name: "user", Usage: "user identity stored for pushes"}, jobsFlag(), noTUIFlag()),
		Action: func(c *cli.Context) error {
			name := c.Args().First()
			if name == "" {
				return errors.New("clone NAME [DIR]")
			}
			if err := reference.ValidateName(name); err != nil {
				return err
			}
			dir := c.Args().Get(1)
			if dir == "" {
				dir = path.Base(name)
			}
			cfg, err := wcConfig(c, nil)
			if err != nil {
				return err
			}
			cfg.Name = name
			ctx, cancel := signalCtx()
			defer cancel()
			var tr *worktree.Tree
			var fr worktree.FetchResult
			err = runTransfer(ctx, c, "clone "+name, func(ctx context.Context, log *slog.Logger, prog client.Progress) error {
				cl, err := dialConfig(ctx, cfg, log)
				if err != nil {
					return err
				}
				defer cl.Close()
				cfg.Ticket = worktree.TicketFromView(cl.View()).Encode()
				tr, fr, err = worktree.Clone(ctx, cl, dir, cfg, prog)
				return err
			})
			if err != nil {
				return err
			}
			defer tr.Close()
			fmt.Printf("cloned %s into %s: %s, %d objects fetched (%d bytes)\n", name, dir, fetchedDesc(fr), fr.Stats.Fetched, fr.Stats.Bytes)
			return nil
		},
	}
}

func initCmd() *cli.Command {
	return &cli.Command{
		Name:      "init",
		Usage:     "make the current directory a working copy of NAME, with nothing synced yet",
		ArgsUsage: "NAME",
		Flags:     append(wcFlags(), &cli.StringFlag{Name: "user", Usage: "user identity stored for pushes"}, noTUIFlag()),
		Action: func(c *cli.Context) error {
			name := c.Args().First()
			if name == "" {
				return errors.New("init NAME")
			}
			if err := reference.ValidateName(name); err != nil {
				return err
			}
			cfg, err := wcConfig(c, nil)
			if err != nil {
				return err
			}
			cfg.Name = name
			wd, err := os.Getwd()
			if err != nil {
				return err
			}
			ctx, cancel := signalCtx()
			defer cancel()
			var tr *worktree.Tree
			var fr worktree.FetchResult
			err = runTransfer(ctx, c, "init "+name, func(ctx context.Context, log *slog.Logger, prog client.Progress) error {
				cl, err := dialConfig(ctx, cfg, log)
				if err != nil {
					return err
				}
				defer cl.Close()
				cfg.Ticket = worktree.TicketFromView(cl.View()).Encode()
				tr, fr, err = worktree.Init(ctx, cl, wd, cfg, prog)
				return err
			})
			if err != nil {
				return err
			}
			defer tr.Close()
			if fr.Exists {
				fmt.Printf("initialised working copy of %s; the reference exists (%s): status shows everything as new, pull merges\n", name, fetchedDesc(fr))
			} else {
				fmt.Printf("initialised working copy of %s; the reference does not exist yet: push creates it\n", name)
			}
			return nil
		},
	}
}

// withCluster opens the working copy, dials with its config and runs fn
// under the progress display, refreshing the stored ticket afterwards.
func withCluster(c *cli.Context, title string, fn func(ctx context.Context, tr *worktree.Tree, cl *client.Cluster, prog client.Progress) error) error {
	tr, err := openWC()
	if err != nil {
		return err
	}
	defer tr.Close()
	cfg, err := wcConfig(c, &tr.Config)
	if err != nil {
		return err
	}
	ctx, cancel := signalCtx()
	defer cancel()
	return runTransfer(ctx, c, title+" "+tr.Config.Name, func(ctx context.Context, log *slog.Logger, prog client.Progress) error {
		cl, err := dialConfig(ctx, cfg, log)
		if err != nil {
			return err
		}
		defer cl.Close()
		if err := fn(ctx, tr, cl, prog); err != nil {
			return err
		}
		return tr.RefreshTicket(cl)
	})
}

func fetchCmd() *cli.Command {
	return &cli.Command{
		Name:  "fetch",
		Usage: "record the reference's current tree as the remote and fetch its objects",
		Flags: append(wcFlags(), noTUIFlag()),
		Action: func(c *cli.Context) error {
			var fr worktree.FetchResult
			var name string
			err := withCluster(c, "fetch", func(ctx context.Context, tr *worktree.Tree, cl *client.Cluster, prog client.Progress) (err error) {
				name = tr.Config.Name
				fr, err = tr.Fetch(ctx, cl, prog)
				return err
			})
			if err != nil {
				return err
			}
			switch {
			case !fr.Exists:
				fmt.Printf("%s does not exist on the cluster\n", name)
			case fr.UpToDate:
				fmt.Printf("%s: up to date (%s)\n", name, fr.Key.String()[:16])
			default:
				fmt.Printf("fetched %s: %s, %d objects fetched (%d bytes)\n", name, fetchedDesc(fr), fr.Stats.Fetched, fr.Stats.Bytes)
			}
			return nil
		},
	}
}

func pullCmd() *cli.Command {
	return &cli.Command{
		Name:  "pull",
		Usage: "fetch and apply the cluster's changes over the working directory",
		Flags: append(wcFlags(), &cli.BoolFlag{Name: "force", Usage: "take the cluster's side on conflicting paths"}, jobsFlag(), noTUIFlag()),
		Action: func(c *cli.Context) error {
			var r worktree.PullResult
			err := withCluster(c, "pull", func(ctx context.Context, tr *worktree.Tree, cl *client.Cluster, prog client.Progress) (err error) {
				r, err = tr.Pull(ctx, cl, c.Bool("force"), c.Int("jobs"), prog)
				return err
			})
			if errors.Is(err, worktree.ErrConflict) {
				fmt.Fprintln(os.Stderr, "conflicts:")
				for _, cf := range r.Conflicts {
					fmt.Fprintf(os.Stderr, "  %s (local: %s, cluster: %s)\n", cf.Path, cf.Local.Kind, cf.Incoming.Kind)
				}
			}
			if err != nil {
				return err
			}
			if r.UpToDate {
				fmt.Println("already up to date")
				return nil
			}
			fmt.Printf("pulled: %d paths updated", len(r.Applied))
			if len(r.Conflicts) > 0 {
				fmt.Printf(", %d conflicts taken from the cluster", len(r.Conflicts))
			}
			fmt.Println()
			return nil
		},
	}
}

func pushCmd() *cli.Command {
	return &cli.Command{
		Name:  "push",
		Usage: "build the working directory's tree, upload it and write the reference",
		Flags: append(wcFlags(),
			&cli.StringFlag{Name: "user", Usage: "user identity recorded in the reference (default: the stored one, then the OS user)"},
			&cli.BoolFlag{Name: "force", Usage: "replace the reference unconditionally"},
			&cli.StringFlag{Name: "message", Aliases: []string{"m"}, Usage: "commit message; on a branch (a reference naming a commit) every push makes a commit, and a message makes one on any reference"},
			jobsFlag(), noTUIFlag()),
		Action: func(c *cli.Context) error {
			var r worktree.PushResult
			var name string
			err := withCluster(c, "push", func(ctx context.Context, tr *worktree.Tree, cl *client.Cluster, prog client.Progress) error {
				name = tr.Config.Name
				u, err := pushUser(c, tr.Config)
				if err != nil {
					return err
				}
				r, err = tr.Push(ctx, cl, u, c.String("message"), c.Bool("force"), c.Int("jobs"), prog)
				return err
			})
			if err != nil {
				return err
			}
			switch {
			case r.Nothing:
				fmt.Println("nothing to push")
			case r.Recovered:
				fmt.Printf("%s already holds %s (an earlier push completed); state updated\n", name, pushedKey(r).String()[:16])
			case r.Commit.Type() == key.Commit:
				fmt.Printf("pushed %s: commit %s, root %s, %d objects, %d uploaded, version %x\n", name, r.Commit.String()[:16], r.Root.String()[:16], r.Stats.Keys, r.Stats.Uploaded, r.Stats.Version)
			default:
				fmt.Printf("pushed %s: root %s, %d objects, %d uploaded, version %x\n", name, r.Root.String()[:16], r.Stats.Keys, r.Stats.Uploaded, r.Stats.Version)
			}
			return nil
		},
	}
}

// fetchedDesc names what a fetch found: the tree, or the commit and its
// tree on a branch.
func fetchedDesc(fr worktree.FetchResult) string {
	if fr.Key.Type() == key.Commit {
		return fmt.Sprintf("commit %s, root %s", fr.Key.String()[:16], fr.Tree.String()[:16])
	}
	return "root " + fr.Key.String()[:16]
}

// pushedKey is what the reference names after a push: the commit on a
// branch, else the tree.
func pushedKey(r worktree.PushResult) key.Key {
	if r.Commit.Type() == key.Commit {
		return r.Commit
	}
	return r.Root
}

func statusCmd() *cli.Command {
	return &cli.Command{
		Name:  "status",
		Usage: "list the working directory's changes since the last sync, and whether the cluster moved",
		Flags: []cli.Flag{jobsFlag()},
		Action: func(c *cli.Context) error {
			tr, err := openWC()
			if err != nil {
				return err
			}
			defer tr.Close()
			st, err := tr.Status(c.Int("jobs"))
			if err != nil {
				return err
			}
			fmt.Printf("reference %s, synced to %s\n", tr.Config.Name, tr.State.Base.String()[:16])
			switch st.Remote {
			case worktree.RemoteUpToDate:
				fmt.Println("remote: up to date")
			case worktree.RemoteAbsent:
				fmt.Println("remote: the reference does not exist on the cluster")
			case worktree.RemoteMoved:
				a, m, d := 0, 0, 0
				for _, ch := range st.Incoming {
					switch ch.Kind {
					case worktree.Added:
						a++
					case worktree.Deleted:
						d++
					default:
						m++
					}
				}
				fmt.Printf("remote: moved since your last fetch (+%d ~%d -%d; run pull)\n", a, m, d)
			}
			if len(st.Changes) > 0 {
				fmt.Println("changes:")
				for _, ch := range st.Changes {
					fmt.Printf("  %-9s %s\n", ch.Kind, describeChange(ch))
				}
			}
			if st.MetaOnly > 0 {
				fmt.Printf("%d paths differ only in mtime, ownership or xattrs\n", st.MetaOnly)
			}
			if len(st.Changes) == 0 && st.MetaOnly == 0 {
				fmt.Println("nothing to push")
			}
			return nil
		},
	}
}

// describeChange renders a status line's path with its detail: a trailing
// slash for directories, the old and new type or mode.
func describeChange(ch worktree.Change) string {
	p := ch.Path
	if worktree.IsDir(ch.New) || (ch.New == nil && worktree.IsDir(ch.Old)) {
		p += "/"
	}
	switch ch.Kind {
	case worktree.TypeChanged:
		return fmt.Sprintf("%s (%s → %s)", p, worktree.TypeName(ch.Old.Mode), worktree.TypeName(ch.New.Mode))
	case worktree.ModeChanged:
		return fmt.Sprintf("%s (%04o → %04o)", p, ch.Old.Mode&0o7777, ch.New.Mode&0o7777)
	}
	return p
}

func diffCmd() *cli.Command {
	return &cli.Command{
		Name:      "diff",
		Usage:     "unified diffs of the working directory against the last synced tree",
		ArgsUsage: "[PATH...]",
		Flags: []cli.Flag{
			&cli.BoolFlag{Name: "remote", Usage: "against the tree last fetched from the cluster"},
			&cli.BoolFlag{Name: "incoming", Usage: "the last synced tree against the fetched one (what pull would apply)"},
			&cli.BoolFlag{Name: "stat", Usage: "one line per changed path with line counts"},
			jobsFlag(),
		},
		Action: func(c *cli.Context) error {
			if c.Bool("remote") && c.Bool("incoming") {
				return errors.New("--remote and --incoming exclude each other")
			}
			tr, err := openWC()
			if err != nil {
				return err
			}
			defer tr.Close()
			var changes []worktree.Change
			var old, new worktree.Source
			treeSrc := worktree.TreeSource{Get: tr.Get}
			switch {
			case c.Bool("incoming"):
				if !tr.State.HasRemote {
					return worktree.ErrNoRemote
				}
				changes, err = worktree.DiffTrees(tr.Get, tr.State.Base, tr.State.Remote)
				old, new = treeSrc, treeSrc
			case c.Bool("remote"):
				if !tr.State.HasRemote {
					return worktree.ErrNoRemote
				}
				changes, err = worktree.Scan(tr.Root, tr.State.Remote, tr.Get, time.Now(), c.Int("jobs"))
				old, new = treeSrc, worktree.DiskSource{Root: tr.Root}
			default:
				changes, err = worktree.Scan(tr.Root, tr.State.Base, tr.Get, tr.State.SyncedAt, c.Int("jobs"))
				old, new = treeSrc, worktree.DiskSource{Root: tr.Root}
			}
			if err != nil {
				return err
			}
			if c.NArg() > 0 {
				changes, err = filterPaths(tr.Root, changes, c.Args().Slice())
				if err != nil {
					return err
				}
			}
			if c.Bool("stat") {
				return worktree.Stat(os.Stdout, changes, old, new)
			}
			return worktree.Unified(os.Stdout, changes, old, new)
		},
	}
}

// filterPaths keeps the changes at or below the given paths, which are
// relative to the current directory.
func filterPaths(root string, changes []worktree.Change, args []string) ([]worktree.Change, error) {
	var prefixes []string
	for _, a := range args {
		abs, err := filepath.Abs(a)
		if err != nil {
			return nil, err
		}
		rel, err := filepath.Rel(root, abs)
		if err != nil || rel == ".." || strings.HasPrefix(rel, "../") {
			return nil, fmt.Errorf("%s is outside the working copy", a)
		}
		prefixes = append(prefixes, filepath.ToSlash(rel))
	}
	var out []worktree.Change
	for _, ch := range changes {
		for _, p := range prefixes {
			if p == "." || ch.Path == p || strings.HasPrefix(ch.Path, p+"/") {
				out = append(out, ch)
				break
			}
		}
	}
	return out, nil
}
