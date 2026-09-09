package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"charm.land/bubbles/v2/progress"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/amber-store/dstore/client"
	"github.com/amber-store/dstore/view"
	"github.com/urfave/cli/v2"
)

// transferFunc runs a push or pull, reporting through log and prog.
type transferFunc func(ctx context.Context, log *slog.Logger, prog client.Progress) error

// runTransfer runs fn under a progress display: the TUI when stderr is a
// terminal, plain log lines and a status line every few seconds otherwise
// or with --no-tui. The transfer's own error is returned.
func runTransfer(ctx context.Context, c *cli.Context, title string, fn transferFunc) error {
	if c.Bool("no-tui") || !isTerminal(os.Stderr) {
		return runPlain(ctx, c, fn)
	}
	return runTUI(ctx, c, title, fn)
}

func noTUIFlag() cli.Flag {
	return &cli.BoolFlag{Name: "no-tui", Usage: "plain log lines instead of the progress display", EnvVars: []string{"DSTORE_NO_TUI"}}
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// latest keeps the newest progress report for a renderer to pick up; the
// client calls set for every record, so nothing else happens here.
type latest struct {
	mu  sync.Mutex
	rep client.ProgressReport
}

func (l *latest) set(r client.ProgressReport) {
	l.mu.Lock()
	l.rep = r
	l.mu.Unlock()
}

func (l *latest) get() client.ProgressReport {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rep
}

func runPlain(ctx context.Context, c *cli.Context, fn transferFunc) error {
	var l latest
	meter := newRateMeter(5 * time.Second)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case now := <-t.C:
				r := l.get()
				fmt.Fprintln(os.Stderr, statusLine(r, meter.add(now, r.Bytes)))
			}
		}
	}()
	err := fn(ctx, logger(c), l.set)
	close(done)
	wg.Wait()
	return err
}

// rateMeter measures throughput over a sliding window.
type rateMeter struct {
	window  time.Duration
	samples []rateSample
}

type rateSample struct {
	t time.Time
	n int64
}

func newRateMeter(window time.Duration) *rateMeter { return &rateMeter{window: window} }

// add records n bytes moved by t and returns the rate over the window in
// bytes per second.
func (m *rateMeter) add(t time.Time, n int64) float64 {
	m.samples = append(m.samples, rateSample{t, n})
	for len(m.samples) > 2 && t.Sub(m.samples[1].t) >= m.window {
		m.samples = m.samples[1:]
	}
	first := m.samples[0]
	d := t.Sub(first.t)
	if d <= 0 || n < first.n {
		return 0
	}
	return float64(n-first.n) / d.Seconds()
}

// statusLine summarises a report: objects, bytes, rate and time left.
func statusLine(r client.ProgressReport, rate float64) string {
	s := fmt.Sprintf("%d/%d objects", r.Objects, r.TotalObjects)
	if r.TotalBytes > 0 {
		s += fmt.Sprintf("  %s / %s", client.HumanBytes(r.Bytes), client.HumanBytes(r.TotalBytes))
	} else if r.Bytes > 0 {
		s += "  " + client.HumanBytes(r.Bytes)
	}
	s += fmt.Sprintf("  %s/s", client.HumanBytes(int64(rate)))
	if left := r.TotalBytes - r.Bytes; rate > 0 && left > 0 {
		eta := time.Duration(float64(left) / rate * float64(time.Second)).Round(time.Second)
		s += "  eta " + eta.String()
	}
	return s
}

// fraction is the completed share of a transfer: by bytes once the total
// is known, by objects before that.
func fraction(r client.ProgressReport) float64 {
	var f float64
	switch {
	case r.TotalBytes > 0:
		f = float64(r.Bytes) / float64(r.TotalBytes)
	case r.TotalObjects > 0:
		f = float64(r.Objects) / float64(r.TotalObjects)
	}
	return min(max(f, 0), 1)
}

// Messages of the transfer UI.
type (
	tickMsg  time.Time
	eventMsg struct {
		at    time.Time
		level slog.Level
		text  string
	}
	doneMsg struct{ err error }
)

const (
	maxEvents = 12
	tickEvery = 100 * time.Millisecond
)

var (
	titleStyle = lipgloss.NewStyle().Bold(true)
	faintStyle = lipgloss.NewStyle().Faint(true)
	warnStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("3"))
	errStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	okStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
)

// uiModel is the Bubble Tea model of a transfer: a progress bar, a status
// line, a per-node table and the last few client events.
type uiModel struct {
	title      string
	start      time.Time
	now        time.Time
	width      int
	bar        progress.Model
	l          *latest
	meter      *rateMeter
	nodeMeters map[view.NodeID]*rateMeter
	rep        client.ProgressReport
	rate       float64
	nodeRates  map[view.NodeID]float64
	events     []eventMsg
	cancel     context.CancelFunc
	cancelling bool
	done       bool
	err        error
}

func newUIModel(title string, l *latest, cancel context.CancelFunc) uiModel {
	now := time.Now()
	bar := progress.New(progress.WithDefaultBlend())
	bar.SetWidth(60)
	return uiModel{
		title: title, start: now, now: now, width: 80, bar: bar, l: l, cancel: cancel,
		meter: newRateMeter(5 * time.Second), nodeMeters: map[view.NodeID]*rateMeter{}, nodeRates: map[view.NodeID]float64{},
	}
}

func tick() tea.Cmd {
	return tea.Tick(tickEvery, func(t time.Time) tea.Msg { return tickMsg(t) })
}

func (m uiModel) Init() tea.Cmd { return tick() }

func (m uiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.bar.SetWidth(min(max(msg.Width-4, 20), 80))
	case tea.KeyPressMsg:
		if msg.String() == "ctrl+c" && !m.cancelling {
			m.cancelling = true
			m.cancel()
			m.events = appendEvent(m.events, eventMsg{at: time.Now(), level: slog.LevelWarn, text: "cancelling"})
		}
	case tickMsg:
		m.observe(time.Time(msg))
		return m, tick()
	case eventMsg:
		m.events = appendEvent(m.events, msg)
	case doneMsg:
		m.done, m.err = true, msg.err
		m.observe(time.Now())
		return m, tea.Quit
	}
	return m, nil
}

// observe takes the latest report and updates the rates.
func (m *uiModel) observe(now time.Time) {
	m.now = now
	m.rep = m.l.get()
	m.rate = m.meter.add(now, m.rep.Bytes)
	for _, n := range m.rep.Nodes {
		nm := m.nodeMeters[n.ID]
		if nm == nil {
			nm = newRateMeter(5 * time.Second)
			m.nodeMeters[n.ID] = nm
		}
		m.nodeRates[n.ID] = nm.add(now, n.Bytes)
	}
}

func appendEvent(events []eventMsg, e eventMsg) []eventMsg {
	events = append(events, e)
	if len(events) > maxEvents {
		events = events[len(events)-maxEvents:]
	}
	return events
}

func (m uiModel) View() tea.View {
	var b strings.Builder
	elapsed := m.now.Sub(m.start).Round(time.Second)
	fmt.Fprintf(&b, "%s  %s\n", titleStyle.Render(m.title), faintStyle.Render("elapsed "+elapsed.String()))
	b.WriteString(m.bar.ViewAs(fraction(m.rep)) + "\n")
	b.WriteString(statusLine(m.rep, m.rate) + "\n")
	if len(m.rep.Nodes) > 0 {
		b.WriteString(faintStyle.Render(fmt.Sprintf("%-10s %-7s %7s %7s %-15s %11s %12s", "node", "path", "rtt", "batches", "state", "sent", "rate")) + "\n")
		for _, n := range m.rep.Nodes {
			path := "relay"
			if n.Direct {
				path = "direct"
			}
			rtt := "-"
			if n.RTT > 0 {
				rtt = n.RTT.Round(time.Millisecond).String()
			}
			fmt.Fprintf(&b, "%-10s %-7s %7s %7d %-15s %11s %10s/s\n", view.ShortID(n.ID), path, rtt, n.InFlight, nodeState(n), client.HumanBytes(n.Bytes), client.HumanBytes(int64(m.nodeRates[n.ID])))
		}
	}
	if len(m.events) > 0 {
		b.WriteString(faintStyle.Render("events") + "\n")
		for _, e := range m.events {
			b.WriteString(formatEvent(e) + "\n")
		}
	}
	if m.done {
		if m.err != nil {
			b.WriteString(errStyle.Render("failed: "+m.err.Error()) + "\n")
		} else {
			b.WriteString(okStyle.Render("done") + "\n")
		}
	}
	return tea.NewView(b.String())
}

// nodeState names what the client is doing with a node right now.
func nodeState(n client.NodeProgress) string {
	switch {
	case n.InFlight == 0:
		return "idle"
	case n.Awaiting == n.InFlight:
		return "waiting for ack"
	}
	return "sending"
}

func formatEvent(e eventMsg) string {
	line := e.at.Format("15:04:05") + "  " + e.text
	switch {
	case e.level >= slog.LevelError:
		return errStyle.Render(line)
	case e.level >= slog.LevelWarn:
		return warnStyle.Render(line)
	}
	return line
}

func runTUI(ctx context.Context, c *cli.Context, title string, fn transferFunc) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var l latest
	var in io.Reader = os.Stdin
	if !isTerminal(os.Stdin) {
		in = nil
	}
	p := tea.NewProgram(newUIModel(title, &l, cancel), tea.WithOutput(os.Stderr), tea.WithInput(in), tea.WithoutSignalHandler())
	log := slog.New(&teaHandler{level: logLevel(c), send: p.Send})
	result := make(chan error, 1)
	go func() {
		err := fn(ctx, log, l.set)
		result <- err
		p.Send(doneMsg{err: err})
	}()
	if _, err := p.Run(); err != nil {
		cancel()
		<-result
		return err
	}
	return <-result
}

// teaHandler is a slog.Handler that turns log records into UI events.
type teaHandler struct {
	level slog.Level
	send  func(tea.Msg)
	attrs []slog.Attr
}

func (h *teaHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.level }

func (h *teaHandler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	b.WriteString(r.Message)
	write := func(a slog.Attr) bool {
		b.WriteString(" " + a.Key + "=" + attrValue(a))
		return true
	}
	for _, a := range h.attrs {
		write(a)
	}
	r.Attrs(write)
	h.send(eventMsg{at: r.Time, level: r.Level, text: b.String()})
	return nil
}

func (h *teaHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &teaHandler{level: h.level, send: h.send, attrs: append(append([]slog.Attr{}, h.attrs...), attrs...)}
}

func (h *teaHandler) WithGroup(string) slog.Handler { return h }

func attrValue(a slog.Attr) string {
	if a.Key == "bytes" && a.Value.Kind() == slog.KindInt64 {
		return client.HumanBytes(a.Value.Int64())
	}
	s := a.Value.String()
	if strings.ContainsAny(s, " \t") {
		return fmt.Sprintf("%q", s)
	}
	return s
}
