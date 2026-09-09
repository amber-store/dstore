package main

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/amber-store/dstore/client"
	"github.com/amber-store/dstore/view"
)

func TestUIModel(t *testing.T) {
	var l latest
	cancelled := false
	m := newUIModel("push demo", &l, func() { cancelled = true })
	l.set(client.ProgressReport{
		Objects: 5, TotalObjects: 10, Bytes: 512, TotalBytes: 1024,
		Nodes: []client.NodeProgress{{ID: view.NodeID{0xab, 0xcd}, Direct: true, RTT: 3 * time.Millisecond, InFlight: 1, Bytes: 512}},
	})
	var mm tea.Model = m
	mm, _ = mm.Update(tea.WindowSizeMsg{Width: 100, Height: 40})
	mm, _ = mm.Update(tickMsg(m.start))
	l.set(client.ProgressReport{Objects: 5, TotalObjects: 10, Bytes: 1024, TotalBytes: 2048,
		Nodes: []client.NodeProgress{{ID: view.NodeID{0xab, 0xcd}, Direct: true, RTT: 3 * time.Millisecond, InFlight: 1, Bytes: 1024}}})
	mm, _ = mm.Update(tickMsg(m.start.Add(time.Second)))
	mm, _ = mm.Update(eventMsg{at: time.Now(), level: slog.LevelWarn, text: "upload retry node=abcd reason=busy"})
	out := mm.View().Content
	for _, want := range []string{"push demo", "5/10 objects", "1.0 KiB / 2.0 KiB", "512 B/s", "eta 2s", "50%", "direct", "3ms", "sending", "upload retry node=abcd reason=busy"} {
		if !strings.Contains(out, want) {
			t.Errorf("view lacks %q:\n%s", want, out)
		}
	}
	l.set(client.ProgressReport{Objects: 5, TotalObjects: 10, Bytes: 2048, TotalBytes: 2048,
		Nodes: []client.NodeProgress{{ID: view.NodeID{0xab, 0xcd}, Direct: true, InFlight: 1, Awaiting: 1, Bytes: 2048}}})
	mm, _ = mm.Update(tickMsg(m.start.Add(2 * time.Second)))
	if out := mm.View().Content; !strings.Contains(out, "waiting for ack") {
		t.Errorf("view lacks the ack state:\n%s", out)
	}
	mm, _ = mm.Update(tea.KeyPressMsg{Code: 'c', Mod: tea.ModCtrl})
	if !cancelled {
		t.Fatal("ctrl+c did not cancel the transfer")
	}
	mm, cmd := mm.Update(doneMsg{err: context.Canceled})
	if cmd == nil {
		t.Fatal("done did not quit")
	}
	if _, ok := cmd().(tea.QuitMsg); !ok {
		t.Fatalf("done produced %T, want QuitMsg", cmd())
	}
	if out := mm.View().Content; !strings.Contains(out, "failed: context canceled") || !strings.Contains(out, "cancelling") {
		t.Errorf("final view:\n%s", out)
	}
}

func TestRateMeter(t *testing.T) {
	m := newRateMeter(5 * time.Second)
	t0 := time.Unix(0, 0)
	if r := m.add(t0, 0); r != 0 {
		t.Fatalf("first sample rate %v", r)
	}
	if r := m.add(t0.Add(time.Second), 1000); r != 1000 {
		t.Fatalf("rate after 1 s %v, want 1000", r)
	}
	// Ten seconds at 100 B/s: the window forgets the fast start.
	for i := 2; i <= 11; i++ {
		m.add(t0.Add(time.Duration(i)*time.Second), 1000+int64(i-1)*100)
	}
	if r := m.add(t0.Add(12*time.Second), 2100); r < 99 || r > 101 {
		t.Fatalf("windowed rate %v, want about 100", r)
	}
}

func TestTeaHandler(t *testing.T) {
	var got []eventMsg
	h := &teaHandler{level: slog.LevelInfo, send: func(m tea.Msg) { got = append(got, m.(eventMsg)) }}
	log := slog.New(h).With("node", "abcd")
	log.Debug("hidden")
	log.Info("uploaded", "objects", 3, "bytes", int64(3<<20), "took", 2*time.Second, "path", "direct")
	log.Warn("upload failed", "err", "timeout: no recent network activity")
	if len(got) != 2 {
		t.Fatalf("%d events, want 2: %+v", len(got), got)
	}
	if want := "uploaded node=abcd objects=3 bytes=3.0 MiB took=2s path=direct"; got[0].text != want {
		t.Errorf("event %q, want %q", got[0].text, want)
	}
	if want := `upload failed node=abcd err="timeout: no recent network activity"`; got[1].text != want || got[1].level != slog.LevelWarn {
		t.Errorf("event %q level %v", got[1].text, got[1].level)
	}
}

func TestStatusLine(t *testing.T) {
	r := client.ProgressReport{Objects: 3, TotalObjects: 4}
	if got := statusLine(r, 0); got != "3/4 objects  0 B/s" {
		t.Errorf("objects only: %q", got)
	}
	if f := fraction(r); f != 0.75 {
		t.Errorf("fraction by objects %v", f)
	}
	r.Bytes, r.TotalBytes = 3000, 1000 // a re-send overshoots the total
	if f := fraction(r); f != 1 {
		t.Errorf("fraction clamps to 1, got %v", f)
	}
}
