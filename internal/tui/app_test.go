package tui

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/config"
	"github.com/Max2535/mqx/internal/testutil/fakebroker"
)

func TestPanelsFollowCapabilities(t *testing.T) {
	tests := []struct {
		name   string
		caps   []string
		want   []string
		absent []string
	}{
		{"kafka-like", kafkaCaps,
			[]string{"Topics", "Consumers", "Groups", "Cluster", "Schemas", "Connect", "KSQL", "ACLs", "Metrics"},
			[]string{"Topology", "Connections", "Users", "Policies", "Queues"}},
		{"rabbit-like", rabbitCaps,
			[]string{"Queues", "Consumers", "Topology", "Connections", "Cluster", "Users", "Policies", "Metrics"},
			[]string{"Groups", "Schemas", "Connect", "KSQL", "ACLs", "Topics"}},
		{"core only", []string{}, []string{"Topics"}, []string{"Consumers", "Metrics"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, c := fakeCtx(t, "local", false, tt.caps)
			f.AddTopic("orders", 1)
			h := newHarness(t, Options{}, c)
			if got := h.titles(); !slices.Equal(got, tt.want) {
				t.Errorf("panels = %v, want %v", got, tt.want)
			}
			h.wantView(tt.want...)
			for _, title := range tt.absent {
				if slices.Contains(h.titles(), title) {
					t.Errorf("panel %q shown without its capability", title)
				}
			}
		})
	}
}

func TestHeaderAndNavigation(t *testing.T) {
	f, c := fakeCtx(t, "prod-kafka", false, kafkaCaps)
	seedKafka(f)
	c.Brokers = []string{"localhost:9092"}
	h := newHarness(t, Options{}, c)
	h.wantView("ctx: prod-kafka", "broker: fake", "localhost:9092", "● connected", "orders", "payments")
	h.wantNotView("READ-ONLY")
	h.keys("tab")
	if got := h.a.panels[h.a.active].Title(); got != "Consumers" {
		t.Fatalf("after tab active = %q", got)
	}
	h.keys("shift+tab", "shift+tab")
	if got := h.a.panels[h.a.active].Title(); got != "Metrics" {
		t.Fatalf("shift+tab should wrap to the last panel, got %q", got)
	}
	h.keys("?")
	h.wantView("Keys", "Global", "next panel")
	h.keys("x")
	if h.a.overlay != nil {
		t.Fatal("any key should close help")
	}
	h.keys("q")
	if !h.quit {
		t.Fatal("q should quit")
	}
}

func TestReadOnlyHidesMutatingActions(t *testing.T) {
	f, c := fakeCtx(t, "prod-kafka", true, kafkaCaps)
	seedKafka(f)
	h := newHarness(t, Options{}, c)
	h.wantView("READ-ONLY")
	for _, k := range []string{"p", "d", "n", "x", "e", "a"} {
		h.keys(k)
		if h.a.overlay != nil {
			t.Fatalf("key %q opened %T on a read_only context", k, h.a.overlay)
		}
	}
	h.wantNotView("publish", "delete topic", "new topic", "purge", "alter config")

	h.keys("enter") // topic detail
	for _, k := range []string{"p", "d", "x", "e", "a"} {
		h.keys(k)
	}
	h.keys("m") // message browser: navigation still works
	h.wantView("topic orders")
	h.keys("p")

	h.keys("esc", "esc")
	h.gotoPanel("Groups")
	h.keys("down", "enter")
	h.wantView("billing-0")
	for _, k := range []string{"o", "d", "x"} {
		h.keys(k)
	}
	h.wantNotView("reset offsets", "delete group", "remove static member")
	if h.a.overlay != nil {
		t.Fatalf("overlay %T opened on a read_only context", h.a.overlay)
	}
	wantCalls(t, f)

	// The guard itself refuses even if a view asked.
	msg := h.a.e.mutate(1, "Delete topic orders", "", func(context.Context) tea.Msg { return nil })()
	if s, ok := msg.(statusMsg); !ok || !s.err || !strings.Contains(s.text, "read_only") {
		t.Fatalf("guard on read_only = %#v", msg)
	}
}

func TestConfirmDialog(t *testing.T) {
	tests := []struct {
		name  string
		keys  []string
		calls []string
	}{
		{"esc cancels", []string{"esc"}, nil},
		{"n cancels", []string{"n"}, nil},
		{"enter on default Cancel", []string{"enter"}, nil},
		{"y confirms", []string{"y"}, []string{"DeleteTopic orders"}},
		{"enter on Confirm", []string{"right", "enter"}, []string{"DeleteTopic orders"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, c := fakeCtx(t, "prod-kafka", false, kafkaCaps)
			seedKafka(f)
			h := newHarness(t, Options{}, c)
			h.keys("d")
			h.wantView("Delete topic orders on prod-kafka?", "Cancel", "Confirm")
			wantCalls(t, f)
			h.keys(tt.keys...)
			if h.a.overlay != nil {
				t.Fatal("dialog still open")
			}
			wantCalls(t, f, tt.calls...)
			if tt.calls != nil {
				h.wantStatus("Delete topic orders: done", false)
				h.wantNotView("orders ") // reloaded without the topic
			} else {
				h.wantStatus("cancelled", false)
			}
		})
	}
}

func TestDeepLinks(t *testing.T) {
	t.Run("topic opens the message browser", func(t *testing.T) {
		f, c := fakeCtx(t, "local", false, kafkaCaps)
		seedKafka(f)
		h := newHarness(t, Options{Topic: "orders"}, c)
		if _, ok := h.top().(*messageBrowser); !ok {
			t.Fatalf("top view = %T, want message browser", h.top())
		}
		if h.a.e.topic != "orders" {
			t.Errorf("selected topic = %q", h.a.e.topic)
		}
		h.wantView("Topics › Messages", "topic orders", "3 messages", "o-1")
	})
	t.Run("group opens the group detail", func(t *testing.T) {
		f, c := fakeCtx(t, "local", false, kafkaCaps)
		seedKafka(f)
		h := newHarness(t, Options{Group: "billing"}, c)
		if got := h.a.panels[h.a.active].Title(); got != "Groups" {
			t.Fatalf("active panel = %q", got)
		}
		h.wantView("Groups › billing", "state Stable", "assignor range", "epoch 3", "billing-0", "orders:0,1", "Lag total 2")
	})
	t.Run("unknown topic and group are status errors", func(t *testing.T) {
		f, c := fakeCtx(t, "local", false, kafkaCaps)
		seedKafka(f)
		h := newHarness(t, Options{Topic: "nope"}, c)
		h.wantStatus(`topic "nope" not found`, true)
		h = newHarness(t, Options{Group: "nope"}, c)
		h.wantStatus(`group "nope" not found`, true)
		h.wantView("billing")
	})
	t.Run("context selects the context", func(t *testing.T) {
		_, a := fakeCtx(t, "a", false, kafkaCaps)
		fb, b := fakeCtx(t, "b", false, rabbitCaps)
		fb.AddTopic("jobs", 1)
		h := newHarness(t, Options{Context: "b"}, a, b)
		h.wantView("ctx: b", "Queues", "jobs")
	})
	t.Run("unknown context falls back to current-context", func(t *testing.T) {
		_, a := fakeCtx(t, "a", false, kafkaCaps)
		h := newHarness(t, Options{Context: "nope", Topic: "orders"}, a)
		h.wantStatus(`context "nope" not found`, true)
		h.wantView("ctx: a")
	})
}

func TestContextSwitching(t *testing.T) {
	fa, a := fakeCtx(t, "kafka-dev", false, kafkaCaps)
	seedKafka(fa)
	fb, b := fakeCtx(t, "rabbit-prod", true, rabbitCaps)
	fb.AddTopic("jobs", 1)
	h := newHarness(t, Options{}, a, b)
	oldGen := h.a.e.gen

	h.keys("c")
	h.wantView("Switch context", "kafka-dev", "rabbit-prod", "read-only")
	h.keys("down", "enter")

	if !fa.Closed() {
		t.Error("old broker not closed")
	}
	if h.a.e.ctx.Name != "rabbit-prod" {
		t.Fatalf("context = %q", h.a.e.ctx.Name)
	}
	if !slices.Contains(h.titles(), "Topology") || slices.Contains(h.titles(), "Groups") {
		t.Errorf("panels not rebuilt for the new broker: %v", h.titles())
	}
	h.wantView("ctx: rabbit-prod", "READ-ONLY", "jobs")

	// A late result from the old context is dropped.
	h.send(routedMsg{gen: oldGen, to: 1, msg: loadedMsg{err: errors.New("stale")}})
	h.wantNotView("stale")

	// Switching to an unknown context keeps the current one.
	h.send(switchContextMsg{name: "gone"})
	h.wantStatus(`context "gone" not found`, true)
	if h.a.e.ctx.Name != "rabbit-prod" {
		t.Fatal("current context lost")
	}
}

// failingBroker makes ListTopics fail while keeping every capability of the fake.
type failingBroker struct{ *fakebroker.Fake }

func (failingBroker) ListTopics(context.Context) ([]broker.Topic, error) {
	return nil, errors.New("boom: broker unreachable")
}

func TestErrorsRenderWithoutPanic(t *testing.T) {
	t.Run("open error", func(t *testing.T) {
		f, c := fakeCtx(t, "down", false, nil)
		f.OpenErr = errors.New("dial tcp: connection refused")
		h := newHarness(t, Options{}, c)
		h.wantStatus("connection refused", true)
		h.wantView("No context open", "✕")
		h.send(tea.WindowSizeMsg{Width: 5, Height: 3})
		_ = h.view()
	})
	t.Run("load error inline", func(t *testing.T) {
		f, c := fakeCtx(t, "flaky", false, kafkaCaps)
		cfg := &config.Config{CurrentContext: c.Name, Contexts: []config.Context{c}}
		ac := startConfig(context.Background(), cfg, Options{})
		ac.open = func(context.Context, config.Context) (broker.Broker, error) { return failingBroker{f}, nil }
		h := &harness{t: t, a: newApp(ac)}
		h.run(h.a.Init())
		h.wantView("error: boom: broker unreachable")
		for _, size := range [][2]int{{20, 6}, {200, 60}, {1, 1}} {
			h.send(tea.WindowSizeMsg{Width: size[0], Height: size[1]})
			_ = h.view()
		}
	})
	t.Run("no context", func(t *testing.T) {
		h := &harness{t: t, a: newApp(appConfig{cfg: &config.Config{}})}
		h.run(h.a.Init())
		h.wantStatus("no context selected", true)
		h.keys("c", "enter", "esc", "tab", "x")
		_ = h.view()
	})
}
