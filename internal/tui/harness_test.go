package tui

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
	"github.com/muesli/termenv"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/config"
	"github.com/Max2535/mqx/internal/testutil/fakebroker"
)

func TestMain(m *testing.M) {
	// Render colours so tests can check that JSON is highlighted; views are
	// compared after stripping escape codes.
	lipgloss.SetColorProfile(termenv.ANSI)
	os.Exit(m.Run())
}

var (
	kafkaCaps = []string{broker.CapTopicDescriber, broker.CapClusterInspector, broker.CapConsumerInspector,
		broker.CapGroupInspector, broker.CapLagReporter, broker.CapOffsetManager, broker.CapConsumerTerminator,
		broker.CapTopicAdmin, broker.CapPartitionAdder, broker.CapPurger, broker.CapSchemaRegistry,
		broker.CapConnectManager, broker.CapKSQLRunner, broker.CapACLAdmin, broker.CapMetricsReporter}
	rabbitCaps = []string{broker.CapTopicDescriber, broker.CapClusterInspector, broker.CapConsumerInspector,
		broker.CapConnectionInspector, broker.CapConsumerTerminator, broker.CapTopicAdmin, broker.CapPurger,
		broker.CapTopologyInspector, broker.CapTopologyEditor, broker.CapRouteSimulator, broker.CapUserAdmin,
		broker.CapPolicyAdmin, broker.CapMetricsReporter}
)

// clock is a deterministic, advanceable time source.
type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newClock() *clock { return &clock{t: time.Date(2026, 10, 8, 9, 0, 0, 0, time.UTC)} }

// harness drives the root model synchronously: every returned tea.Cmd is run
// and its message fed back until nothing is left. Timers are disabled.
type harness struct {
	t    *testing.T
	a    *app
	quit bool
}

// fakeCtx returns a context opening f under a chosen name.
func fakeCtx(t *testing.T, name string, readOnly bool, caps []string) (*fakebroker.Fake, config.Context) {
	t.Helper()
	f, c := fakebroker.New(t)
	f.Caps = caps
	clk := newClock()
	f.Now = clk.now
	c.Name, c.ReadOnly = name, readOnly
	return f, c
}

func newHarness(t *testing.T, opts Options, contexts ...config.Context) *harness {
	t.Helper()
	cfg := &config.Config{CurrentContext: contexts[0].Name, Contexts: contexts}
	c := startConfig(context.Background(), cfg, opts)
	c.now = newClock().now
	h := &harness{t: t, a: newApp(c)}
	h.run(h.a.Init())
	return h
}

func (h *harness) run(cmd tea.Cmd) {
	h.t.Helper()
	queue := []tea.Cmd{cmd}
	for steps := 0; len(queue) > 0; steps++ {
		if steps > 10000 {
			h.t.Fatal("command loop did not settle")
		}
		c := queue[0]
		queue = queue[1:]
		if c == nil {
			continue
		}
		switch m := c().(type) {
		case nil:
		case tea.BatchMsg:
			queue = append(queue, m...)
		case tea.QuitMsg:
			h.quit = true
		default:
			_, next := h.a.Update(m)
			queue = append(queue, next)
		}
	}
}

func (h *harness) send(msgs ...tea.Msg) {
	h.t.Helper()
	for _, m := range msgs {
		_, cmd := h.a.Update(m)
		h.run(cmd)
	}
}

// keys sends named keys ("enter", "esc", "ctrl+s", ...) or single characters.
func (h *harness) keys(names ...string) {
	h.t.Helper()
	for _, n := range names {
		h.send(keyMsg(n))
	}
}

// typeText sends s as typed runes.
func (h *harness) typeText(s string) {
	h.t.Helper()
	for _, r := range s {
		h.send(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
}

func keyMsg(n string) tea.KeyMsg {
	special := map[string]tea.KeyType{
		"enter": tea.KeyEnter, "esc": tea.KeyEsc, "tab": tea.KeyTab, "shift+tab": tea.KeyShiftTab,
		"up": tea.KeyUp, "down": tea.KeyDown, "left": tea.KeyLeft, "right": tea.KeyRight,
		"ctrl+s": tea.KeyCtrlS, "ctrl+c": tea.KeyCtrlC, "backspace": tea.KeyBackspace,
	}
	if t, ok := special[n]; ok {
		return tea.KeyMsg{Type: t}
	}
	return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(n)}
}

func (h *harness) view() string { return ansi.Strip(h.a.View()) }

func (h *harness) titles() []string {
	out := make([]string, len(h.a.panels))
	for i, p := range h.a.panels {
		out[i] = p.Title()
	}
	return out
}

// top returns the visible view of the active panel.
func (h *harness) top() panel {
	h.t.Helper()
	s, ok := h.a.panels[h.a.active].(*stack)
	if !ok {
		h.t.Fatalf("active panel is %T, not a stack", h.a.panels[h.a.active])
	}
	return s.top()
}

// gotoPanel activates the panel with the given title.
func (h *harness) gotoPanel(title string) {
	h.t.Helper()
	for range h.a.panels {
		if h.a.panels[h.a.active].Title() == title {
			return
		}
		h.keys("tab")
	}
	h.t.Fatalf("no panel %q in %v", title, h.titles())
}

func (h *harness) wantView(substrs ...string) {
	h.t.Helper()
	v := h.view()
	for _, s := range substrs {
		if !strings.Contains(v, s) {
			h.t.Errorf("view does not contain %q:\n%s", s, v)
		}
	}
}

func (h *harness) wantNotView(substrs ...string) {
	h.t.Helper()
	v := h.view()
	for _, s := range substrs {
		if strings.Contains(v, s) {
			h.t.Errorf("view unexpectedly contains %q:\n%s", s, v)
		}
	}
}

func (h *harness) wantStatus(substr string, isErr bool) {
	h.t.Helper()
	if !strings.Contains(h.a.status.text, substr) || h.a.status.err != isErr {
		h.t.Errorf("status = %q (err=%v), want %q (err=%v)", h.a.status.text, h.a.status.err, substr, isErr)
	}
}

func wantCalls(t *testing.T, f *fakebroker.Fake, want ...string) {
	t.Helper()
	if strings.Join(f.Calls, "|") != strings.Join(want, "|") {
		t.Errorf("calls = %q, want %q", f.Calls, want)
	}
}

// seedKafka adds a topic with messages and a consumer group with lag.
func seedKafka(f *fakebroker.Fake) {
	f.AddTopic("orders", 2)
	f.AddTopic("payments", 1)
	f.AddMessages("orders", 0,
		broker.Message{Key: []byte("o-1"), Value: []byte(`{"order_id":"o-1","total":12.5}`),
			Headers: []broker.Header{{Key: "source", Value: []byte("web")}}},
		broker.Message{Key: []byte("o-2"), Value: []byte("plain text")},
	)
	f.AddMessages("orders", 1, broker.Message{Key: []byte("o-3"), Value: []byte{0x00, 0x01, 0xff}})
	f.AddGroup(broker.GroupDescription{Name: "billing", State: "Stable", GroupProtocol: "classic", ProtocolType: "consumer",
		Assignor: "range", Epoch: 3, Coordinator: broker.Node{ID: "1", Host: "localhost", Port: 9092},
		Members: []broker.GroupMember{{MemberID: "m-1", InstanceID: "billing-0", ClientID: "billing-app", Host: "/10.0.0.1",
			Subscriptions: []string{"orders"}, Assignment: []broker.TopicPartitions{{Topic: "orders", Partitions: []int32{0, 1}}}}},
	}, map[string]map[int32]int64{"orders": {0: 1, 1: 0}})
	f.AddGroup(broker.GroupDescription{Name: "archiver", State: "Empty", GroupProtocol: "classic", ProtocolType: "consumer"},
		map[string]map[int32]int64{"orders": {0: 2, 1: 1}})
}
