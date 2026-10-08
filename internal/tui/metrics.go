package tui

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/Max2535/mqx/internal/broker"
)

const (
	metricsInterval = 2 * time.Second
	metricsHistory  = 120
)

// metricsView samples MetricsReporter every two seconds while visible and
// graphs counter rates and gauges as sparklines.
type metricsView struct {
	e        *env
	id       int
	target   string // "" = cluster-wide
	useTopic bool
	last     *broker.MetricSample
	rates    map[string]*ring[float64]
	gauges   map[string]*ring[float64]
	err      error
	active   bool
	ticking  bool
	seq      int
	keys     struct{ target, reset key.Binding }
}

type metricsTickMsg struct{ seq int }

type sampleMsg struct {
	target string
	s      broker.MetricSample
	err    error
}

func newMetricsPanel(e *env) panel {
	v := &metricsView{e: e, id: e.newID()}
	v.keys.target = key.NewBinding(key.WithKeys("t"), key.WithHelp("t", "selected topic / cluster"))
	v.keys.reset = key.NewBinding(key.WithKeys("r"), key.WithHelp("r", "reset graphs"))
	v.reset()
	return newStack(v)
}

func (v *metricsView) reset() {
	v.last, v.err = nil, nil
	v.rates, v.gauges = map[string]*ring[float64]{}, map[string]*ring[float64]{}
}

// ID implements panel.
func (v *metricsView) ID() int { return v.id }

// Title implements panel.
func (v *metricsView) Title() string { return "Metrics" }

// Capturing implements panel.
func (v *metricsView) Capturing() bool { return false }

// Init implements panel.
func (v *metricsView) Init() tea.Cmd { return v.sample() }

func (v *metricsView) sample() tea.Cmd {
	target, mr := v.target, as[broker.MetricsReporter](v.e)
	return v.e.call(v.id, func(ctx context.Context) tea.Msg {
		s, err := mr.Sample(ctx, target)
		return sampleMsg{target: target, s: s, err: err}
	})
}

func (v *metricsView) schedule() tea.Cmd {
	return v.e.after(v.id, metricsInterval, metricsTickMsg{seq: v.seq})
}

// Update implements panel.
func (v *metricsView) Update(msg tea.Msg) (panel, tea.Cmd) {
	switch m := msg.(type) {
	case activateMsg:
		v.active = m.active
		if m.active && !v.ticking {
			v.ticking = true
			v.seq++
			return v, tea.Batch(v.sample(), v.schedule())
		}
	case metricsTickMsg:
		if m.seq != v.seq || !v.active {
			v.ticking = false
			return v, nil
		}
		return v, tea.Batch(v.sample(), v.schedule())
	case sampleMsg:
		if m.target == v.target {
			v.record(m)
		}
	case tea.KeyMsg:
		switch {
		case key.Matches(m, v.keys.target):
			v.useTopic = !v.useTopic
			v.target = ""
			if v.useTopic {
				v.target = v.e.topic
			}
			v.reset()
			return v, v.sample()
		case key.Matches(m, v.keys.reset):
			v.reset()
			return v, v.sample()
		}
	}
	return v, nil
}

func (v *metricsView) record(m sampleMsg) {
	v.err = m.err
	if m.err != nil {
		return
	}
	if v.last != nil {
		for name, r := range broker.Rates(*v.last, m.s) {
			push(v.rates, name, r)
		}
	}
	for name, g := range m.s.Gauges {
		push(v.gauges, name, g)
	}
	s := m.s
	v.last = &s
}

func push(m map[string]*ring[float64], name string, x float64) {
	r, ok := m[name]
	if !ok {
		r = newRing[float64](metricsHistory)
		m[name] = r
	}
	r.push(x)
}

// Keys implements panel.
func (v *metricsView) Keys() []key.Binding { return []key.Binding{v.keys.target, v.keys.reset} }

// View implements panel.
func (v *metricsView) View(width, height int) string {
	target := "cluster-wide"
	if v.target != "" {
		target = v.e.kind() + " " + v.target
	}
	lines := []string{st.title.Render("Metrics: "+target) + st.muted.Render(fmt.Sprintf("  every %s", metricsInterval))}
	if v.useTopic && v.target == "" {
		lines = append(lines, st.muted.Render("No "+v.e.kind()+" selected in the Topics panel; showing cluster-wide."))
	}
	if v.err != nil {
		lines = append(lines, st.err.Render(truncate("error: "+v.err.Error(), width)))
	}
	graphW := max(10, width-34)
	lines = append(lines, "", st.header.Render("Rates (per second)"))
	if len(v.rates) == 0 {
		lines = append(lines, st.muted.Render("  collecting… rates need two samples"))
	}
	for _, name := range sortedNames(v.rates) {
		vals := v.rates[name].slice()
		lines = append(lines, fmt.Sprintf("  %s %s %s", pad(name, 16), pad(fmt.Sprintf("%.1f/s", vals[len(vals)-1]), 12),
			st.ok.Render(sparkline(vals, graphW))))
	}
	lines = append(lines, "", st.header.Render("Gauges"))
	if len(v.gauges) == 0 {
		lines = append(lines, st.muted.Render("  (none)"))
	}
	for _, name := range sortedNames(v.gauges) {
		vals := v.gauges[name].slice()
		cur, peak := vals[len(vals)-1], 0.0
		for _, x := range vals {
			peak = max(peak, x)
		}
		lines = append(lines, fmt.Sprintf("  %s %s %s %s", pad(name, 16), pad(fmt.Sprintf("%.0f", cur), 12),
			st.warn.Render(bar(cur, peak, 10)), st.ok.Render(sparkline(vals, max(5, graphW-11)))))
	}
	return clipLines(strings.Join(lines, "\n"), width, height)
}

func sortedNames(m map[string]*ring[float64]) []string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}
