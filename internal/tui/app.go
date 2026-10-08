package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/help"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/config"
)

const navWidth = 18

// appConfig is everything the root model needs; tests inject the clock,
// timers and broker opener.
type appConfig struct {
	cfg     *config.Config
	initial string // context to open first; "" = none
	links   deepLinks
	status  statusMsg // initial status, e.g. an unknown --context
	open    func(ctx context.Context, c config.Context) (broker.Broker, error)
	base    context.Context
	timeout time.Duration
	now     func() time.Time
	tick    func(d time.Duration, msg tea.Msg) tea.Cmd
	animate bool // run the spinner
}

type connState int

const (
	connNone connState = iota
	connConnecting
	connOK
	connFailed
)

type globalKeys struct {
	quit, help, contexts, next, prev key.Binding
}

func newGlobalKeys() globalKeys {
	return globalKeys{
		quit:     key.NewBinding(key.WithKeys("q", "ctrl+c"), key.WithHelp("q", "quit")),
		help:     key.NewBinding(key.WithKeys("?"), key.WithHelp("?", "help")),
		contexts: key.NewBinding(key.WithKeys("c"), key.WithHelp("c", "context")),
		next:     key.NewBinding(key.WithKeys("tab"), key.WithHelp("tab", "next panel")),
		prev:     key.NewBinding(key.WithKeys("shift+tab"), key.WithHelp("shift+tab", "prev panel")),
	}
}

func (k globalKeys) list() []key.Binding { return []key.Binding{k.next, k.contexts, k.help, k.quit} }

// app is the root model: header, navigation, the active panel, footer and overlays.
type app struct {
	appConfig
	e         *env
	gen       int
	cancelEnv context.CancelFunc
	panels    []panel
	active    int
	overlay   overlay
	conn      connState
	connErr   string
	target    string // context being opened
	width     int
	height    int
	keys      globalKeys
	help      help.Model
	spinner   spinner.Model
}

// openedMsg reports the result of opening a context.
type openedMsg struct {
	c       config.Context
	b       broker.Broker
	err     error
	pingErr error
}

func newApp(c appConfig) *app {
	if c.timeout <= 0 {
		c.timeout = 30 * time.Second
	}
	if c.now == nil {
		c.now = time.Now
	}
	if c.base == nil {
		c.base = context.Background()
	}
	return &app{appConfig: c, width: 100, height: 30, keys: newGlobalKeys(), help: help.New(),
		spinner: spinner.New(spinner.WithSpinner(spinner.Dot))}
}

// Init implements tea.Model.
func (a *app) Init() tea.Cmd {
	var cmds []tea.Cmd
	if a.animate {
		cmds = append(cmds, a.spinner.Tick)
	}
	if a.initial != "" {
		cmds = append(cmds, a.openContext(a.initial))
	} else if a.status.text == "" {
		a.status = statusMsg{text: "no context selected; press c to choose one", err: true}
	}
	return tea.Batch(cmds...)
}

// openContext connects to a context in the background.
func (a *app) openContext(name string) tea.Cmd {
	c, err := a.cfg.Find(name)
	if err != nil {
		return statusErr(err)
	}
	a.conn, a.target, a.connErr = connConnecting, name, ""
	open, base, timeout := a.open, a.base, a.timeout
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(base, timeout)
		defer cancel()
		b, err := open(ctx, c)
		if err != nil {
			return openedMsg{c: c, err: err}
		}
		return openedMsg{c: c, b: b, pingErr: b.Ping(ctx)}
	}
}

// opened swaps in a newly opened broker and rebuilds the panels for its capabilities.
func (a *app) opened(m openedMsg) tea.Cmd {
	if m.err != nil {
		a.conn, a.connErr = connFailed, m.err.Error()
		if a.e != nil {
			a.conn, a.connErr = connOK, ""
		}
		return statusErr(m.err)
	}
	a.shutdown()
	a.gen++
	ids := 0
	base, cancel := context.WithCancel(a.base)
	a.cancelEnv = cancel
	a.e = &env{ctx: m.c, b: m.b, base: base, timeout: a.timeout, now: a.now, tick: a.tick, gen: a.gen, ids: &ids,
		spin: a.spinner.View()}
	a.conn, a.connErr = connOK, ""
	if m.pingErr != nil {
		a.conn, a.connErr = connFailed, m.pingErr.Error()
	}
	links := a.links
	a.links = deepLinks{}
	a.panels = buildPanels(a.e, links)
	a.active = 0
	if links.group != "" {
		if i := a.panelIndex("Groups"); i >= 0 {
			a.active = i
		} else {
			a.status = statusMsg{text: fmt.Sprintf("context %q has no consumer groups; ignoring --group %s", m.c.Name, links.group), err: true}
		}
	}
	cmds := make([]tea.Cmd, 0, len(a.panels)+2)
	for _, p := range a.panels {
		cmds = append(cmds, p.Init())
	}
	cmds = append(cmds, a.activate(a.active, true))
	if a.status.text == "" || !a.status.err {
		cmds = append(cmds, statusInfo(fmt.Sprintf("connected to %s (%s)", m.c.Name, m.b.Name())))
	}
	return tea.Batch(cmds...)
}

func (a *app) panelIndex(title string) int {
	for i, p := range a.panels {
		if p.Title() == title {
			return i
		}
	}
	return -1
}

// shutdown closes the current panels and broker.
func (a *app) shutdown() {
	for _, p := range a.panels {
		if c, ok := p.(closer); ok {
			c.close()
		}
	}
	if a.cancelEnv != nil {
		a.cancelEnv()
	}
	if a.e != nil && a.e.b != nil {
		_ = a.e.b.Close()
	}
	a.panels, a.e = nil, nil
}

func (a *app) activate(i int, on bool) tea.Cmd {
	if i < 0 || i >= len(a.panels) {
		return nil
	}
	var cmd tea.Cmd
	a.panels[i], cmd = a.panels[i].Update(activateMsg{active: on})
	return cmd
}

func (a *app) switchPanel(delta int) tea.Cmd {
	if len(a.panels) == 0 {
		return nil
	}
	off := a.activate(a.active, false)
	a.active = (a.active + delta + len(a.panels)) % len(a.panels)
	return tea.Batch(off, a.activate(a.active, true))
}

// Update implements tea.Model.
func (a *app) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch m := msg.(type) {
	case tea.WindowSizeMsg:
		a.width, a.height = m.Width, m.Height
		return a, nil
	case spinner.TickMsg:
		var cmd tea.Cmd
		a.spinner, cmd = a.spinner.Update(m)
		if a.e != nil {
			a.e.spin = a.spinner.View()
		}
		return a, cmd
	case tea.KeyMsg:
		return a, a.key(m)
	case statusMsg:
		a.status = m
		return a, nil
	case openOverlayMsg:
		a.overlay = m.o
		return a, nil
	case confirmMsg:
		a.overlay = newConfirm(m)
		return a, nil
	case switchContextMsg:
		return a, a.openContext(m.name)
	case openedMsg:
		return a, a.opened(m)
	case routedMsg:
		if a.e == nil || m.gen != a.e.gen {
			return a, nil // a result for a context that is no longer open
		}
		cmds := make([]tea.Cmd, len(a.panels))
		for i, p := range a.panels {
			a.panels[i], cmds[i] = p.Update(m)
		}
		return a, tea.Batch(cmds...)
	}
	if a.overlay != nil {
		var cmd tea.Cmd
		a.overlay, cmd = a.overlay.Update(msg)
		return a, cmd
	}
	return a, a.forward(msg)
}

func (a *app) forward(msg tea.Msg) tea.Cmd {
	if a.active >= len(a.panels) {
		return nil
	}
	var cmd tea.Cmd
	a.panels[a.active], cmd = a.panels[a.active].Update(msg)
	return cmd
}

func (a *app) key(m tea.KeyMsg) tea.Cmd {
	if m.String() == "ctrl+c" {
		return tea.Quit
	}
	if a.overlay != nil {
		var cmd tea.Cmd
		a.overlay, cmd = a.overlay.Update(m)
		return cmd
	}
	if a.active < len(a.panels) && a.panels[a.active].Capturing() {
		return a.forward(m)
	}
	switch {
	case key.Matches(m, a.keys.quit):
		return tea.Quit
	case key.Matches(m, a.keys.help):
		a.overlay = a.helpOverlay()
		return nil
	case key.Matches(m, a.keys.contexts):
		current := a.target
		if a.e != nil {
			current = a.e.ctx.Name
		}
		a.overlay = newContextSwitcher(a.cfg.Contexts, current)
		return nil
	case key.Matches(m, a.keys.next):
		return a.switchPanel(1)
	case key.Matches(m, a.keys.prev):
		return a.switchPanel(-1)
	}
	return a.forward(m)
}

func (a *app) helpOverlay() *helpOverlay {
	h := &helpOverlay{}
	if a.active < len(a.panels) {
		h.groups = append(h.groups, a.panels[a.active].Keys())
		h.titles = append(h.titles, a.panels[a.active].Title())
	}
	h.groups = append(h.groups, []key.Binding{a.keys.next, a.keys.prev, a.keys.contexts, a.keys.help, a.keys.quit})
	h.titles = append(h.titles, "Global")
	return h
}

// View implements tea.Model.
func (a *app) View() string {
	w, h := max(40, a.width), max(10, a.height)
	bodyH := h - 3
	var body string
	switch {
	case a.overlay != nil:
		body = lipgloss.Place(w, bodyH, lipgloss.Center, lipgloss.Center, a.overlay.View(w, bodyH))
	case a.e == nil:
		body = lipgloss.Place(w, bodyH, lipgloss.Center, lipgloss.Center,
			st.muted.Render("No context open. Press c to choose one, q to quit."))
	default:
		body = a.bodyView(w, bodyH)
	}
	return a.headerView(w) + "\n" + fit(body, w, bodyH) + "\n" + a.footerView(w)
}

func (a *app) headerView(width int) string {
	parts := []string{st.title.Render("mqx")}
	name := a.target
	if a.e != nil {
		name = a.e.ctx.Name
	}
	if name != "" {
		parts = append(parts, "ctx: "+st.navActive.Render(name))
	}
	if a.e != nil {
		parts = append(parts, "broker: "+a.e.b.Name(), st.muted.Render(endpoint(a.e.ctx)))
		if a.e.ctx.ReadOnly {
			parts = append(parts, st.badge.Render("READ-ONLY"))
		}
	}
	switch a.conn {
	case connConnecting:
		parts = append(parts, st.warn.Render(a.spinner.View()+" connecting to "+a.target))
	case connOK:
		parts = append(parts, st.ok.Render("● connected"))
	case connFailed:
		parts = append(parts, st.err.Render("✕ "+a.connErr))
	}
	return truncate(strings.Join(parts, "  "), width)
}

func (a *app) bodyView(width, height int) string {
	var nav strings.Builder
	for i, p := range a.panels {
		if i > 0 {
			nav.WriteString("\n")
		}
		if i == a.active {
			nav.WriteString(st.navActive.Render(pad("▸ "+p.Title(), navWidth)))
		} else {
			nav.WriteString(st.nav.Render(pad("  "+p.Title(), navWidth)))
		}
	}
	navCol := lipgloss.NewStyle().Width(navWidth).Height(height).
		Border(lipgloss.NormalBorder(), false, true, false, false).BorderForeground(colMuted).Render(fit(nav.String(), navWidth, height))
	pw := width - navWidth - 2
	content := ""
	if a.active < len(a.panels) {
		content = a.panels[a.active].View(pw, height)
	}
	return lipgloss.JoinHorizontal(lipgloss.Top, navCol, " "+strings.ReplaceAll(fit(content, pw, height), "\n", "\n "))
}

func (a *app) footerView(width int) string {
	status := st.muted.Render(a.status.text)
	if a.status.err {
		status = st.err.Render(a.status.text)
	}
	var keys []key.Binding
	if a.overlay == nil && a.active < len(a.panels) {
		keys = a.panels[a.active].Keys()
	}
	keys = append(keys, a.keys.list()...)
	a.help.Width = width
	return truncate(status, width) + "\n" + a.help.ShortHelpView(keys)
}

// fit clips s to height lines of at most width cells and pads it to exactly height lines.
func fit(s string, width, height int) string {
	lines := strings.Split(clipLines(s, width, height), "\n")
	for len(lines) < height {
		lines = append(lines, "")
	}
	return strings.Join(lines, "\n")
}
