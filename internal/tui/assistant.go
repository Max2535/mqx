package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/cursor"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/Max2535/mqx/internal/assistant"
	"github.com/Max2535/mqx/internal/config"
)

// assistTimeout bounds one model request plus the broker queries it asks for.
const assistTimeout = 5 * time.Minute

// assistSetup enables the Assistant panel: how to reach the model and the
// assistant section of the config.
type assistSetup struct {
	send assistant.Sender
	cfg  config.Assistant
}

// assistStepMsg carries the result of one assistant step.
type assistStepMsg struct {
	turn assistant.Turn
	err  error
}

type assistEntryKind int

const (
	entryQuestion assistEntryKind = iota
	entryCall
	entryAnswer
	entryNote
	entryError
)

type assistEntry struct {
	kind assistEntryKind
	text string
}

// assistantView is the Assistant panel: ask about the connected broker in
// plain language. Claude answers using read-only broker queries, so the panel
// never needs the mutation guard; changes come back as mqx commands to run.
type assistantView struct {
	e       *env
	id      int
	setup   *assistSetup
	session *assistant.Session
	tools   []assistant.Tool
	input   textinput.Model
	entries []assistEntry
	busy    bool
	scroll  int // lines scrolled up from the bottom
	keys    struct{ ask, clear, up, down, pgUp, pgDown, send, leave key.Binding }
}

func newAssistantPanel(e *env) panel {
	v := &assistantView{e: e, id: e.newID(), setup: e.assist}
	v.tools = assistant.Tools(e.b, assistant.Options{SendPayloads: e.assist.cfg.SendPayloads})
	ti := textinput.New()
	ti.Prompt = "› "
	ti.Placeholder = "why is billing lagging? which topics have a single replica?"
	ti.CharLimit = 2000
	_ = ti.Cursor.SetMode(cursor.CursorStatic)
	v.input = ti
	v.keys.ask = key.NewBinding(key.WithKeys("enter", "i"), key.WithHelp("enter", "ask"))
	v.keys.clear = key.NewBinding(key.WithKeys("x"), key.WithHelp("x", "new conversation"))
	v.keys.up = key.NewBinding(key.WithKeys("up", "k"), key.WithHelp("↑/k", "scroll up"))
	v.keys.down = key.NewBinding(key.WithKeys("down", "j"), key.WithHelp("↓/j", "scroll down"))
	v.keys.pgUp = key.NewBinding(key.WithKeys("pgup"), key.WithHelp("pgup", "page up"))
	v.keys.pgDown = key.NewBinding(key.WithKeys("pgdown"), key.WithHelp("pgdown", "page down"))
	v.keys.send = key.NewBinding(key.WithKeys("enter"), key.WithHelp("enter", "send"))
	v.keys.leave = key.NewBinding(key.WithKeys("esc"), key.WithHelp("esc", "stop typing"))
	return newStack(v)
}

// ID implements panel.
func (v *assistantView) ID() int { return v.id }

// Title implements panel.
func (v *assistantView) Title() string { return "Assistant" }

// Init implements panel.
func (v *assistantView) Init() tea.Cmd { return nil }

// Capturing implements panel.
func (v *assistantView) Capturing() bool { return v.input.Focused() }

// Keys implements panel.
func (v *assistantView) Keys() []key.Binding {
	if v.input.Focused() {
		return []key.Binding{v.keys.send, v.keys.leave, v.keys.pgUp, v.keys.pgDown}
	}
	ask := v.keys.ask
	ask.SetEnabled(!v.busy)
	return []key.Binding{ask, v.keys.clear, v.keys.up, v.keys.down, v.keys.pgUp, v.keys.pgDown}
}

// Update implements panel.
func (v *assistantView) Update(msg tea.Msg) (panel, tea.Cmd) {
	switch m := msg.(type) {
	case assistStepMsg:
		return v, v.stepped(m)
	case tea.KeyMsg:
		if v.input.Focused() {
			return v, v.typing(m)
		}
		switch {
		case key.Matches(m, v.keys.ask) && !v.busy:
			return v, v.input.Focus()
		case key.Matches(m, v.keys.clear) && !v.busy:
			v.session, v.entries, v.scroll = nil, nil, 0
			return v, statusInfo("started a new conversation")
		case key.Matches(m, v.keys.up):
			v.scroll++
		case key.Matches(m, v.keys.down):
			v.scroll = max(0, v.scroll-1)
		case key.Matches(m, v.keys.pgUp):
			v.scroll += 10
		case key.Matches(m, v.keys.pgDown):
			v.scroll = max(0, v.scroll-10)
		}
	}
	return v, nil
}

func (v *assistantView) typing(m tea.KeyMsg) tea.Cmd {
	switch {
	case key.Matches(m, v.keys.leave):
		v.input.Blur()
		return nil
	case key.Matches(m, v.keys.pgUp):
		v.scroll += 10
		return nil
	case key.Matches(m, v.keys.pgDown):
		v.scroll = max(0, v.scroll-10)
		return nil
	case key.Matches(m, v.keys.send):
		q := strings.TrimSpace(v.input.Value())
		if q == "" || v.busy {
			return nil
		}
		v.input.Reset()
		v.input.Blur()
		return v.ask(q)
	}
	var cmd tea.Cmd
	v.input, cmd = v.input.Update(m)
	return cmd
}

func (v *assistantView) ask(q string) tea.Cmd {
	if v.session == nil {
		ac := assistant.Context{Name: v.e.ctx.Name, Broker: v.e.b.Name(), ReadOnly: v.e.ctx.ReadOnly}
		v.session = assistant.NewSession(v.setup.send,
			assistant.Settings{Model: v.setup.cfg.Model, Effort: v.setup.cfg.Effort}, ac, v.tools)
	}
	v.entries = append(v.entries, assistEntry{entryQuestion, q})
	v.scroll = 0
	v.session.Ask(q)
	return v.step()
}

func (v *assistantView) step() tea.Cmd {
	v.busy = true
	s := v.session
	return v.e.callFor(v.id, assistTimeout, func(ctx context.Context) tea.Msg {
		turn, err := s.Step(ctx)
		return assistStepMsg{turn: turn, err: err}
	})
}

func (v *assistantView) stepped(m assistStepMsg) tea.Cmd {
	v.busy = false
	if m.err != nil {
		v.entries = append(v.entries, assistEntry{entryError, m.err.Error()})
		return nil
	}
	if m.turn.Text != "" {
		v.entries = append(v.entries, assistEntry{entryAnswer, m.turn.Text})
	}
	for _, c := range m.turn.Calls {
		text := c.Tool + " " + compactArgs(c.Args)
		if c.Err != "" {
			text += " → " + c.Err
		}
		v.entries = append(v.entries, assistEntry{entryCall, strings.TrimSpace(text)})
	}
	if m.turn.Note != "" {
		v.entries = append(v.entries, assistEntry{entryNote, m.turn.Note})
	}
	if !m.turn.Done {
		return v.step()
	}
	return nil
}

// compactArgs renders {"group":"billing"} as group=billing.
func compactArgs(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "{}" {
		return ""
	}
	m, err := parseJSONObject(raw)
	if err != nil {
		return raw
	}
	parts := make([]string, 0, len(m))
	for _, kv := range m {
		parts = append(parts, kv[0]+"="+kv[1])
	}
	return strings.Join(parts, " ")
}

// View implements panel.
func (v *assistantView) View(width, height int) string {
	var lines []string
	for _, e := range v.entries {
		lines = append(lines, v.render(e, width)...)
	}
	if v.busy {
		lines = append(lines, st.muted.Render(v.e.spin+" thinking…"))
	}
	footer := []string{"", v.inputLine(width)}
	room := max(1, height-len(footer))
	if len(lines) == 0 {
		lines = strings.Split(wrap(v.intro(), width), "\n")
	}
	v.scroll = min(v.scroll, max(0, len(lines)-room))
	end := len(lines) - v.scroll
	start := max(0, end-room)
	body := lines[start:end]
	for len(body) < room {
		body = append(body, "")
	}
	return strings.Join(append(body, footer...), "\n")
}

func (v *assistantView) inputLine(width int) string {
	if v.input.Focused() {
		v.input.Width = max(10, width-3)
		return v.input.View()
	}
	if v.busy {
		return st.muted.Render("› waiting for the answer…")
	}
	return st.muted.Render("› press enter to ask")
}

func (v *assistantView) render(e assistEntry, width int) []string {
	switch e.kind {
	case entryQuestion:
		return append([]string{""}, strings.Split(st.navActive.Render(wrap("› "+e.text, width)), "\n")...)
	case entryCall:
		return []string{st.muted.Render(truncate("  ⋯ "+e.text, width))}
	case entryNote:
		return strings.Split(st.warn.Render(wrap(e.text, width)), "\n")
	case entryError:
		return strings.Split(st.err.Render(wrap(e.text, width)), "\n")
	}
	return strings.Split(wrap(e.text, width), "\n")
}

func (v *assistantView) intro() string {
	data := "Only metadata is sent to the API (topic, group and node details), never message contents; " +
		"set assistant.send_payloads: true in the config to let it peek at messages."
	if v.setup.cfg.SendPayloads {
		data = "assistant.send_payloads is on: it may read message keys, headers and values and send them to the API."
	}
	model := v.setup.cfg.Model
	if model == "" {
		model = assistant.DefaultModel
	}
	return fmt.Sprintf("Ask about %s in plain language. Claude (%s) answers with %d read-only queries against the broker "+
		"and suggests mqx commands for changes; it never changes anything itself.\n\n%s\n\n"+
		"Needs ANTHROPIC_API_KEY (or `ant auth login`). Press enter to ask.", v.e.ctx.Name, model, len(v.tools), data)
}
