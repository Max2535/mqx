package tui

import (
	"context"
	"strings"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
)

// closer is implemented by views that hold resources (e.g. a follow stream)
// to release when they leave the stack.
type closer interface{ close() }

// stack is a top-level panel: a root view plus the views opened from it
// (detail, message browser). esc goes back.
type stack struct {
	views []panel
	back  key.Binding
}

func newStack(root panel) *stack {
	return &stack{views: []panel{root}, back: key.NewBinding(key.WithKeys("esc", "backspace"), key.WithHelp("esc", "back"))}
}

func (s *stack) top() panel { return s.views[len(s.views)-1] }

// ID implements panel.
func (s *stack) ID() int { return s.views[0].ID() }

// Title implements panel.
func (s *stack) Title() string { return s.views[0].Title() }

// Init implements panel.
func (s *stack) Init() tea.Cmd { return s.views[0].Init() }

// Capturing implements panel.
func (s *stack) Capturing() bool { return s.top().Capturing() }

// Update implements panel.
func (s *stack) Update(msg tea.Msg) (panel, tea.Cmd) {
	switch m := msg.(type) {
	case routedMsg:
		for i, v := range s.views {
			if v.ID() != m.to {
				continue
			}
			if p, ok := m.msg.(pushMsg); ok {
				s.truncate(i + 1)
				s.views = append(s.views, p.v)
				return s, p.v.Init()
			}
			var cmd tea.Cmd
			s.views[i], cmd = v.Update(m.msg)
			return s, cmd
		}
		return s, nil
	case tea.KeyMsg:
		if len(s.views) > 1 && !s.top().Capturing() && key.Matches(m, s.back) {
			s.truncate(len(s.views) - 1)
			var cmd tea.Cmd
			s.views[len(s.views)-1], cmd = s.top().Update(activateMsg{active: true})
			return s, cmd
		}
	}
	var cmd tea.Cmd
	s.views[len(s.views)-1], cmd = s.top().Update(msg)
	return s, cmd
}

// truncate drops the views from n on, closing them.
func (s *stack) truncate(n int) {
	for _, v := range s.views[n:] {
		if c, ok := v.(closer); ok {
			c.close()
		}
	}
	s.views = s.views[:n]
}

func (s *stack) close() { s.truncate(1) }

// View implements panel.
func (s *stack) View(width, height int) string {
	if len(s.views) == 1 {
		return s.top().View(width, height)
	}
	crumbs := make([]string, len(s.views))
	for i, v := range s.views {
		crumbs[i] = v.Title()
	}
	return st.muted.Render(truncate(strings.Join(crumbs, " › "), width)) + "\n" + s.top().View(width, height-1)
}

// Keys implements panel.
func (s *stack) Keys() []key.Binding {
	keys := s.top().Keys()
	if len(s.views) > 1 {
		keys = append(keys, s.back)
	}
	return keys
}

// textView shows a scrollable block of text loaded from the broker.
type textView struct {
	e       *env
	id      int
	title   string
	load    func(ctx context.Context) (string, error)
	text    string
	err     error
	loading bool
	offset  int
	keys    struct{ up, down, refresh key.Binding }
}

type textLoadedMsg struct {
	text string
	err  error
}

func newTextView(e *env, title string, load func(ctx context.Context) (string, error)) *textView {
	v := &textView{e: e, id: e.newID(), title: title, load: load}
	v.keys.up = key.NewBinding(key.WithKeys("up", "k"), key.WithHelp("↑/k", "scroll up"))
	v.keys.down = key.NewBinding(key.WithKeys("down", "j"), key.WithHelp("↓/j", "scroll down"))
	v.keys.refresh = key.NewBinding(key.WithKeys("r"), key.WithHelp("r", "refresh"))
	return v
}

// ID implements panel.
func (v *textView) ID() int { return v.id }

// Title implements panel.
func (v *textView) Title() string { return v.title }

// Capturing implements panel.
func (v *textView) Capturing() bool { return false }

// Init implements panel.
func (v *textView) Init() tea.Cmd {
	v.loading = true
	load := v.load
	return v.e.call(v.id, func(ctx context.Context) tea.Msg {
		text, err := load(ctx)
		return textLoadedMsg{text: text, err: err}
	})
}

// Update implements panel.
func (v *textView) Update(msg tea.Msg) (panel, tea.Cmd) {
	switch m := msg.(type) {
	case textLoadedMsg:
		v.loading, v.text, v.err = false, m.text, m.err
	case tea.KeyMsg:
		switch {
		case key.Matches(m, v.keys.up):
			v.offset = max(0, v.offset-1)
		case key.Matches(m, v.keys.down):
			v.offset = min(max(0, countLines(v.text)-1), v.offset+1)
		case key.Matches(m, v.keys.refresh):
			return v, v.Init()
		}
	}
	return v, nil
}

// View implements panel.
func (v *textView) View(width, height int) string {
	head := st.title.Render(v.title)
	if v.loading {
		head += " " + st.muted.Render(v.e.spin+" loading…")
	}
	if v.err != nil {
		return head + "\n" + st.err.Render(clipLines("error: "+v.err.Error(), width, height-1))
	}
	lines := strings.Split(v.text, "\n")
	lines = lines[min(v.offset, len(lines)):]
	return head + "\n" + clipLines(strings.Join(lines, "\n"), width, height-1)
}

// Keys implements panel.
func (v *textView) Keys() []key.Binding { return []key.Binding{v.keys.up, v.keys.down, v.keys.refresh} }
