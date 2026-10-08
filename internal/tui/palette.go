package tui

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/charmbracelet/bubbles/cursor"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
)

// command is one entry of the command palette.
type command struct {
	group string // where it comes from: a panel title, "Panel", "Context", ...
	label string
	key   string // the shortcut that does the same, "" if none
	run   func() tea.Msg
}

func (c command) id() string { return c.group + "\x00" + c.label }

// recentCommands remembers the palette entries run this session, newest first.
type recentCommands struct{ ids []string }

func (r *recentCommands) add(id string) {
	r.ids = slices.DeleteFunc(r.ids, func(s string) bool { return s == id })
	r.ids = append([]string{id}, r.ids...)
	if len(r.ids) > 20 {
		r.ids = r.ids[:20]
	}
}

// rank is 0 for the newest entry, len(ids) for one never run.
func (r *recentCommands) rank(id string) int {
	if i := slices.Index(r.ids, id); i >= 0 {
		return i
	}
	return len(r.ids)
}

// palette is the ':' overlay: type to fuzzy-find any action of the current
// view, a row to jump to, a panel or a context, and run it with enter.
// Actions run through their normal keys, so forms, confirmation and the
// read_only guard apply exactly as when the key is pressed.
type palette struct {
	input  textinput.Model
	all    []command
	shown  []command
	cursor int
	recent *recentCommands
}

func newPalette(cmds []command, recent *recentCommands) *palette {
	ti := textinput.New()
	ti.Prompt = "> "
	ti.Placeholder = "type to search actions, rows, panels and contexts"
	ti.CharLimit = 0
	_ = ti.Cursor.SetMode(cursor.CursorStatic)
	ti.Focus()
	p := &palette{input: ti, all: cmds, recent: recent}
	p.filter()
	return p
}

// filter ranks the commands for the current query: by match quality, then by
// how recently they ran. An empty query lists recent commands first.
func (p *palette) filter() {
	q := p.input.Value()
	type scored struct {
		c     command
		score int
	}
	var hits []scored
	for _, c := range p.all {
		s, ok := fuzzyScore(q, c.label+" "+c.group)
		if ok {
			hits = append(hits, scored{c, s})
		}
	}
	sort.SliceStable(hits, func(i, j int) bool {
		if hits[i].score != hits[j].score {
			return hits[i].score > hits[j].score
		}
		return p.recent.rank(hits[i].c.id()) < p.recent.rank(hits[j].c.id())
	})
	p.shown = p.shown[:0]
	for _, h := range hits {
		p.shown = append(p.shown, h.c)
	}
	p.cursor = 0
}

// Update implements overlay.
func (p *palette) Update(msg tea.Msg) (overlay, tea.Cmd) {
	km, ok := msg.(tea.KeyMsg)
	if !ok {
		return p, nil
	}
	switch km.String() {
	case "esc":
		return nil, nil
	case "up", "ctrl+p", "shift+tab":
		if len(p.shown) > 0 {
			p.cursor = (p.cursor - 1 + len(p.shown)) % len(p.shown)
		}
		return p, nil
	case "down", "ctrl+n", "tab":
		if len(p.shown) > 0 {
			p.cursor = (p.cursor + 1) % len(p.shown)
		}
		return p, nil
	case "enter":
		if len(p.shown) == 0 {
			return p, nil
		}
		c := p.shown[p.cursor]
		p.recent.add(c.id())
		return nil, c.run
	}
	before := p.input.Value()
	var cmd tea.Cmd
	p.input, cmd = p.input.Update(km)
	if p.input.Value() != before {
		p.filter()
	}
	return p, cmd
}

// View implements overlay.
func (p *palette) View(width, height int) string {
	inner := min(max(40, width-10), 80)
	var b strings.Builder
	b.WriteString(st.title.Render("Commands") + "\n\n")
	p.input.Width = inner - 3
	b.WriteString(p.input.View() + "\n\n")
	rows := max(3, height-10)
	if len(p.shown) == 0 {
		b.WriteString(st.muted.Render("no matching commands") + "\n")
	}
	first := max(0, min(p.cursor-rows/2, len(p.shown)-rows))
	last := min(len(p.shown), first+rows)
	groupW, keyW := 0, 0
	for _, c := range p.shown[first:last] {
		groupW, keyW = max(groupW, len(c.group)), max(keyW, len(c.key))
	}
	groupW, keyW = min(groupW, 14), min(keyW, 10)
	labelW := max(10, inner-groupW-keyW-6)
	for i := first; i < last; i++ {
		c := p.shown[i]
		line := st.muted.Render(pad(truncate(c.group, groupW), groupW)) + "  " +
			pad(truncate(c.label, labelW), labelW) + "  " + st.muted.Render(truncate(c.key, keyW))
		if i == p.cursor {
			line = st.navActive.Render("▸ ") + line
		} else {
			line = "  " + line
		}
		b.WriteString(line + "\n")
	}
	if len(p.shown) > rows {
		b.WriteString(st.muted.Render(fmt.Sprintf("%d of %d", p.cursor+1, len(p.shown))) + "\n")
	}
	b.WriteString("\n" + st.muted.Render("↑/↓ choose • enter run • esc close"))
	return st.dialog.Width(inner + 2).Render(b.String())
}
