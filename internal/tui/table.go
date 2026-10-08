package tui

import (
	"strings"

	"github.com/charmbracelet/bubbles/cursor"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// tableKeys are the navigation bindings every list shares.
type tableKeys struct {
	up, down, pgUp, pgDown, top, bottom, filter key.Binding
}

func newTableKeys() tableKeys {
	return tableKeys{
		up:     key.NewBinding(key.WithKeys("up", "k"), key.WithHelp("↑/k", "up")),
		down:   key.NewBinding(key.WithKeys("down", "j"), key.WithHelp("↓/j", "down")),
		pgUp:   key.NewBinding(key.WithKeys("pgup"), key.WithHelp("pgup", "page up")),
		pgDown: key.NewBinding(key.WithKeys("pgdown"), key.WithHelp("pgdn", "page down")),
		top:    key.NewBinding(key.WithKeys("home", "g"), key.WithHelp("g", "top")),
		bottom: key.NewBinding(key.WithKeys("end", "G"), key.WithHelp("G", "bottom")),
		filter: key.NewBinding(key.WithKeys("/"), key.WithHelp("/", "filter")),
	}
}

// table is a scrollable, filterable list of rows with a highlighted cursor.
type table struct {
	cols    []string
	rows    [][]string
	visible []int // indices into rows that pass the filter
	cursor  int   // index into visible
	offset  int   // first visible row shown
	keys    tableKeys

	filter    textinput.Model
	filtering bool
}

func newTable(cols ...string) *table {
	ti := textinput.New()
	ti.Prompt = "/"
	_ = ti.Cursor.SetMode(cursor.CursorStatic)
	return &table{cols: cols, keys: newTableKeys(), filter: ti}
}

// setRows replaces the rows, keeping the filter and, when possible, the cursor position.
func (t *table) setRows(rows [][]string) {
	sel := t.selected()
	var selKey string
	if sel >= 0 && sel < len(t.rows) && len(t.rows[sel]) > 0 {
		selKey = t.rows[sel][0]
	}
	t.rows = rows
	t.applyFilter()
	for i, idx := range t.visible {
		if len(rows[idx]) > 0 && rows[idx][0] == selKey {
			t.cursor = i
			return
		}
	}
	t.cursor = min(t.cursor, max(0, len(t.visible)-1))
}

func (t *table) applyFilter() {
	q := strings.ToLower(t.filter.Value())
	t.visible = t.visible[:0]
	for i, r := range t.rows {
		if q == "" || strings.Contains(strings.ToLower(strings.Join(r, " ")), q) {
			t.visible = append(t.visible, i)
		}
	}
	if t.cursor >= len(t.visible) {
		t.cursor = max(0, len(t.visible)-1)
	}
}

// selected returns the index into rows under the cursor, or -1.
func (t *table) selected() int {
	if t.cursor < 0 || t.cursor >= len(t.visible) {
		return -1
	}
	return t.visible[t.cursor]
}

// selectRow moves the cursor to rows[i], clearing the filter if it hides it.
func (t *table) selectRow(i int) {
	for vi, idx := range t.visible {
		if idx == i {
			t.cursor = vi
			return
		}
	}
	t.filter.SetValue("")
	t.applyFilter()
	if i >= 0 && i < len(t.visible) {
		t.cursor = i
	}
}

func (t *table) capturing() bool { return t.filtering }

// update handles navigation and filter keys; it reports whether it used the key.
func (t *table) update(msg tea.KeyMsg) (bool, tea.Cmd) {
	if t.filtering {
		switch msg.Type {
		case tea.KeyEnter:
			t.filtering = false
			t.filter.Blur()
			return true, nil
		case tea.KeyEsc:
			t.filtering = false
			t.filter.Blur()
			t.filter.SetValue("")
			t.applyFilter()
			return true, nil
		}
		var cmd tea.Cmd
		t.filter, cmd = t.filter.Update(msg)
		t.applyFilter()
		return true, cmd
	}
	if key.Matches(msg, t.keys.filter) {
		t.filtering = true
		return true, t.filter.Focus()
	}
	return t.updateNav(msg)
}

// updateNav handles cursor movement only.
func (t *table) updateNav(msg tea.KeyMsg) (bool, tea.Cmd) {
	n := len(t.visible)
	switch {
	case key.Matches(msg, t.keys.up):
		t.cursor = max(0, t.cursor-1)
	case key.Matches(msg, t.keys.down):
		t.cursor = max(0, min(n-1, t.cursor+1))
	case key.Matches(msg, t.keys.pgUp):
		t.cursor = max(0, t.cursor-10)
	case key.Matches(msg, t.keys.pgDown):
		t.cursor = max(0, min(n-1, t.cursor+10))
	case key.Matches(msg, t.keys.top):
		t.cursor = 0
	case key.Matches(msg, t.keys.bottom):
		t.cursor = max(0, n-1)
	default:
		return false, nil
	}
	return true, nil
}

func (t *table) bindings() []key.Binding {
	return []key.Binding{t.keys.up, t.keys.down, t.keys.filter}
}

// view renders the header, the rows that fit in height and the filter line.
func (t *table) view(width, height int) string {
	var b strings.Builder
	if t.filtering || t.filter.Value() != "" {
		t.filter.Width = max(1, width-2)
		b.WriteString(t.filter.View() + "\n")
		height--
	}
	widths := t.widths(width)
	b.WriteString(st.header.Render(t.line(t.cols, widths)))
	height--
	if len(t.visible) == 0 {
		b.WriteString("\n" + st.muted.Render("  (none)"))
		return b.String()
	}
	if height < 1 {
		height = 1
	}
	if t.cursor < t.offset {
		t.offset = t.cursor
	}
	if t.cursor >= t.offset+height {
		t.offset = t.cursor - height + 1
	}
	t.offset = max(0, min(t.offset, max(0, len(t.visible)-height)))
	for i := t.offset; i < len(t.visible) && i < t.offset+height; i++ {
		l := t.line(t.rows[t.visible[i]], widths)
		if i == t.cursor {
			l = st.selected.Render(l)
		}
		b.WriteString("\n" + l)
	}
	return b.String()
}

func (t *table) line(cells []string, widths []int) string {
	parts := make([]string, len(widths))
	for i, w := range widths {
		c := ""
		if i < len(cells) {
			c = cells[i]
		}
		parts[i] = pad(c, w)
	}
	return strings.Join(parts, " ")
}

// widths sizes columns to their content, shrinking the widest ones to fit.
func (t *table) widths(total int) []int {
	n := len(t.cols)
	if n == 0 {
		return nil
	}
	w := make([]int, n)
	for i, c := range t.cols {
		w[i] = lipgloss.Width(c)
	}
	for _, r := range t.rows {
		for i := 0; i < n && i < len(r); i++ {
			w[i] = max(w[i], min(lipgloss.Width(r[i]), 60))
		}
	}
	avail := total - (n - 1)
	for sum(w) > avail {
		widest := 0
		for i := range w {
			if w[i] > w[widest] {
				widest = i
			}
		}
		if w[widest] <= 4 {
			break
		}
		w[widest]--
	}
	return w
}

func sum(xs []int) int {
	s := 0
	for _, x := range xs {
		s += x
	}
	return s
}
