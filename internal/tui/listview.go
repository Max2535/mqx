package tui

import (
	"context"
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
)

// resource describes one list a listView shows: how to load it, what enter
// opens and which actions its keys run.
type resource struct {
	title string
	cols  []string
	load  func(ctx context.Context) (listing, error)
	// loader, when set, replaces load: it runs on the UI goroutine to capture
	// state (such as the selected topic) and returns the load to run.
	loader  func() func(ctx context.Context) (listing, error)
	open    func(r row) panel
	actions []action
	// onSelect runs whenever the cursor lands on a row.
	onSelect func(r row)
	// focus, after the first load, selects the row with this key and opens it
	// with focusOpen (or open). focusKind names it in the not-found error.
	focus     string
	focusKind string
	focusOpen func(r row) panel
	// stale reports whether the list must reload when its panel is shown again.
	stale func() bool
	// loaded sees every successful load, on the UI goroutine.
	loaded func(l listing)
}

// listing is a loaded resource. cols, when set, overrides resource.cols.
type listing struct {
	header string
	cols   []string
	rows   []row
	// extra carries facts beyond the rows to resource.loaded.
	extra any
}

type loadedMsg struct {
	cur int
	l   listing
	err error
}

type listKeys struct {
	refresh, open, next key.Binding
}

// listView renders one or more resources as a filterable table with a header
// and an output box, and runs their actions. 'v' cycles between resources.
type listView struct {
	name    string // panel title; defaults to the first resource's title
	e       *env
	id      int
	res     []resource
	cur     int
	t       *table
	lst     listing
	loading bool
	loaded  bool
	err     error
	out     string
	keys    listKeys
}

func newListView(e *env, res ...resource) *listView {
	lv := &listView{e: e, id: e.newID(), res: res, keys: listKeys{
		refresh: key.NewBinding(key.WithKeys("r"), key.WithHelp("r", "refresh")),
		open:    key.NewBinding(key.WithKeys("enter"), key.WithHelp("enter", "open")),
		next:    key.NewBinding(key.WithKeys("v"), key.WithHelp("v", "next view")),
	}}
	lv.t = newTable(res[0].cols...)
	return lv
}

func (lv *listView) r() *resource { return &lv.res[lv.cur] }

// ID implements panel.
func (lv *listView) ID() int { return lv.id }

// Title implements panel.
func (lv *listView) Title() string {
	if lv.name != "" {
		return lv.name
	}
	return lv.res[0].title
}

// Init implements panel.
func (lv *listView) Init() tea.Cmd { return lv.load() }

// Capturing implements panel.
func (lv *listView) Capturing() bool { return lv.t.capturing() }

func (lv *listView) load() tea.Cmd {
	lv.loading = true
	cur, load := lv.cur, lv.r().load
	if lv.r().loader != nil {
		load = lv.r().loader()
	}
	return lv.e.call(lv.id, func(ctx context.Context) tea.Msg {
		l, err := load(ctx)
		return loadedMsg{cur: cur, l: l, err: err}
	})
}

// selectedRow returns the row under the cursor, or nil.
func (lv *listView) selectedRow() *row {
	i := lv.t.selected()
	if i < 0 || i >= len(lv.lst.rows) {
		return nil
	}
	return &lv.lst.rows[i]
}

// Update implements panel.
func (lv *listView) Update(msg tea.Msg) (panel, tea.Cmd) {
	if ok, cmd, reload, out := handleAction(lv.e, lv.id, msg); ok {
		if out != "" {
			lv.out = out
		}
		if reload {
			return lv, tea.Batch(cmd, lv.load())
		}
		return lv, cmd
	}
	switch m := msg.(type) {
	case activateMsg:
		if m.active && lv.loaded && lv.r().stale != nil && lv.r().stale() {
			return lv, lv.load()
		}
	case loadedMsg:
		return lv, lv.applyLoaded(m)
	case jumpMsg:
		for i, r := range lv.lst.rows {
			if r.key == m.key {
				lv.t.selectRow(i)
				lv.selected()
				return lv, nil
			}
		}
		return lv, statusErr(fmt.Errorf("%q is no longer in the list", m.key))
	case tea.KeyMsg:
		return lv, lv.key(m)
	}
	return lv, nil
}

func (lv *listView) applyLoaded(m loadedMsg) tea.Cmd {
	if m.cur != lv.cur {
		return nil
	}
	lv.loading, lv.loaded, lv.err = false, true, m.err
	if m.err != nil {
		lv.t.setRows(nil)
		lv.lst = listing{}
		return nil
	}
	lv.lst = m.l
	if lv.r().loaded != nil {
		lv.r().loaded(m.l)
	}
	cols := lv.r().cols
	if m.l.cols != nil {
		cols = m.l.cols
	}
	lv.t.cols = cols
	cells := make([][]string, len(m.l.rows))
	for i, r := range m.l.rows {
		cells[i] = r.cells
	}
	lv.t.setRows(cells)
	res := lv.r()
	if res.focus == "" {
		lv.selected()
		return nil
	}
	focus := res.focus
	res.focus = ""
	for i, r := range lv.lst.rows {
		if r.key != focus {
			continue
		}
		lv.t.selectRow(i)
		lv.selected()
		open := res.focusOpen
		if open == nil {
			open = res.open
		}
		if open == nil {
			return nil
		}
		return lv.e.push(lv.id, open(r))
	}
	return statusErr(fmt.Errorf("%s %q not found in context %q", res.focusKind, focus, lv.e.ctx.Name))
}

func (lv *listView) selected() {
	if r := lv.selectedRow(); r != nil && lv.r().onSelect != nil {
		lv.r().onSelect(*r)
	}
}

func (lv *listView) key(m tea.KeyMsg) tea.Cmd {
	if lv.t.capturing() {
		_, cmd := lv.t.update(m)
		lv.selected()
		return cmd
	}
	if used, cmd := lv.t.update(m); used {
		lv.selected()
		return cmd
	}
	res := lv.r()
	switch {
	case key.Matches(m, lv.keys.refresh):
		return lv.load()
	case key.Matches(m, lv.keys.open) && res.open != nil:
		if r := lv.selectedRow(); r != nil {
			return lv.e.push(lv.id, res.open(*r))
		}
		return nil
	case key.Matches(m, lv.keys.next) && len(lv.res) > 1:
		lv.cur = (lv.cur + 1) % len(lv.res)
		lv.t = newTable(lv.r().cols...)
		lv.lst, lv.err, lv.out = listing{}, nil, ""
		return lv.load()
	}
	r := lv.selectedRow()
	for _, a := range res.actions {
		if a.applies(lv.e, r) && key.Matches(m, a.key) {
			return startAction(lv.e, lv.id, a, r)
		}
	}
	return nil
}

// jumpTargets implements jumper.
func (lv *listView) jumpTargets() (string, []string) {
	keys := make([]string, len(lv.lst.rows))
	for i, r := range lv.lst.rows {
		keys[i] = r.key
	}
	return lv.r().title, keys
}

// Keys implements panel.
func (lv *listView) Keys() []key.Binding {
	keys := lv.t.bindings()
	keys = append(keys, lv.keys.refresh)
	if lv.r().open != nil {
		keys = append(keys, lv.keys.open)
	}
	if len(lv.res) > 1 {
		keys = append(keys, lv.keys.next)
	}
	r := lv.selectedRow()
	for _, a := range lv.r().actions {
		if a.applies(lv.e, r) {
			keys = append(keys, a.key)
		}
	}
	return keys
}

// View implements panel.
func (lv *listView) View(width, height int) string {
	var b strings.Builder
	title := lv.r().title
	if len(lv.res) > 1 {
		names := make([]string, len(lv.res))
		for i, r := range lv.res {
			names[i] = r.title
			if i == lv.cur {
				names[i] = st.navActive.Render("[" + r.title + "]")
			}
		}
		title = strings.Join(names, "  ")
	} else {
		title = st.title.Render(title)
	}
	if lv.loading {
		title += " " + st.muted.Render(lv.e.spin+" loading…")
	}
	b.WriteString(truncate(title, width))
	used := 1
	if lv.err != nil {
		b.WriteString("\n" + st.err.Render(clipLines("error: "+lv.err.Error(), width, 3)))
		used += min(3, countLines("error: "+lv.err.Error()))
	}
	if h := lv.lst.header; h != "" {
		n := min(countLines(h), max(1, height/2))
		b.WriteString("\n" + clipLines(h, width, n))
		used += n
	}
	outLines := 0
	if lv.out != "" {
		outLines = min(countLines(lv.out)+1, max(2, height/3))
	}
	b.WriteString("\n" + lv.t.view(width, max(2, height-used-outLines)))
	if outLines > 0 {
		b.WriteString("\n" + st.title.Render("Result") + "\n" + clipLines(lv.out, width, outLines-1))
	}
	return b.String()
}
