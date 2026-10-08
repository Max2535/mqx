package tui

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/config"
)

// contextStore edits the config file the TUI was started with. Editing the
// file is allowed on read_only contexts: read_only guards the broker, not the
// local config.
type contextStore struct {
	cfg  *config.Config
	path string
}

// update applies edit, validates and saves; on error nothing changes.
func (s *contextStore) update(edit func(*config.Config) error) error {
	if s.path == "" {
		return errors.New("no config file to save to; start mqx with --config <path>")
	}
	return s.cfg.Update(s.path, broker.Validator(), edit)
}

// switchContextMsg asks the root model to open another context.
type switchContextMsg struct{ name string }

// contextsChangedMsg reports a saved edit. old is "" for a new context and
// name is "" for a deleted one.
type contextsChangedMsg struct {
	old, name string
	status    string
}

// deleteContextMsg asks the root model to delete a context, after confirmation.
type deleteContextMsg struct{ name string }

// probedMsg is the result of testing a context's connection.
type probedMsg struct {
	name    string
	elapsed time.Duration
	caps    []string
	err     error
}

// probeContext opens c, pings it and lists its capabilities, then closes it.
func probeContext(base context.Context, timeout time.Duration, now func() time.Time,
	open func(context.Context, config.Context) (broker.Broker, error), c config.Context) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(base, timeout)
		defer cancel()
		start := now()
		b, err := open(ctx, c)
		if err != nil {
			return probedMsg{name: c.Name, elapsed: now().Sub(start), err: err}
		}
		defer b.Close()
		err = b.Ping(ctx)
		return probedMsg{name: c.Name, elapsed: now().Sub(start), caps: broker.Capabilities(b), err: err}
	}
}

// contextSwitcher lists the config's contexts: enter opens one for this
// session, u makes it the saved default, t tests it, n/e/d create, edit and delete.
type contextSwitcher struct {
	store   *contextStore
	open    string // context open in this session
	probe   func(config.Context) tea.Cmd
	t       *table
	result  string // last test result
	failed  bool
	testing string // context being tested
}

func newContextSwitcher(store *contextStore, open, focus string, probe func(config.Context) tea.Cmd) *contextSwitcher {
	s := &contextSwitcher{store: store, open: open, probe: probe, t: newTable("NAME", "BROKER", "MODE", "ENDPOINT")}
	if focus == "" {
		focus = open
	}
	s.refresh(focus)
	return s
}

// refresh rebuilds the rows from the config and selects focus when present.
func (s *contextSwitcher) refresh(focus string) {
	contexts := s.store.cfg.Contexts
	rows := make([][]string, len(contexts))
	sel := 0
	for i, c := range contexts {
		name := "  " + c.Name
		if c.Name == s.open {
			name = "* " + c.Name
		}
		if c.Name == s.store.cfg.CurrentContext {
			name += " (default)"
		}
		mode := "read-write"
		if c.ReadOnly {
			mode = "read-only"
		}
		if c.Name == focus {
			sel = i
		}
		rows[i] = []string{name, c.Broker, mode, endpoint(c)}
	}
	s.t.setRows(rows)
	s.t.selectRow(sel)
}

func (s *contextSwitcher) selected() (config.Context, bool) {
	i := s.t.selected()
	if i < 0 || i >= len(s.store.cfg.Contexts) {
		return config.Context{}, false
	}
	return s.store.cfg.Contexts[i], true
}

// Update implements overlay.
func (s *contextSwitcher) Update(msg tea.Msg) (overlay, tea.Cmd) {
	if m, ok := msg.(probedMsg); ok {
		s.showProbe(m)
		return s, nil
	}
	km, ok := msg.(tea.KeyMsg)
	if !ok {
		return s, nil
	}
	if s.t.capturing() {
		_, cmd := s.t.update(km)
		return s, cmd
	}
	k := km.String()
	switch k {
	case "esc", "q", "c":
		return nil, nil
	case "n":
		return s, openOverlay(newBrokerPicker(s.store))
	}
	c, ok := s.selected()
	if !ok {
		_, cmd := s.t.update(km)
		return s, cmd
	}
	switch k {
	case "enter":
		return nil, func() tea.Msg { return switchContextMsg{name: c.Name} }
	case "t":
		s.testing, s.result = c.Name, ""
		return s, s.probe(c.Clone())
	case "e":
		return s, openOverlay(newContextForm(s.store, c, false))
	case "u":
		if err := s.store.update(func(cfg *config.Config) error { return cfg.Use(c.Name) }); err != nil {
			return s, statusErr(err)
		}
		s.refresh(c.Name)
		return s, statusInfo(fmt.Sprintf("default context is now %s (saved to %s)", c.Name, s.store.path))
	case "d":
		detail := fmt.Sprintf("Removes it from %s. The broker itself is not touched.", s.store.path)
		if c.Name == s.open {
			detail += "\nIt is open now and will be closed."
		}
		return s, func() tea.Msg {
			return confirmMsg{prompt: fmt.Sprintf("Delete context %q?", c.Name), detail: detail,
				run: func() tea.Msg { return deleteContextMsg{name: c.Name} }}
		}
	}
	_, cmd := s.t.update(km)
	return s, cmd
}

func (s *contextSwitcher) showProbe(m probedMsg) {
	if m.name != s.testing {
		return // a result for an earlier test
	}
	s.testing = ""
	ms := m.elapsed.Round(time.Millisecond)
	if m.err != nil {
		s.result, s.failed = fmt.Sprintf("✕ %v (%s)", m.err, ms), true
		return
	}
	s.result, s.failed = fmt.Sprintf("✓ %s reachable in %s • %d capabilities: %s",
		m.name, ms, len(m.caps), strings.Join(m.caps, ", ")), false
}

// View implements overlay.
func (s *contextSwitcher) View(width, height int) string {
	w := min(max(40, width-10), 100)
	var b strings.Builder
	b.WriteString(st.title.Render("Contexts") + st.muted.Render("  "+s.store.path) + "\n\n")
	if len(s.store.cfg.Contexts) == 0 {
		b.WriteString(st.muted.Render("No contexts yet. Press n to create one.") + "\n")
	} else {
		b.WriteString(s.t.view(w, min(len(s.store.cfg.Contexts)+1, max(3, height-10))) + "\n")
	}
	switch {
	case s.testing != "":
		b.WriteString("\n" + st.warn.Render("testing "+s.testing+"…") + "\n")
	case s.result != "" && s.failed:
		b.WriteString("\n" + st.err.Render(wrap(s.result, w)) + "\n")
	case s.result != "":
		b.WriteString("\n" + st.ok.Render(wrap(s.result, w)) + "\n")
	}
	b.WriteString("\n" + st.muted.Render(wrap("enter open • u set default • t test • n new • e edit • d delete • / filter • esc close", w)))
	return st.dialog.Render(b.String())
}

// newBrokerPicker asks for the broker type of a new context.
func newBrokerPicker(store *contextStore) *form {
	types := broker.Types()
	if i := slices.Index(types, "kafka"); i > 0 { // the most common choice first
		types[0], types[i] = types[i], types[0]
	}
	fields := []field{{key: "broker", label: "broker", kind: fieldChoice, choices: types,
		hint: "the broker type; its fields come next"}}
	return newForm("New context", fields, func(v values) (tea.Cmd, error) {
		return openOverlay(newContextForm(store, config.Context{Broker: v["broker"]}, true)), nil
	})
}

// newContextForm edits c, or creates it when isNew. Fields come from the
// broker's driver, so a new broker needs no change here. Credentials are only
// ever env var names; config.Context.Set and Validate refuse secrets.
func newContextForm(store *contextStore, c config.Context, isNew bool) *form {
	specs := broker.ContextFields(c.Broker)
	fields := []field{{key: "name", label: "name", hint: "unique, e.g. prod-kafka", value: c.Name}}
	for _, sp := range specs {
		fd := field{key: sp.Key, label: sp.Key, hint: sp.Help, value: c.Get(sp.Key)}
		switch {
		case sp.Kind == config.FieldBool:
			fd.kind = fieldBool
		case len(sp.Choices) > 0:
			fd.kind, fd.choices = fieldChoice, sp.Choices
		}
		fields = append(fields, fd)
	}
	title := fmt.Sprintf("Edit context %s (%s)", c.Name, c.Broker)
	if isNew {
		title = fmt.Sprintf("New %s context", c.Broker)
	}
	old := c.Name
	return newForm(title, fields, func(v values) (tea.Cmd, error) {
		next := c.Clone() // fields this form does not show are kept
		next.Name = v["name"]
		var errs []error
		for _, sp := range specs {
			if err := next.Set(sp.Key, v[sp.Key]); err != nil {
				errs = append(errs, err)
			}
		}
		if err := errors.Join(errs...); err != nil {
			return nil, err
		}
		err := store.update(func(cfg *config.Config) error {
			if isNew {
				if cfg.CurrentContext == "" { // the first context becomes the default, like `mqx ctx add`
					cfg.CurrentContext = next.Name
				}
				return cfg.Add(next)
			}
			return cfg.Replace(old, next)
		})
		if err != nil {
			return nil, err
		}
		changed := contextsChangedMsg{old: old, name: next.Name, status: fmt.Sprintf("saved context %s to %s", next.Name, store.path)}
		if isNew {
			changed.old = ""
		}
		return func() tea.Msg { return changed }, nil
	})
}
