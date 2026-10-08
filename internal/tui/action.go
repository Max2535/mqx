package tui

import (
	"context"
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
)

// row is one selectable line of a list view. key identifies it (a topic,
// group or exchange name); data holds the broker model it came from.
type row struct {
	key   string
	cells []string
	data  any
}

// action is something a key does in a view: open a form, ask for
// confirmation when it mutates, call the broker and report the result.
type action struct {
	key key.Binding
	// mutating actions go through the guard and are hidden on read_only contexts.
	mutating bool
	// mutatingIf decides per submission (e.g. a KSQL statement); the key stays
	// visible and the guard refuses the mutating submissions on read_only contexts.
	mutatingIf func(v values) bool
	needsRow   bool
	when       func(r *row) bool
	// form, when set, collects values before the action runs.
	form  func(r *row) (title string, fields []field)
	check func(r *row, v values) error
	// describe names the action for the confirm dialog and status bar: "Delete topic orders".
	describe func(r *row, v values) string
	// preview runs before the confirm dialog (e.g. an offset-reset dry run).
	preview func(ctx context.Context, r *row, v values) (string, error)
	run     func(ctx context.Context, r *row, v values) (string, error)
	// cmd replaces the broker call for actions that only navigate.
	cmd func(r *row) tea.Cmd
}

// binding returns the action's key, disabled on read_only contexts when it mutates.
func (a action) binding(e *env) key.Binding {
	if a.mutating {
		return e.mut(a.key)
	}
	return a.key
}

// applies reports whether the action can run on r (nil when nothing is selected).
func (a action) applies(e *env, r *row) bool {
	if !a.binding(e).Enabled() || (a.needsRow && r == nil) {
		return false
	}
	return a.when == nil || a.when(r)
}

func (a action) isMutating(v values) bool {
	return a.mutating || (a.mutatingIf != nil && a.mutatingIf(v))
}

func (a action) what(r *row, v values) string {
	if a.describe != nil {
		return a.describe(r, v)
	}
	return a.key.Help().Desc
}

// previewMsg carries a computed preview back to the view before confirmation.
type previewMsg struct {
	a    action
	r    *row
	v    values
	text string
	err  error
}

// actionDoneMsg reports a finished action.
type actionDoneMsg struct {
	what    string
	out     string
	err     error
	mutated bool
}

// startAction opens the action's form or runs it.
func startAction(e *env, id int, a action, r *row) tea.Cmd {
	if r != nil {
		cp := *r
		r = &cp
	}
	if a.cmd != nil {
		return a.cmd(r)
	}
	if a.form == nil {
		return execAction(e, id, a, r, values{})
	}
	title, fields := a.form(r)
	return openOverlay(newForm(title, fields, func(v values) (tea.Cmd, error) {
		if a.check != nil {
			if err := a.check(r, v); err != nil {
				return nil, err
			}
		}
		return execAction(e, id, a, r, v), nil
	}))
}

func execAction(e *env, id int, a action, r *row, v values) tea.Cmd {
	what := a.what(r, v)
	if !a.isMutating(v) {
		return e.call(id, func(ctx context.Context) tea.Msg {
			out, err := a.run(ctx, r, v)
			return actionDoneMsg{what: what, out: out, err: err}
		})
	}
	if a.preview != nil && e.writable() {
		return e.call(id, func(ctx context.Context) tea.Msg {
			text, err := a.preview(ctx, r, v)
			return previewMsg{a: a, r: r, v: v, text: text, err: err}
		})
	}
	return guarded(e, id, a, r, v, "")
}

func guarded(e *env, id int, a action, r *row, v values, detail string) tea.Cmd {
	what := a.what(r, v)
	return e.mutate(id, what, detail, func(ctx context.Context) tea.Msg {
		out, err := a.run(ctx, r, v)
		return actionDoneMsg{what: what, out: out, err: err, mutated: true}
	})
}

// handleAction processes previewMsg and actionDoneMsg for a view. It reports
// whether msg was one of them, the follow-up command, whether the view should
// reload, and the action's output to display.
func handleAction(e *env, id int, msg tea.Msg) (handled bool, cmd tea.Cmd, reload bool, out string) {
	switch m := msg.(type) {
	case previewMsg:
		if m.err != nil {
			return true, statusErr(fmt.Errorf("%s: %w", m.a.what(m.r, m.v), m.err)), false, ""
		}
		return true, guarded(e, id, m.a, m.r, m.v, m.text), false, ""
	case actionDoneMsg:
		if m.err != nil {
			return true, statusErr(fmt.Errorf("%s: %w", m.what, m.err)), false, ""
		}
		text := m.what + ": done"
		if first, _, _ := strings.Cut(m.out, "\n"); first != "" && !strings.Contains(m.out, "\n") {
			text = m.what + ": " + first
		}
		return true, statusInfo(text), m.mutated, m.out
	}
	return false, nil, false, ""
}
