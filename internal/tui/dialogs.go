package tui

import (
	"strings"

	"github.com/charmbracelet/bubbles/cursor"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
)

// confirmDialog asks before a mutating action. Focus starts on Cancel, so a
// stray enter never mutates; y confirms, n or esc cancels. A dangerous action
// sets typed: then only typing it and pressing enter confirms.
type confirmDialog struct {
	prompt    string
	detail    string
	run       tea.Cmd
	onConfirm bool // focus on [Confirm]
	typed     string
	input     textinput.Model
	mismatch  bool
}

func newConfirm(m confirmMsg) *confirmDialog {
	d := &confirmDialog{prompt: m.prompt, detail: m.detail, run: m.run, typed: m.typed}
	if d.typed != "" {
		d.input = textinput.New()
		d.input.Prompt = "> "
		d.input.CharLimit = 0
		_ = d.input.Cursor.SetMode(cursor.CursorStatic)
		d.input.Focus()
	}
	return d
}

// Update implements overlay.
func (d *confirmDialog) Update(msg tea.Msg) (overlay, tea.Cmd) {
	km, ok := msg.(tea.KeyMsg)
	if !ok {
		return d, nil
	}
	if d.typed != "" {
		return d.updateTyped(km)
	}
	switch km.String() {
	case "y", "Y":
		return nil, d.run
	case "n", "N", "esc", "q":
		return nil, statusInfo("cancelled")
	case "left", "right", "tab", "shift+tab", "h", "l":
		d.onConfirm = !d.onConfirm
	case "enter", " ":
		if d.onConfirm {
			return nil, d.run
		}
		return nil, statusInfo("cancelled")
	}
	return d, nil
}

func (d *confirmDialog) updateTyped(km tea.KeyMsg) (overlay, tea.Cmd) {
	switch km.String() {
	case "esc":
		return nil, statusInfo("cancelled")
	case "enter":
		if d.input.Value() == d.typed {
			return nil, d.run
		}
		d.mismatch = true
		return d, nil
	}
	var cmd tea.Cmd
	d.input, cmd = d.input.Update(km)
	d.mismatch = false
	return d, cmd
}

// View implements overlay.
func (d *confirmDialog) View(width, height int) string {
	inner := min(max(40, width-10), 90)
	var b strings.Builder
	b.WriteString(st.warn.Render(wrap(d.prompt, inner)) + "\n")
	if d.detail != "" {
		b.WriteString("\n" + clipLines(d.detail, inner, max(3, height-12)) + "\n")
	}
	if d.typed != "" {
		b.WriteString("\nType " + st.navActive.Render(d.typed) + " to confirm:\n" + d.input.View() + "\n")
		if d.mismatch {
			b.WriteString(st.err.Render("does not match") + "\n")
		}
		b.WriteString("\n" + st.muted.Render("enter confirm • esc cancel"))
		return st.dialog.Render(b.String())
	}
	cancel, confirm := st.button.Render("Cancel"), st.button.Render("Confirm")
	if d.onConfirm {
		confirm = st.buttonOn.Render("Confirm")
	} else {
		cancel = st.buttonOn.Render("Cancel")
	}
	b.WriteString("\n" + cancel + "  " + confirm + "\n")
	b.WriteString(st.muted.Render("←/→ choose • enter select • y confirm • n/esc cancel"))
	return st.dialog.Render(b.String())
}

// helpOverlay shows every binding of the root and the active panel.
type helpOverlay struct {
	groups [][]key.Binding
	titles []string
}

// Update implements overlay: any key closes it.
func (h *helpOverlay) Update(msg tea.Msg) (overlay, tea.Cmd) {
	if _, ok := msg.(tea.KeyMsg); ok {
		return nil, nil
	}
	return h, nil
}

// View implements overlay.
func (h *helpOverlay) View(_, _ int) string {
	var b strings.Builder
	b.WriteString(st.title.Render("Keys") + "\n")
	for i, g := range h.groups {
		if len(g) == 0 {
			continue
		}
		b.WriteString("\n" + st.header.Render(h.titles[i]) + "\n")
		for _, k := range g {
			if !k.Enabled() {
				continue
			}
			hk := k.Help()
			b.WriteString("  " + pad(hk.Key, 12) + " " + hk.Desc + "\n")
		}
	}
	b.WriteString("\n" + st.muted.Render("press any key to close"))
	return st.dialog.Render(b.String())
}
