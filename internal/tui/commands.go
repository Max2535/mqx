package tui

import (
	"slices"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
)

// maxJumpRows caps how many rows of the current list the palette offers.
const maxJumpRows = 500

// panelKeyMsg delivers a key to the active panel, skipping global keys, so a
// palette entry does exactly what pressing the action's key does.
type panelKeyMsg struct{ key tea.KeyMsg }

// gotoPanelMsg activates the panel at index i.
type gotoPanelMsg struct{ i int }

// jumpMsg selects the row with key in the active panel's list.
type jumpMsg struct{ key string }

// jumper is a view whose rows the palette can jump to.
type jumper interface {
	jumpTargets() (kind string, keys []string)
}

// commands lists everything the palette offers in the current state.
func (a *app) commands() []command {
	var cmds []command
	if a.active < len(a.panels) {
		p := a.panels[a.active]
		cmds = append(cmds, keyCommands(p.Title(), p.Keys())...)
		if j, ok := topView(p).(jumper); ok {
			kind, keys := j.jumpTargets()
			for _, k := range keys[:min(len(keys), maxJumpRows)] {
				msg := jumpMsg{key: k}
				cmds = append(cmds, command{group: kind, label: k, run: func() tea.Msg { return msg }})
			}
		}
		for i, p := range a.panels {
			if i != a.active {
				msg := gotoPanelMsg{i: i}
				cmds = append(cmds, command{group: "Panel", label: "go to " + p.Title(), run: func() tea.Msg { return msg }})
			}
		}
	}
	for _, c := range a.cfg.Contexts {
		if c.Name != a.openName() {
			msg := switchContextMsg{name: c.Name}
			cmds = append(cmds, command{group: "Context", label: "switch to " + c.Name, run: func() tea.Msg { return msg }})
		}
	}
	return append(cmds,
		command{group: "Context", label: "manage contexts (new, edit, test, delete)", key: "c",
			run: func() tea.Msg { return openOverlayMsg{o: a.switcher("")} }},
		command{group: "Global", label: "show all keys", key: "?",
			run: func() tea.Msg { return openOverlayMsg{o: a.helpOverlay()} }},
		command{group: "Global", label: "quit", key: "q", run: tea.Quit},
	)
}

// keyCommands turns a view's enabled bindings into palette entries, leaving
// out plain cursor movement.
func keyCommands(group string, bindings []key.Binding) []command {
	nav := newTableKeys()
	skip := []key.Binding{nav.up, nav.down, nav.pgUp, nav.pgDown, nav.top, nav.bottom}
	var cmds []command
	for _, b := range bindings {
		if !b.Enabled() || len(b.Keys()) == 0 || slices.ContainsFunc(skip, func(s key.Binding) bool {
			return s.Help() == b.Help()
		}) {
			continue
		}
		msg := panelKeyMsg{key: keyFor(b.Keys()[0])}
		cmds = append(cmds, command{group: group, label: b.Help().Desc, key: b.Help().Key,
			run: func() tea.Msg { return msg }})
	}
	return cmds
}

// topView is the view a panel shows: the top of its stack.
func topView(p panel) panel {
	if s, ok := p.(*stack); ok {
		return s.top()
	}
	return p
}

// keyFor builds the key message for a binding's key name.
func keyFor(name string) tea.KeyMsg {
	special := map[string]tea.KeyType{
		"enter": tea.KeyEnter, "esc": tea.KeyEsc, "tab": tea.KeyTab, "shift+tab": tea.KeyShiftTab,
		"up": tea.KeyUp, "down": tea.KeyDown, "left": tea.KeyLeft, "right": tea.KeyRight,
		"home": tea.KeyHome, "end": tea.KeyEnd, "pgup": tea.KeyPgUp, "pgdown": tea.KeyPgDown,
		"backspace": tea.KeyBackspace, "delete": tea.KeyDelete, " ": tea.KeySpace,
		"ctrl+d": tea.KeyCtrlD, "ctrl+u": tea.KeyCtrlU, "ctrl+s": tea.KeyCtrlS,
	}
	if t, ok := special[name]; ok {
		return tea.KeyMsg{Type: t}
	}
	return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(name)}
}
