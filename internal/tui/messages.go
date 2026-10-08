package tui

import (
	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
)

// statusMsg sets the status bar.
type statusMsg struct {
	text string
	err  bool
}

func statusInfo(text string) tea.Cmd {
	return func() tea.Msg { return statusMsg{text: text} }
}

func statusErr(err error) tea.Cmd {
	return func() tea.Msg { return statusMsg{text: err.Error(), err: true} }
}

// openOverlayMsg asks the root model to show an overlay (form, dialog).
type openOverlayMsg struct{ o overlay }

func openOverlay(o overlay) tea.Cmd {
	return func() tea.Msg { return openOverlayMsg{o: o} }
}

// pushMsg asks the panel stack holding the sending view to open v on top of it.
type pushMsg struct{ v panel }

// activateMsg tells a panel it became (or stopped being) the visible one.
type activateMsg struct{ active bool }

// panel is one screen of the TUI: a top-level entry of the navigation list,
// or a view pushed on top of one (detail, message browser).
type panel interface {
	// ID routes results of the panel's requests back to it.
	ID() int
	Title() string
	Init() tea.Cmd
	Update(tea.Msg) (panel, tea.Cmd)
	View(width, height int) string
	// Keys lists the enabled bindings for the footer and help overlay.
	Keys() []key.Binding
	// Capturing reports whether the panel is reading text input, so global
	// keys (q, c, tab, ?) must be passed through to it.
	Capturing() bool
}

// overlay is a modal shown over the body: a form, a confirm dialog, the
// context switcher or help. Update returns nil once the overlay closes.
type overlay interface {
	Update(tea.Msg) (overlay, tea.Cmd)
	View(width, height int) string
}
