package tui

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
)

// Colours use the 16-colour ANSI palette so they follow the terminal theme.
var (
	colAccent = lipgloss.Color("6") // cyan
	colMuted  = lipgloss.Color("8")
	colErr    = lipgloss.Color("1")
	colOK     = lipgloss.Color("2")
	colWarn   = lipgloss.Color("3")
	colKey    = lipgloss.Color("4")
	colString = lipgloss.Color("2")
	colNumber = lipgloss.Color("5")
)

// styles groups the Lip Gloss styles; it is a value so tests and themes need no globals.
type styles struct {
	title, muted, err, ok, warn, selected, header, badge, navActive, nav, border, dialog, button, buttonOn lipgloss.Style
	jsonKey, jsonString, jsonNumber, jsonLiteral, jsonPunct                                                lipgloss.Style
}

func newStyles() styles {
	return styles{
		title:       lipgloss.NewStyle().Bold(true).Foreground(colAccent),
		muted:       lipgloss.NewStyle().Foreground(colMuted),
		err:         lipgloss.NewStyle().Foreground(colErr),
		ok:          lipgloss.NewStyle().Foreground(colOK),
		warn:        lipgloss.NewStyle().Foreground(colWarn),
		selected:    lipgloss.NewStyle().Reverse(true),
		header:      lipgloss.NewStyle().Bold(true).Underline(true),
		badge:       lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("0")).Background(colWarn).Padding(0, 1),
		navActive:   lipgloss.NewStyle().Bold(true).Foreground(colAccent),
		nav:         lipgloss.NewStyle(),
		border:      lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(colMuted),
		dialog:      lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(colAccent).Padding(0, 1),
		button:      lipgloss.NewStyle().Padding(0, 1),
		buttonOn:    lipgloss.NewStyle().Padding(0, 1).Reverse(true).Bold(true),
		jsonKey:     lipgloss.NewStyle().Foreground(colKey),
		jsonString:  lipgloss.NewStyle().Foreground(colString),
		jsonNumber:  lipgloss.NewStyle().Foreground(colNumber),
		jsonLiteral: lipgloss.NewStyle().Foreground(colWarn),
		jsonPunct:   lipgloss.NewStyle(),
	}
}

var st = newStyles() // immutable after init

// truncate cuts s to width display cells, adding an ellipsis.
func truncate(s string, width int) string {
	if width <= 0 {
		return ""
	}
	return ansi.Truncate(s, width, "…")
}

// pad truncates or right-pads s to exactly width cells.
func pad(s string, width int) string {
	s = truncate(s, width)
	if w := lipgloss.Width(s); w < width {
		s += strings.Repeat(" ", width-w)
	}
	return s
}

// clipLines keeps at most n lines of s, each truncated to width.
func clipLines(s string, width, n int) string {
	if n <= 0 {
		return ""
	}
	lines := strings.Split(s, "\n")
	if len(lines) > n {
		lines = lines[:n]
	}
	for i, l := range lines {
		lines[i] = truncate(l, width)
	}
	return strings.Join(lines, "\n")
}

// countLines returns the number of lines in s ("" has none).
func countLines(s string) int {
	if s == "" {
		return 0
	}
	return strings.Count(s, "\n") + 1
}
