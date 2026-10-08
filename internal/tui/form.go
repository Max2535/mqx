package tui

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/charmbracelet/bubbles/cursor"
	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type fieldKind int

const (
	fieldText fieldKind = iota
	fieldSecret
	fieldArea
	fieldBool   // space or ←/→ toggles; value "true" or "false"
	fieldChoice // ←/→ or space cycles through choices
)

// field is one input of a form.
type field struct {
	key   string // name in the submitted values
	label string
	hint  string // placeholder
	value string // initial value
	kind  fieldKind
	// choices are a fieldChoice's values; "" shows as "(none)".
	choices []string
}

// values are the submitted form fields by key.
type values map[string]string

// form is an overlay of labelled inputs. Enter moves to the next single-line
// field and submits on the last one; ctrl+s submits from anywhere; esc cancels.
// submit validates and returns the command to run; an error keeps the form open.
type form struct {
	title  string
	fields []field
	inputs []textinput.Model
	areas  []textarea.Model
	picks  []int // selected index of each bool (0 false, 1 true) and choice field
	focus  int
	err    string
	submit func(values) (tea.Cmd, error)
}

func newForm(title string, fields []field, submit func(values) (tea.Cmd, error)) *form {
	f := &form{title: title, fields: fields, submit: submit,
		inputs: make([]textinput.Model, len(fields)), areas: make([]textarea.Model, len(fields)),
		picks: make([]int, len(fields))}
	for i, fd := range fields {
		switch fd.kind {
		case fieldBool:
			if fd.value == "true" {
				f.picks[i] = 1
			}
			continue
		case fieldChoice:
			f.picks[i] = max(0, slices.Index(fd.choices, fd.value))
			continue
		}
		if fd.kind == fieldArea {
			ta := textarea.New()
			ta.Placeholder = fd.hint
			ta.ShowLineNumbers = false
			ta.SetHeight(6)
			_ = ta.Cursor.SetMode(cursor.CursorStatic)
			ta.SetValue(fd.value)
			f.areas[i] = ta
			continue
		}
		ti := textinput.New()
		ti.Prompt = ""
		ti.Placeholder = fd.hint
		ti.CharLimit = 0
		_ = ti.Cursor.SetMode(cursor.CursorStatic)
		if fd.kind == fieldSecret {
			ti.EchoMode = textinput.EchoPassword
		}
		ti.SetValue(fd.value)
		f.inputs[i] = ti
	}
	f.setFocus(0)
	return f
}

func (f *form) setFocus(i int) {
	if len(f.fields) == 0 {
		return
	}
	f.focus = (i + len(f.fields)) % len(f.fields)
	for j := range f.fields {
		if f.fields[j].picked() {
			continue
		}
		if f.fields[j].kind == fieldArea {
			if j == f.focus {
				f.areas[j].Focus()
			} else {
				f.areas[j].Blur()
			}
			continue
		}
		if j == f.focus {
			f.inputs[j].Focus()
		} else {
			f.inputs[j].Blur()
		}
	}
}

func (f *form) values() values {
	v := values{}
	for i, fd := range f.fields {
		switch fd.kind {
		case fieldBool:
			v[fd.key] = strconv.FormatBool(f.picks[i] == 1)
		case fieldChoice:
			v[fd.key] = fd.choices[f.picks[i]]
		case fieldArea:
			v[fd.key] = f.areas[i].Value()
		default:
			v[fd.key] = strings.TrimSpace(f.inputs[i].Value())
		}
	}
	return v
}

// Update implements overlay.
func (f *form) Update(msg tea.Msg) (overlay, tea.Cmd) {
	km, ok := msg.(tea.KeyMsg)
	if !ok {
		return f, nil
	}
	area := len(f.fields) > 0 && f.fields[f.focus].kind == fieldArea
	if len(f.fields) > 0 && f.fields[f.focus].picked() && f.pick(km.String()) {
		return f, nil
	}
	switch km.String() {
	case "esc":
		return nil, statusInfo("cancelled")
	case "ctrl+s":
		return f.doSubmit()
	case "tab", "down":
		if km.String() == "tab" || !area {
			f.setFocus(f.focus + 1)
			return f, nil
		}
	case "shift+tab", "up":
		if km.String() == "shift+tab" || !area {
			f.setFocus(f.focus - 1)
			return f, nil
		}
	case "enter":
		if !area {
			if f.focus == len(f.fields)-1 {
				return f.doSubmit()
			}
			f.setFocus(f.focus + 1)
			return f, nil
		}
	}
	if len(f.fields) == 0 {
		return f, nil
	}
	if f.fields[f.focus].picked() {
		return f, nil // typing does nothing on a toggle or choice
	}
	var cmd tea.Cmd
	if area {
		f.areas[f.focus], cmd = f.areas[f.focus].Update(msg)
	} else {
		f.inputs[f.focus], cmd = f.inputs[f.focus].Update(msg)
	}
	return f, cmd
}

func (fd field) picked() bool { return fd.kind == fieldBool || fd.kind == fieldChoice }

// pick changes the focused bool or choice field; it reports whether k was used.
func (f *form) pick(k string) bool {
	n := 2
	if f.fields[f.focus].kind == fieldChoice {
		n = len(f.fields[f.focus].choices)
	}
	switch k {
	case " ", "right", "l", "x":
		f.picks[f.focus] = (f.picks[f.focus] + 1) % n
	case "left", "h":
		f.picks[f.focus] = (f.picks[f.focus] - 1 + n) % n
	default:
		return false
	}
	return true
}

func (f *form) doSubmit() (overlay, tea.Cmd) {
	cmd, err := f.submit(f.values())
	if err != nil {
		f.err = err.Error()
		return f, nil
	}
	return nil, cmd
}

// View implements overlay.
func (f *form) View(width, height int) string {
	inner := min(max(40, width-10), 90)
	labelW := 0
	for _, fd := range f.fields {
		labelW = max(labelW, lipgloss.Width(fd.label))
	}
	var b strings.Builder
	b.WriteString(st.title.Render(f.title) + "\n\n")
	first, last := 0, 0
	if len(f.fields) > 0 {
		first, last = f.window(height - 8)
	}
	if first > 0 {
		b.WriteString(st.muted.Render(fmt.Sprintf("↑ %d more", first)) + "\n")
	}
	for i := first; i < last; i++ {
		fd := f.fields[i]
		label := pad(fd.label, labelW)
		if i == f.focus {
			label = st.navActive.Render(label)
		}
		if fd.kind == fieldArea {
			f.areas[i].SetWidth(inner - 2)
			b.WriteString(label + "\n" + f.areas[i].View() + "\n")
			continue
		}
		if fd.picked() {
			b.WriteString(label + " : " + f.pickView(i) + "\n")
			continue
		}
		f.inputs[i].Width = max(10, inner-labelW-3)
		b.WriteString(label + " : " + f.inputs[i].View() + "\n")
	}
	if last < len(f.fields) {
		b.WriteString(st.muted.Render(fmt.Sprintf("↓ %d more", len(f.fields)-last)) + "\n")
	}
	onPick := len(f.fields) > 0 && f.fields[f.focus].picked()
	if onPick && f.fields[f.focus].hint != "" {
		b.WriteString(st.muted.Render(wrap(f.fields[f.focus].hint, inner)) + "\n")
	}
	if f.err != "" {
		b.WriteString("\n" + st.err.Render(wrap(f.err, inner)) + "\n")
	}
	keys := "tab next • enter next/submit • ctrl+s submit • esc cancel"
	if onPick {
		keys = "←/→/space change • " + keys
	}
	b.WriteString("\n" + st.muted.Render(keys))
	return st.dialog.Width(inner + 2).Render(b.String())
}

func (f *form) pickView(i int) string {
	fd := f.fields[i]
	var text string
	if fd.kind == fieldBool {
		text = "[ ]"
		if f.picks[i] == 1 {
			text = "[x]"
		}
	} else {
		text = fd.choices[f.picks[i]]
		if text == "" {
			text = "(none)"
		}
		text = "‹ " + text + " ›"
	}
	if i == f.focus {
		return st.navActive.Render(text)
	}
	return text
}

// window returns the range of fields to show in about rows lines, keeping the
// focused one visible. Text areas count as several lines.
func (f *form) window(rows int) (first, last int) {
	lines := func(i int) int {
		if f.fields[i].kind == fieldArea {
			return 7 // label + 6 rows
		}
		return 1
	}
	rows = max(rows, lines(f.focus)+2)
	used := 0
	for i := range f.fields {
		used += lines(i)
	}
	if used <= rows {
		return 0, len(f.fields)
	}
	rows -= 2 // the "more" markers
	first, last, used = f.focus, f.focus+1, lines(f.focus)
	for {
		grew := false
		if last < len(f.fields) && used+lines(last) <= rows {
			used += lines(last)
			last++
			grew = true
		}
		if first > 0 && used+lines(first-1) <= rows {
			first--
			used += lines(first)
			grew = true
		}
		if !grew {
			return first, last
		}
	}
}

// wrap breaks s into lines of at most width cells.
func wrap(s string, width int) string {
	return lipgloss.NewStyle().Width(width).Render(s)
}
