package tui

import (
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
)

// field is one input of a form.
type field struct {
	key   string // name in the submitted values
	label string
	hint  string // placeholder
	value string // initial value
	kind  fieldKind
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
	focus  int
	err    string
	submit func(values) (tea.Cmd, error)
}

func newForm(title string, fields []field, submit func(values) (tea.Cmd, error)) *form {
	f := &form{title: title, fields: fields, submit: submit,
		inputs: make([]textinput.Model, len(fields)), areas: make([]textarea.Model, len(fields))}
	for i, fd := range fields {
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
		if fd.kind == fieldArea {
			v[fd.key] = f.areas[i].Value()
		} else {
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
	var cmd tea.Cmd
	if area {
		f.areas[f.focus], cmd = f.areas[f.focus].Update(msg)
	} else {
		f.inputs[f.focus], cmd = f.inputs[f.focus].Update(msg)
	}
	return f, cmd
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
func (f *form) View(width, _ int) string {
	inner := min(max(40, width-10), 90)
	labelW := 0
	for _, fd := range f.fields {
		labelW = max(labelW, lipgloss.Width(fd.label))
	}
	var b strings.Builder
	b.WriteString(st.title.Render(f.title) + "\n\n")
	for i, fd := range f.fields {
		label := pad(fd.label, labelW)
		if i == f.focus {
			label = st.navActive.Render(label)
		}
		if fd.kind == fieldArea {
			f.areas[i].SetWidth(inner - 2)
			b.WriteString(label + "\n" + f.areas[i].View() + "\n")
			continue
		}
		f.inputs[i].Width = max(10, inner-labelW-3)
		b.WriteString(label + " : " + f.inputs[i].View() + "\n")
	}
	if f.err != "" {
		b.WriteString("\n" + st.err.Render(wrap(f.err, inner)) + "\n")
	}
	b.WriteString("\n" + st.muted.Render("tab next • enter next/submit • ctrl+s submit • esc cancel"))
	return st.dialog.Width(inner + 2).Render(b.String())
}

// wrap breaks s into lines of at most width cells.
func wrap(s string, width int) string {
	return lipgloss.NewStyle().Width(width).Render(s)
}
