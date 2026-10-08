package tui

import "maps"

const historySize = 10

// formHistory remembers what was submitted in each form during the session,
// newest first, so ctrl+r can bring it back. It lives on the root model and
// survives context switches. Secret fields are never stored.
type formHistory struct {
	byForm map[string][]values
}

func newFormHistory() *formHistory { return &formHistory{byForm: map[string][]values{}} }

// add records v for form, dropping an identical older entry.
func (h *formHistory) add(form string, v values) {
	if h == nil {
		return
	}
	entries := []values{v}
	for _, old := range h.byForm[form] {
		if !maps.Equal(old, v) && len(entries) < historySize {
			entries = append(entries, old)
		}
	}
	h.byForm[form] = entries
}

// get returns the entries for form, newest first.
func (h *formHistory) get(form string) []values {
	if h == nil {
		return nil
	}
	return h.byForm[form]
}
