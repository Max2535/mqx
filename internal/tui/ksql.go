package tui

import (
	"context"
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/key"

	"github.com/Max2535/mqx/internal/broker"
)

// readOnlyKSQL lists the statement verbs that never change ksqlDB state.
var readOnlyKSQL = []string{"SELECT", "SHOW", "LIST", "DESCRIBE", "EXPLAIN", "PRINT"}

// ksqlMutates reports whether a statement may change state, so it needs the guard.
func ksqlMutates(stmt string) bool {
	fields := strings.Fields(strings.ToUpper(stmt))
	if len(fields) == 0 {
		return false
	}
	for _, v := range readOnlyKSQL {
		if fields[0] == v {
			return false
		}
	}
	return true
}

// newKSQLPanel runs ksqlDB statements: queries run directly, statements
// that change state go through the guard.
func newKSQLPanel(e *env) panel {
	kr := as[broker.KSQLRunner](e)
	last := ""
	run := action{
		key:        key.NewBinding(key.WithKeys("e", "enter"), key.WithHelp("e", "run statement")),
		mutatingIf: func(v values) bool { return ksqlMutates(v["statement"]) },
		form: func(*row) (string, []field) {
			return "ksqlDB statement", []field{{key: "statement", label: "Statement (ctrl+s runs)", kind: fieldArea, value: last}}
		},
		check: func(_ *row, v values) error {
			if strings.TrimSpace(v["statement"]) == "" {
				return fmt.Errorf("statement is empty")
			}
			last = v["statement"]
			return nil
		},
		describe: func(_ *row, v values) string { return "Run KSQL " + oneLine(v["statement"], 60) },
		run: func(ctx context.Context, _ *row, v values) (string, error) {
			res, err := kr.RunKSQL(ctx, v["statement"])
			if err != nil {
				return "", err
			}
			return ksqlText(res), nil
		},
	}
	return newStack(newListView(e, resource{
		title: "KSQL",
		load: func(context.Context) (listing, error) {
			return listing{header: st.muted.Render("Press e to enter a statement. SELECT, SHOW, LIST, DESCRIBE, EXPLAIN " +
				"and PRINT run directly; anything else asks for confirmation.")}, nil
		},
		actions: []action{run},
	}))
}

// ksqlText renders a result table or message.
func ksqlText(r *broker.KSQLResult) string {
	if len(r.Columns) == 0 {
		if r.Message == "" {
			return "ok"
		}
		return r.Message
	}
	t := newTable(r.Columns...)
	rows := make([][]string, len(r.Rows))
	for i, rr := range r.Rows {
		cells := make([]string, len(rr))
		for j, c := range rr {
			cells[j] = fmt.Sprint(c)
		}
		rows[i] = cells
	}
	t.setRows(rows)
	t.cursor = -1
	out := t.view(200, len(rows)+1) + fmt.Sprintf("\n(%d rows)", len(rows))
	if r.Message != "" {
		out += "\n" + r.Message
	}
	return out
}

func oneLine(s string, n int) string {
	return truncate(strings.Join(strings.Fields(s), " "), n)
}
