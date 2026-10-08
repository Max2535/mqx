package tui

import (
	"context"
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/key"

	"github.com/Max2535/mqx/internal/broker"
)

// newConnectPanel lists Kafka Connect connectors with their status and runs lifecycle actions.
func newConnectPanel(e *env) panel {
	cm := as[broker.ConnectManager](e)
	lifecycle := func(k, help string, a broker.ConnectorAction) action {
		return action{
			key:      key.NewBinding(key.WithKeys(k), key.WithHelp(k, help)),
			mutating: true,
			needsRow: true,
			describe: func(r *row, _ values) string { return fmt.Sprintf("%s connector %s", upperFirst(string(a)), r.key) },
			run: func(ctx context.Context, r *row, _ values) (string, error) {
				return "", cm.ConnectorAction(ctx, r.key, a)
			},
		}
	}
	actions := []action{
		lifecycle("p", "pause", broker.ConnectorPause),
		lifecycle("s", "resume", broker.ConnectorResume),
		lifecycle("R", "restart", broker.ConnectorRestart),
		{
			key:      key.NewBinding(key.WithKeys("d"), key.WithHelp("d", "delete connector")),
			mutating: true,
			needsRow: true,
			describe: func(r *row, _ values) string { return "Delete connector " + r.key },
			run:      func(ctx context.Context, r *row, _ values) (string, error) { return "", cm.DeleteConnector(ctx, r.key) },
		},
	}
	return newStack(newListView(e, resource{
		title: "Connect",
		cols:  []string{"NAME", "TYPE", "STATE", "WORKER", "TASKS"},
		load: func(ctx context.Context) (listing, error) {
			cs, err := cm.Connectors(ctx)
			if err != nil {
				return listing{}, err
			}
			rows := make([]row, len(cs))
			for i, c := range cs {
				running := 0
				for _, t := range c.Tasks {
					if t.State == "RUNNING" {
						running++
					}
				}
				rows[i] = row{key: c.Name, data: c, cells: []string{c.Name, c.Type, c.State,
					c.Worker, fmt.Sprintf("%d/%d running", running, len(c.Tasks))}}
			}
			return listing{rows: rows}, nil
		},
		open: func(r row) panel {
			return newTextView(e, "Connector "+r.key, func(ctx context.Context) (string, error) {
				c, err := cm.Connector(ctx, r.key)
				if err != nil {
					return "", err
				}
				return connectorText(c), nil
			})
		},
		actions: actions,
	}))
}

func connectorText(c *broker.Connector) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s (%s) state %s on %s\n\n", c.Name, c.Type, c.State, c.Worker)
	b.WriteString(st.header.Render("Config") + "\n")
	b.WriteString(formatKV(c.Config, "\n") + "\n\n")
	b.WriteString(st.header.Render("Tasks") + "\n")
	for _, t := range c.Tasks {
		fmt.Fprintf(&b, "  #%d %s on %s\n", t.ID, t.State, t.Worker)
		if t.Trace != "" {
			b.WriteString(st.err.Render(indent(t.Trace, "    ")) + "\n")
		}
	}
	return b.String()
}

func indent(s, prefix string) string {
	return prefix + strings.ReplaceAll(s, "\n", "\n"+prefix)
}

func upperFirst(s string) string {
	if s == "" || s[0] < 'a' || s[0] > 'z' {
		return s
	}
	return string(s[0]-'a'+'A') + s[1:]
}
