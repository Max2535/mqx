package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Max2535/mqx/internal/broker"
)

func newConnectCmd(o *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "connect",
		Short: "Manage Kafka Connect connectors",
		Long:  "Manage Kafka Connect connectors. Needs connect.url in the context.",
	}
	cmd.AddCommand(newConnectListCmd(o), newConnectGetCmd(o), newConnectPluginsCmd(o),
		newConnectPutCmd(o, true), newConnectPutCmd(o, false), newConnectDeleteCmd(o))
	for _, a := range []broker.ConnectorAction{broker.ConnectorPause, broker.ConnectorResume, broker.ConnectorRestart} {
		cmd.AddCommand(newConnectActionCmd(o, a))
	}
	return cmd
}

// withConnect runs fn with the session's ConnectManager capability.
func (o *options) withConnect(cmd *cobra.Command, fn func(ctx context.Context, s *session, cm broker.ConnectManager) error) error {
	return o.withSession(cmd, func(ctx context.Context, s *session) error {
		cm, err := capability[broker.ConnectManager](s, broker.CapConnectManager)
		if err != nil {
			return err
		}
		return fn(ctx, s, cm)
	})
}

func newConnectListCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Short:   "List connectors with their state",
		Example: "  mqx connect list",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.withConnect(cmd, func(ctx context.Context, _ *session, cm broker.ConnectManager) error {
				cs, err := cm.Connectors(ctx)
				if err != nil {
					return err
				}
				if cs == nil {
					cs = []broker.Connector{}
				}
				return o.render(cmd, cs, func() *table {
					t := newTable("NAME", "TYPE", "STATE", "TASKS", "WORKER", "CLASS")
					for _, c := range cs {
						t.add(c.Name, c.Type, c.State, connectTasks(c.Tasks), c.Worker, c.Config["connector.class"])
					}
					return t
				})
			})
		},
	}
}

// connectTasks summarises task states, e.g. "2/3 RUNNING, 1 FAILED".
func connectTasks(tasks []broker.ConnectorTask) string {
	if len(tasks) == 0 {
		return "0"
	}
	counts := map[string]int{}
	for _, t := range tasks {
		counts[t.State]++
	}
	states := slices.Sorted(maps.Keys(counts))
	parts := make([]string, len(states))
	for i, s := range states {
		parts[i] = fmt.Sprintf("%d %s", counts[s], s)
	}
	return fmt.Sprintf("%d: %s", len(tasks), strings.Join(parts, ", "))
}

func newConnectGetCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:     "get <name>",
		Short:   "Show a connector's config, state and tasks",
		Example: "  mqx connect get orders-sink\n  mqx connect get orders-sink -o json",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.withConnect(cmd, func(ctx context.Context, _ *session, cm broker.ConnectManager) error {
				c, err := cm.Connector(ctx, args[0])
				if err != nil {
					return err
				}
				if o.output == "json" {
					return o.render(cmd, c, nil)
				}
				w := cmd.OutOrStdout()
				fmt.Fprintf(w, "Name:   %s\nType:   %s\nState:  %s\nWorker: %s\n\n", c.Name, cell(c.Type), cell(c.State), cell(c.Worker))
				t := newTable("CONFIG", "VALUE")
				for _, k := range slices.Sorted(maps.Keys(c.Config)) {
					t.add(k, c.Config[k])
				}
				if err := t.write(w); err != nil {
					return err
				}
				if len(c.Tasks) == 0 {
					return nil
				}
				fmt.Fprintln(w)
				tt := newTable("TASK", "STATE", "WORKER", "TRACE")
				for _, task := range c.Tasks {
					trace, _, _ := strings.Cut(task.Trace, "\n") // first line; -o json has the full trace
					tt.add(task.ID, task.State, task.Worker, trace)
				}
				return tt.write(w)
			})
		},
	}
}

func newConnectPluginsCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:     "plugins",
		Short:   "List installed connector plugins",
		Example: "  mqx connect plugins",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.withConnect(cmd, func(ctx context.Context, _ *session, cm broker.ConnectManager) error {
				ps, err := cm.ConnectorPlugins(ctx)
				if err != nil {
					return err
				}
				if ps == nil {
					ps = []broker.ConnectorPlugin{}
				}
				return o.render(cmd, ps, func() *table {
					t := newTable("CLASS", "TYPE", "VERSION")
					for _, p := range ps {
						t.add(p.Class, p.Type, p.Version)
					}
					return t
				})
			})
		},
	}
}

// newConnectPutCmd builds `connect create` (create=true) and `connect update`.
func newConnectPutCmd(o *options, create bool) *cobra.Command {
	var (
		set  []string
		file string
	)
	use, short, verb := "update <name>", "Replace a connector's config", "update"
	if create {
		use, short, verb = "create <name>", "Create a connector", "create"
	}
	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		Long: short + ` from --file (a JSON object of config keys, or {"name":..., "config":{...}}
as accepted by the Connect REST API) and/or repeated --set key=value, which win.`,
		Example: fmt.Sprintf(`  mqx connect %[1]s orders-sink --file orders-sink.json --yes
  mqx connect %[1]s orders-sink --set connector.class=FileStreamSink --set topics=orders --set file=/tmp/out`, verb),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := connectConfig(file, set)
			if err != nil {
				return err
			}
			if len(cfg) == 0 {
				return errors.New("give the connector config with --file or --set key=value")
			}
			return o.withConnect(cmd, func(ctx context.Context, s *session, cm broker.ConnectManager) error {
				_, err := cm.Connector(ctx, args[0])
				exists := err == nil
				switch {
				case err != nil && !errors.Is(err, broker.ErrNotFound):
					return err
				case create && exists:
					return fmt.Errorf("connector %q already exists; use `mqx connect update %s`", args[0], args[0])
				case !create && !exists:
					return fmt.Errorf("connector %q: %w; use `mqx connect create %s`", args[0], broker.ErrNotFound, args[0])
				}
				if err := s.guard(cmd, verb+" connector "+args[0]); err != nil {
					return err
				}
				if err := cm.PutConnector(ctx, args[0], cfg); err != nil {
					return err
				}
				past := map[bool]string{true: "Created", false: "Updated"}[create]
				return o.done(cmd, "%s connector %s.", past, args[0])
			})
		},
	}
	cmd.Flags().StringArrayVar(&set, "set", nil, "config key=value (repeatable)")
	cmd.Flags().StringVar(&file, "file", "", "JSON file with the connector config")
	return cmd
}

// connectConfig merges a JSON config file with --set pairs.
func connectConfig(file string, set []string) (map[string]string, error) {
	cfg := map[string]string{}
	if file != "" {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("read connector config: %w", err)
		}
		var raw map[string]any
		if err := json.Unmarshal(data, &raw); err != nil {
			return nil, fmt.Errorf("connector config %s must be a JSON object: %w", file, err)
		}
		if inner, ok := raw["config"].(map[string]any); ok {
			raw = inner // the Connect REST create format: {"name":..., "config":{...}}
		}
		for k, v := range raw {
			switch x := v.(type) {
			case string:
				cfg[k] = x
			case nil:
			default:
				b, _ := json.Marshal(x)
				cfg[k] = string(b)
			}
		}
	}
	pairs, err := parseKV("set", set)
	if err != nil {
		return nil, err
	}
	maps.Copy(cfg, pairs)
	return cfg, nil
}

func newConnectDeleteCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:     "delete <name>",
		Short:   "Delete a connector",
		Example: "  mqx connect delete orders-sink --yes",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.withConnect(cmd, func(ctx context.Context, s *session, cm broker.ConnectManager) error {
				if err := s.guard(cmd, "delete connector "+args[0]); err != nil {
					return err
				}
				if err := cm.DeleteConnector(ctx, args[0]); err != nil {
					return err
				}
				return o.done(cmd, "Deleted connector %s.", args[0])
			})
		},
	}
}

func newConnectActionCmd(o *options, action broker.ConnectorAction) *cobra.Command {
	short := map[broker.ConnectorAction]string{
		broker.ConnectorPause:   "Pause a connector and its tasks",
		broker.ConnectorResume:  "Resume a paused connector",
		broker.ConnectorRestart: "Restart a connector and its tasks",
	}[action]
	past := map[broker.ConnectorAction]string{
		broker.ConnectorPause: "Paused", broker.ConnectorResume: "Resumed", broker.ConnectorRestart: "Restarted",
	}[action]
	return &cobra.Command{
		Use:     string(action) + " <name>",
		Short:   short,
		Example: fmt.Sprintf("  mqx connect %s orders-sink --yes", action),
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.withConnect(cmd, func(ctx context.Context, s *session, cm broker.ConnectManager) error {
				if err := s.guard(cmd, fmt.Sprintf("%s connector %s", action, args[0])); err != nil {
					return err
				}
				if err := cm.ConnectorAction(ctx, args[0], action); err != nil {
					return err
				}
				return o.done(cmd, "%s connector %s.", past, args[0])
			})
		},
	}
}
