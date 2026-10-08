package cli

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/Max2535/mqx/internal/broker"
)

func newNodesCmd(o *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "nodes",
		Aliases: []string{"brokers"},
		Short:   "List cluster nodes (Kafka brokers or RabbitMQ nodes)",
		Example: "  mqx nodes\n  mqx nodes config 1",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ci, err := capability[broker.ClusterInspector](s, broker.CapClusterInspector)
				if err != nil {
					return err
				}
				nodes, err := ci.Nodes(ctx)
				if err != nil {
					return err
				}
				return o.render(cmd, nodes, func() *table {
					t := newTable("ID", "HOST", "PORT", "RACK", "CONTROLLER", "RUNNING", "DETAILS")
					for _, n := range nodes {
						port := ""
						if n.Port > 0 {
							port = cell(n.Port)
						}
						t.add(n.ID, n.Host, port, n.Rack, n.Controller, n.Running, kv(n.Details))
					}
					return t
				})
			})
		},
	}
	cmd.AddCommand(newNodeConfigCmd(o))
	return cmd
}

func newNodeConfigCmd(o *options) *cobra.Command {
	var all bool
	cmd := &cobra.Command{
		Use:     "config <id>",
		Short:   "Show a node's configuration",
		Example: "  mqx nodes config 1 --all",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ci, err := capability[broker.ClusterInspector](s, broker.CapClusterInspector)
				if err != nil {
					return err
				}
				entries, err := ci.NodeConfig(ctx, args[0])
				if err != nil {
					return err
				}
				shown := entries[:0:0]
				for _, c := range entries {
					if all || c.Source != "DEFAULT_CONFIG" {
						if c.Sensitive {
							c.Value = ""
						}
						shown = append(shown, c)
					}
				}
				return o.render(cmd, shown, func() *table {
					t := newTable("CONFIG", "VALUE", "SOURCE", "READ-ONLY")
					for _, c := range shown {
						t.add(c.Name, configValue(c), c.Source, c.ReadOnly)
					}
					return t
				})
			})
		},
	}
	cmd.Flags().BoolVar(&all, "all", false, "include configs left at their defaults")
	return cmd
}
