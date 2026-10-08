package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/Max2535/mqx/internal/broker"
)

func newVHostsCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:   "vhosts",
		Short: "List RabbitMQ virtual hosts",
		Example: `  mqx vhosts
  mqx vhosts -o json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ti, err := capability[broker.TopologyInspector](s, broker.CapTopologyInspector)
				if err != nil {
					return err
				}
				vs, err := ti.VHosts(ctx)
				if err != nil {
					return err
				}
				return o.render(cmd, nonNilList(vs), func() *table {
					t := newTable("NAME", "MESSAGES", "TRACING", "DESCRIPTION")
					for _, v := range vs {
						t.add(v.Name, v.Messages, v.Tracing, v.Description)
					}
					return t
				})
			})
		},
	}
}

func newVHostCmd(o *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "vhost",
		Short: "Create or delete a RabbitMQ virtual host",
	}
	cmd.AddCommand(newVHostCreateCmd(o), newVHostDeleteCmd(o))
	return cmd
}

func newVHostCreateCmd(o *options) *cobra.Command {
	var v broker.VHost
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create (or update) a virtual host",
		Example: `  mqx vhost create staging --description "staging apps"
  mqx permission set alice --vhost staging --configure '.*' --write '.*' --read '.*'`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, pos []string) error {
			v.Name = pos[0]
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ua, err := capability[broker.UserAdmin](s, broker.CapUserAdmin)
				if err != nil {
					return err
				}
				if err := s.guard(cmd, fmt.Sprintf("create vhost %q", v.Name)); err != nil {
					return err
				}
				if err := ua.PutVHost(ctx, v); err != nil {
					return err
				}
				return o.done(cmd, "created vhost %q", v.Name)
			})
		},
	}
	cmd.Flags().StringVar(&v.Description, "description", "", "description")
	cmd.Flags().BoolVar(&v.Tracing, "tracing", false, "enable the firehose tracer")
	return cmd
}

func newVHostDeleteCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:     "delete <name>",
		Short:   "Delete a virtual host with all its queues, exchanges and messages",
		Example: `  mqx vhost delete staging --yes`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, pos []string) error {
			name := pos[0]
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ua, err := capability[broker.UserAdmin](s, broker.CapUserAdmin)
				if err != nil {
					return err
				}
				if err := s.guard(cmd, fmt.Sprintf("delete vhost %q and everything in it", name)); err != nil {
					return err
				}
				if err := ua.DeleteVHost(ctx, name); err != nil {
					return err
				}
				return o.done(cmd, "deleted vhost %q", name)
			})
		},
	}
}
