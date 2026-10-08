package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/Max2535/mqx/internal/broker"
)

// displayExchange names the default exchange, whose real name is "".
func displayExchange(name string) string {
	if name == "" {
		return "(default)"
	}
	return name
}

// exchangeArg maps the ways to name the default exchange on the command line to "".
func exchangeArg(name string) string {
	switch name {
	case "(default)", "amq.default":
		return ""
	}
	return name
}

// nonNilList keeps JSON output an array when a listing is empty.
func nonNilList[T any](xs []T) []T {
	if xs == nil {
		return []T{}
	}
	return xs
}

func newExchangesCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:   "exchanges",
		Short: "List RabbitMQ exchanges in the context's vhost",
		Example: `  mqx exchanges
  mqx exchanges -o json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ti, err := capability[broker.TopologyInspector](s, broker.CapTopologyInspector)
				if err != nil {
					return err
				}
				es, err := ti.Exchanges(ctx)
				if err != nil {
					return err
				}
				return o.render(cmd, nonNilList(es), func() *table {
					t := newTable("NAME", "TYPE", "DURABLE", "AUTO-DELETE", "INTERNAL", "ARGUMENTS")
					for _, e := range es {
						t.add(displayExchange(e.Name), e.Type, e.Durable, e.AutoDelete, e.Internal, kv(e.Arguments))
					}
					return t
				})
			})
		},
	}
}

func newExchangeCmd(o *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "exchange",
		Short: "Declare or delete a RabbitMQ exchange",
	}
	cmd.AddCommand(newExchangeDeclareCmd(o), newExchangeDeleteCmd(o))
	return cmd
}

func newExchangeDeclareCmd(o *options) *cobra.Command {
	var (
		ex   broker.Exchange
		args []string
	)
	cmd := &cobra.Command{
		Use:   "declare <name>",
		Short: "Declare an exchange (idempotent when the properties match)",
		Example: `  mqx exchange declare orders --type topic
  mqx exchange declare orders.dlx --type fanout --durable=false --auto-delete
  mqx exchange declare events --type direct --arg alternate-exchange=unrouted`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, pos []string) error {
			ex.Name = pos[0]
			if ex.Type == "" {
				return fmt.Errorf("--type is required: direct, topic, fanout, headers or a plugin type such as x-consistent-hash")
			}
			parsed, err := parseArgs("arg", args)
			if err != nil {
				return err
			}
			if len(parsed) > 0 {
				ex.Arguments = parsed
			}
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				te, err := capability[broker.TopologyEditor](s, broker.CapTopologyEditor)
				if err != nil {
					return err
				}
				if err := s.guard(cmd, fmt.Sprintf("declare %s exchange %q", ex.Type, ex.Name)); err != nil {
					return err
				}
				if err := te.DeclareExchange(ctx, ex); err != nil {
					return err
				}
				return o.done(cmd, "declared %s exchange %q", ex.Type, ex.Name)
			})
		},
	}
	f := cmd.Flags()
	f.StringVar(&ex.Type, "type", "", "exchange type: direct, topic, fanout, headers or a plugin type (required)")
	f.BoolVar(&ex.Durable, "durable", true, "survive a broker restart")
	f.BoolVar(&ex.AutoDelete, "auto-delete", false, "delete when the last binding is removed")
	f.BoolVar(&ex.Internal, "internal", false, "only other exchanges can publish to it")
	f.StringArrayVar(&args, "arg", nil, "argument key=value, repeatable (numbers and booleans are typed)")
	return cmd
}

func newExchangeDeleteCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:     "delete <name>",
		Short:   "Delete an exchange and its bindings",
		Example: `  mqx exchange delete orders --yes`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, pos []string) error {
			name := pos[0]
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				te, err := capability[broker.TopologyEditor](s, broker.CapTopologyEditor)
				if err != nil {
					return err
				}
				if err := s.guard(cmd, fmt.Sprintf("delete exchange %q", name)); err != nil {
					return err
				}
				if err := te.DeleteExchange(ctx, name); err != nil {
					return err
				}
				return o.done(cmd, "deleted exchange %q", name)
			})
		},
	}
}
