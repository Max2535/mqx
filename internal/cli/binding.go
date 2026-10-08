package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/Max2535/mqx/internal/broker"
)

func newBindingsCmd(o *options) *cobra.Command {
	var source, destination string
	cmd := &cobra.Command{
		Use:   "bindings",
		Short: "List RabbitMQ bindings (exchange to queue and exchange to exchange)",
		Example: `  mqx bindings
  mqx bindings --source orders
  mqx bindings --destination billing -o json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ti, err := capability[broker.TopologyInspector](s, broker.CapTopologyInspector)
				if err != nil {
					return err
				}
				all, err := ti.Bindings(ctx)
				if err != nil {
					return err
				}
				bs := []broker.Binding{}
				for _, b := range all {
					if (!cmd.Flags().Changed("source") || b.Source == exchangeArg(source)) &&
						(destination == "" || b.Destination == destination) {
						bs = append(bs, b)
					}
				}
				return o.render(cmd, bs, func() *table {
					t := newTable("SOURCE", "DESTINATION", "TYPE", "ROUTING KEY", "ARGUMENTS")
					for _, b := range bs {
						t.add(displayExchange(b.Source), b.Destination, b.DestinationType, b.RoutingKey, kv(b.Arguments))
					}
					return t
				})
			})
		},
	}
	cmd.Flags().StringVar(&source, "source", "", `only bindings from this exchange ("(default)" for the default exchange)`)
	cmd.Flags().StringVar(&destination, "destination", "", "only bindings to this queue or exchange")
	return cmd
}

// bindingFlags are shared by bind and unbind.
type bindingFlags struct {
	b    broker.Binding
	args []string
}

func (bf *bindingFlags) register(cmd *cobra.Command) {
	f := cmd.Flags()
	f.StringVar(&bf.b.Source, "source", "", "source exchange (required)")
	f.StringVar(&bf.b.Destination, "destination", "", "destination queue or exchange (required)")
	f.StringVar(&bf.b.DestinationType, "destination-type", "queue", "queue or exchange")
	f.StringVar(&bf.b.RoutingKey, "key", "", "routing (binding) key")
	f.StringArrayVar(&bf.args, "arg", nil, "binding argument key=value, repeatable (e.g. x-match=all for headers)")
	_ = cmd.MarkFlagRequired("source")
	_ = cmd.MarkFlagRequired("destination")
}

func (bf *bindingFlags) binding() (broker.Binding, error) {
	b := bf.b
	b.Source = exchangeArg(b.Source)
	if b.DestinationType != "queue" && b.DestinationType != "exchange" {
		return b, fmt.Errorf("--destination-type %q: want queue or exchange", b.DestinationType)
	}
	args, err := parseArgs("arg", bf.args)
	if err != nil {
		return b, err
	}
	if len(args) > 0 {
		b.Arguments = args
	}
	return b, nil
}

func describeBindingCLI(b broker.Binding) string {
	return fmt.Sprintf("exchange %q to %s %q with key %q", displayExchange(b.Source), b.DestinationType, b.Destination, b.RoutingKey)
}

func newBindCmd(o *options) *cobra.Command {
	var bf bindingFlags
	cmd := &cobra.Command{
		Use:   "bind",
		Short: "Bind a queue or an exchange to an exchange",
		Example: `  mqx bind --source orders --destination billing --key 'order.*.created'
  mqx bind --source orders --destination audit --destination-type exchange --key '#'
  mqx bind --source headers-ex --destination q --arg x-match=all --arg region=eu`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			b, err := bf.binding()
			if err != nil {
				return err
			}
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				te, err := capability[broker.TopologyEditor](s, broker.CapTopologyEditor)
				if err != nil {
					return err
				}
				if err := s.guard(cmd, "bind "+describeBindingCLI(b)); err != nil {
					return err
				}
				if err := te.Bind(ctx, b); err != nil {
					return err
				}
				return o.done(cmd, "bound %s", describeBindingCLI(b))
			})
		},
	}
	bf.register(cmd)
	return cmd
}

func newUnbindCmd(o *options) *cobra.Command {
	var bf bindingFlags
	cmd := &cobra.Command{
		Use:   "unbind",
		Short: "Remove a binding",
		Long: `Remove a binding. It is found by source, destination and routing key; when
several bindings differ only in their arguments, pass --arg or --properties-key
(shown by "mqx bindings -o json").`,
		Example: `  mqx unbind --source orders --destination billing --key 'order.*.created'
  mqx unbind --source orders --destination audit --destination-type exchange --key '#'`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			b, err := bf.binding()
			if err != nil {
				return err
			}
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				te, err := capability[broker.TopologyEditor](s, broker.CapTopologyEditor)
				if err != nil {
					return err
				}
				if err := s.guard(cmd, "unbind "+describeBindingCLI(b)); err != nil {
					return err
				}
				if err := te.Unbind(ctx, b); err != nil {
					return err
				}
				return o.done(cmd, "unbound %s", describeBindingCLI(b))
			})
		},
	}
	bf.register(cmd)
	cmd.Flags().StringVar(&bf.b.PropertiesKey, "properties-key", "", "management API id of the binding, to pick one exactly")
	return cmd
}
