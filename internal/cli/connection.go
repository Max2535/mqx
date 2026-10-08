package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/Max2535/mqx/internal/broker"
)

func newConnectionsCmd(o *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "connections",
		Short: "List client connections, or close one",
		Example: `  mqx connections
  mqx connections -o json
  mqx connections close "10.0.0.5:53412 -> 10.0.0.9:5672" --reason "stuck consumer" --yes`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ci, err := capability[broker.ConnectionInspector](s, broker.CapConnectionInspector)
				if err != nil {
					return err
				}
				cs, err := ci.Connections(ctx)
				if err != nil {
					return err
				}
				return o.render(cmd, nonNilList(cs), func() *table {
					t := newTable("NAME", "USER", "CLIENT", "STATE", "CHANNELS", "PROTOCOL", "CONNECTED", "RECV B/S", "SEND B/S")
					for _, c := range cs {
						client := c.ClientProps["connection_name"]
						if client == "" {
							client = c.ClientProps["product"]
						}
						t.add(c.Name, c.User, client, c.State, c.Channels, c.Protocol, c.ConnectedAt, c.RecvRate, c.SendRate)
					}
					return t
				})
			})
		},
	}
	cmd.AddCommand(newConnectionCloseCmd(o))
	return cmd
}

func newConnectionCloseCmd(o *options) *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:   "close <name>",
		Short: "Close a client connection, disconnecting its consumers",
		Long: `Close a client connection. Every consumer and channel on it goes away; the
client sees the reason in the close frame. Find names with "mqx connections"
or "mqx consumers <queue>".`,
		Example: `  mqx connections close "10.0.0.5:53412 -> 10.0.0.9:5672" --reason "stuck consumer" --yes`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, pos []string) error {
			name := pos[0]
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ct, err := capability[broker.ConsumerTerminator](s, broker.CapConsumerTerminator)
				if err != nil {
					return err
				}
				if err := s.guard(cmd, fmt.Sprintf("close connection %q", name)); err != nil {
					return err
				}
				if err := ct.TerminateConsumer(ctx, broker.ConsumerTarget{Connection: name, Reason: reason}); err != nil {
					return err
				}
				return o.done(cmd, "closed connection %q", name)
			})
		},
	}
	cmd.Flags().StringVar(&reason, "reason", "closed by mqx", "reason sent to the client")
	return cmd
}

func newChannelsCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:   "channels",
		Short: "List AMQP channels",
		Example: `  mqx channels
  mqx channels -o json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ci, err := capability[broker.ConnectionInspector](s, broker.CapConnectionInspector)
				if err != nil {
					return err
				}
				cs, err := ci.Channels(ctx)
				if err != nil {
					return err
				}
				return o.render(cmd, nonNilList(cs), func() *table {
					t := newTable("NAME", "USER", "STATE", "CONSUMERS", "PREFETCH", "UNACKED", "CONFIRM", "PUBLISH/S", "DELIVER/S")
					for _, c := range cs {
						t.add(c.Name, c.User, c.State, c.Consumers, c.Prefetch, c.Unacked, c.Confirm, c.PublishRate, c.DeliverRate)
					}
					return t
				})
			})
		},
	}
}
