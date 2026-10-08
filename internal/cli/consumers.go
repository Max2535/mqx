package cli

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/Max2535/mqx/internal/broker"
)

func newConsumersCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:   "consumers <topic|queue>",
		Short: "Show who is consuming a topic or queue",
		Example: `  mqx consumers orders      # Kafka: group members assigned partitions of orders
  mqx consumers orders.q    # RabbitMQ: consumers attached to the queue`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ci, err := capability[broker.ConsumerInspector](s, broker.CapConsumerInspector)
				if err != nil {
					return err
				}
				cs, err := ci.Consumers(ctx, args[0])
				if err != nil {
					return err
				}
				if len(cs) == 0 && o.output != "json" {
					return o.done(cmd, "No consumers on %s.", args[0])
				}
				return o.render(cmd, cs, func() *table { return consumerTable(cs) })
			})
		},
	}
}

// consumerTable picks Kafka-style or RabbitMQ-style columns from the data.
func consumerTable(cs []broker.Consumer) *table {
	queueStyle := false
	for _, c := range cs {
		if c.Tag != "" || c.Connection != "" {
			queueStyle = true
		}
	}
	if queueStyle {
		t := newTable("TAG", "CONNECTION", "CHANNEL", "USER", "ACK", "PREFETCH", "ACTIVE")
		for _, c := range cs {
			t.add(c.Tag, c.Connection, c.Channel, c.User, c.AckRequired, c.Prefetch, c.Active)
		}
		return t
	}
	t := newTable("GROUP", "MEMBER", "INSTANCE", "CLIENT", "HOST", "PARTITIONS")
	for _, c := range cs {
		t.add(c.Group, c.MemberID, c.InstanceID, c.ClientID, c.Host, c.Partitions)
	}
	return t
}
