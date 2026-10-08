package cli

import (
	"context"
	"fmt"
	"regexp"
	"sort"

	"github.com/spf13/cobra"

	"github.com/Max2535/mqx/internal/broker"
)

func newTopicsCmd(o *options) *cobra.Command {
	var (
		internal bool
		match    string
	)
	cmd := &cobra.Command{
		Use:     "topics",
		Aliases: []string{"queues", "ls"},
		Short:   "List Kafka topics or RabbitMQ queues",
		Example: `  mqx topics
  mqx topics --match '^orders\.' -o json
  mqx queues --context dev-rabbit`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var re *regexp.Regexp
			if match != "" {
				var err error
				if re, err = regexp.Compile(match); err != nil {
					return fmt.Errorf("--match: %w", err)
				}
			}
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				all, err := s.b.ListTopics(ctx)
				if err != nil {
					return err
				}
				topics := make([]broker.Topic, 0, len(all))
				for _, t := range all {
					if (t.Internal && !internal) || (re != nil && !re.MatchString(t.Name)) {
						continue
					}
					topics = append(topics, t)
				}
				sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
				return o.render(cmd, topics, func() *table {
					t := newTable("NAME", "KIND", "PARTITIONS", "REPLICAS", "MESSAGES", "CONSUMERS")
					for _, tp := range topics {
						t.add(tp.Name, tp.Kind, optInt(tp.Partitions), optInt(tp.Replicas), unknown(tp.Messages), unknown(int64(tp.Consumers)))
					}
					return t
				})
			})
		},
	}
	cmd.Flags().BoolVar(&internal, "internal", false, "include internal topics such as __consumer_offsets")
	cmd.Flags().StringVar(&match, "match", "", "only names matching this regex")
	return cmd
}

// optInt prints 0 as "-" for fields a broker does not have (RabbitMQ partitions).
func optInt(n int) string {
	if n == 0 {
		return ""
	}
	return fmt.Sprint(n)
}

// unknown prints -1 as "?".
func unknown(n int64) string {
	if n < 0 {
		return "?"
	}
	return fmt.Sprint(n)
}
