package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/Max2535/mqx/internal/broker"
)

func newTopicCmd(o *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "topic",
		Aliases: []string{"queue"},
		Short:   "Describe and administer one topic or queue",
	}
	cmd.AddCommand(newTopicDescribeCmd(o), newTopicCreateCmd(o), newTopicDeleteCmd(o), newTopicConfigCmd(o),
		newTopicAlterConfigCmd(o), newTopicAddPartitionsCmd(o), newTopicDeleteRecordsCmd(o))
	return cmd
}

func newTopicDescribeCmd(o *options) *cobra.Command {
	var allConfigs bool
	cmd := &cobra.Command{
		Use:     "describe <name>",
		Short:   "Show partitions, offsets and configs",
		Example: "  mqx topic describe orders\n  mqx queue describe orders.q -o json",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				td, err := capability[broker.TopicDescriber](s, broker.CapTopicDescriber)
				if err != nil {
					return err
				}
				d, err := td.DescribeTopic(ctx, args[0])
				if err != nil {
					return err
				}
				if o.output == "json" {
					return o.render(cmd, d, nil)
				}
				w := cmd.OutOrStdout()
				tp := d.Topic
				fmt.Fprintf(w, "Name:       %s\nKind:       %s\n", tp.Name, tp.Kind)
				if tp.Partitions > 0 {
					fmt.Fprintf(w, "Partitions: %d\nReplicas:   %d\n", tp.Partitions, tp.Replicas)
				}
				fmt.Fprintf(w, "Messages:   %s\nConsumers:  %s\n", unknown(tp.Messages), unknown(int64(tp.Consumers)))
				for _, k := range sortedKeys(tp.Details) {
					fmt.Fprintf(w, "%-11s %s\n", k+":", tp.Details[k])
				}
				if len(d.Partitions) > 0 {
					fmt.Fprintln(w)
					t := newTable("PARTITION", "LEADER", "REPLICAS", "ISR", "START", "END", "MESSAGES")
					for _, p := range d.Partitions {
						t.add(p.ID, p.Leader, p.Replicas, p.ISR, p.Start, p.End, p.End-p.Start)
					}
					if err := t.write(w); err != nil {
						return err
					}
				}
				return writeConfigs(w, d.Configs, allConfigs)
			})
		},
	}
	cmd.Flags().BoolVar(&allConfigs, "all-configs", false, "include configs left at their defaults")
	return cmd
}

func writeConfigs(w interface{ Write([]byte) (int, error) }, entries []broker.ConfigEntry, all bool) error {
	t := newTable("CONFIG", "VALUE", "SOURCE")
	for _, c := range entries {
		if !all && c.IsDefault() {
			continue
		}
		t.add(c.Name, configValue(c), c.Source)
	}
	if len(t.rows) == 0 {
		return nil
	}
	fmt.Fprintln(w)
	return t.write(w)
}

func configValue(c broker.ConfigEntry) string {
	if c.Sensitive {
		return "<sensitive>"
	}
	return c.Value
}

func newTopicCreateCmd(o *options) *cobra.Command {
	var (
		spec       broker.TopicSpec
		configs    []string
		arguments  []string
		queueType  string
		ephemeral  bool
		partitions int32
		rf         int16
	)
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a topic or declare a queue",
		Example: `  mqx topic create orders --partitions 6 --replication-factor 3 --set retention.ms=86400000
  mqx queue create orders.q --type quorum --arg x-max-length=10000`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			spec.Name, spec.Partitions, spec.ReplicationFactor = args[0], partitions, rf
			if spec.Configs, err = parseKV("set", configs); err != nil {
				return err
			}
			if spec.Arguments, err = parseArgs("arg", arguments); err != nil {
				return err
			}
			if queueType != "" {
				spec.Arguments["x-queue-type"] = queueType
			}
			spec.Durable = !ephemeral
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ta, err := capability[broker.TopicAdmin](s, broker.CapTopicAdmin)
				if err != nil {
					return err
				}
				if err := s.guard(cmd, "create "+args[0]); err != nil {
					return err
				}
				if err := ta.CreateTopic(ctx, spec); err != nil {
					return err
				}
				return o.done(cmd, "Created %s.", args[0])
			})
		},
	}
	f := cmd.Flags()
	f.Int32Var(&partitions, "partitions", 0, "Kafka: partition count (default: broker default)")
	f.Int16Var(&rf, "replication-factor", 0, "Kafka: replication factor (default: broker default)")
	f.StringArrayVar(&configs, "set", nil, "Kafka: topic config key=value (repeatable)")
	f.StringVar(&queueType, "type", "", "RabbitMQ: queue type classic, quorum or stream")
	f.StringArrayVar(&arguments, "arg", nil, "RabbitMQ: queue argument key=value (repeatable)")
	f.BoolVar(&ephemeral, "transient", false, "RabbitMQ: non-durable queue")
	f.BoolVar(&spec.AutoDelete, "auto-delete", false, "RabbitMQ: delete when the last consumer leaves")
	return cmd
}

func newTopicDeleteCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:     "delete <name>",
		Short:   "Delete a topic or queue and all its messages",
		Example: "  mqx topic delete orders --yes",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ta, err := capability[broker.TopicAdmin](s, broker.CapTopicAdmin)
				if err != nil {
					return err
				}
				if err := s.guard(cmd, "delete "+args[0]+" and all its messages"); err != nil {
					return err
				}
				if err := ta.DeleteTopic(ctx, args[0]); err != nil {
					return err
				}
				return o.done(cmd, "Deleted %s.", args[0])
			})
		},
	}
}

func newTopicConfigCmd(o *options) *cobra.Command {
	var all bool
	cmd := &cobra.Command{
		Use:     "config <name>",
		Short:   "Show a topic's or queue's config",
		Example: "  mqx topic config orders --all",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ta, err := capability[broker.TopicAdmin](s, broker.CapTopicAdmin)
				if err != nil {
					return err
				}
				entries, err := ta.TopicConfig(ctx, args[0])
				if err != nil {
					return err
				}
				shown := entries[:0:0]
				for _, c := range entries {
					if all || !c.IsDefault() {
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

func newTopicAlterConfigCmd(o *options) *cobra.Command {
	var (
		set   []string
		unset []string
	)
	cmd := &cobra.Command{
		Use:     "alter-config <name>",
		Aliases: []string{"set-config"},
		Short:   "Set or remove topic config overrides",
		Example: "  mqx topic alter-config orders --set retention.ms=3600000 --delete cleanup.policy",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			changes, err := parseKV("set", set)
			if err != nil {
				return err
			}
			for _, k := range unset {
				changes[k] = ""
			}
			if len(changes) == 0 {
				return fmt.Errorf("nothing to change; pass --set key=value or --delete key")
			}
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ta, err := capability[broker.TopicAdmin](s, broker.CapTopicAdmin)
				if err != nil {
					return err
				}
				if err := s.guard(cmd, fmt.Sprintf("alter config of %s (%s)", args[0], kv(changes))); err != nil {
					return err
				}
				if err := ta.AlterTopicConfig(ctx, args[0], changes); err != nil {
					return err
				}
				return o.done(cmd, "Updated config of %s.", args[0])
			})
		},
	}
	cmd.Flags().StringArrayVar(&set, "set", nil, "key=value to set (repeatable)")
	cmd.Flags().StringArrayVar(&unset, "delete", nil, "key whose override to remove (repeatable)")
	return cmd
}

func newTopicAddPartitionsCmd(o *options) *cobra.Command {
	var total int
	cmd := &cobra.Command{
		Use:     "add-partitions <name>",
		Short:   "Grow a Kafka topic to a total partition count",
		Example: "  mqx topic add-partitions orders --total 12",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if total <= 0 {
				return fmt.Errorf("--total must be a positive partition count")
			}
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				pa, err := capability[broker.PartitionAdder](s, broker.CapPartitionAdder)
				if err != nil {
					return err
				}
				if err := s.guard(cmd, fmt.Sprintf("grow %s to %d partitions (cannot be undone; changes key→partition mapping)", args[0], total)); err != nil {
					return err
				}
				if err := pa.AddPartitions(ctx, args[0], total); err != nil {
					return err
				}
				return o.done(cmd, "%s now has %d partitions.", args[0], total)
			})
		},
	}
	cmd.Flags().IntVar(&total, "total", 0, "new total partition count (required)")
	return cmd
}

func newTopicDeleteRecordsCmd(o *options) *cobra.Command {
	var (
		partitions []string
		before     string
	)
	cmd := &cobra.Command{
		Use:   "delete-records <topic>",
		Short: "Delete Kafka records below an offset or time (all by default)",
		Example: `  mqx topic delete-records orders --yes
  mqx topic delete-records orders --partitions 0,1 --before 5000
  mqx topic delete-records orders --before 2026-10-01T00:00:00Z`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts, err := purgeOptions(partitions, before)
			if err != nil {
				return err
			}
			return runPurge(cmd, o, args[0], opts)
		},
	}
	cmd.Flags().StringSliceVar(&partitions, "partitions", nil, "only these partitions, e.g. 0,2")
	cmd.Flags().StringVar(&before, "before", "", "delete records below this offset, time or duration ago ("+positionHelp+")")
	return cmd
}

func newPurgeCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:     "purge <queue|topic>",
		Short:   "Remove every message from a queue or topic, keeping it",
		Example: "  mqx purge orders.q --yes",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPurge(cmd, o, args[0], broker.PurgeOptions{})
		},
	}
}

func purgeOptions(partitions []string, before string) (broker.PurgeOptions, error) {
	var opts broker.PurgeOptions
	var err error
	if opts.Partitions, err = parsePartitions(partitions); err != nil {
		return opts, err
	}
	if before != "" {
		p, err := parsePosition(before, time.Now())
		if err != nil {
			return opts, fmt.Errorf("--before: %w", err)
		}
		if p.Kind != broker.AtOffset && p.Kind != broker.AtTime {
			return opts, fmt.Errorf("--before must be an offset or a time")
		}
		opts.Before = &p
	}
	return opts, nil
}

func runPurge(cmd *cobra.Command, o *options, name string, opts broker.PurgeOptions) error {
	return o.withSession(cmd, func(ctx context.Context, s *session) error {
		p, err := capability[broker.Purger](s, broker.CapPurger)
		if err != nil {
			return err
		}
		what := "delete all messages of " + name
		if opts.Before != nil || len(opts.Partitions) > 0 {
			what = "delete records of " + name
		}
		if err := s.guard(cmd, what); err != nil {
			return err
		}
		res, err := p.Purge(ctx, name, opts)
		if err != nil {
			return err
		}
		if o.output == "json" {
			return o.render(cmd, res, nil)
		}
		var parts []string
		for _, pp := range res.Partitions {
			parts = append(parts, fmt.Sprintf("%d→%d", pp.Partition, pp.LowMark))
		}
		msg := fmt.Sprintf("Purged %s", name)
		if res.Messages >= 0 {
			msg += fmt.Sprintf(": %d message(s) removed", res.Messages)
		}
		if len(parts) > 0 {
			msg += "; new low watermarks " + strings.Join(parts, " ")
		}
		return o.done(cmd, "%s.", msg)
	})
}
