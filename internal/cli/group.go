package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/groupdiag"
)

func newGroupsCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:     "groups",
		Short:   "List consumer groups",
		Example: "  mqx groups\n  mqx groups -o json",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				gi, err := capability[broker.GroupInspector](s, broker.CapGroupInspector)
				if err != nil {
					return err
				}
				groups, err := gi.ListGroups(ctx)
				if err != nil {
					return err
				}
				if groups == nil {
					groups = []broker.GroupSummary{}
				}
				return o.render(cmd, groups, func() *table {
					t := newTable("NAME", "STATE", "PROTOCOL", "MEMBERS")
					for _, g := range groups {
						t.add(g.Name, g.State, g.GroupProtocol, g.Members)
					}
					return t
				})
			})
		},
	}
}

func newGroupCmd(o *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "group",
		Short: "Describe, debug and manage one consumer group",
		Long: `Describe a consumer group, follow its rebalances, diagnose common problems
and manage its offsets and members.`,
	}
	cmd.AddCommand(newGroupDescribeCmd(o), newGroupLagCmd(o), newGroupWatchCmd(o), newGroupDiagnoseCmd(o),
		newGroupResetOffsetsCmd(o), newGroupDeleteCmd(o), newGroupRemoveMemberCmd(o))
	return cmd
}

func newGroupDescribeCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:     "describe <group>",
		Short:   "Show state, coordinator, assignor, epoch and members",
		Example: "  mqx group describe billing\n  mqx group describe billing -o json",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				gi, err := capability[broker.GroupInspector](s, broker.CapGroupInspector)
				if err != nil {
					return err
				}
				d, err := gi.DescribeGroup(ctx, args[0])
				if err != nil {
					return err
				}
				if o.output == "json" {
					return o.render(cmd, d, nil)
				}
				return writeGroupDescription(cmd.OutOrStdout(), d)
			})
		},
	}
}

func writeGroupDescription(w io.Writer, d *broker.GroupDescription) error {
	coordinator := d.Coordinator.ID
	if d.Coordinator.Host != "" {
		coordinator = fmt.Sprintf("%s (%s:%d)", d.Coordinator.ID, d.Coordinator.Host, d.Coordinator.Port)
	}
	epoch := "-"
	if d.Epoch >= 0 {
		epoch = fmt.Sprint(d.Epoch)
	}
	fmt.Fprintf(w, "Group:          %s\nState:          %s\nCoordinator:    %s\n", d.Name, d.State, coordinator)
	fmt.Fprintf(w, "Protocol type:  %s\nGroup protocol: %s\nAssignor:       %s\nEpoch:          %s\nMembers:        %d\n",
		cell(d.ProtocolType), cell(d.GroupProtocol), cell(d.Assignor), epoch, len(d.Members))
	if len(d.Members) == 0 {
		return nil
	}
	fmt.Fprintln(w)
	kip848 := d.GroupProtocol == "consumer"
	header := []string{"MEMBER", "INSTANCE", "CLIENT", "HOST", "SUBSCRIPTIONS", "ASSIGNMENT"}
	if kip848 {
		header = append(header, "TARGET", "EPOCH")
	}
	t := newTable(header...)
	for _, m := range d.Members {
		subs := strings.Join(m.Subscriptions, ",")
		if m.Pattern != "" {
			subs = strings.TrimPrefix(subs+",/"+m.Pattern+"/", ",")
		}
		row := []any{m.MemberID, m.InstanceID, m.ClientID, m.Host, subs, groupAssignment(m.Assignment)}
		if kip848 {
			row = append(row, groupAssignment(m.Target), m.MemberEpoch)
		}
		t.add(row...)
	}
	return t.write(w)
}

// groupAssignment renders "orders[0,1] payments[2]".
func groupAssignment(tps []broker.TopicPartitions) string {
	parts := make([]string, 0, len(tps))
	for _, tp := range tps {
		parts = append(parts, tp.Topic+"["+int32s(tp.Partitions)+"]")
	}
	return strings.Join(parts, " ")
}

// groupLag is the JSON form of `mqx group lag`.
type groupLag struct {
	Group      string                `json:"group"`
	TotalLag   int64                 `json:"total_lag"`
	Partitions []broker.PartitionLag `json:"partitions"`
}

func newGroupLagCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:     "lag <group>",
		Short:   "Show committed offset, end offset and lag per partition",
		Example: "  mqx group lag billing\n  mqx group lag billing -o json",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				lr, err := capability[broker.LagReporter](s, broker.CapLagReporter)
				if err != nil {
					return err
				}
				lags, err := lr.Lag(ctx, args[0])
				if err != nil {
					return err
				}
				res := groupLag{Group: args[0], Partitions: lags}
				if res.Partitions == nil {
					res.Partitions = []broker.PartitionLag{}
				}
				for _, l := range lags {
					res.TotalLag += max(l.Lag, 0)
				}
				if o.output == "json" {
					return o.render(cmd, res, nil)
				}
				t := newTable("TOPIC", "PARTITION", "COMMITTED", "END", "LAG", "MEMBER", "CLIENT", "HOST")
				for _, l := range lags {
					t.add(l.Topic, l.Partition, groupOffset(l.Committed), l.End, groupOffset(l.Lag), l.MemberID, l.ClientID, l.Host)
				}
				if err := t.write(cmd.OutOrStdout()); err != nil {
					return err
				}
				_, err = fmt.Fprintf(cmd.OutOrStdout(), "\nTotal lag: %d\n", res.TotalLag)
				return err
			})
		},
	}
}

// groupOffset prints -1 (unknown, nothing committed) as "-".
func groupOffset(n int64) string {
	if n < 0 {
		return "-"
	}
	return fmt.Sprint(n)
}

func newGroupWatchCmd(o *options) *cobra.Command {
	var (
		interval time.Duration
		count    int
	)
	cmd := &cobra.Command{
		Use:   "watch <group>",
		Short: "Follow a group's state changes, joins, leaves and rebalance durations",
		Long: `Describe the group every --interval and print what changed: state transitions,
members joining and leaving, epoch bumps, assignment changes and how long each
rebalance took. Runs until interrupted or --count observations were made.`,
		Example: `  mqx group watch billing
  mqx group watch billing --interval 500ms --count 120
  mqx group watch billing -o json | jq .`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if interval <= 0 {
				return errors.New("--interval must be positive")
			}
			s, err := o.open(cmd)
			if err != nil {
				return err
			}
			defer s.Close()
			gi, err := capability[broker.GroupInspector](s, broker.CapGroupInspector)
			if err != nil {
				return err
			}
			return o.watchGroup(cmd, gi, args[0], interval, count)
		},
	}
	cmd.Flags().DurationVar(&interval, "interval", 2*time.Second, "time between observations")
	cmd.Flags().IntVar(&count, "count", 0, "stop after this many observations (0 = until interrupted)")
	return cmd
}

func (o *options) watchGroup(cmd *cobra.Command, gi broker.GroupInspector, group string, interval time.Duration, count int) error {
	w := groupdiag.NewWatcher()
	enc := json.NewEncoder(cmd.OutOrStdout())
	for i := 0; count <= 0 || i < count; i++ {
		if i > 0 && !sleepCtx(cmd.Context(), interval) {
			return nil // interrupted
		}
		ctx, cancel := o.requestContext(cmd)
		d, err := gi.DescribeGroup(ctx, group)
		cancel()
		if err != nil {
			if cmd.Context().Err() != nil {
				return nil
			}
			return err
		}
		for _, e := range w.Observe(d) {
			if o.output == "json" {
				if err := enc.Encode(e); err != nil {
					return err
				}
				continue
			}
			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s  %-18s  %s\n",
				e.At.Local().Format("15:04:05.000"), e.Kind, e.Detail); err != nil {
				return err
			}
		}
	}
	return nil
}

// sleepCtx waits for d and reports false when ctx ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func newGroupDiagnoseCmd(o *options) *cobra.Command {
	var (
		samples  int
		interval time.Duration
	)
	cmd := &cobra.Command{
		Use:   "diagnose <group>",
		Short: "Detect stale members, idle members, shared group ids and other problems",
		Long: `Sample the group --samples times, --interval apart, and report problems with
how to fix them. Rules that need progress over time, such as detecting a member
left behind by an unclean shutdown, compare the first and last sample, so use at
least 2 samples. Exits 0 even when problems are found.`,
		Example: `  mqx group diagnose billing
  mqx group diagnose billing --samples 3 --interval 10s -o json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if samples < 1 {
				return errors.New("--samples must be at least 1")
			}
			if interval <= 0 && samples > 1 {
				return errors.New("--interval must be positive")
			}
			s, err := o.open(cmd)
			if err != nil {
				return err
			}
			defer s.Close()
			snaps, err := o.sampleGroup(cmd, s, args[0], samples, interval)
			if err != nil {
				return err
			}
			findings := groupdiag.Diagnose(snaps)
			if findings == nil {
				findings = []groupdiag.Finding{}
			}
			if o.output == "json" {
				return o.render(cmd, findings, nil)
			}
			return writeFindings(cmd.OutOrStdout(), args[0], findings)
		},
	}
	cmd.Flags().IntVar(&samples, "samples", 2, "number of snapshots to compare")
	cmd.Flags().DurationVar(&interval, "interval", 5*time.Second, "time between snapshots")
	return cmd
}

func (o *options) sampleGroup(cmd *cobra.Command, s *session, group string, samples int, interval time.Duration) ([]groupdiag.Snapshot, error) {
	if samples > 1 && o.output != "json" {
		fmt.Fprintf(cmd.ErrOrStderr(), "Sampling group %s %d times, %s apart...\n", group, samples, interval)
	}
	var snaps []groupdiag.Snapshot
	for i := range samples {
		if i > 0 && !sleepCtx(cmd.Context(), interval) {
			return nil, cmd.Context().Err()
		}
		ctx, cancel := o.requestContext(cmd)
		snap, err := groupdiag.Collect(ctx, s.b, group)
		cancel()
		if err != nil {
			if errors.Is(err, broker.ErrUnsupported) {
				return nil, fmt.Errorf("context %q (%s) cannot diagnose groups: %w", s.cfg.Name, s.b.Name(), err)
			}
			return nil, err
		}
		snaps = append(snaps, snap)
	}
	return snaps, nil
}

func writeFindings(w io.Writer, group string, findings []groupdiag.Finding) error {
	if len(findings) == 0 {
		_, err := fmt.Fprintf(w, "No problems found in group %s.\n", group)
		return err
	}
	for i, f := range findings {
		if i > 0 {
			fmt.Fprintln(w)
		}
		fmt.Fprintf(w, "[%s] %s: %s\n  %s\n  Fix: %s\n", f.Severity, f.Rule, f.Title, f.Detail, f.Fix)
		if len(f.Members) > 0 {
			fmt.Fprintf(w, "  Members: %s\n", strings.Join(f.Members, ", "))
		}
	}
	return nil
}

func newGroupResetOffsetsCmd(o *options) *cobra.Command {
	var (
		topic, to  string
		shift      int64
		partitions []string
		dryRun     bool
	)
	cmd := &cobra.Command{
		Use:   "reset-offsets <group>",
		Short: "Move a group's committed offsets on a topic",
		Long: `Move the committed offsets of an empty group (stop its consumers first) on one
topic, to a position (--to) or relative to the current offsets (--shift).
--dry-run shows the changes without applying them.`,
		Example: `  mqx group reset-offsets billing --topic orders --to earliest --dry-run
  mqx group reset-offsets billing --topic orders --to 2026-10-08T09:00:00Z --yes
  mqx group reset-offsets billing --topic orders --to 1h --partitions 0,1
  mqx group reset-offsets billing --topic orders --shift -100`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			req, err := groupResetRequest(cmd, topic, to, shift, partitions, dryRun)
			if err != nil {
				return err
			}
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				om, err := capability[broker.OffsetManager](s, broker.CapOffsetManager)
				if err != nil {
					return err
				}
				if !dryRun {
					if err := s.guard(cmd, fmt.Sprintf("reset offsets of group %s on topic %s", args[0], topic)); err != nil {
						return err
					}
				}
				changes, err := om.ResetOffsets(ctx, args[0], req)
				if err != nil {
					return err
				}
				if changes == nil {
					changes = []broker.OffsetChange{}
				}
				if dryRun && o.output != "json" {
					fmt.Fprintln(cmd.ErrOrStderr(), "Dry run: no offsets were changed.")
				}
				return o.render(cmd, changes, func() *table {
					t := newTable("TOPIC", "PARTITION", "OLD", "NEW", "CHANGE")
					for _, c := range changes {
						delta := "-"
						if c.Old >= 0 {
							delta = fmt.Sprintf("%+d", c.New-c.Old)
						}
						t.add(c.Topic, c.Partition, groupOffset(c.Old), c.New, delta)
					}
					return t
				})
			})
		},
	}
	f := cmd.Flags()
	f.StringVar(&topic, "topic", "", "topic whose offsets to move (required)")
	f.StringVar(&to, "to", "", "target position: "+positionHelp)
	f.Int64Var(&shift, "shift", 0, "move by N offsets from the committed offset (negative = back)")
	f.StringSliceVar(&partitions, "partitions", nil, "only these partitions, e.g. 0,2")
	f.BoolVar(&dryRun, "dry-run", false, "show the changes without applying them")
	return cmd
}

func groupResetRequest(cmd *cobra.Command, topic, to string, shift int64, partitions []string, dryRun bool) (broker.OffsetReset, error) {
	if topic == "" {
		return broker.OffsetReset{}, errors.New("--topic is required")
	}
	toSet, shiftSet := cmd.Flags().Changed("to"), cmd.Flags().Changed("shift")
	switch {
	case toSet == shiftSet:
		return broker.OffsetReset{}, errors.New("give exactly one of --to or --shift")
	case shiftSet && shift == 0:
		return broker.OffsetReset{}, errors.New("--shift must not be 0")
	}
	ps, err := parsePartitions(partitions)
	if err != nil {
		return broker.OffsetReset{}, err
	}
	req := broker.OffsetReset{Topic: topic, Partitions: ps, Shift: shift, DryRun: dryRun}
	if toSet {
		if req.To, err = parsePosition(to, time.Now()); err != nil {
			return broker.OffsetReset{}, fmt.Errorf("--to: %w", err)
		}
	}
	return req, nil
}

func newGroupDeleteCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:     "delete <group>",
		Short:   "Delete an empty consumer group and its committed offsets",
		Example: "  mqx group delete old-billing --yes",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				om, err := capability[broker.OffsetManager](s, broker.CapOffsetManager)
				if err != nil {
					return err
				}
				if err := s.guard(cmd, "delete group "+args[0]); err != nil {
					return err
				}
				if err := om.DeleteGroup(ctx, args[0]); err != nil {
					return err
				}
				return o.done(cmd, "Deleted group %s.", args[0])
			})
		},
	}
}

func newGroupRemoveMemberCmd(o *options) *cobra.Command {
	var instanceID, reason string
	cmd := &cobra.Command{
		Use:   "remove-member <group>",
		Short: "Remove a static member (group.instance.id) from a group",
		Long: `Remove a static member, for example one left behind by a crashed pod, so its
partitions are reassigned now instead of after session.timeout.ms.`,
		Example: "  mqx group remove-member billing --instance-id billing-pod-2 --reason \"pod deleted\" --yes",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if instanceID == "" {
				return errors.New("--instance-id is required; see INSTANCE in `mqx group describe " + args[0] + "`")
			}
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				ct, err := capability[broker.ConsumerTerminator](s, broker.CapConsumerTerminator)
				if err != nil {
					return err
				}
				if err := s.guard(cmd, fmt.Sprintf("remove member %s from group %s", instanceID, args[0])); err != nil {
					return err
				}
				target := broker.ConsumerTarget{Group: args[0], InstanceID: instanceID, Reason: reason}
				if err := ct.TerminateConsumer(ctx, target); err != nil {
					return err
				}
				return o.done(cmd, "Removed member %s from group %s.", instanceID, args[0])
			})
		},
	}
	cmd.Flags().StringVar(&instanceID, "instance-id", "", "the member's group.instance.id (required)")
	cmd.Flags().StringVar(&reason, "reason", "", "reason recorded by the broker (Kafka 3.2+)")
	return cmd
}
