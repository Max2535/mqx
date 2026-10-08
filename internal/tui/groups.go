package tui

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"

	"github.com/Max2535/mqx/internal/broker"
)

// newGroupsPanel lists consumer groups; enter opens the group detail with
// members, assignments and per-partition lag. focus deep-links to a group.
func newGroupsPanel(e *env, focus string) panel {
	gi := as[broker.GroupInspector](e)
	name := func(r *row) string { return r.key }
	open := func(r row) panel { return newGroupDetail(e, r.key) }
	return newStack(newListView(e, resource{
		title: "Groups",
		cols:  []string{"GROUP", "STATE", "PROTOCOL", "TYPE", "MEMBERS"},
		load: func(ctx context.Context) (listing, error) {
			groups, err := gi.ListGroups(ctx)
			if err != nil {
				return listing{}, err
			}
			rows := make([]row, len(groups))
			for i, g := range groups {
				rows[i] = row{key: g.Name, data: g, cells: []string{g.Name, g.State, g.GroupProtocol, g.ProtocolType, itoa(g.Members)}}
			}
			return listing{rows: rows}, nil
		},
		open:      open,
		focus:     focus,
		focusKind: "group",
		actions:   groupActions(e, name, true),
	}))
}

func newGroupDetail(e *env, group string) panel {
	actions := groupActions(e, func(*row) string { return group }, false)
	if e.has(broker.CapConsumerTerminator) {
		actions = append(actions, action{
			key:      key.NewBinding(key.WithKeys("x"), key.WithHelp("x", "remove static member")),
			mutating: true,
			needsRow: true,
			when:     func(r *row) bool { return r.data.(broker.GroupMember).InstanceID != "" },
			describe: func(r *row, _ values) string {
				return fmt.Sprintf("Remove static member %s from group %s", r.data.(broker.GroupMember).InstanceID, group)
			},
			run: func(ctx context.Context, r *row, _ values) (string, error) {
				return "", as[broker.ConsumerTerminator](e).TerminateConsumer(ctx, broker.ConsumerTarget{
					Group: group, InstanceID: r.data.(broker.GroupMember).InstanceID, Reason: "removed from mqx TUI"})
			},
		})
	}
	return newListView(e, resource{
		title: group,
		cols:  []string{"MEMBER", "INSTANCE", "CLIENT", "HOST", "SUBSCRIPTIONS", "ASSIGNMENT"},
		load: func(ctx context.Context) (listing, error) {
			d, err := as[broker.GroupInspector](e).DescribeGroup(ctx, group)
			if err != nil {
				return listing{}, err
			}
			var lag []broker.PartitionLag
			if e.has(broker.CapLagReporter) {
				if lag, err = as[broker.LagReporter](e).Lag(ctx, group); err != nil {
					return listing{}, fmt.Errorf("lag: %w", err)
				}
			}
			return groupListing(d, lag, e.has(broker.CapLagReporter)), nil
		},
		actions: actions,
	})
}

func groupListing(d *broker.GroupDescription, lag []broker.PartitionLag, withLag bool) listing {
	var h strings.Builder
	fmt.Fprintf(&h, "state %s  protocol %s/%s  assignor %s  epoch %d  coordinator %s %s:%d  members %d",
		stateStyle(d.State), d.GroupProtocol, d.ProtocolType, d.Assignor, d.Epoch,
		d.Coordinator.ID, d.Coordinator.Host, d.Coordinator.Port, len(d.Members))
	if withLag {
		h.WriteString("\n" + lagTable(lag))
	}
	rows := make([]row, len(d.Members))
	for i, m := range d.Members {
		rows[i] = row{key: m.MemberID, data: m, cells: []string{m.MemberID, m.InstanceID, m.ClientID, m.Host,
			strings.Join(m.Subscriptions, ","), assignment(m.Assignment)}}
	}
	return listing{header: h.String(), rows: rows}
}

func stateStyle(s string) string {
	switch s {
	case "Stable":
		return st.ok.Render(s)
	case "Dead", "Empty":
		return st.muted.Render(s)
	default:
		return st.warn.Render(s)
	}
}

func assignment(tps []broker.TopicPartitions) string {
	parts := make([]string, len(tps))
	for i, tp := range tps {
		parts[i] = tp.Topic + ":" + joinInts(tp.Partitions)
	}
	return strings.Join(parts, " ")
}

// lagTable renders per-partition lag and the total.
func lagTable(lag []broker.PartitionLag) string {
	var total int64
	t := newTable("TOPIC", "PART", "COMMITTED", "END", "LAG", "CLIENT")
	rows := make([][]string, len(lag))
	for i, l := range lag {
		total += l.Lag
		committed := itoa(l.Committed)
		if l.Committed < 0 {
			committed = "-"
		}
		rows[i] = []string{l.Topic, itoa(l.Partition), committed, itoa(l.End), itoa(l.Lag), l.ClientID}
	}
	t.setRows(rows)
	t.cursor = -1
	return st.header.Render(fmt.Sprintf("Lag total %d", total)) + "\n" + t.view(100, len(rows)+1)
}

// groupActions are the offset-management actions on the group named by group(r).
func groupActions(e *env, group func(r *row) string, needsRow bool) []action {
	if !e.has(broker.CapOffsetManager) {
		return nil
	}
	om := as[broker.OffsetManager](e)
	return []action{{
		key:      key.NewBinding(key.WithKeys("o"), key.WithHelp("o", "reset offsets")),
		mutating: true, needsRow: needsRow,
		form: func(r *row) (string, []field) {
			return "Reset offsets of " + group(r), []field{
				{key: "topic", label: "Topic", value: e.topic},
				{key: "to", label: "To", hint: "earliest, latest, offset, +N/-N shift, RFC3339 or 15m", value: "earliest"},
				{key: "partitions", label: "Partitions", hint: "empty = all"},
			}
		},
		check: func(_ *row, v values) error {
			_, err := offsetReset(v, e.now())
			return err
		},
		describe: func(r *row, v values) string {
			return fmt.Sprintf("Reset offsets of group %s on topic %s to %s", group(r), v["topic"], v["to"])
		},
		preview: func(ctx context.Context, r *row, v values) (string, error) {
			req, err := offsetReset(v, e.now())
			if err != nil {
				return "", err
			}
			req.DryRun = true
			changes, err := om.ResetOffsets(ctx, group(r), req)
			if err != nil {
				return "", err
			}
			return offsetChanges(changes), nil
		},
		run: func(ctx context.Context, r *row, v values) (string, error) {
			req, err := offsetReset(v, e.now())
			if err != nil {
				return "", err
			}
			changes, err := om.ResetOffsets(ctx, group(r), req)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("moved %d partitions", len(changes)), nil
		},
	}, {
		key:      key.NewBinding(key.WithKeys("d"), key.WithHelp("d", "delete group")),
		mutating: true, needsRow: needsRow,
		describe: func(r *row, _ values) string { return "Delete group " + group(r) },
		run: func(ctx context.Context, r *row, _ values) (string, error) {
			return "", om.DeleteGroup(ctx, group(r))
		},
	}}
}

// offsetReset parses the reset form: a +N/-N "to" shifts, anything else is a position.
func offsetReset(v values, now time.Time) (broker.OffsetReset, error) {
	req := broker.OffsetReset{Topic: v["topic"]}
	if req.Topic == "" {
		return req, fmt.Errorf("topic is required")
	}
	to := strings.TrimSpace(v["to"])
	if strings.HasPrefix(to, "+") || strings.HasPrefix(to, "-") {
		n, err := strconv.ParseInt(to, 10, 64)
		if err != nil || n == 0 {
			return req, fmt.Errorf("invalid shift %q; want +N or -N", to)
		}
		req.Shift = n
	} else {
		pos, err := parsePosition(to, now)
		if err != nil {
			return req, err
		}
		req.To = pos
	}
	var err error
	req.Partitions, err = parsePartitions(v["partitions"])
	return req, err
}

func offsetChanges(changes []broker.OffsetChange) string {
	var b strings.Builder
	b.WriteString("Dry run:\n")
	for _, c := range changes {
		old := itoa(c.Old)
		if c.Old < 0 {
			old = "-"
		}
		fmt.Fprintf(&b, "  %s[%d]  %s → %d\n", c.Topic, c.Partition, old, c.New)
	}
	return strings.TrimRight(b.String(), "\n")
}
