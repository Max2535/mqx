package tui

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/Max2535/mqx/internal/broker"
)

// newTopicsPanel lists topics or queues. Enter opens the detail view (when
// the broker describes topics) or the message browser. focus deep-links to a
// topic's message browser.
func newTopicsPanel(e *env, focus string) panel {
	title := "Topics"
	if e.kind() == "queue" {
		title = "Queues"
	}
	var lv *listView
	browse := func(r row) panel { return newMessageBrowser(e, r.key) }
	open := browse
	if e.has(broker.CapTopicDescriber) {
		open = func(r row) panel { return newTopicDetail(e, r.key) }
	}
	name := func(r *row) string { return r.key }
	actions := []action{{
		key: key.NewBinding(key.WithKeys("m"), key.WithHelp("m", "messages")),
		cmd: func(r *row) tea.Cmd {
			if r == nil {
				return nil
			}
			return e.push(lv.id, newMessageBrowser(e, r.key))
		},
	}}
	actions = append(actions, topicActions(e, name, true)...)
	if e.has(broker.CapTopicAdmin) {
		actions = append(actions, createTopicAction(e))
	}
	lv = newListView(e, resource{
		title:     title,
		cols:      topicCols(e),
		load:      func(ctx context.Context) (listing, error) { return loadTopics(ctx, e) },
		open:      open,
		focus:     focus,
		focusKind: e.kind(),
		focusOpen: browse,
		onSelect:  func(r row) { e.topic = r.key },
		actions:   actions,
	})
	return newStack(lv)
}

func topicCols(e *env) []string {
	if e.kind() == "queue" {
		return []string{"NAME", "MESSAGES", "CONSUMERS", "DETAILS"}
	}
	return []string{"NAME", "PARTITIONS", "REPLICAS", "MESSAGES", "CONSUMERS", "DETAILS"}
}

func loadTopics(ctx context.Context, e *env) (listing, error) {
	topics, err := e.b.ListTopics(ctx)
	if err != nil {
		return listing{}, err
	}
	// Internal topics (__consumer_offsets, ...) go last; user topics are what people browse.
	sort.SliceStable(topics, func(i, j int) bool {
		if topics[i].Internal != topics[j].Internal {
			return !topics[i].Internal
		}
		return topics[i].Name < topics[j].Name
	})
	rows := make([]row, len(topics))
	for i, t := range topics {
		details := formatKV(t.Details, " ")
		if t.Internal {
			details = strings.TrimSpace("internal " + details)
		}
		cells := []string{t.Name, count(t.Messages), count(int64(t.Consumers)), details}
		if e.kind() != "queue" {
			cells = []string{t.Name, itoa(t.Partitions), itoa(t.Replicas), count(t.Messages), count(int64(t.Consumers)), details}
		}
		rows[i] = row{key: t.Name, cells: cells, data: t}
	}
	return listing{rows: rows}, nil
}

// count renders a number that may be unknown (-1).
func count(n int64) string {
	if n < 0 {
		return "?"
	}
	return strconv.FormatInt(n, 10)
}

// newTopicDetail shows partitions and configs of one topic or queue.
func newTopicDetail(e *env, topic string) panel {
	var lv *listView
	name := func(*row) string { return topic }
	actions := []action{{
		key: key.NewBinding(key.WithKeys("m"), key.WithHelp("m", "messages")),
		cmd: func(*row) tea.Cmd { return e.push(lv.id, newMessageBrowser(e, topic)) },
	}}
	actions = append(actions, topicActions(e, name, false)...)
	lv = newListView(e, resource{
		title: topic,
		cols:  []string{"PARTITION", "LEADER", "REPLICAS", "ISR", "START", "END", "MESSAGES"},
		load: func(ctx context.Context) (listing, error) {
			d, err := as[broker.TopicDescriber](e).DescribeTopic(ctx, topic)
			if err != nil {
				return listing{}, err
			}
			return topicDetailListing(d), nil
		},
		actions: actions,
	})
	return lv
}

func topicDetailListing(d *broker.TopicDetail) listing {
	var h strings.Builder
	t := d.Topic
	fmt.Fprintf(&h, "%s %s  messages %s  consumers %s", t.Kind, t.Name, count(t.Messages), count(int64(t.Consumers)))
	if t.Partitions > 0 {
		fmt.Fprintf(&h, "  partitions %d  replicas %d", t.Partitions, t.Replicas)
	}
	if len(t.Details) > 0 {
		h.WriteString("\n" + st.muted.Render(formatKV(t.Details, "  ")))
	}
	if len(d.Configs) > 0 {
		h.WriteString("\n" + st.header.Render("Configs"))
		for _, c := range d.Configs {
			if c.IsDefault() {
				continue
			}
			v := c.Value
			if c.Sensitive {
				v = "******"
			}
			fmt.Fprintf(&h, "\n  %s = %s %s", c.Name, v, st.muted.Render(c.Source))
		}
	}
	rows := make([]row, len(d.Partitions))
	for i, p := range d.Partitions {
		rows[i] = row{key: itoa(p.ID), data: p, cells: []string{itoa(p.ID), itoa(p.Leader), joinInts(p.Replicas),
			joinInts(p.ISR), itoa(p.Start), itoa(p.End), itoa(p.End - p.Start)}}
	}
	return listing{header: h.String(), rows: rows}
}

// topicActions are the publish and admin actions on one topic, named by topic(r).
func topicActions(e *env, topic func(r *row) string, needsRow bool) []action {
	kind := e.kind()
	actions := []action{publishAction(e, topic, needsRow)}
	if e.has(broker.CapTopicAdmin) {
		actions = append(actions, action{
			key:      key.NewBinding(key.WithKeys("e"), key.WithHelp("e", "alter config")),
			mutating: true, needsRow: needsRow,
			form: func(r *row) (string, []field) {
				return "Alter config of " + kind + " " + topic(r), []field{{key: "changes", kind: fieldArea,
					label: "Changes (key=value per line; empty value removes the override)"}}
			},
			check: func(_ *row, v values) error {
				m, err := parseKVMap(v["changes"])
				if err == nil && len(m) == 0 {
					err = fmt.Errorf("no changes")
				}
				return err
			},
			describe: func(r *row, v values) string {
				m, _ := parseKVMap(v["changes"])
				return fmt.Sprintf("Alter config of %s %s (%s)", kind, topic(r), formatKV(m, ", "))
			},
			run: func(ctx context.Context, r *row, v values) (string, error) {
				m, err := parseKVMap(v["changes"])
				if err != nil {
					return "", err
				}
				return "", as[broker.TopicAdmin](e).AlterTopicConfig(ctx, topic(r), m)
			},
		})
	}
	if e.has(broker.CapPartitionAdder) {
		actions = append(actions, action{
			key:      key.NewBinding(key.WithKeys("a"), key.WithHelp("a", "add partitions")),
			mutating: true, needsRow: needsRow,
			form: func(r *row) (string, []field) {
				return "Add partitions to " + topic(r), []field{{key: "total", label: "New partition count"}}
			},
			check: func(_ *row, v values) error {
				_, err := positiveInt(v["total"])
				return err
			},
			describe: func(r *row, v values) string {
				return fmt.Sprintf("Increase partitions of topic %s to %s", topic(r), v["total"])
			},
			run: func(ctx context.Context, r *row, v values) (string, error) {
				n, err := positiveInt(v["total"])
				if err != nil {
					return "", err
				}
				return "", as[broker.PartitionAdder](e).AddPartitions(ctx, topic(r), n)
			},
		})
	}
	if e.has(broker.CapPurger) {
		actions = append(actions, purgeAction(e, topic, needsRow))
	}
	if e.has(broker.CapTopicAdmin) {
		actions = append(actions, action{
			key:      key.NewBinding(key.WithKeys("d"), key.WithHelp("d", "delete "+kind)),
			mutating: true, needsRow: needsRow,
			describe: func(r *row, _ values) string { return fmt.Sprintf("Delete %s %s", kind, topic(r)) },
			run: func(ctx context.Context, r *row, _ values) (string, error) {
				return "", as[broker.TopicAdmin](e).DeleteTopic(ctx, topic(r))
			},
		})
	}
	return actions
}

func purgeAction(e *env, topic func(r *row) string, needsRow bool) action {
	a := action{
		key:      key.NewBinding(key.WithKeys("x"), key.WithHelp("x", "purge")),
		mutating: true, needsRow: needsRow,
		describe: func(r *row, v values) string {
			if b := v["before"]; b != "" {
				return fmt.Sprintf("Delete records of topic %s below offset %s", topic(r), b)
			}
			return fmt.Sprintf("Purge all messages of %s %s", e.kind(), topic(r))
		},
		run: func(ctx context.Context, r *row, v values) (string, error) {
			opts, err := purgeOptions(v)
			if err != nil {
				return "", err
			}
			res, err := as[broker.Purger](e).Purge(ctx, topic(r), opts)
			if err != nil {
				return "", err
			}
			return "removed " + count(res.Messages) + " messages", nil
		},
	}
	if e.kafkaLike() {
		a.form = func(r *row) (string, []field) {
			return "Delete records of " + topic(r), []field{
				{key: "partitions", label: "Partitions", hint: "empty = all"},
				{key: "before", label: "Before offset", hint: "empty = everything"},
			}
		}
		a.check = func(_ *row, v values) error {
			_, err := purgeOptions(v)
			return err
		}
	}
	return a
}

func purgeOptions(v values) (broker.PurgeOptions, error) {
	var opts broker.PurgeOptions
	parts, err := parsePartitions(v["partitions"])
	if err != nil {
		return opts, err
	}
	opts.Partitions = parts
	if b := v["before"]; b != "" {
		n, err := strconv.ParseInt(b, 10, 64)
		if err != nil || n < 0 {
			return opts, fmt.Errorf("invalid offset %q", b)
		}
		opts.Before = &broker.Position{Kind: broker.AtOffset, Offset: n}
	}
	return opts, nil
}

func createTopicAction(e *env) action {
	kind := e.kind()
	return action{
		key:      key.NewBinding(key.WithKeys("n"), key.WithHelp("n", "new "+kind)),
		mutating: true,
		form: func(*row) (string, []field) {
			fields := []field{{key: "name", label: "Name"}}
			if e.kafkaLike() {
				fields = append(fields,
					field{key: "partitions", label: "Partitions", hint: "empty = broker default"},
					field{key: "replication", label: "Replication factor", hint: "empty = broker default"},
					field{key: "configs", label: "Configs (key=value per line)", kind: fieldArea})
			}
			if e.rabbitLike() {
				fields = append(fields,
					field{key: "durable", label: "Durable", hint: "Y/n"},
					field{key: "auto_delete", label: "Auto-delete", hint: "y/N"},
					field{key: "arguments", label: "Arguments", hint: "x-queue-type=quorum, x-message-ttl=60000"})
			}
			return "New " + kind, fields
		},
		check: func(_ *row, v values) error {
			_, err := topicSpec(v)
			return err
		},
		describe: func(_ *row, v values) string { return fmt.Sprintf("Create %s %s", kind, v["name"]) },
		run: func(ctx context.Context, _ *row, v values) (string, error) {
			spec, err := topicSpec(v)
			if err != nil {
				return "", err
			}
			return "", as[broker.TopicAdmin](e).CreateTopic(ctx, spec)
		},
	}
}

func topicSpec(v values) (broker.TopicSpec, error) {
	spec := broker.TopicSpec{Name: v["name"]}
	if spec.Name == "" {
		return spec, fmt.Errorf("name is required")
	}
	if p := v["partitions"]; p != "" {
		n, err := strconv.ParseInt(p, 10, 32)
		if err != nil || n < 1 {
			return spec, fmt.Errorf("invalid partitions %q", p)
		}
		spec.Partitions = int32(n)
	}
	if r := v["replication"]; r != "" {
		n, err := strconv.ParseInt(r, 10, 16)
		if err != nil || n < 1 {
			return spec, fmt.Errorf("invalid replication factor %q", r)
		}
		spec.ReplicationFactor = int16(n)
	}
	var err error
	if spec.Configs, err = parseKVMap(v["configs"]); err != nil {
		return spec, fmt.Errorf("configs: %w", err)
	}
	if spec.Durable, err = parseYes(v["durable"], true); err != nil {
		return spec, err
	}
	if spec.AutoDelete, err = parseYes(v["auto_delete"], false); err != nil {
		return spec, err
	}
	if spec.Arguments, err = parseArgs(v["arguments"]); err != nil {
		return spec, fmt.Errorf("arguments: %w", err)
	}
	return spec, nil
}

func positiveInt(s string) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n < 1 {
		return 0, fmt.Errorf("invalid number %q; want a positive integer", s)
	}
	return n, nil
}
