// Package groupdiag debugs consumer group rebalances: it collects snapshots
// of a group, runs rule-based diagnostics over them and turns a sequence of
// descriptions into a state-change timeline. Everything but Collect is pure.
package groupdiag

import (
	"context"
	"fmt"
	"time"

	"github.com/Max2535/mqx/internal/broker"
)

// Snapshot is what the rules look at: one description of the group, its lag
// and the partition counts of the topics it subscribes to, at one time.
type Snapshot struct {
	At         time.Time
	Group      *broker.GroupDescription
	Lag        []broker.PartitionLag
	Partitions map[string]int // topic -> partition count for subscribed topics
}

// Collect builds a snapshot using GroupInspector, LagReporter and ListTopics of b.
func Collect(ctx context.Context, b broker.Broker, group string) (Snapshot, error) {
	gi, err := broker.As[broker.GroupInspector](b, broker.CapGroupInspector)
	if err != nil {
		return Snapshot{}, err
	}
	lr, err := broker.As[broker.LagReporter](b, broker.CapLagReporter)
	if err != nil {
		return Snapshot{}, err
	}
	d, err := gi.DescribeGroup(ctx, group)
	if err != nil {
		return Snapshot{}, err
	}
	lag, err := lr.Lag(ctx, group)
	if err != nil {
		return Snapshot{}, fmt.Errorf("lag of group %q: %w", group, err)
	}
	topics, err := b.ListTopics(ctx)
	if err != nil {
		return Snapshot{}, fmt.Errorf("list topics: %w", err)
	}
	wanted := subscribedTopics(d)
	for _, l := range lag {
		wanted[l.Topic] = true
	}
	parts := map[string]int{}
	for _, t := range topics {
		if wanted[t.Name] {
			parts[t.Name] = t.Partitions
		}
	}
	at := d.DescribedAt
	if at.IsZero() {
		at = time.Now()
	}
	return Snapshot{At: at, Group: d, Lag: lag, Partitions: parts}, nil
}

// subscribedTopics is every topic a member subscribes to or is assigned.
func subscribedTopics(d *broker.GroupDescription) map[string]bool {
	out := map[string]bool{}
	for _, m := range d.Members {
		for _, t := range m.Subscriptions {
			out[t] = true
		}
		for _, a := range m.Assignment {
			out[a.Topic] = true
		}
	}
	return out
}
