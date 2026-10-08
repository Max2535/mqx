package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/Max2535/mqx/internal/broker"
)

// Group protocols as reported in broker.GroupDescription.GroupProtocol.
const (
	protocolClassic  = "classic"
	protocolConsumer = "consumer"
)

// listedGroup is one ListGroups entry, including the KIP-848 group type.
type listedGroup struct {
	name, state, protocolType, groupType string
}

// listGroups sends ListGroups to every broker. Version 5+ reports the group
// type (classic, consumer, share, streams).
func (k *Kafka) listGroups(ctx context.Context) ([]listedGroup, error) {
	req := kmsg.NewPtrListGroupsRequest()
	var out []listedGroup
	for _, shard := range k.cl.RequestSharded(ctx, req) {
		if shard.Err != nil {
			return nil, wrapErr("list groups", shard.Err)
		}
		resp := shard.Resp.(*kmsg.ListGroupsResponse)
		if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
			return nil, wrapErr("list groups", err)
		}
		for _, g := range resp.Groups {
			out = append(out, listedGroup{name: g.Group, state: g.GroupState, protocolType: g.ProtocolType, groupType: g.GroupType})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out, nil
}

// ListGroups implements broker.GroupInspector.
func (k *Kafka) ListGroups(ctx context.Context) ([]broker.GroupSummary, error) {
	listed, err := k.listGroups(ctx)
	if err != nil {
		return nil, err
	}
	descs, err := k.describeGroups(ctx, listed)
	if err != nil {
		return nil, err
	}
	out := make([]broker.GroupSummary, 0, len(listed))
	for _, l := range listed {
		s := broker.GroupSummary{Name: l.name, State: l.state, ProtocolType: l.protocolType, GroupProtocol: groupProtocol(l.groupType)}
		if d, ok := descs[l.name]; ok {
			s.State, s.Members = d.State, len(d.Members)
		}
		out = append(out, s)
	}
	return out, nil
}

func groupProtocol(groupType string) string {
	if groupType == "" {
		return protocolClassic // brokers before KIP-848 only have classic groups
	}
	return groupType
}

// describeGroups describes many listed groups with one batch per protocol.
func (k *Kafka) describeGroups(ctx context.Context, listed []listedGroup) (map[string]*broker.GroupDescription, error) {
	var classic, consumer []string
	for _, l := range listed {
		switch groupProtocol(l.groupType) {
		case protocolClassic:
			classic = append(classic, l.name)
		case protocolConsumer:
			consumer = append(consumer, l.name)
		}
	}
	out := map[string]*broker.GroupDescription{}
	if len(classic) > 0 {
		ds, err := k.adm.DescribeGroups(ctx, classic...)
		if err != nil {
			return nil, wrapErr("describe groups", err)
		}
		for name, d := range ds {
			if d.Err == nil {
				out[name] = fromClassic(d)
			}
		}
	}
	if len(consumer) > 0 {
		ds, err := k.adm.DescribeConsumerGroups(ctx, consumer...)
		if err != nil {
			return nil, wrapErr("describe consumer groups", err)
		}
		for name, d := range ds {
			if d.Err == nil {
				out[name] = fromConsumer(d)
			}
		}
	}
	return out, nil
}

// DescribeGroup implements broker.GroupInspector. Groups using the KIP-848
// consumer protocol are described with ConsumerGroupDescribe, others with the
// classic DescribeGroups.
func (k *Kafka) DescribeGroup(ctx context.Context, group string) (*broker.GroupDescription, error) {
	if d, ok := k.describeConsumerGroup(ctx, group); ok {
		return d, nil
	}
	ds, err := k.adm.DescribeGroups(ctx, group)
	if err != nil {
		return nil, wrapErr(fmt.Sprintf("describe group %q", group), err)
	}
	d, ok := ds[group]
	if !ok || errors.Is(d.Err, kerr.GroupIDNotFound) || (d.Err == nil && d.State == "Dead") {
		return nil, fmt.Errorf("group %q: %w; run `mqx groups` to list groups", group, broker.ErrNotFound)
	}
	if d.Err != nil {
		return nil, wrapErr(fmt.Sprintf("describe group %q", group), d.Err)
	}
	return fromClassic(d), nil
}

// describeConsumerGroup describes a KIP-848 group. ok is false when the group
// is not a consumer-protocol group or the broker predates KIP-848.
func (k *Kafka) describeConsumerGroup(ctx context.Context, group string) (*broker.GroupDescription, bool) {
	ds, err := k.adm.DescribeConsumerGroups(ctx, group)
	if err != nil {
		return nil, false
	}
	d, ok := ds[group]
	if !ok || d.Err != nil || d.State == "Dead" {
		return nil, false
	}
	return fromConsumer(d), true
}

func fromClassic(d kadm.DescribedGroup) *broker.GroupDescription {
	out := &broker.GroupDescription{
		Name: d.Group, State: d.State, ProtocolType: d.ProtocolType, GroupProtocol: protocolClassic,
		Assignor: d.Protocol, Coordinator: node(d.Coordinator), Epoch: -1, Members: []broker.GroupMember{},
		DescribedAt: time.Now(),
	}
	for _, m := range d.Members {
		gm := broker.GroupMember{MemberID: m.MemberID, InstanceID: deref(m.InstanceID), ClientID: m.ClientID, Host: m.ClientHost}
		if meta, ok := m.Join.AsConsumer(); ok {
			gm.Subscriptions = slices.Sorted(slices.Values(meta.Topics))
		}
		if a, ok := m.Assigned.AsConsumer(); ok {
			for _, t := range a.Topics {
				ps := slices.Clone(t.Partitions)
				slices.Sort(ps)
				gm.Assignment = append(gm.Assignment, broker.TopicPartitions{Topic: t.Topic, Partitions: ps})
			}
			sortTPs(gm.Assignment)
		}
		out.Members = append(out.Members, gm)
	}
	return out
}

func fromConsumer(d kadm.DescribedConsumerGroup) *broker.GroupDescription {
	out := &broker.GroupDescription{
		Name: d.Group, State: d.State, ProtocolType: "consumer", GroupProtocol: protocolConsumer,
		Assignor: d.AssignorName, Coordinator: node(d.Coordinator), Epoch: d.Epoch, Members: []broker.GroupMember{},
		DescribedAt: time.Now(),
	}
	for _, m := range d.Members {
		gm := broker.GroupMember{
			MemberID: m.MemberID, InstanceID: deref(m.InstanceID), ClientID: m.ClientID, Host: m.ClientHost,
			Subscriptions: slices.Sorted(slices.Values(m.SubscribedTopics)), Pattern: deref(m.SubscribedTopicRegex),
			Assignment: fromTopicsSet(m.Assignment), Target: fromTopicsSet(m.TargetAssignment), MemberEpoch: m.MemberEpoch,
		}
		out.Members = append(out.Members, gm)
	}
	return out
}

func fromTopicsSet(s kadm.TopicsSet) []broker.TopicPartitions {
	var out []broker.TopicPartitions
	for t, ps := range s {
		tp := broker.TopicPartitions{Topic: t}
		for p := range ps {
			tp.Partitions = append(tp.Partitions, p)
		}
		slices.Sort(tp.Partitions)
		out = append(out, tp)
	}
	sortTPs(out)
	return out
}

func sortTPs(tps []broker.TopicPartitions) {
	sort.Slice(tps, func(i, j int) bool { return tps[i].Topic < tps[j].Topic })
}

func node(b kadm.BrokerDetail) broker.Node {
	return broker.Node{ID: strconv.Itoa(int(b.NodeID)), Host: b.Host, Port: b.Port, Rack: deref(b.Rack), Running: true}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// Consumers implements broker.ConsumerInspector: every member of every group
// that is assigned partitions of the topic.
func (k *Kafka) Consumers(ctx context.Context, topic string) ([]broker.Consumer, error) {
	if _, err := k.topicDetail(ctx, topic); err != nil {
		return nil, err
	}
	listed, err := k.listGroups(ctx)
	if err != nil {
		return nil, err
	}
	descs, err := k.describeGroups(ctx, listed)
	if err != nil {
		return nil, err
	}
	var out []broker.Consumer
	for _, l := range listed {
		d, ok := descs[l.name]
		if !ok {
			continue
		}
		for _, m := range d.Members {
			for _, a := range m.Assignment {
				if a.Topic != topic {
					continue
				}
				out = append(out, broker.Consumer{
					Group: d.Name, MemberID: m.MemberID, InstanceID: m.InstanceID, ClientID: m.ClientID,
					Host: m.Host, Partitions: a.Partitions, Active: true,
				})
			}
		}
	}
	return out, nil
}

// Lag implements broker.LagReporter. Partitions of subscribed topics without a
// committed offset are reported with Committed and Lag -1.
func (k *Kafka) Lag(ctx context.Context, group string) ([]broker.PartitionLag, error) {
	d, err := k.DescribeGroup(ctx, group)
	if err != nil {
		return nil, err
	}
	committed, err := k.adm.FetchOffsets(ctx, group)
	if err != nil {
		return nil, wrapErr(fmt.Sprintf("fetch offsets of group %q", group), err)
	}
	owner := map[string]map[int32]broker.GroupMember{}
	subscribed := map[string]bool{}
	for _, m := range d.Members {
		for _, t := range m.Subscriptions {
			subscribed[t] = true
		}
		for _, a := range m.Assignment {
			subscribed[a.Topic] = true
			if owner[a.Topic] == nil {
				owner[a.Topic] = map[int32]broker.GroupMember{}
			}
			for _, p := range a.Partitions {
				owner[a.Topic][p] = m
			}
		}
	}
	topics := map[string]bool{}
	for t := range committed {
		topics[t] = true
	}
	for t := range subscribed {
		topics[t] = true
	}
	names := make([]string, 0, len(topics))
	for t := range topics {
		names = append(names, t)
	}
	sort.Strings(names)
	ends, err := k.adm.ListEndOffsets(ctx, names...)
	if err != nil {
		return nil, wrapErr("list end offsets", err)
	}
	var out []broker.PartitionLag
	for _, t := range names {
		parts := make([]int32, 0, len(ends[t]))
		for p := range ends[t] {
			if p >= 0 {
				parts = append(parts, p)
			}
		}
		slices.Sort(parts)
		for _, p := range parts {
			end := ends[t][p]
			if end.Err != nil {
				continue
			}
			pl := broker.PartitionLag{Topic: t, Partition: p, Committed: -1, End: end.Offset, Lag: -1}
			if c, ok := committed.Lookup(t, p); ok && c.Err == nil && c.At >= 0 {
				pl.Committed, pl.Lag = c.At, max(0, end.Offset-c.At)
			} else if !subscribed[t] {
				continue // a topic the group consumed once; only committed partitions matter
			}
			if m, ok := owner[t][p]; ok {
				pl.MemberID, pl.ClientID, pl.Host = m.MemberID, m.ClientID, m.Host
			}
			out = append(out, pl)
		}
	}
	return out, nil
}
