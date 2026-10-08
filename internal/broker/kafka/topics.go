package kafka

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/Max2535/mqx/internal/broker"
)

// maxGroupsForConsumerCount bounds the per-topic consumer count in ListTopics:
// beyond it, fetching every group's offsets is too expensive and Consumers is -1.
const maxGroupsForConsumerCount = 200

// Ping implements broker.Broker with a metadata request.
func (k *Kafka) Ping(ctx context.Context) error {
	if _, err := k.adm.BrokerMetadata(ctx); err != nil {
		return wrapErr("ping kafka", err)
	}
	return nil
}

// ListTopics implements broker.Broker.
func (k *Kafka) ListTopics(ctx context.Context) ([]broker.Topic, error) {
	tds, err := k.adm.ListTopicsWithInternal(ctx)
	if err != nil {
		return nil, wrapErr("list topics", err)
	}
	names := tds.Names()
	marks, err := k.watermarks(ctx, names...)
	if err != nil {
		return nil, err
	}
	consumers := k.consumerCounts(ctx)
	out := make([]broker.Topic, 0, len(names))
	for _, name := range names {
		td := tds[name]
		if td.Err != nil {
			continue
		}
		t := broker.Topic{
			Name:       name,
			Kind:       "topic",
			Partitions: len(td.Partitions),
			Internal:   td.IsInternal,
			Messages:   marks.messages(name),
			Consumers:  -1,
		}
		if len(td.Partitions) > 0 {
			t.Replicas = len(td.Partitions.Sorted()[0].Replicas)
		}
		if consumers != nil {
			t.Consumers = consumers[name]
		}
		out = append(out, t)
	}
	return out, nil
}

// consumerCounts returns the number of groups with committed offsets per
// topic, or nil when it cannot be computed cheaply.
func (k *Kafka) consumerCounts(ctx context.Context) map[string]int {
	groups, err := k.adm.ListGroups(ctx)
	if err != nil || len(groups) > maxGroupsForConsumerCount {
		return nil
	}
	counts := map[string]int{}
	if len(groups) == 0 {
		return counts
	}
	for _, resp := range k.adm.FetchManyOffsets(ctx, groups.Groups()...) {
		if resp.Err != nil {
			return nil
		}
		for topic := range resp.Fetched {
			counts[topic]++
		}
	}
	return counts
}

// DescribeTopic implements broker.TopicDescriber.
func (k *Kafka) DescribeTopic(ctx context.Context, name string) (*broker.TopicDetail, error) {
	td, err := k.topicDetail(ctx, name)
	if err != nil {
		return nil, err
	}
	marks, err := k.watermarks(ctx, name)
	if err != nil {
		return nil, err
	}
	d := &broker.TopicDetail{Topic: broker.Topic{
		Name: name, Kind: "topic", Partitions: len(td.Partitions), Internal: td.IsInternal,
		Messages: marks.messages(name), Consumers: -1,
	}}
	for _, p := range td.Partitions.Sorted() {
		d.Topic.Replicas = len(p.Replicas)
		lo, hi := marks.get(name, p.Partition)
		d.Partitions = append(d.Partitions, broker.PartitionInfo{
			ID: p.Partition, Leader: p.Leader, Replicas: p.Replicas, ISR: p.ISR, Start: lo, End: hi,
		})
	}
	if c := k.consumerCounts(ctx); c != nil {
		d.Topic.Consumers = c[name]
	}
	if d.Configs, err = k.TopicConfig(ctx, name); err != nil {
		return nil, err
	}
	return d, nil
}

// topicDetail returns one topic's metadata or a wrapped broker.ErrNotFound.
func (k *Kafka) topicDetail(ctx context.Context, name string) (kadm.TopicDetail, error) {
	tds, err := k.adm.ListTopicsWithInternal(ctx, name)
	if err != nil {
		return kadm.TopicDetail{}, wrapErr(fmt.Sprintf("describe topic %q", name), err)
	}
	td, ok := tds[name]
	if !ok {
		return kadm.TopicDetail{}, fmt.Errorf("topic %q: %w; run `mqx topics` to list topics", name, broker.ErrNotFound)
	}
	if td.Err != nil {
		return kadm.TopicDetail{}, wrapErr(fmt.Sprintf("topic %q", name), td.Err)
	}
	return td, nil
}

// partitionsOf returns the topic's partition ids, restricted to only when
// non-empty. Unknown partitions in only are an error.
func (k *Kafka) partitionsOf(ctx context.Context, topic string, only []int32) ([]int32, error) {
	td, err := k.topicDetail(ctx, topic)
	if err != nil {
		return nil, err
	}
	all := td.Partitions.Numbers()
	if len(only) == 0 {
		return all, nil
	}
	for _, p := range only {
		if !slices.Contains(all, p) {
			return nil, fmt.Errorf("topic %q has no partition %d (it has %d partitions): %w", topic, p, len(all), broker.ErrNotFound)
		}
	}
	out := slices.Clone(only)
	slices.Sort(out)
	return slices.Compact(out), nil
}

// marks holds low and high watermarks per topic and partition.
type marks struct{ low, high kadm.ListedOffsets }

func (k *Kafka) watermarks(ctx context.Context, topics ...string) (marks, error) {
	if len(topics) == 0 {
		return marks{}, nil
	}
	low, err := k.adm.ListStartOffsets(ctx, topics...)
	if err != nil {
		return marks{}, wrapErr("list start offsets", err)
	}
	high, err := k.adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return marks{}, wrapErr("list end offsets", err)
	}
	return marks{low: low, high: high}, nil
}

func (m marks) get(topic string, p int32) (low, high int64) {
	lo, _ := m.low.Lookup(topic, p)
	hi, _ := m.high.Lookup(topic, p)
	return max(lo.Offset, 0), max(hi.Offset, 0)
}

// messages is the sum of partition sizes, or -1 when unknown.
func (m marks) messages(topic string) int64 {
	ps, ok := m.high[topic]
	if !ok {
		return -1
	}
	var n int64
	for p, hi := range ps {
		if hi.Err != nil || p < 0 {
			return -1
		}
		lo, _ := m.low.Lookup(topic, p)
		n += hi.Offset - max(lo.Offset, 0)
	}
	return n
}

// describeConfigs reads every config of one topic or broker, including the
// read-only flag that kadm does not expose.
func (k *Kafka) describeConfigs(ctx context.Context, typ kmsg.ConfigResourceType, name string) ([]broker.ConfigEntry, error) {
	req := kmsg.NewPtrDescribeConfigsRequest()
	res := kmsg.NewDescribeConfigsRequestResource()
	res.ResourceType = typ
	res.ResourceName = name
	req.Resources = append(req.Resources, res)
	kresp, err := req.RequestWith(ctx, k.cl)
	if err != nil {
		return nil, wrapErr("describe configs of "+name, err)
	}
	var out []broker.ConfigEntry
	for _, r := range kresp.Resources {
		if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
			return nil, wrapErr(fmt.Sprintf("describe configs of %q", name), err)
		}
		for _, c := range r.Configs {
			e := broker.ConfigEntry{
				Name: c.Name, Source: c.Source.String(), Sensitive: c.IsSensitive, ReadOnly: c.ReadOnly,
			}
			if c.Value != nil && !c.IsSensitive {
				e.Value = *c.Value
			}
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// TopicConfig implements broker.TopicAdmin.
func (k *Kafka) TopicConfig(ctx context.Context, name string) ([]broker.ConfigEntry, error) {
	return k.describeConfigs(ctx, kmsg.ConfigResourceTypeTopic, name)
}

// Nodes implements broker.ClusterInspector.
func (k *Kafka) Nodes(ctx context.Context) ([]broker.Node, error) {
	m, err := k.adm.BrokerMetadata(ctx)
	if err != nil {
		return nil, wrapErr("list brokers", err)
	}
	out := make([]broker.Node, 0, len(m.Brokers))
	for _, b := range m.Brokers {
		n := broker.Node{
			ID: strconv.Itoa(int(b.NodeID)), Host: b.Host, Port: b.Port,
			Controller: b.NodeID == m.Controller, Running: true,
		}
		if b.Rack != nil {
			n.Rack = *b.Rack
		}
		if m.Cluster != "" {
			n.Details = map[string]string{"cluster_id": m.Cluster}
		}
		out = append(out, n)
	}
	return out, nil
}

// NodeConfig implements broker.ClusterInspector.
func (k *Kafka) NodeConfig(ctx context.Context, id string) ([]broker.ConfigEntry, error) {
	if _, err := strconv.ParseInt(id, 10, 32); err != nil {
		return nil, fmt.Errorf("broker id %q must be a number; run `mqx nodes` to list ids", id)
	}
	return k.describeConfigs(ctx, kmsg.ConfigResourceTypeBroker, id)
}
