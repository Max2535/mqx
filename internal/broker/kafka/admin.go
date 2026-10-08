package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/Max2535/mqx/internal/broker"
)

// CreateTopic implements broker.TopicAdmin. Zero partitions or replication
// factor use the broker defaults.
func (k *Kafka) CreateTopic(ctx context.Context, spec broker.TopicSpec) error {
	if spec.Name == "" {
		return errors.New("create topic needs a name")
	}
	partitions, rf := spec.Partitions, spec.ReplicationFactor
	if partitions <= 0 {
		partitions = -1
	}
	if rf <= 0 {
		rf = -1
	}
	configs := make(map[string]*string, len(spec.Configs))
	for name, v := range spec.Configs {
		configs[name] = kadm.StringPtr(v)
	}
	resp, err := k.adm.CreateTopic(ctx, partitions, rf, configs, spec.Name)
	if err == nil {
		err = resp.Err
	}
	switch {
	case err == nil:
		return nil
	case errors.Is(err, kerr.TopicAlreadyExists):
		return fmt.Errorf("create topic %q: %w; delete it first or pick another name", spec.Name, err)
	case errors.Is(err, kerr.InvalidReplicationFactor):
		return fmt.Errorf("create topic %q: %w; the replication factor cannot exceed the number of brokers (see `mqx nodes`)", spec.Name, err)
	}
	if resp.ErrMessage != "" {
		return wrapErr(fmt.Sprintf("create topic %q (%s)", spec.Name, resp.ErrMessage), err)
	}
	return wrapErr(fmt.Sprintf("create topic %q", spec.Name), err)
}

// DeleteTopic implements broker.TopicAdmin.
func (k *Kafka) DeleteTopic(ctx context.Context, name string) error {
	resp, err := k.adm.DeleteTopic(ctx, name)
	if err == nil {
		err = resp.Err
	}
	return wrapErr(fmt.Sprintf("delete topic %q", name), err)
}

// AlterTopicConfig implements broker.TopicAdmin with an incremental alter.
// An empty value deletes the override, reverting to the default.
func (k *Kafka) AlterTopicConfig(ctx context.Context, name string, changes map[string]string) error {
	if len(changes) == 0 {
		return errors.New("alter topic config needs at least one change")
	}
	alters := make([]kadm.AlterConfig, 0, len(changes))
	for key, v := range changes {
		if v == "" {
			alters = append(alters, kadm.AlterConfig{Op: kadm.DeleteConfig, Name: key})
			continue
		}
		alters = append(alters, kadm.AlterConfig{Op: kadm.SetConfig, Name: key, Value: kadm.StringPtr(v)})
	}
	sort.Slice(alters, func(i, j int) bool { return alters[i].Name < alters[j].Name })
	resps, err := k.adm.AlterTopicConfigs(ctx, alters, name)
	if err != nil {
		return wrapErr(fmt.Sprintf("alter config of %q", name), err)
	}
	for _, r := range resps {
		if r.Err != nil {
			if r.ErrMessage != "" {
				return wrapErr(fmt.Sprintf("alter config of %q (%s)", name, r.ErrMessage), r.Err)
			}
			return wrapErr(fmt.Sprintf("alter config of %q", name), r.Err)
		}
	}
	return nil
}

// AddPartitions implements broker.PartitionAdder.
func (k *Kafka) AddPartitions(ctx context.Context, topic string, total int) error {
	td, err := k.topicDetail(ctx, topic)
	if err != nil {
		return err
	}
	if total <= len(td.Partitions) {
		return fmt.Errorf("topic %q already has %d partitions; the new total must be larger (Kafka cannot remove partitions)",
			topic, len(td.Partitions))
	}
	resps, err := k.adm.UpdatePartitions(ctx, total, topic)
	if err == nil {
		err = resps.Error()
	}
	return wrapErr(fmt.Sprintf("add partitions to %q", topic), err)
}

// Purge implements broker.Purger with DeleteRecords: everything below the
// high watermark, or below opts.Before, in the selected partitions.
func (k *Kafka) Purge(ctx context.Context, topic string, opts broker.PurgeOptions) (broker.PurgeResult, error) {
	parts, err := k.partitionsOf(ctx, topic, opts.Partitions)
	if err != nil {
		return broker.PurgeResult{}, err
	}
	m, err := k.watermarks(ctx, topic)
	if err != nil {
		return broker.PurgeResult{}, err
	}
	before := broker.Position{Kind: broker.Latest}
	if opts.Before != nil {
		before = *opts.Before
	}
	targets, err := k.resolvePositions(ctx, topic, parts, m, before)
	if err != nil {
		return broker.PurgeResult{}, err
	}
	del := kadm.Offsets{}
	for _, p := range parts {
		del.Add(kadm.Offset{Topic: topic, Partition: p, At: targets[p], LeaderEpoch: -1})
	}
	resps, err := k.adm.DeleteRecords(ctx, del)
	if err != nil {
		return broker.PurgeResult{}, wrapErr(fmt.Sprintf("delete records of %q", topic), err)
	}
	res := broker.PurgeResult{}
	for _, p := range parts {
		r, ok := resps.Lookup(topic, p)
		if !ok {
			return res, fmt.Errorf("delete records of %q: no response for partition %d", topic, p)
		}
		if r.Err != nil {
			if errors.Is(r.Err, kerr.PolicyViolation) {
				return res, fmt.Errorf("delete records of %q partition %d: %w; delete-records is refused on compacted topics", topic, p, r.Err)
			}
			return res, wrapErr(fmt.Sprintf("delete records of %q partition %d", topic, p), r.Err)
		}
		oldLow, _ := m.get(topic, p)
		res.Messages += max(0, r.LowWatermark-oldLow)
		res.Partitions = append(res.Partitions, broker.PartitionPurge{Partition: p, LowMark: r.LowWatermark})
	}
	return res, nil
}
