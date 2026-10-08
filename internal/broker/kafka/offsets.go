package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/Max2535/mqx/internal/broker"
)

// ResetOffsets implements broker.OffsetManager. The group must have no
// members; a group that does not exist yet is created by the commit, like
// kafka-consumer-groups.sh does.
func (k *Kafka) ResetOffsets(ctx context.Context, group string, req broker.OffsetReset) ([]broker.OffsetChange, error) {
	if req.Topic == "" {
		return nil, errors.New("reset offsets needs a topic")
	}
	d, err := k.DescribeGroup(ctx, group)
	switch {
	case errors.Is(err, broker.ErrNotFound):
	case err != nil:
		return nil, err
	case len(d.Members) > 0:
		return nil, fmt.Errorf("group %q has %d active members (state %s); stop its consumers first, "+
			"then retry (offsets can only be reset on an empty group)", group, len(d.Members), d.State)
	}
	parts, err := k.partitionsOf(ctx, req.Topic, req.Partitions)
	if err != nil {
		return nil, err
	}
	m, err := k.watermarks(ctx, req.Topic)
	if err != nil {
		return nil, err
	}
	committed, err := k.adm.FetchOffsets(ctx, group)
	if err != nil && !errors.Is(err, kerr.GroupIDNotFound) {
		return nil, wrapErr(fmt.Sprintf("fetch offsets of group %q", group), err)
	}
	old := make(map[int32]int64, len(parts))
	for _, p := range parts {
		old[p] = -1
		if c, ok := committed.Lookup(req.Topic, p); ok && c.Err == nil && c.At >= 0 {
			old[p] = c.At
		}
	}
	targets, err := k.resetTargets(ctx, req, parts, m, old)
	if err != nil {
		return nil, err
	}
	changes := make([]broker.OffsetChange, 0, len(parts))
	commit := kadm.Offsets{}
	for _, p := range parts {
		changes = append(changes, broker.OffsetChange{Topic: req.Topic, Partition: p, Old: old[p], New: targets[p]})
		commit.Add(kadm.Offset{Topic: req.Topic, Partition: p, At: targets[p], LeaderEpoch: -1})
	}
	if req.DryRun {
		return changes, nil
	}
	resps, err := k.adm.CommitOffsets(ctx, group, commit)
	if err == nil {
		err = resps.Error()
	}
	if err != nil {
		if errors.Is(err, kerr.UnknownMemberID) || errors.Is(err, kerr.RebalanceInProgress) {
			return nil, fmt.Errorf("reset offsets of group %q: %w; a consumer joined meanwhile, stop it and retry", group, err)
		}
		return nil, wrapErr(fmt.Sprintf("reset offsets of group %q", group), err)
	}
	return changes, nil
}

// resetTargets computes the new committed offset of each partition.
func (k *Kafka) resetTargets(ctx context.Context, req broker.OffsetReset, parts []int32, m marks, old map[int32]int64) (map[int32]int64, error) {
	if req.Shift == 0 {
		return k.resolvePositions(ctx, req.Topic, parts, m, req.To)
	}
	out := make(map[int32]int64, len(parts))
	for _, p := range parts {
		if old[p] < 0 {
			return nil, fmt.Errorf("partition %d of %q has no committed offset to shift from; use --to instead", p, req.Topic)
		}
		low, high := m.get(req.Topic, p)
		out[p] = min(max(old[p]+req.Shift, low), high)
	}
	return out, nil
}

// DeleteGroup implements broker.OffsetManager.
func (k *Kafka) DeleteGroup(ctx context.Context, group string) error {
	resp, err := k.adm.DeleteGroup(ctx, group)
	if err == nil {
		err = resp.Err
	}
	switch {
	case err == nil:
		return nil
	case errors.Is(err, kerr.NonEmptyGroup):
		return fmt.Errorf("delete group %q: %w; stop its consumers first", group, err)
	case errors.Is(err, kerr.GroupIDNotFound):
		return fmt.Errorf("group %q: %w; run `mqx groups` to list groups", group, broker.ErrNotFound)
	}
	return wrapErr(fmt.Sprintf("delete group %q", group), err)
}

// TerminateConsumer implements broker.ConsumerTerminator by removing a static
// member (group.instance.id) from its group with LeaveGroup.
func (k *Kafka) TerminateConsumer(ctx context.Context, t broker.ConsumerTarget) error {
	if t.Connection != "" {
		return fmt.Errorf("kafka has no connections to close; remove a static member with a group and instance id: %w", broker.ErrUnsupported)
	}
	if t.Group == "" || t.InstanceID == "" {
		return errors.New("removing a kafka consumer needs the group and the static member's instance id (group.instance.id)")
	}
	d, err := k.DescribeGroup(ctx, t.Group)
	if err != nil {
		return err
	}
	var ids []string
	for _, m := range d.Members {
		if m.InstanceID != "" {
			ids = append(ids, m.InstanceID)
		}
	}
	if !slices.Contains(ids, t.InstanceID) {
		hint := "the group has no static members; dynamic members leave after session.timeout.ms"
		if len(ids) > 0 {
			hint = fmt.Sprintf("static members: %v", ids)
		}
		return fmt.Errorf("static member %q in group %q: %w; %s", t.InstanceID, t.Group, broker.ErrNotFound, hint)
	}
	b := kadm.LeaveGroup(t.Group).InstanceIDs(t.InstanceID)
	if t.Reason != "" {
		b = b.Reason(t.Reason)
	}
	resps, err := k.adm.LeaveGroup(ctx, b)
	if err != nil {
		return wrapErr(fmt.Sprintf("remove member %q from group %q", t.InstanceID, t.Group), err)
	}
	for _, r := range resps {
		if r.Err != nil {
			return wrapErr(fmt.Sprintf("remove member %q from group %q", r.InstanceID, t.Group), r.Err)
		}
	}
	return nil
}
