package rabbitmq

import (
	"context"
	"errors"
	"fmt"

	"github.com/Max2535/mqx/internal/broker"
)

// CreateTopic declares a queue. Declaring an existing queue with the same
// properties succeeds; different properties fail with the broker's reason.
func (r *RabbitMQ) CreateTopic(ctx context.Context, spec broker.TopicSpec) error {
	if spec.Name == "" {
		return errors.New("queue name is required (server-named queues cannot be declared through the management API)")
	}
	if spec.Partitions > 0 || spec.ReplicationFactor > 0 || len(spec.Configs) > 0 {
		return fmt.Errorf("partitions, replication factor and topic configs are Kafka settings; "+
			"use durable, auto-delete and arguments (e.g. x-queue-type=quorum): %w", broker.ErrUnsupported)
	}
	body := map[string]any{"durable": spec.Durable, "auto_delete": spec.AutoDelete, "arguments": nonNil(spec.Arguments)}
	if err := r.mgmt.put(ctx, apiPath("queues", r.settings.vhost, spec.Name), body); err != nil {
		return fmt.Errorf("declare queue %q: %w", spec.Name, err)
	}
	return nil
}

func nonNil(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

// DeleteTopic deletes a queue and its messages.
func (r *RabbitMQ) DeleteTopic(ctx context.Context, name string) error {
	if err := r.mgmt.delete(ctx, apiPath("queues", r.settings.vhost, name), nil); err != nil {
		return fmt.Errorf("delete queue %q: %w", name, err)
	}
	return nil
}

// TopicConfig lists a queue's arguments, effective policy and properties.
func (r *RabbitMQ) TopicConfig(ctx context.Context, name string) ([]broker.ConfigEntry, error) {
	q, err := r.queue(ctx, name)
	if err != nil {
		return nil, err
	}
	return q.configEntries(), nil
}

// AlterTopicConfig is unsupported: queue arguments are fixed at declaration.
func (r *RabbitMQ) AlterTopicConfig(context.Context, string, map[string]string) error {
	return fmt.Errorf("queue arguments are immutable; use `mqx policy set` to change TTL, length limits "+
		"and similar settings on existing queues: %w", broker.ErrUnsupported)
}

// Purge removes every ready message from a queue and reports how many.
func (r *RabbitMQ) Purge(ctx context.Context, topic string, opts broker.PurgeOptions) (broker.PurgeResult, error) {
	if len(opts.Partitions) > 0 || opts.Before != nil {
		return broker.PurgeResult{}, fmt.Errorf("RabbitMQ purges whole queues; drop the partition and position options: %w",
			broker.ErrUnsupported)
	}
	ch, err := r.channel(ctx)
	if err != nil {
		return broker.PurgeResult{}, err
	}
	defer ch.Close()
	n, err := ch.QueuePurge(topic, false)
	if err != nil {
		return broker.PurgeResult{}, amqpErr("purge queue", topic, err)
	}
	return broker.PurgeResult{Messages: int64(n)}, nil
}
