package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Max2535/mqx/internal/broker"
)

// Publish implements broker.Broker with a synchronous produce. A message with
// a Schema has its JSON Value (or Key with KeySide) encoded with that registry
// schema in the Confluent wire format.
func (k *Kafka) Publish(ctx context.Context, topic string, msg broker.Message) error {
	td, err := k.topicDetail(ctx, topic)
	if err != nil {
		return err
	}
	if msg.Partition != broker.AnyPartition {
		if _, ok := td.Partitions[msg.Partition]; !ok || msg.Partition < 0 {
			return fmt.Errorf("topic %q has no partition %d (it has %d partitions); omit the partition to let the producer choose",
				topic, msg.Partition, len(td.Partitions))
		}
	}
	key, value := msg.Key, msg.Value
	if msg.Schema != nil {
		if k.serde == nil {
			return errors.New("publishing with a schema needs schema_registry configured in the context")
		}
		data := value
		if msg.Schema.KeySide {
			data = key
		}
		encoded, err := k.serde.encode(ctx, topic, msg.Schema, data)
		if err != nil {
			return err
		}
		if msg.Schema.KeySide {
			key = encoded
		} else {
			value = encoded
		}
	}
	rec := &kgo.Record{Topic: topic, Partition: msg.Partition, Key: key, Value: value, Timestamp: msg.Timestamp}
	if msg.Partition == broker.AnyPartition {
		rec.Partition = -1 // the partitioner picks
	}
	for _, h := range msg.Headers {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: h.Key, Value: h.Value})
	}
	if err := k.cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return wrapErr(fmt.Sprintf("publish to %q", topic), err)
	}
	return nil
}
