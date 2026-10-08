// Package broker defines the broker-neutral core interface, the optional
// capability interfaces, their models and the adapter registry.
//
// Every adapter implements Broker. Everything else is a capability: the CLI and
// TUI type-assert for it and show or hide features accordingly. Adapters never
// enforce read_only or confirmation; that is the job of the mutation guard in
// the CLI and TUI.
package broker

import (
	"context"
	"errors"
	"time"
)

// ErrUnsupported is returned when an adapter implements a capability but a
// particular operation or option does not apply to it. Wrap it with a hint.
var ErrUnsupported = errors.New("not supported by this broker")

// ErrNotFound is returned when a named topic, queue, group or other object does not exist.
var ErrNotFound = errors.New("not found")

// Broker is the core every adapter implements.
type Broker interface {
	// Name returns the broker type, e.g. "kafka".
	Name() string
	Ping(ctx context.Context) error
	// ListTopics lists Kafka topics or RabbitMQ queues.
	ListTopics(ctx context.Context) ([]Topic, error)
	// Publish sends one message to a Kafka topic or a RabbitMQ queue.
	// For RabbitMQ, msg.Exchange routes through an exchange with msg.RoutingKey instead.
	Publish(ctx context.Context, topic string, msg Message) error
	// Peek streams messages without consuming them. The channel closes when
	// the range is exhausted, Limit is reached or ctx ends. A failure after
	// the stream started arrives as a final Message with Err set.
	Peek(ctx context.Context, topic string, opts PeekOptions) (<-chan Message, error)
	Close() error
}

// Topic is a Kafka topic or a RabbitMQ queue.
type Topic struct {
	Name       string            `json:"name"`
	Kind       string            `json:"kind"`                 // "topic" or "queue"
	Partitions int               `json:"partitions,omitempty"` // kafka
	Replicas   int               `json:"replicas,omitempty"`   // kafka replication factor
	Internal   bool              `json:"internal,omitempty"`
	Messages   int64             `json:"messages"`  // queue depth, or sum of partition sizes; -1 unknown
	Consumers  int               `json:"consumers"` // -1 unknown
	Details    map[string]string `json:"details,omitempty"`
}

// Header is one message header. Kafka allows duplicate keys, so this is a slice element, not a map.
type Header struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// AnyPartition lets the producer's partitioner pick the partition on Publish.
const AnyPartition int32 = -1

// NewMessage returns a message for Publish with the partition left to the producer.
func NewMessage(value []byte) Message {
	return Message{Partition: AnyPartition, Value: value}
}

// Message is a message read by Peek or sent by Publish. On Publish, Partition
// selects a Kafka partition; use AnyPartition (see NewMessage) to let the
// producer choose.
type Message struct {
	Topic     string    `json:"topic"`
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	Key       []byte    `json:"key,omitempty"`
	Value     []byte    `json:"value"`
	Headers   []Header  `json:"headers,omitempty"`
	Timestamp time.Time `json:"timestamp"`

	// RabbitMQ.
	Exchange    string            `json:"exchange,omitempty"`
	RoutingKey  string            `json:"routing_key,omitempty"`
	Properties  map[string]string `json:"properties,omitempty"` // content_type, correlation_id, ...
	Redelivered bool              `json:"redelivered,omitempty"`

	// Schema Registry: set on peek when Value was decoded, or on publish to encode Value (JSON).
	Schema *SchemaRef `json:"schema,omitempty"`

	// Err is set only on the last message of a Peek stream that ended in failure.
	Err error `json:"-"`
}

// SchemaRef identifies a registered schema. On publish, set Subject (and
// optionally Version) or ID; on peek, adapters fill ID, Format and Subject.
type SchemaRef struct {
	// Name is the import name a schema uses for a reference (e.g. a .proto
	// path); it defaults to Subject.
	Name    string `json:"name,omitempty"`
	ID      int    `json:"id,omitempty"`
	Subject string `json:"subject,omitempty"`
	Version int    `json:"version,omitempty"`
	Format  string `json:"format,omitempty"` // AVRO, PROTOBUF, JSON
	// KeySide applies the schema to the key instead of the value.
	KeySide bool `json:"key_side,omitempty"`
}

// HeaderValue returns the first header named key.
func (m Message) HeaderValue(key string) ([]byte, bool) {
	for _, h := range m.Headers {
		if h.Key == key {
			return h.Value, true
		}
	}
	return nil, false
}

// PositionKind selects where a read starts or stops.
type PositionKind int

// Position kinds.
const (
	Earliest PositionKind = iota // the zero value: from the beginning
	Latest                       // from the end: only new messages
	AtOffset                     // an absolute offset
	AtTime                       // the first offset at or after a timestamp
	Tail                         // the last N messages of each partition
)

// Position is a point in a partition.
type Position struct {
	Kind   PositionKind
	Offset int64     // AtOffset: absolute offset; Tail: N
	Time   time.Time // AtTime
}

// PeekOptions controls Peek.
type PeekOptions struct {
	// Limit caps the number of messages delivered after filtering; 0 means no cap.
	Limit int
	// Partitions restricts a Kafka peek; empty means all.
	Partitions []int32
	// From is where to start. RabbitMQ only reads from the head of the queue.
	From Position
	// To is where to stop (exclusive for AtOffset). Zero means: the end as of
	// the start of the peek, unless Follow is set.
	To *Position
	// Follow keeps streaming new messages until ctx ends or Limit is reached.
	Follow bool
	Filter Filter
	// Decode decodes Schema Registry framed payloads into JSON when a registry is configured.
	Decode bool
}

// ConfigEntry is one config value of a topic, queue or broker.
type ConfigEntry struct {
	Name      string `json:"name"`
	Value     string `json:"value"`
	Source    string `json:"source,omitempty"` // e.g. DYNAMIC_TOPIC_CONFIG, DEFAULT_CONFIG
	Sensitive bool   `json:"sensitive,omitempty"`
	ReadOnly  bool   `json:"read_only,omitempty"`
}

// IsDefault reports whether the entry comes from a default rather than an override.
func (c ConfigEntry) IsDefault() bool {
	return c.Source == "" || c.Source == "DEFAULT_CONFIG" || c.Source == "STATIC_BROKER_CONFIG"
}
