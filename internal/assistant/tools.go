package assistant

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/groupdiag"
)

// Tool is one read-only broker query the model may call.
type Tool struct {
	Name        string
	Description string
	// Params are the JSON Schema properties of the input; Required lists the
	// mandatory ones.
	Params   map[string]any
	Required []string
	// Run executes the call; its result is sent to the model as JSON.
	Run func(ctx context.Context, in Input) (any, error)
}

// Input is a tool call's arguments.
type Input map[string]any

// String returns the string argument name, "" when absent.
func (in Input) String(name string) string {
	s, _ := in[name].(string)
	return s
}

// Int returns the integer argument name, def when absent.
func (in Input) Int(name string, def int) int {
	if f, ok := in[name].(float64); ok {
		return int(f)
	}
	return def
}

// Options decide which tools a session offers.
type Options struct {
	// SendPayloads offers peek_messages, which sends message keys, headers
	// and values to the API. Off unless the config enables it.
	SendPayloads bool
}

const (
	maxPeek     = 20
	maxValueLen = 2000
)

// Tools returns the read-only tools b supports. None of them changes broker
// state, so the assistant never needs the mutation guard: it can only
// suggest commands for the user to run.
func Tools(b broker.Broker, opts Options) []Tool {
	str := func(desc string) map[string]any { return map[string]any{"type": "string", "description": desc} }
	tools := []Tool{{
		Name:        "list_topics",
		Description: "List the topics (Kafka) or queues (RabbitMQ) with partitions, replicas, message and consumer counts.",
		Run:         func(ctx context.Context, _ Input) (any, error) { return b.ListTopics(ctx) },
	}}
	if t, ok := capability[broker.TopicDescriber](b, broker.CapTopicDescriber); ok {
		tools = append(tools, Tool{
			Name:        "describe_topic",
			Description: "Describe one topic or queue: partitions with leader, replicas, ISR and offsets, and non-default configs.",
			Params:      map[string]any{"name": str("topic or queue name")},
			Required:    []string{"name"},
			Run: func(ctx context.Context, in Input) (any, error) {
				return t.DescribeTopic(ctx, in.String("name"))
			},
		})
	}
	if c, ok := capability[broker.ClusterInspector](b, broker.CapClusterInspector); ok {
		tools = append(tools, Tool{
			Name:        "list_nodes",
			Description: "List the brokers or cluster nodes, their hosts, racks, controller role and whether they are running.",
			Run:         func(ctx context.Context, _ Input) (any, error) { return c.Nodes(ctx) },
		})
	}
	if c, ok := capability[broker.ConsumerInspector](b, broker.CapConsumerInspector); ok {
		tools = append(tools, Tool{
			Name:        "topic_consumers",
			Description: "List who consumes a topic or queue: groups and members on Kafka, consumers and their connections on RabbitMQ.",
			Params:      map[string]any{"topic": str("topic or queue name")},
			Required:    []string{"topic"},
			Run: func(ctx context.Context, in Input) (any, error) {
				return c.Consumers(ctx, in.String("topic"))
			},
		})
	}
	if g, ok := capability[broker.GroupInspector](b, broker.CapGroupInspector); ok {
		tools = append(tools,
			Tool{
				Name:        "list_groups",
				Description: "List the Kafka consumer groups with state, protocol and member count.",
				Run:         func(ctx context.Context, _ Input) (any, error) { return g.ListGroups(ctx) },
			},
			Tool{
				Name:        "describe_group",
				Description: "Describe a consumer group: state, coordinator, assignor, epoch, members with subscriptions and assignments.",
				Params:      map[string]any{"group": str("consumer group id")},
				Required:    []string{"group"},
				Run: func(ctx context.Context, in Input) (any, error) {
					return g.DescribeGroup(ctx, in.String("group"))
				},
			})
	}
	if l, ok := capability[broker.LagReporter](b, broker.CapLagReporter); ok {
		tools = append(tools, Tool{
			Name:        "group_lag",
			Description: "Per-partition committed offset, end offset and lag of a consumer group.",
			Params:      map[string]any{"group": str("consumer group id")},
			Required:    []string{"group"},
			Run: func(ctx context.Context, in Input) (any, error) {
				return l.Lag(ctx, in.String("group"))
			},
		})
		if broker.Has(b, broker.CapGroupInspector) {
			tools = append(tools, Tool{
				Name: "diagnose_group",
				Description: "Run mqx's rule-based diagnosis of a consumer group on one snapshot: stale members, " +
					"more members than partitions, mixed subscriptions, lag and similar findings with fixes.",
				Params:   map[string]any{"group": str("consumer group id")},
				Required: []string{"group"},
				Run: func(ctx context.Context, in Input) (any, error) {
					snap, err := groupdiag.Collect(ctx, b, in.String("group"))
					if err != nil {
						return nil, err
					}
					return groupdiag.Diagnose([]groupdiag.Snapshot{snap}), nil
				},
			})
		}
	}
	if t, ok := capability[broker.TopologyInspector](b, broker.CapTopologyInspector); ok {
		tools = append(tools,
			Tool{
				Name:        "list_exchanges",
				Description: "List the RabbitMQ exchanges with type and flags.",
				Run:         func(ctx context.Context, _ Input) (any, error) { return t.Exchanges(ctx) },
			},
			Tool{
				Name:        "list_bindings",
				Description: "List the RabbitMQ bindings (exchange to queue and exchange to exchange) with routing keys.",
				Run:         func(ctx context.Context, _ Input) (any, error) { return t.Bindings(ctx) },
			})
	}
	if r, ok := capability[broker.RouteSimulator](b, broker.CapRouteSimulator); ok {
		tools = append(tools, Tool{
			Name:        "route",
			Description: "Dry-run RabbitMQ routing: which queues a routing key reaches from an exchange, without publishing.",
			Params:      map[string]any{"exchange": str("exchange name"), "routing_key": str("routing key")},
			Required:    []string{"exchange", "routing_key"},
			Run: func(ctx context.Context, in Input) (any, error) {
				return r.Route(ctx, in.String("exchange"), in.String("routing_key"), nil)
			},
		})
	}
	if opts.SendPayloads {
		tools = append(tools, peekTool(b))
	}
	return tools
}

func peekTool(b broker.Broker) Tool {
	return Tool{
		Name: "peek_messages",
		Description: fmt.Sprintf("Read up to %d messages from the start of a topic or queue (RabbitMQ messages are requeued). "+
			"Values longer than %d bytes are cut.", maxPeek, maxValueLen),
		Params: map[string]any{
			"topic": map[string]any{"type": "string", "description": "topic or queue name"},
			"limit": map[string]any{"type": "integer", "description": fmt.Sprintf("1 to %d, default 5", maxPeek)},
		},
		Required: []string{"topic"},
		Run: func(ctx context.Context, in Input) (any, error) {
			limit := min(max(in.Int("limit", 5), 1), maxPeek)
			ch, err := b.Peek(ctx, in.String("topic"), broker.PeekOptions{Limit: limit})
			if err != nil {
				return nil, err
			}
			type peeked struct {
				Partition int32             `json:"partition"`
				Offset    int64             `json:"offset"`
				Key       string            `json:"key,omitempty"`
				Headers   map[string]string `json:"headers,omitempty"`
				Value     string            `json:"value"`
				Time      string            `json:"timestamp"`
			}
			var out []peeked
			for m := range ch {
				p := peeked{Partition: m.Partition, Offset: m.Offset, Key: text(m.Key), Value: text(m.Value),
					Time: m.Timestamp.UTC().Format("2006-01-02T15:04:05Z")}
				for _, h := range m.Headers {
					if p.Headers == nil {
						p.Headers = map[string]string{}
					}
					p.Headers[h.Key] = text(h.Value)
				}
				out = append(out, p)
			}
			return out, ctx.Err()
		},
	}
}

// text renders bytes for the model: UTF-8 cut to maxValueLen, or a note for binary.
func text(b []byte) string {
	if !utf8.Valid(b) {
		return fmt.Sprintf("<%d bytes of binary>", len(b))
	}
	if len(b) > maxValueLen {
		return strings.ToValidUTF8(string(b[:maxValueLen]), "") + "…(cut)"
	}
	return string(b)
}

// capability returns b as T when b reports the capability name.
func capability[T any](b broker.Broker, name string) (T, bool) {
	t, ok := b.(T)
	return t, ok && broker.Has(b, name)
}

// maxResult caps a tool result sent to the model.
const maxResult = 40_000

// encodeResult renders a tool result as JSON, cut to maxResult bytes.
func encodeResult(v any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false) // keep <, > and & readable in payloads
	if err := enc.Encode(v); err != nil {
		return fmt.Sprintf("cannot encode result: %v", err)
	}
	data := bytes.TrimSpace(buf.Bytes())
	if len(data) > maxResult {
		return string(data[:maxResult]) + "…(result cut; ask for something narrower)"
	}
	return string(data)
}
