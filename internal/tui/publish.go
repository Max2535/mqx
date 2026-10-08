package tui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/charmbracelet/bubbles/key"

	"github.com/Max2535/mqx/internal/broker"
)

var errInvalidJSON = errors.New("payload is not valid JSON")

// publishFields returns the publish form's inputs. Kafka-like brokers get a
// partition; RabbitMQ-like ones get exchange, routing key and properties.
func publishFields(e *env) []field {
	fields := []field{{key: "key", label: "Key", hint: "optional"}}
	if e.rabbitLike() {
		fields = append(fields,
			field{key: "exchange", label: "Exchange", hint: "empty = default exchange (publish to the queue)"},
			field{key: "routing_key", label: "Routing key", hint: "empty = the queue name"},
			field{key: "properties", label: "Properties", hint: "content_type=application/json, correlation_id=…"},
		)
	}
	if e.kafkaLike() {
		fields = append(fields, field{key: "partition", label: "Partition", hint: "empty = partitioner picks"})
	}
	return append(fields,
		field{key: "headers", label: "Headers (k=v per line)", kind: fieldArea},
		field{key: "json", label: "Validate JSON", hint: "y/N"},
		field{key: "payload", label: "Payload", kind: fieldArea},
	)
}

// buildMessage turns submitted publish form values into a broker.Message.
func buildMessage(v values) (broker.Message, error) {
	m := broker.NewMessage([]byte(v["payload"]))
	if k := v["key"]; k != "" {
		m.Key = []byte(k)
	}
	if p := strings.TrimSpace(v["partition"]); p != "" {
		n, err := strconv.ParseInt(p, 10, 32)
		if err != nil || n < 0 {
			return broker.Message{}, fmt.Errorf("invalid partition %q; want a non-negative integer or empty", p)
		}
		m.Partition = int32(n)
	}
	headers, err := parseKV(v["headers"])
	if err != nil {
		return broker.Message{}, fmt.Errorf("headers: %w", err)
	}
	for _, h := range headers {
		m.Headers = append(m.Headers, broker.Header{Key: h[0], Value: []byte(h[1])})
	}
	props, err := parseKVMap(v["properties"])
	if err != nil {
		return broker.Message{}, fmt.Errorf("properties: %w", err)
	}
	m.Properties = props
	m.Exchange, m.RoutingKey = v["exchange"], v["routing_key"]
	validate, err := parseYes(v["json"], false)
	if err != nil {
		return broker.Message{}, err
	}
	if validate && !json.Valid(m.Value) {
		return broker.Message{}, errInvalidJSON
	}
	return m, nil
}

// publishAction publishes to the topic named by topic(r). It is mutating:
// hidden on read_only contexts and confirmed before sending.
func publishAction(e *env, topic func(r *row) string, needsRow bool) action {
	return action{
		key:      key.NewBinding(key.WithKeys("p"), key.WithHelp("p", "publish")),
		mutating: true,
		needsRow: needsRow,
		form: func(r *row) (string, []field) {
			return "Publish to " + e.kind() + " " + topic(r), publishFields(e)
		},
		check: func(_ *row, v values) error {
			_, err := buildMessage(v)
			return err
		},
		describe: func(r *row, v values) string {
			what := fmt.Sprintf("Publish %d bytes to %s %s", len(v["payload"]), e.kind(), topic(r))
			if ex := v["exchange"]; ex != "" {
				what = fmt.Sprintf("Publish %d bytes via exchange %s (key %q)", len(v["payload"]), ex, v["routing_key"])
			}
			return what
		},
		run: func(ctx context.Context, r *row, v values) (string, error) {
			m, err := buildMessage(v)
			if err != nil {
				return "", err
			}
			if err := e.b.Publish(ctx, topic(r), m); err != nil {
				return "", err
			}
			return "published", nil
		},
	}
}
