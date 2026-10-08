package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/Max2535/mqx/internal/broker"
)

// queueJSON is the subset of a management API queue object mqx uses.
type queueJSON struct {
	Name                   string         `json:"name"`
	VHost                  string         `json:"vhost"`
	Type                   string         `json:"type"`
	State                  string         `json:"state"`
	Durable                bool           `json:"durable"`
	AutoDelete             bool           `json:"auto_delete"`
	Exclusive              bool           `json:"exclusive"`
	Arguments              map[string]any `json:"arguments"`
	Policy                 string         `json:"policy"`
	OperatorPolicy         string         `json:"operator_policy"`
	EffectivePolicy        map[string]any `json:"effective_policy_definition"`
	Node                   string         `json:"node"`
	Messages               *int64         `json:"messages"`
	MessagesReady          *int64         `json:"messages_ready"`
	MessagesUnacknowledged *int64         `json:"messages_unacknowledged"`
	Consumers              *int           `json:"consumers"`
	MessageStats           messageStats   `json:"message_stats"`
}

type messageStats struct {
	Publish    float64 `json:"publish"`
	DeliverGet float64 `json:"deliver_get"`
	Ack        float64 `json:"ack"`
	Redeliver  float64 `json:"redeliver"`
}

var queueColumns = strings.Join([]string{
	"name", "vhost", "type", "state", "durable", "auto_delete", "exclusive", "arguments", "policy",
	"operator_policy", "effective_policy_definition", "node", "messages", "messages_ready",
	"messages_unacknowledged", "consumers",
}, ",")

func (q queueJSON) topic() broker.Topic {
	t := broker.Topic{Name: q.Name, Kind: "queue", Messages: -1, Consumers: -1, Details: map[string]string{}}
	switch {
	case q.Messages != nil:
		t.Messages = *q.Messages
	case q.MessagesReady != nil && q.MessagesUnacknowledged != nil:
		t.Messages = *q.MessagesReady + *q.MessagesUnacknowledged
	}
	if q.Consumers != nil {
		t.Consumers = *q.Consumers
	}
	for k, v := range map[string]string{
		"type": q.queueType(), "state": q.State, "durable": strconv.FormatBool(q.Durable),
		"policy": q.Policy, "node": q.Node,
	} {
		if v != "" {
			t.Details[k] = v
		}
	}
	if q.AutoDelete {
		t.Details["auto_delete"] = "true"
	}
	if q.Exclusive {
		t.Details["exclusive"] = "true"
	}
	return t
}

// queueType is classic, quorum or stream; older brokers omit "type".
func (q queueJSON) queueType() string {
	if q.Type != "" {
		return q.Type
	}
	if t, ok := q.Arguments["x-queue-type"].(string); ok {
		return t
	}
	return "classic"
}

// ListTopics lists the queues of the vhost.
func (r *RabbitMQ) ListTopics(ctx context.Context) ([]broker.Topic, error) {
	var qs []queueJSON
	path := withQuery(apiPath("queues", r.settings.vhost), url.Values{"columns": {queueColumns}})
	if err := r.mgmt.get(ctx, path, &qs); err != nil {
		return nil, fmt.Errorf("list queues: %w", err)
	}
	out := make([]broker.Topic, len(qs))
	for i, q := range qs {
		out[i] = q.topic()
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (r *RabbitMQ) queue(ctx context.Context, name string) (queueJSON, error) {
	var q queueJSON
	if err := r.mgmt.get(ctx, apiPath("queues", r.settings.vhost, name), &q); err != nil {
		return q, fmt.Errorf("queue %q in vhost %q: %w", name, r.settings.vhost, err)
	}
	return q, nil
}

// DescribeTopic implements broker.TopicDescriber.
func (r *RabbitMQ) DescribeTopic(ctx context.Context, name string) (*broker.TopicDetail, error) {
	q, err := r.queue(ctx, name)
	if err != nil {
		return nil, err
	}
	return &broker.TopicDetail{Topic: q.topic(), Configs: q.configEntries()}, nil
}

// configEntries lists arguments, the effective policy and read-only properties.
func (q queueJSON) configEntries() []broker.ConfigEntry {
	var out []broker.ConfigEntry
	for _, k := range sortedKeys(q.Arguments) {
		out = append(out, broker.ConfigEntry{Name: k, Value: stringify(q.Arguments[k]), Source: "argument", ReadOnly: true})
	}
	for _, k := range sortedKeys(q.EffectivePolicy) {
		out = append(out, broker.ConfigEntry{Name: k, Value: stringify(q.EffectivePolicy[k]), Source: "policy"})
	}
	props := []struct{ k, v string }{
		{"type", q.queueType()},
		{"durable", strconv.FormatBool(q.Durable)},
		{"auto_delete", strconv.FormatBool(q.AutoDelete)},
		{"exclusive", strconv.FormatBool(q.Exclusive)},
		{"policy", q.Policy},
		{"operator_policy", q.OperatorPolicy},
		{"node", q.Node},
		{"state", q.State},
	}
	for _, p := range props {
		if p.v != "" {
			out = append(out, broker.ConfigEntry{Name: p.k, Value: p.v, Source: "property", ReadOnly: true})
		}
	}
	return out
}

// Nodes implements broker.ClusterInspector.
func (r *RabbitMQ) Nodes(ctx context.Context) ([]broker.Node, error) {
	var ns []map[string]any
	path := withQuery(apiPath("nodes"), url.Values{"columns": {
		"name,running,type,uptime,mem_used,mem_limit,disk_free,disk_free_limit,fd_used,fd_total,proc_used,proc_total,partitions",
	}})
	if err := r.mgmt.get(ctx, path, &ns); err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	out := make([]broker.Node, 0, len(ns))
	for _, n := range ns {
		name, _ := n["name"].(string)
		running, _ := n["running"].(bool)
		node := broker.Node{ID: name, Host: hostOfNode(name), Running: running, Details: map[string]string{}}
		for k, v := range n {
			if k != "name" && k != "running" && v != nil {
				node.Details[k] = stringify(v)
			}
		}
		out = append(out, node)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// hostOfNode returns "host" for a node named "rabbit@host".
func hostOfNode(name string) string {
	if _, host, ok := strings.Cut(name, "@"); ok {
		return host
	}
	return name
}

// NodeConfig returns a node's scalar properties (versions, limits, config files, ...).
func (r *RabbitMQ) NodeConfig(ctx context.Context, id string) ([]broker.ConfigEntry, error) {
	var n map[string]any
	if err := r.mgmt.get(ctx, apiPath("nodes", id), &n); err != nil {
		return nil, fmt.Errorf("node %q: %w", id, err)
	}
	var out []broker.ConfigEntry
	for _, k := range sortedKeys(n) {
		switch v := n[k].(type) {
		case string, float64, bool:
			out = append(out, broker.ConfigEntry{Name: k, Value: stringify(v), Source: "node", ReadOnly: true})
		case []any:
			if allScalars(v) {
				out = append(out, broker.ConfigEntry{Name: k, Value: stringify(v), Source: "node", ReadOnly: true})
			}
		}
	}
	return out, nil
}

func allScalars(xs []any) bool {
	for _, x := range xs {
		switch x.(type) {
		case string, float64, bool:
		default:
			return false
		}
	}
	return true
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// stringify renders a decoded JSON value: strings as-is, whole numbers without
// a decimal point, lists of scalars comma-joined, everything else as JSON.
func stringify(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	case []any:
		if allScalars(x) {
			parts := make([]string, len(x))
			for i, e := range x {
				parts[i] = stringify(e)
			}
			return strings.Join(parts, ",")
		}
	}
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(data)
}
