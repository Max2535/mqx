//go:build integration

package rabbitmq_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Max2535/mqx/internal/broker"
)

func TestQueuesAndConfig(t *testing.T) {
	e := newEnv(t)
	ctx := testCtx(t)
	durableQueue(t, e, "classic", map[string]any{"x-max-length": 100})
	durableQueue(t, e, "quorum", map[string]any{"x-queue-type": "quorum"})
	must(t, e.b.PutPolicy(ctx, broker.Policy{Name: "ttl", Pattern: "^classic$", ApplyTo: "queues",
		Definition: map[string]any{"message-ttl": 60000}}))
	// Declaring again with identical properties is fine.
	durableQueue(t, e, "classic", map[string]any{"x-max-length": 100})
	if err := e.b.CreateTopic(ctx, broker.TopicSpec{Name: "classic", Durable: false}); err == nil {
		t.Error("redeclaring with different properties must fail")
	}

	ts, err := e.b.ListTopics(ctx)
	must(t, err)
	types := map[string]string{}
	for _, tp := range ts {
		types[tp.Name] = tp.Details["type"]
	}
	if types["classic"] != "classic" || types["quorum"] != "quorum" {
		t.Errorf("queue types = %v", types)
	}
	eventually(t, 15*time.Second, func() error {
		d, err := e.b.DescribeTopic(ctx, "classic")
		if err != nil {
			return err
		}
		have := map[string]string{}
		for _, c := range d.Configs {
			have[c.Source+":"+c.Name] = c.Value
		}
		if have["argument:x-max-length"] != "100" || have["policy:message-ttl"] != "60000" ||
			have["property:durable"] != "true" || have["property:policy"] != "ttl" {
			return fmt.Errorf("configs = %v", have)
		}
		return nil
	})
	cfg, err := e.b.TopicConfig(ctx, "quorum")
	must(t, err)
	if !slices.ContainsFunc(cfg, func(c broker.ConfigEntry) bool { return c.Name == "x-queue-type" && c.Value == "quorum" }) {
		t.Errorf("quorum config = %+v", cfg)
	}
	if err := e.b.AlterTopicConfig(ctx, "classic", map[string]string{"x": "1"}); !errors.Is(err, broker.ErrUnsupported) {
		t.Errorf("AlterTopicConfig = %v", err)
	}
	must(t, e.b.DeleteTopic(ctx, "quorum"))
	if _, err := e.b.DescribeTopic(ctx, "quorum"); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("describe deleted queue = %v", err)
	}
	if err := e.b.DeleteTopic(ctx, "quorum"); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("delete missing queue = %v", err)
	}

	nodes, err := e.b.Nodes(ctx)
	must(t, err)
	if len(nodes) != 1 || !nodes[0].Running || nodes[0].Host == "" {
		t.Fatalf("nodes = %+v", nodes)
	}
	nc, err := e.b.NodeConfig(ctx, nodes[0].ID)
	must(t, err)
	if len(nc) == 0 {
		t.Error("node config is empty")
	}
	vs, err := e.b.VHosts(ctx)
	must(t, err)
	if !slices.ContainsFunc(vs, func(v broker.VHost) bool { return v.Name == e.vhost }) {
		t.Errorf("vhosts %+v lack %q", vs, e.vhost)
	}
}

func TestConsumersAndConnections(t *testing.T) {
	e := newEnv(t)
	ctx := testCtx(t)
	durableQueue(t, e, "work", nil)
	conn := e.dial(t, "it-consumer")
	closed := conn.NotifyClose(make(chan *amqp.Error, 1))
	ch, err := conn.Channel()
	must(t, err)
	must(t, ch.Qos(7, 0, false))
	_, err = ch.Consume("work", "tag-1", false, false, false, false, nil)
	must(t, err)

	var cs []broker.Consumer
	eventually(t, 15*time.Second, func() error { // the consumer list comes from management stats
		var err error
		if cs, err = e.b.Consumers(ctx, "work"); err != nil {
			return err
		}
		if len(cs) != 1 || cs[0].Tag != "tag-1" || cs[0].Prefetch != 7 || !cs[0].AckRequired || !cs[0].Active ||
			cs[0].Connection == "" || cs[0].User != srv.Username {
			return fmt.Errorf("consumers = %+v", cs)
		}
		return nil
	})
	if _, err := e.b.Consumers(ctx, "missing"); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("consumers of a missing queue = %v", err)
	}

	var name string
	eventually(t, 15*time.Second, func() error {
		cn, err := e.b.Connections(ctx)
		if err != nil {
			return err
		}
		for _, c := range cn {
			if c.ClientProps["connection_name"] == "it-consumer" {
				if c.VHost != e.vhost || c.User != srv.Username || c.Channels != 1 || c.ConnectedAt.IsZero() {
					return fmt.Errorf("connection = %+v", c)
				}
				name = c.Name
				return nil
			}
		}
		return fmt.Errorf("connection not listed in %+v", cn)
	})
	if name != cs[0].Connection {
		t.Errorf("consumer connection %q != listed %q", cs[0].Connection, name)
	}
	eventually(t, 15*time.Second, func() error {
		chs, err := e.b.Channels(ctx)
		if err != nil {
			return err
		}
		for _, c := range chs {
			if c.Connection == name && c.Consumers == 1 && c.Prefetch == 7 {
				return nil
			}
		}
		return fmt.Errorf("channels = %+v", chs)
	})

	must(t, e.b.TerminateConsumer(ctx, broker.ConsumerTarget{Connection: name, Reason: "mqx integration test"}))
	select {
	case err := <-closed:
		if err == nil || !strings.Contains(err.Reason, "mqx integration test") {
			t.Errorf("close reason = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("connection was not closed")
	}
	if err := e.b.TerminateConsumer(ctx, broker.ConsumerTarget{Connection: "nope"}); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("terminate missing connection = %v", err)
	}
}

func TestTopologyEditing(t *testing.T) {
	e := newEnv(t)
	ctx := testCtx(t)
	durableQueue(t, e, "q1", nil)
	must(t, e.b.DeclareExchange(ctx, broker.Exchange{Name: "src", Type: "topic", Durable: true}))
	must(t, e.b.DeclareExchange(ctx, broker.Exchange{Name: "dst", Type: "fanout", AutoDelete: false, Internal: true,
		Arguments: map[string]any{"alternate-exchange": "src"}}))
	bindings := []broker.Binding{
		{Source: "src", Destination: "q1", DestinationType: "queue", RoutingKey: "a.#"},
		{Source: "src", Destination: "q1", DestinationType: "queue", RoutingKey: ""},
		{Source: "src", Destination: "q1", DestinationType: "queue", RoutingKey: "k", Arguments: map[string]any{"x-n": 1}},
		{Source: "src", Destination: "q1", DestinationType: "queue", RoutingKey: "k", Arguments: map[string]any{"x-n": 2}},
		{Source: "src", Destination: "dst", DestinationType: "exchange", RoutingKey: "b.*"},
	}
	for _, b := range bindings {
		must(t, e.b.Bind(ctx, b))
	}

	es, err := e.b.Exchanges(ctx)
	must(t, err)
	byName := map[string]broker.Exchange{}
	for _, x := range es {
		byName[x.Name] = x
	}
	if x := byName["src"]; x.Type != "topic" || !x.Durable {
		t.Errorf("src = %+v", x)
	}
	if x := byName["dst"]; x.Type != "fanout" || !x.Internal || x.Arguments["alternate-exchange"] != "src" {
		t.Errorf("dst = %+v", x)
	}
	if _, ok := byName[""]; !ok {
		t.Error("default exchange missing")
	}

	countFrom := func(src string) int {
		bs, err := e.b.Bindings(ctx)
		must(t, err)
		n := 0
		for _, b := range bs {
			if b.Source == src {
				if b.PropertiesKey == "" {
					t.Errorf("binding without properties key: %+v", b)
				}
				n++
			}
		}
		return n
	}
	if n := countFrom("src"); n != len(bindings) {
		t.Fatalf("bindings from src = %d, want %d", n, len(bindings))
	}
	if n := countFrom(""); n != 1 {
		t.Errorf("default exchange bindings = %d, want 1", n)
	}

	if err := e.b.Unbind(ctx, broker.Binding{Source: "src", Destination: "q1", RoutingKey: "k"}); err == nil {
		t.Error("ambiguous unbind must fail")
	}
	for _, b := range []broker.Binding{
		{Source: "src", Destination: "q1", RoutingKey: "a.#"},
		{Source: "src", Destination: "q1", RoutingKey: ""},
		{Source: "src", Destination: "q1", RoutingKey: "k", Arguments: map[string]any{"x-n": 2}},
		{Source: "src", Destination: "dst", DestinationType: "exchange", RoutingKey: "b.*"},
	} {
		must(t, e.b.Unbind(ctx, b))
	}
	bs, err := e.b.Bindings(ctx)
	must(t, err)
	var left []broker.Binding
	for _, b := range bs {
		if b.Source == "src" {
			left = append(left, b)
		}
	}
	if len(left) != 1 || left[0].RoutingKey != "k" || fmt.Sprint(left[0].Arguments["x-n"]) != "1" {
		t.Errorf("remaining bindings = %+v", left)
	}
	must(t, e.b.Unbind(ctx, left[0]))
	if err := e.b.Unbind(ctx, broker.Binding{Source: "src", Destination: "q1", RoutingKey: "zz"}); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("unbind missing = %v", err)
	}

	must(t, e.b.DeleteExchange(ctx, "dst"))
	must(t, e.b.DeleteExchange(ctx, "src"))
	if err := e.b.DeleteExchange(ctx, "src"); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("delete missing exchange = %v", err)
	}
}
