//go:build integration

package rabbitmq_test

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Max2535/mqx/internal/broker"
)

// topicPatterns are bound from r.topic to queues t.0, t.1, ...
var topicPatterns = []string{
	"*", "#", "a.*", "a.#", "#.a", "a.#.b", "*.b.*", "", "a.", "*.*", "a#", "#.*", ".a", "a.*.c", "#.b.#", "a..b", "#.#",
}

// routeTopology builds direct, topic, fanout, headers and internal exchanges,
// exchange-to-exchange bindings with cycles, and alternate exchanges set by
// argument and by policy.
func routeTopology(t *testing.T, e *env) {
	t.Helper()
	ctx := testCtx(t)
	ex := func(name, typ string, internal bool, args map[string]any) {
		must(t, e.b.DeclareExchange(ctx, broker.Exchange{Name: name, Type: typ, Durable: true, Internal: internal, Arguments: args}))
	}
	bindQ := func(src, q, key string, args map[string]any) {
		must(t, e.b.Bind(ctx, broker.Binding{Source: src, Destination: q, DestinationType: "queue", RoutingKey: key, Arguments: args}))
	}
	bindE := func(src, dst, key string) {
		must(t, e.b.Bind(ctx, broker.Binding{Source: src, Destination: dst, DestinationType: "exchange", RoutingKey: key}))
	}
	queues := []string{"q.d.a", "q.d.b", "q.d.ab", "q.f1", "q.f2", "q.ae", "q.h", "q.int"}
	for i := range topicPatterns {
		queues = append(queues, fmt.Sprintf("t.%d", i))
	}
	for _, q := range queues {
		durableQueue(t, e, q, nil)
	}
	ex("r.ae", "fanout", false, nil)
	bindQ("r.ae", "q.ae", "", nil)
	ex("r.direct", "direct", false, map[string]any{"alternate-exchange": "r.ae"})
	ex("r.topic", "topic", false, nil)
	ex("r.fanout", "fanout", false, nil)
	ex("r.headers", "headers", false, nil)
	ex("r.internal", "fanout", true, nil)
	ex("r.ae-src", "direct", false, map[string]any{"alternate-exchange": "r.ae"})
	ex("r.pol-src", "direct", false, nil)
	ex("r.missing-ae", "direct", false, map[string]any{"alternate-exchange": "r.does-not-exist"})

	bindQ("r.direct", "q.d.a", "a", nil)
	bindQ("r.direct", "q.d.b", "b", nil)
	bindQ("r.direct", "q.d.ab", "a", nil)
	bindQ("r.direct", "q.d.ab", "b", nil)
	for i, p := range topicPatterns {
		bindQ("r.topic", fmt.Sprintf("t.%d", i), p, nil)
	}
	bindQ("r.fanout", "q.f1", "ignored", nil)
	bindQ("r.fanout", "q.f2", "", nil)
	bindQ("r.headers", "q.h", "", map[string]any{"x-match": "all", "h": "1"})
	bindQ("r.internal", "q.int", "", nil)
	bindQ("r.ae-src", "q.d.a", "hit", nil)

	bindE("r.direct", "r.topic", "a")       // direct -> topic
	bindE("r.direct", "r.fanout", "f")      // direct -> fanout
	bindE("r.direct", "r.headers", "h")     // direct -> headers (not simulated)
	bindE("r.topic", "r.fanout", "x.#")     // topic -> fanout
	bindE("r.fanout", "r.topic", "")        // cycle: topic -> fanout -> topic
	bindE("r.fanout", "r.direct", "zzz")    // cycle: direct -> fanout -> direct, original key kept
	bindE("r.topic", "r.internal", "int.#") // internal exchanges are reachable through bindings

	must(t, e.b.PutPolicy(ctx, broker.Policy{Name: "ae-pol", Pattern: `^r\.pol-src$`, ApplyTo: "exchanges",
		Definition: map[string]any{"alternate-exchange": "r.ae"}}))
	eventually(t, 15*time.Second, func() error { // wait until the policy shows up on the exchange
		res, err := e.b.Route(ctx, "r.pol-src", "x", nil)
		if err != nil {
			return err
		}
		if len(res.Alternate) == 0 {
			return errNotYet
		}
		return nil
	})
}

func TestRouteMatchesBroker(t *testing.T) {
	e := newEnv(t)
	ctx := testCtx(t)
	routeTopology(t, e)

	ts, err := e.b.ListTopics(ctx)
	must(t, err)
	var queues []string
	for _, tp := range ts {
		queues = append(queues, tp.Name)
	}
	conn := e.dial(t, "route-observer")
	ch, err := conn.Channel()
	must(t, err)
	defer ch.Close()

	topicKeys := []string{
		"", "a", "b", "x", "a.b", "a.b.c", "x.a", "a.", ".a", ".", "..", "a.x.c", "x.b.y", "x.b", "a#", "a.*",
		"x.y", "int.z", "a..b", "a.b.b", "a.x.y.b", "b.a", "x.y.a",
	}
	cases := map[string][]string{
		"r.direct":     {"a", "b", "f", "h", "x.y", "", "A", "zzz", "a.b"},
		"r.topic":      topicKeys,
		"r.fanout":     {"anything", "", "a", "x.a"},
		"r.ae-src":     {"hit", "miss"},
		"r.pol-src":    {"x"},
		"r.missing-ae": {"x"},
		"":             {"q.d.a", "t.3", "nope", ""},
		"r.headers":    {"k"},
	}
	checked := 0
	for _, exName := range sortedNames(cases) {
		for _, key := range cases[exName] {
			name := fmt.Sprintf("%s/%q", exName, key)
			sim, err := e.b.Route(ctx, exName, key, nil)
			if err != nil {
				t.Fatalf("%s: Route: %v", name, err)
			}

			m := broker.NewMessage([]byte(name))
			m.Headers = []broker.Header{{Key: "h", Value: []byte("1")}} // matches the headers binding
			m.Exchange, m.RoutingKey = exName, key
			topic := ""
			if exName == "" {
				topic = key // the default exchange routes to the queue named by the key
			}
			err = e.b.Publish(ctx, topic, m)
			if err != nil && !strings.Contains(err.Error(), "unroutable") {
				t.Fatalf("%s: publish: %v", name, err)
			}

			var got []string
			for _, q := range queues {
				info, err := ch.QueueDeclarePassive(q, false, false, false, false, nil)
				must(t, err)
				if info.Messages > 0 {
					got = append(got, q)
					if info.Messages > 1 {
						t.Errorf("%s: queue %s got %d copies; RabbitMQ delivers once per queue", name, q, info.Messages)
					}
					_, err := ch.QueuePurge(q, false)
					must(t, err)
				}
			}
			if (err != nil) != (len(got) == 0) {
				t.Errorf("%s: publish error %v but queues %v", name, err, got)
			}
			// Queues behind the headers exchange are not simulated.
			reality := slices.DeleteFunc(slices.Clone(got), func(q string) bool { return q == "q.h" })
			if reality == nil {
				reality = []string{}
			}
			if !slices.Equal(sim.Queues, reality) {
				t.Errorf("%s: simulated %v, broker routed to %v (hops %+v)", name, sim.Queues, got, sim.Hops)
			}
			if slices.Contains(got, "q.h") && !slices.Contains(sim.NotSimulated, "r.headers") {
				t.Errorf("%s: reached the headers exchange but NotSimulated = %v", name, sim.NotSimulated)
			}
			checked++
		}
	}
	t.Logf("compared %d routing keys with the broker", checked)

	res, err := e.b.Route(ctx, "r.headers", "k", nil)
	must(t, err)
	if !slices.Equal(res.NotSimulated, []string{"r.headers"}) || len(res.Queues) != 0 {
		t.Errorf("headers exchange: %+v", res)
	}
	if _, err := e.b.Route(ctx, "r.internal", "", nil); err == nil || !strings.Contains(err.Error(), "internal") {
		t.Errorf("route from an internal exchange = %v", err)
	}
	m := broker.NewMessage([]byte("x"))
	m.Exchange = "r.internal"
	if err := e.b.Publish(ctx, "", m); err == nil {
		t.Error("the broker accepted a publish to an internal exchange")
	}
}

func sortedNames(m map[string][]string) []string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}
