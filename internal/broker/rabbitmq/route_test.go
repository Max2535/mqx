package rabbitmq

import (
	"errors"
	"reflect"
	"testing"

	"github.com/Max2535/mqx/internal/broker"
)

func TestTopicMatch(t *testing.T) {
	tests := []struct {
		binding, key string
		want         bool
	}{
		// exact words
		{"a.b.c", "a.b.c", true},
		{"a.b.c", "a.b", false},
		{"a.b", "a.b.c", false},
		{"a.b.c", "a.x.c", false},
		// *
		{"*", "a", true},
		{"*", "", false},
		{"*", "a.b", false},
		{"*", ".", false},
		{"*.*", ".", true},
		{"a.*", "a.b", true},
		{"a.*", "a", false},
		{"a.*", "a.", true}, // "*" matches the empty word
		{"a.*.c", "a..c", true},
		{"*.b.*", "a.b.c", true},
		{"*.b.*", "a.b", false},
		// #
		{"#", "", true},
		{"#", "a", true},
		{"#", "a.b.c", true},
		{"#", ".", true},
		{"a.#", "a", true},
		{"a.#", "a.b.c", true},
		{"a.#", "b.a", false},
		{"#.a", "a", true},
		{"#.a", "x.y.a", true},
		{"#.a", "a.x", false},
		{"a.#.b", "a.b", true},
		{"a.#.b", "a.x.b", true},
		{"a.#.b", "a.x.y.b", true},
		{"a.#.b", "a.x.y", false},
		{"#.#", "", true},
		{"#.#", "a.b", true},
		{"#.*", "", false},
		{"#.*", "a", true},
		{"*.#", "a", true},
		{"#.*.#", "a.b.c", true},
		{"a.#.#.b", "a.b", true},
		{"#.b.#", "a.b.c", true},
		{"#.b.#", "a.c", false},
		// empty keys and empty words
		{"", "", true},
		{"", "a", false},
		{"a", "", false},
		{"a.", "a.", true},
		{"a.", "a", false},
		{".a", ".a", true},
		{"..", "..", true},
		{"*.*.*", "..", true},
		// wildcards only as whole words
		{"a#", "a#", true},
		{"a#", "ab", false},
		{"*b", "ab", false},
		{"*b", "*b", true},
		{"a.*", "a.*", true},
	}
	for _, tt := range tests {
		if got := topicMatch(tt.binding, tt.key); got != tt.want {
			t.Errorf("topicMatch(%q, %q) = %v, want %v", tt.binding, tt.key, got, tt.want)
		}
	}
}

func q(src, dst, key string) bindingJSON {
	return bindingJSON{Source: src, Destination: dst, DestinationType: "queue", RoutingKey: key}
}

func e2e(src, dst, key string) bindingJSON {
	return bindingJSON{Source: src, Destination: dst, DestinationType: "exchange", RoutingKey: key}
}

func testGraph() routeGraph {
	es := []exchangeJSON{
		{Name: "", Type: "direct"},
		{Name: "direct", Type: "direct"},
		{Name: "topic", Type: "topic"},
		{Name: "fan", Type: "fanout"},
		{Name: "hdr", Type: "headers"},
		{Name: "hash", Type: "x-consistent-hash"},
		{Name: "cyc1", Type: "fanout"},
		{Name: "cyc2", Type: "fanout"},
		{Name: "with-ae", Type: "direct", Arguments: map[string]any{"alternate-exchange": "ae"}},
		{Name: "policy-ae", Type: "direct", Policy: "ae-pol"},
		{Name: "both-ae", Type: "direct", Policy: "ae-pol", Arguments: map[string]any{"alternate-exchange": "ae2"}},
		{Name: "missing-ae", Type: "direct", Arguments: map[string]any{"alternate-exchange": "nope"}},
		{Name: "ae", Type: "fanout"},
		{Name: "ae2", Type: "fanout"},
		{Name: "internal", Type: "fanout", Internal: true},
		{Name: "to-internal", Type: "fanout"},
	}
	bs := []bindingJSON{
		q("", "q1", "q1"), q("", "q2", "q2"), q("", "q3", "q3"),
		q("direct", "q1", "k"), q("direct", "q2", "k"), q("direct", "q3", "other"),
		q("topic", "q1", "a.*"), q("topic", "q2", "a.#"), q("topic", "q1", "#"),
		e2e("direct", "fan", "k"), q("fan", "q3", "ignored"),
		e2e("direct", "hdr", "k"),
		e2e("cyc1", "cyc2", ""), e2e("cyc2", "cyc1", ""), q("cyc2", "q1", ""), e2e("cyc2", "cyc2", ""),
		q("ae", "q-ae", ""), q("ae2", "q-ae2", ""), q("with-ae", "q1", "hit"),
		e2e("to-internal", "internal", ""), q("internal", "q-int", ""),
	}
	pol := map[string]map[string]any{"ae-pol": {"alternate-exchange": "ae"}}
	return newRouteGraph(es, bs, pol)
}

func TestSimulate(t *testing.T) {
	tests := []struct {
		name, exchange, key string
		queues              []string
		notSimulated        []string
		alternate           []string
		wantErr             error
	}{
		{name: "default exchange", exchange: "", key: "q2", queues: []string{"q2"}},
		{name: "default exchange miss", exchange: "", key: "nope", queues: []string{}},
		{name: "direct plus e2e fanout and headers", exchange: "direct", key: "k",
			queues: []string{"q1", "q2", "q3"}, notSimulated: []string{"hdr"}},
		{name: "direct other", exchange: "direct", key: "other", queues: []string{"q3"}},
		{name: "direct miss", exchange: "direct", key: "K", queues: []string{}},
		{name: "topic dedupes queues", exchange: "topic", key: "a.b", queues: []string{"q1", "q2"}},
		{name: "topic hash only", exchange: "topic", key: "x", queues: []string{"q1"}},
		{name: "fanout", exchange: "fan", key: "anything", queues: []string{"q3"}},
		{name: "headers not simulated", exchange: "hdr", key: "k", queues: []string{}, notSimulated: []string{"hdr"}},
		{name: "plugin not simulated", exchange: "hash", key: "k", queues: []string{}, notSimulated: []string{"hash"}},
		{name: "cycle", exchange: "cyc1", key: "", queues: []string{"q1"}},
		{name: "alternate argument", exchange: "with-ae", key: "miss", queues: []string{"q-ae"}, alternate: []string{"ae"}},
		{name: "alternate not used on match", exchange: "with-ae", key: "hit", queues: []string{"q1"}},
		{name: "alternate from policy", exchange: "policy-ae", key: "x", queues: []string{"q-ae"}, alternate: []string{"ae"}},
		{name: "argument beats policy", exchange: "both-ae", key: "x", queues: []string{"q-ae2"}, alternate: []string{"ae2"}},
		{name: "missing alternate ignored", exchange: "missing-ae", key: "x", queues: []string{}},
		{name: "internal reached via e2e", exchange: "to-internal", key: "", queues: []string{"q-int"}},
		{name: "internal start refused", exchange: "internal", key: "", wantErr: errors.New("internal")},
		{name: "unknown exchange", exchange: "zzz", key: "", wantErr: broker.ErrNotFound},
	}
	g := testGraph()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := simulate(g, tt.exchange, tt.key)
			if tt.wantErr != nil {
				if err == nil {
					t.Fatalf("want error %v", tt.wantErr)
				}
				if errors.Is(tt.wantErr, broker.ErrNotFound) && !errors.Is(err, broker.ErrNotFound) {
					t.Fatalf("error %v is not ErrNotFound", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(res.Queues, tt.queues) {
				t.Errorf("Queues = %v, want %v", res.Queues, tt.queues)
			}
			if !reflect.DeepEqual(res.NotSimulated, tt.notSimulated) {
				t.Errorf("NotSimulated = %v, want %v", res.NotSimulated, tt.notSimulated)
			}
			if !reflect.DeepEqual(res.Alternate, tt.alternate) {
				t.Errorf("Alternate = %v, want %v", res.Alternate, tt.alternate)
			}
		})
	}
}

func TestSimulateHops(t *testing.T) {
	res, err := simulate(testGraph(), "direct", "k")
	if err != nil {
		t.Fatal(err)
	}
	depth := map[string]int{}
	for _, h := range res.Hops {
		depth[h.Exchange+">"+h.Destination] = h.Depth
	}
	want := map[string]int{"direct>q1": 0, "direct>q2": 0, "direct>fan": 0, "direct>hdr": 0, "fan>q3": 1}
	if !reflect.DeepEqual(depth, want) {
		t.Errorf("hops = %v, want %v", depth, want)
	}
	res, err = simulate(testGraph(), "with-ae", "miss")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Hops) != 2 || res.Hops[0].BindingKey != AlternateHopKey || res.Hops[1].Depth != 1 {
		t.Errorf("alternate hops = %+v", res.Hops)
	}
}
