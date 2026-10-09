package tui

import (
	"testing"

	"github.com/Max2535/mqx/internal/broker"
)

func TestCreateTopicHintsAndReplicationCheck(t *testing.T) {
	f, c := fakeCtx(t, "dev", false, kafkaCaps)
	seedKafka(f)
	f.SetNodes(broker.Node{ID: "1", Host: "localhost", Running: true})
	h := newHarness(t, Options{}, c)

	h.keys("n")
	h.wantView("New topic")
	h.typeText("orders")
	h.wantView("topic orders already exists")
	h.keys("backspace", "backspace", "backspace", "backspace", "backspace", "backspace")
	h.typeText("app.events_v2")
	h.wantNotView("already exists")
	h.wantView("names mixing '.' and '_'")

	focusField(h, "replication")
	h.typeText("3")
	h.wantView("more than the cluster's broker count (1)")
	h.keys("ctrl+s")
	h.wantView("replication factor 3 is more than the cluster's broker count (1)")
	wantCalls(t, f)

	h.keys("backspace")
	h.typeText("1")
	h.wantNotView("broker count")
	focusField(h, "configs")
	h.typeText("min.insync.replicas=2")
	h.wantView("min.insync.replicas=2 is more than the replication factor 1")
	h.keys("ctrl+s")
	h.wantView("Confirm", "• min.insync.replicas=2 is more than the replication factor 1")
}

func TestReplicationFactorOneHintNeedsSeveralBrokers(t *testing.T) {
	e := &env{nodes: 3, b: &kafkaOnly{}}
	if got := kafkaTopicHints(e, values{"name": "a", "replication": "1"}); len(got) != 1 {
		t.Fatalf("hints with 3 brokers = %q, want the single-copy hint", got)
	}
	e.nodes = 1
	if got := kafkaTopicHints(e, values{"name": "a", "replication": "1"}); len(got) != 0 {
		t.Fatalf("hints with 1 broker = %q, want none", got)
	}
}

// kafkaOnly is enough of a broker for env.kafkaLike.
type kafkaOnly struct{ broker.Broker }

func (kafkaOnly) Name() string { return "kafka" }

func TestAddPartitionsHintsAndCheck(t *testing.T) {
	f, c := fakeCtx(t, "dev", false, kafkaCaps)
	seedKafka(f) // orders has 2 partitions
	h := newHarness(t, Options{}, c)

	h.keys(":")
	h.typeText("orders")
	h.keys("enter", "a")
	h.wantView("Add partitions to orders")
	h.typeText("2")
	h.wantView("orders has 2 partitions; Kafka can only add")
	h.keys("ctrl+s")
	h.wantView("orders already has 2 partitions; enter a larger count")
	wantCalls(t, f)

	h.keys("backspace")
	h.typeText("4")
	h.wantView("per-key order is not kept")
	h.keys("ctrl+s")
	h.wantView("Confirm", "going from 2 to 4 partitions")
}

func TestQueueHints(t *testing.T) {
	cases := []struct {
		name string
		v    values
		want []string
	}{
		{"classic durable", values{}, nil},
		{"classic transient", values{"durable": "n"}, []string{"a non-durable queue is gone after a broker restart"}},
		{"quorum auto-delete", values{"auto_delete": "y", "arguments": "x-queue-type=quorum"},
			[]string{"quorum queues cannot be auto-delete; RabbitMQ refuses it"}},
		{"stream transient", values{"durable": "n", "arguments": "x-queue-type=stream"},
			[]string{"stream queues are always durable; RabbitMQ refuses durable=no"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := queueHints(tc.v)
			if len(got) != len(tc.want) || (len(got) > 0 && got[0] != tc.want[0]) {
				t.Fatalf("queueHints(%v) = %q, want %q", tc.v, got, tc.want)
			}
		})
	}
}
