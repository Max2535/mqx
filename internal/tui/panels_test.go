package tui

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Max2535/mqx/internal/broker"
)

func TestGroupResetOffsetsPreviewThenConfirm(t *testing.T) {
	f, c := fakeCtx(t, "dev", false, kafkaCaps)
	seedKafka(f)
	h := newHarness(t, Options{Group: "archiver"}, c)
	h.wantView("Groups › archiver", "Lag total 0")

	h.keys("o")
	h.wantView("Reset offsets of archiver", "Topic", "To")
	h.keys("ctrl+s") // topic is prefilled with the selected topic

	// The dry run is shown in the confirm dialog before anything changes.
	h.wantView("Reset offsets of group archiver on topic orders to earliest on dev?", "Dry run:", "orders[0]  2 → 0", "orders[1]  1 → 0")
	wantCalls(t, f)
	h.keys("y")
	wantCalls(t, f, "ResetOffsets archiver orders")
	h.wantView("Lag total 3") // reloaded
}

func TestGroupActions(t *testing.T) {
	tests := []struct {
		name  string
		group string
		keys  []string
		want  string
		calls []string
	}{
		{"remove static member", "billing", []string{"x"}, "Remove static member billing-0 from group billing on dev?",
			[]string{"RemoveMember billing billing-0"}},
		{"delete group", "archiver", []string{"d"}, "Delete group archiver on dev?", []string{"DeleteGroup archiver"}},
		{"reset with active members fails in preview", "billing", []string{"o", "ctrl+s"}, "", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, c := fakeCtx(t, "dev", false, kafkaCaps)
			seedKafka(f)
			h := newHarness(t, Options{Group: tt.group}, c)
			h.keys(tt.keys...)
			if tt.want == "" {
				h.wantStatus("has 1 active members", true)
				return
			}
			h.wantView(tt.want)
			h.keys("y")
			wantCalls(t, f, tt.calls...)
		})
	}
}

func TestTopicAdminActions(t *testing.T) {
	f, c := fakeCtx(t, "dev", false, kafkaCaps)
	seedKafka(f)
	h := newHarness(t, Options{}, c)

	h.keys("n")
	h.typeText("audit")
	h.keys("tab")
	h.typeText("3")
	h.keys("ctrl+s", "y")
	h.keys("up") // the cursor stays on orders after the reload; move to audit
	h.keys("a")
	h.typeText("4")
	h.keys("enter")
	h.wantView("Increase partitions of topic audit to 4 on dev?")
	h.keys("y")
	h.keys("down", "x") // orders: delete records
	h.keys("ctrl+s", "y")
	h.keys("e")
	h.typeText("retention.ms=1000")
	h.keys("ctrl+s", "y")
	wantCalls(t, f, "CreateTopic audit 3", "AddPartitions audit 4", "Purge orders", "AlterTopicConfig orders retention.ms=1000")

	h.keys("enter") // detail shows the new override and partitions
	h.wantView("Topics › orders", "retention.ms = 1000", "PARTITION", "LEADER")
}

func TestConsumersFollowSelectedTopic(t *testing.T) {
	f, c := fakeCtx(t, "rabbit", false, rabbitCaps)
	f.AddTopic("jobs", 1)
	f.AddTopic("mail", 1)
	f.SetConsumers("mail", broker.Consumer{Tag: "ctag-1", Connection: "10.0.0.5:5555 -> 10.0.0.1:5672", Channel: "ch 1", User: "app",
		AckRequired: true, Prefetch: 10, Active: true})
	h := newHarness(t, Options{}, c)
	h.gotoPanel("Consumers")
	h.wantView("0 consumers of queue jobs")
	h.keys("tab", "tab", "tab", "tab", "tab", "tab", "tab") // back to Queues
	h.keys("down")
	h.gotoPanel("Consumers")
	h.wantView("1 consumers of queue mail", "ctag-1", "app")
	h.keys("x")
	h.wantView("Close connection 10.0.0.5:5555 -> 10.0.0.1:5672 on rabbit?")
	h.keys("y")
	wantCalls(t, f, "CloseConnection 10.0.0.5:5555 -> 10.0.0.1:5672")
}

func TestConnectionsPanel(t *testing.T) {
	f, c := fakeCtx(t, "rabbit", false, rabbitCaps)
	f.SetConnections([]broker.Connection{{Name: "conn-1", User: "app", VHost: "/", State: "running", Channels: 2}},
		[]broker.Channel{{Name: "conn-1 (1)", User: "app", VHost: "/", State: "running", Prefetch: 5}})
	h := newHarness(t, Options{}, c)
	h.gotoPanel("Connections")
	h.wantView("[Connections]", "conn-1", "running")
	h.keys("v")
	h.wantView("[Channels]", "conn-1 (1)", "PREFETCH")
	h.keys("v", "x", "y")
	wantCalls(t, f, "CloseConnection conn-1")
}

func TestTopologyPanel(t *testing.T) {
	f, c := fakeCtx(t, "rabbit", false, rabbitCaps)
	f.AddTopic("orders.q", 1)
	f.AddExchange(broker.Exchange{Name: "orders", Type: "direct", Durable: true})
	f.AddBinding(broker.Binding{Source: "orders", Destination: "orders.q", DestinationType: "queue", RoutingKey: "created"})
	f.AddBinding(broker.Binding{Source: "orders", Destination: "audit", DestinationType: "exchange", RoutingKey: "created"})
	h := newHarness(t, Options{}, c)
	h.gotoPanel("Topology")
	h.wantView("1 exchanges, 2 bindings", "orders", "direct", "durable", `├─ key "created"`, "→ queue orders.q", "→ exchange audit")

	// Route dry run is read-only: no confirmation, no recorded call.
	h.keys("t")
	h.wantView("Route dry run")
	h.keys("tab")
	h.typeText("created")
	h.keys("ctrl+s")
	h.wantView("Result", "queues: orders.q", "orders (direct)")
	wantCalls(t, f)

	h.keys("b")
	h.keys("tab", "tab")
	h.typeText("mail.q")
	h.keys("tab")
	h.typeText("mail")
	h.keys("ctrl+s")
	h.wantView(`Bind queue mail.q to exchange orders with key "mail" on rabbit?`)
	h.keys("y")
	h.keys("down", "u", "y") // first binding row
	h.keys("n")
	h.typeText("events")
	h.keys("tab", "ctrl+s", "y")
	wantCalls(t, f, "Bind orders queue mail.q mail", "Unbind orders queue orders.q created", "DeclareExchange events direct")
	h.keys("v")
	h.wantView("[VHosts]", "/")
}

func TestKSQLGuardsOnlyMutatingStatements(t *testing.T) {
	f, c := fakeCtx(t, "prod", true, kafkaCaps)
	h := newHarness(t, Options{}, c)
	h.gotoPanel("KSQL")
	h.keys("e")
	h.typeText("SELECT * FROM orders EMIT CHANGES LIMIT 2;")
	h.keys("ctrl+s")
	h.wantView("ID", "TOTAL", "(2 rows)")
	h.keys("e")
	for range 50 {
		h.keys("backspace")
	}
	h.typeText("DROP STREAM orders;")
	h.keys("ctrl+s")
	h.wantStatus("read_only", true)
	wantCalls(t, f)

	if ksqlMutates("show streams;") || !ksqlMutates("CREATE STREAM s AS SELECT 1;") || ksqlMutates("   ") {
		t.Error("ksqlMutates misclassifies statements")
	}
}

func TestEcosystemPanels(t *testing.T) {
	f, c := fakeCtx(t, "dev", false, kafkaCaps)
	ctx := context.Background()
	_, _ = f.RegisterSchema(ctx, "orders-value", broker.Schema{Schema: `{"type":"record","name":"Order","fields":[]}`})
	_ = f.PutConnector(ctx, "mirror", map[string]string{"connector.class": "Mirror"})
	_ = f.CreateACL(ctx, broker.ACL{Principal: "User:alice", Host: "*", ResourceType: "topic", ResourceName: "orders",
		PatternType: "literal", Operation: "read", Permission: "allow"})
	f.SetNodes(broker.Node{ID: "1", Host: "kafka-1", Port: 9092, Controller: true, Running: true})
	f.Calls = nil
	h := newHarness(t, Options{}, c)

	h.gotoPanel("Schemas")
	h.keys("enter")
	h.wantView("compatibility BACKWARD", "VERSION")
	h.keys("enter")
	h.wantView("orders-value v1", `"type": "record"`)
	h.keys("esc", "esc", "d", "y")

	h.gotoPanel("Connect")
	h.wantView("mirror", "RUNNING")
	h.keys("p", "y")
	h.wantView("PAUSED")
	h.keys("enter")
	h.wantView("Connector mirror", "connector.class=Mirror")
	h.keys("esc")

	h.gotoPanel("ACLs")
	h.wantView("User:alice", "allow")
	h.keys("d", "y")

	h.gotoPanel("Cluster")
	h.wantView("kafka-1:9092")
	h.keys("enter")
	h.wantView("node.id", "STATIC_BROKER_CONFIG")
	wantCalls(t, f, "DeleteSubject orders-value", "ConnectorAction mirror pause", "DeleteACLs 1")
}

func TestRabbitAdminPanels(t *testing.T) {
	f, c := fakeCtx(t, "rabbit", false, rabbitCaps)
	h := newHarness(t, Options{}, c)
	h.gotoPanel("Users")
	h.keys("n")
	h.typeText("bob")
	h.keys("tab")
	h.typeText("monitoring")
	h.keys("tab")
	h.typeText("s3cret")
	h.wantNotView("s3cret") // passwords are masked
	h.keys("enter", "y")
	h.wantView("bob", "monitoring")
	h.keys("v", "v")
	h.wantView("[Permissions]")
	h.keys("n", "ctrl+s")
	h.wantView("user and vhost are required")
	h.keys("esc")

	h.gotoPanel("Policies")
	h.keys("n")
	h.typeText("ttl")
	h.keys("tab", "tab", "tab", "tab", "tab")
	for range 2 {
		h.keys("backspace")
	}
	h.typeText(`{"message-ttl":60000}`)
	h.keys("ctrl+s", "y")
	h.wantView("ttl", `{"message-ttl":60000}`)
	h.keys("v")
	h.keys("n")
	h.typeText("sh1")
	h.keys("tab", "tab")
	for range 2 {
		h.keys("backspace")
	}
	h.typeText(`{"src-uri":"amqp://user:pw@host"}`)
	h.keys("ctrl+s", "y")
	h.wantView("[Shovels]", "sh1", "running", "amqp://user:******@host")
	h.wantNotView(":pw@")
	wantCalls(t, f, "PutUser bob", "PutPolicy ttl", "PutParameter shovel sh1")
}

func TestMetricsView(t *testing.T) {
	f, c := fakeCtx(t, "dev", false, kafkaCaps)
	f.AddTopic("orders", 1)
	clk := newClock()
	f.Now = clk.now
	h := newHarness(t, Options{}, c)
	h.gotoPanel("Metrics")
	h.wantView("Metrics: cluster-wide", "collecting", "connections")
	v := h.top().(*metricsView)

	// Each tick samples again; 10 messages over 2s is 5/s.
	for i := range 3 {
		f.AddMessages("orders", 0, make([]broker.Message, 10*(i+1))...)
		clk.t = clk.t.Add(2 * time.Second)
		h.send(routedMsg{gen: h.a.e.gen, to: v.id, msg: metricsTickMsg{seq: v.seq}})
	}
	h.wantView("messages_in", "15.0/s")
	if got := v.rates["messages_in"].slice(); len(got) < 3 || got[len(got)-1] != 15 {
		t.Errorf("rates = %v", got)
	}

	// A tick from an old loop or while hidden stops the loop instead of sampling.
	h.keys("tab")
	h.send(routedMsg{gen: h.a.e.gen, to: v.id, msg: metricsTickMsg{seq: v.seq}})
	if v.ticking {
		t.Error("ticking continues while hidden")
	}

	h.gotoPanel("Topics")
	h.gotoPanel("Metrics")
	h.keys("t")
	h.wantView("Metrics: topic orders")
}

func TestSparkline(t *testing.T) {
	tests := []struct {
		name  string
		in    []float64
		width int
		want  string
	}{
		{"empty", nil, 5, ""},
		{"zero width", []float64{1}, 0, ""},
		{"all zero", []float64{0, 0, 0}, 5, "▁▁▁"},
		{"ramp", []float64{0, 1, 2, 3, 4, 5, 6, 7}, 8, "▁▂▃▄▅▆▇█"},
		{"keeps the newest values", []float64{7, 0, 7}, 2, "▁█"},
		{"constant positive", []float64{3, 3}, 4, "██"},
		{"negative values", []float64{-1, 0, 1}, 3, "▁▅█"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sparkline(tt.in, tt.width); got != tt.want {
				t.Errorf("sparkline(%v, %d) = %q, want %q", tt.in, tt.width, got, tt.want)
			}
		})
	}
	if got := bar(5, 10, 4); got != "██░░" {
		t.Errorf("bar = %q", got)
	}
	r := newRing[int](3)
	for i := 1; i <= 5; i++ {
		r.push(i)
	}
	if got := r.slice(); len(got) != 3 || got[0] != 3 || got[2] != 5 {
		t.Errorf("ring = %v", got)
	}
}

func TestParsers(t *testing.T) {
	if _, err := offsetReset(values{"topic": "t", "to": "+0"}, time.Now()); err == nil {
		t.Error("zero shift accepted")
	}
	req, err := offsetReset(values{"topic": "t", "to": "-5", "partitions": "1"}, time.Now())
	if err != nil || req.Shift != -5 || len(req.Partitions) != 1 {
		t.Errorf("shift reset = %+v, %v", req, err)
	}
	req, err = offsetReset(values{"topic": "t", "to": "42"}, time.Now())
	if err != nil || req.To.Kind != broker.AtOffset || req.To.Offset != 42 {
		t.Errorf("offset reset = %+v, %v", req, err)
	}
	args, err := parseArgs("x-message-ttl=60000, x-queue-type=quorum, lazy=true")
	if err != nil || args["x-message-ttl"] != int64(60000) || args["x-queue-type"] != "quorum" || args["lazy"] != true {
		t.Errorf("args = %#v, %v", args, err)
	}
	if got := redactURIs(`{"uri":"amqps://u:p@h/v","x":"http://h"}`); strings.Contains(got, ":p@") {
		t.Errorf("password not redacted: %s", got)
	}
}
