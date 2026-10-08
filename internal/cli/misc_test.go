package cli

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Max2535/mqx/internal/broker"
)

func TestConsumers(t *testing.T) {
	e := newFakeEnv(t, false)
	e.f.AddTopic("orders", 2)
	e.f.AddGroup(broker.GroupDescription{Name: "billing", State: "Stable", Members: []broker.GroupMember{
		{MemberID: "m1", ClientID: "billing-1", Host: "/10.0.0.1",
			Assignment: []broker.TopicPartitions{{Topic: "orders", Partitions: []int32{0, 1}}}},
	}}, nil)
	stdout, _, err := e.run(t, "consumers", "orders")
	checkErr(t, err, "")
	want := [][]string{
		{"GROUP", "MEMBER", "INSTANCE", "CLIENT", "HOST", "PARTITIONS"},
		{"billing", "m1", "-", "billing-1", "/10.0.0.1", "0,1"},
	}
	if got := fields(stdout); !reflect.DeepEqual(got, want) {
		t.Errorf("kafka consumers = %q", got)
	}

	e.f.SetConsumers("orders.q", broker.Consumer{Tag: "ctag-1", Connection: "10.0.0.2:5555 -> 10.0.0.3:5672",
		Channel: "ch 1", AckRequired: true, Prefetch: 10, Active: true})
	stdout, _, err = e.run(t, "consumers", "orders.q")
	checkErr(t, err, "")
	if !strings.Contains(stdout, "TAG") || !strings.Contains(stdout, "ctag-1") || !strings.Contains(stdout, "10") {
		t.Errorf("queue consumers = %s", stdout)
	}

	stdout, _, err = e.run(t, "consumers", "nobody")
	checkErr(t, err, "")
	if !strings.Contains(stdout, "No consumers on nobody.") {
		t.Errorf("empty consumers = %q", stdout)
	}
}

func TestNodes(t *testing.T) {
	e := newFakeEnv(t, false)
	e.f.SetNodes(broker.Node{ID: "1", Host: "kafka-1", Port: 9092, Controller: true, Running: true})
	stdout, _, err := e.run(t, "brokers")
	checkErr(t, err, "")
	want := [][]string{
		{"ID", "HOST", "PORT", "RACK", "CONTROLLER", "RUNNING", "DETAILS"},
		{"1", "kafka-1", "9092", "-", "yes", "yes", "-"},
	}
	if got := fields(stdout); !reflect.DeepEqual(got, want) {
		t.Errorf("nodes = %q", got)
	}
	stdout, _, err = e.run(t, "nodes", "config", "1", "--all")
	checkErr(t, err, "")
	if !strings.Contains(stdout, "node.id") {
		t.Errorf("node config = %s", stdout)
	}
}

func TestMetrics(t *testing.T) {
	e := newFakeEnv(t, false)
	e.f.AddTopic("orders", 1)
	// Each sample advances the fake clock one second and adds two messages.
	clock := time.Date(2026, 10, 8, 9, 0, 0, 0, time.UTC)
	e.f.Now = func() time.Time {
		clock = clock.Add(time.Second)
		return clock
	}
	stdout, _, err := e.run(t, "metrics", "orders", "--interval", "1ms", "--count", "2", "-o", "json")
	checkErr(t, err, "")
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines = %q", lines)
	}
	var rl rateLine
	if err := json.Unmarshal([]byte(lines[0]), &rl); err != nil {
		t.Fatal(err)
	}
	if _, ok := rl.Rates[broker.MetricMessagesIn]; !ok {
		t.Errorf("rates = %v", rl.Rates)
	}
	stdout, _, err = e.run(t, "metrics", "--interval", "1ms", "--count", "1")
	checkErr(t, err, "")
	if !strings.Contains(stdout, "messages_in/s") || !strings.Contains(stdout, "connections") {
		t.Errorf("table = %s", stdout)
	}
	_, _, err = e.run(t, "metrics", "--interval", "0s")
	checkErr(t, err, "--interval must be positive")
}

func TestPing(t *testing.T) {
	e := newFakeEnv(t, false)
	stdout, _, err := e.run(t, "ping")
	checkErr(t, err, "")
	if !strings.Contains(stdout, "fake (fake) is reachable") {
		t.Errorf("ping = %q", stdout)
	}
	e.f.OpenErr = errors.New("connection refused")
	_, _, err = e.run(t, "ping")
	checkErr(t, err, `context "fake": connect to fake: connection refused`)
}

func TestCtxDescribe(t *testing.T) {
	e := newFakeEnv(t, true)
	e.f.Caps = []string{broker.CapLagReporter, broker.CapGroupInspector}
	stdout, _, err := e.run(t, "ctx", "describe", "-o", "json")
	checkErr(t, err, "")
	var info contextInfo
	if err := json.Unmarshal([]byte(stdout), &info); err != nil {
		t.Fatal(err)
	}
	if !info.Reachable || !info.ReadOnly || !reflect.DeepEqual(info.Capabilities, []string{"groups", "lag"}) {
		t.Errorf("info = %+v", info)
	}
	stdout, _, err = e.run(t, "ctx", "describe", "fake", "--offline")
	checkErr(t, err, "")
	if strings.Contains(stdout, "reachable") || !strings.Contains(stdout, "read_only") {
		t.Errorf("offline = %s", stdout)
	}
	_, _, err = e.run(t, "ctx", "describe", "nope")
	checkErr(t, err, `context "nope" not found`)
}

func TestSelectContextOverride(t *testing.T) {
	e := newFakeEnv(t, false)
	_, _, err := e.run(t, "--context", "other", "ping")
	checkErr(t, err, `context "other" not found; available: fake`)
}

func TestBareMqxAndTUI(t *testing.T) {
	var got *TUIOptions
	tui := WithTUI(func(_ context.Context, o TUIOptions) error {
		got = &o
		return nil
	})
	tests := []struct {
		name     string
		tty      bool
		args     []string
		wantTUI  *TUIOptions
		wantHelp bool
		wantErr  string
	}{
		{name: "bare on a terminal opens the TUI", tty: true, args: nil, wantTUI: &TUIOptions{}},
		{name: "bare when piped prints help", tty: false, args: nil, wantHelp: true},
		{name: "bare passes --context", tty: true, args: []string{"--context", "x"}, wantTUI: &TUIOptions{Context: "x"}},
		{
			name:    "tui deep links",
			args:    []string{"tui", "--context", "k", "--topic", "orders", "--group", "billing"},
			wantTUI: &TUIOptions{Context: "k", Topic: "orders", Group: "billing"},
		},
		{name: "tui falls back to global --context", args: []string{"-c", "g", "tui"}, wantTUI: &TUIOptions{Context: "g"}},
		{name: "bare rejects stray args", tty: true, args: []string{"nonsense"}, wantErr: `unknown command "nonsense"`, wantHelp: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got = nil
			stdout, _, err := runWith(t, []Option{tui, withTerminal(tt.tty)}, nil, tt.args...)
			checkErr(t, err, tt.wantErr)
			if !reflect.DeepEqual(got, tt.wantTUI) {
				t.Errorf("TUI opts = %+v, want %+v", got, tt.wantTUI)
			}
			if gotHelp := strings.Contains(stdout, "Usage:"); gotHelp != tt.wantHelp {
				t.Errorf("help printed = %v, want %v", gotHelp, tt.wantHelp)
			}
		})
	}
	_, _, err := runWith(t, nil, nil, "tui")
	checkErr(t, err, "this build has no TUI")
}

func TestParsePosition(t *testing.T) {
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		in      string
		want    broker.Position
		wantErr bool
	}{
		{in: "", want: broker.Position{Kind: broker.Earliest}},
		{in: "earliest", want: broker.Position{Kind: broker.Earliest}},
		{in: "LATEST", want: broker.Position{Kind: broker.Latest}},
		{in: "42", want: broker.Position{Kind: broker.AtOffset, Offset: 42}},
		{in: "-5", want: broker.Position{Kind: broker.Tail, Offset: 5}},
		{in: "2026-10-08T09:00:00Z", want: broker.Position{Kind: broker.AtTime, Time: time.Date(2026, 10, 8, 9, 0, 0, 0, time.UTC)}},
		{in: "15m", want: broker.Position{Kind: broker.AtTime, Time: now.Add(-15 * time.Minute)}},
		{in: "yesterday", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := parsePosition(tt.in, now)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v", err)
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestParseArgs(t *testing.T) {
	got, err := parseArgs("arg", []string{"x-max-length=10", "durable=true", "x-queue-type=quorum", `obj={"a":1}`})
	checkErr(t, err, "")
	want := map[string]any{"x-max-length": int64(10), "durable": true, "x-queue-type": "quorum", "obj": map[string]any{"a": 1.0}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("parseArgs = %#v", got)
	}
}
