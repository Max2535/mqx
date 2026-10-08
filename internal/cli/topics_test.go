package cli

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/Max2535/mqx/internal/broker"
)

func TestTopics(t *testing.T) {
	e := newFakeEnv(t, false)
	e.f.AddTopic("orders", 3)
	e.f.AddTopic("payments", 1)
	e.f.AddMessages("orders", 1, broker.Message{Value: []byte("a")}, broker.Message{Value: []byte("b")})

	stdout, _, err := e.run(t, "topics")
	checkErr(t, err, "")
	want := [][]string{
		{"NAME", "KIND", "PARTITIONS", "REPLICAS", "MESSAGES", "CONSUMERS"},
		{"orders", "topic", "3", "1", "2", "0"},
		{"payments", "topic", "1", "1", "0", "0"},
	}
	if got := fields(stdout); !reflect.DeepEqual(got, want) {
		t.Errorf("topics =\n%q\nwant\n%q", got, want)
	}

	stdout, _, err = e.run(t, "queues", "--match", "^pay", "-o", "json")
	checkErr(t, err, "")
	var topics []broker.Topic
	if err := json.Unmarshal([]byte(stdout), &topics); err != nil {
		t.Fatalf("json: %v\n%s", err, stdout)
	}
	if len(topics) != 1 || topics[0].Name != "payments" {
		t.Errorf("filtered topics = %+v", topics)
	}

	_, _, err = e.run(t, "topics", "--match", "(")
	checkErr(t, err, "--match")
	_, _, err = e.run(t, "topics", "-o", "yaml")
	checkErr(t, err, `unknown --output "yaml"`)
}

func TestTopicDescribe(t *testing.T) {
	e := newFakeEnv(t, false)
	e.f.AddTopic("orders", 2)
	e.f.AddMessages("orders", 0, broker.Message{Value: []byte("a")})
	checkErr(t, e.f.AlterTopicConfig(t.Context(), "orders", map[string]string{"retention.ms": "1000"}), "")

	stdout, _, err := e.run(t, "topic", "describe", "orders")
	checkErr(t, err, "")
	for _, want := range []string{"Name:       orders", "Partitions: 2", "PARTITION", "retention.ms", "1000"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("describe output missing %q:\n%s", want, stdout)
		}
	}
	_, _, err = e.run(t, "topic", "describe", "missing")
	checkErr(t, err, "not found")
}

func TestTopicAdmin(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		wantErr   string
		wantCalls []string
		wantOut   string
	}{
		{
			name:      "create kafka topic",
			args:      []string{"topic", "create", "new", "--partitions", "6", "--set", "retention.ms=1", "--yes"},
			wantCalls: []string{"CreateTopic new 6"},
			wantOut:   "Created new.",
		},
		{name: "create bad config", args: []string{"topic", "create", "new", "--set", "nokey", "--yes"}, wantErr: "want key=value"},
		{name: "delete", args: []string{"topic", "delete", "orders", "--yes"}, wantCalls: []string{"DeleteTopic orders"}},
		{
			name:      "alter config set and delete",
			args:      []string{"topic", "alter-config", "orders", "--set", "a=1", "--delete", "b", "--yes"},
			wantCalls: []string{"AlterTopicConfig orders a=1,b="},
		},
		{name: "alter config needs a change", args: []string{"topic", "alter-config", "orders", "--yes"}, wantErr: "nothing to change"},
		{name: "add partitions", args: []string{"topic", "add-partitions", "orders", "--total", "4", "--yes"}, wantCalls: []string{"AddPartitions orders 4"}},
		{name: "add partitions needs total", args: []string{"topic", "add-partitions", "orders", "--yes"}, wantErr: "--total"},
		{name: "purge", args: []string{"purge", "orders", "--yes"}, wantCalls: []string{"Purge orders"}, wantOut: "2 message(s) removed"},
		{
			name:      "delete records before offset",
			args:      []string{"topic", "delete-records", "orders", "--before", "1", "--partitions", "0", "--yes"},
			wantCalls: []string{"Purge orders"},
			wantOut:   "0→1",
		},
		{name: "delete records bad position", args: []string{"topic", "delete-records", "orders", "--before", "latest", "--yes"}, wantErr: "offset or a time"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newFakeEnv(t, false)
			e.f.AddTopic("orders", 2)
			e.f.AddMessages("orders", 0, broker.Message{Value: []byte("a")}, broker.Message{Value: []byte("b")})
			stdout, _, err := e.run(t, tt.args...)
			checkErr(t, err, tt.wantErr)
			if !reflect.DeepEqual(e.f.Calls, tt.wantCalls) {
				t.Errorf("calls = %q, want %q", e.f.Calls, tt.wantCalls)
			}
			if !strings.Contains(stdout, tt.wantOut) {
				t.Errorf("stdout = %q, want containing %q", stdout, tt.wantOut)
			}
		})
	}
}

func TestUnsupportedCapability(t *testing.T) {
	e := newFakeEnv(t, false)
	e.f.Caps = []string{} // core only
	_, _, err := e.run(t, "topic", "add-partitions", "orders", "--total", "3", "--yes")
	checkErr(t, err, `does not support add-partitions; run `+"`mqx ctx describe`")
}
