package cli

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Max2535/mqx/internal/broker"
)

func seedOrders(e fakeEnv) {
	e.f.AddTopic("orders", 2)
	t0 := time.Date(2026, 10, 8, 9, 0, 0, 0, time.UTC)
	e.f.AddMessages("orders", 0,
		broker.Message{Key: []byte("ord-1"), Value: []byte(`{"total":10}`), Timestamp: t0},
		broker.Message{Key: []byte("ord-2"), Value: []byte("plain text"), Timestamp: t0.Add(time.Minute),
			Headers: []broker.Header{{Key: "source", Value: []byte("mqx")}}},
		broker.Message{Key: []byte("ord-3"), Value: []byte{0xff, 0x00}, Timestamp: t0.Add(2 * time.Minute)},
	)
	e.f.AddMessages("orders", 1, broker.Message{Key: []byte("ord-4"), Value: []byte(`{"total":990}`), Timestamp: t0.Add(3 * time.Minute)})
}

func TestPeek(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantKeys []string
		wantErr  string
	}{
		{name: "all", args: nil, wantKeys: []string{"ord-1", "ord-2", "ord-3", "ord-4"}},
		{name: "limit", args: []string{"-n", "2"}, wantKeys: []string{"ord-1", "ord-2"}},
		{name: "partition", args: []string{"-p", "1"}, wantKeys: []string{"ord-4"}},
		{name: "tail", args: []string{"--from", "-1"}, wantKeys: []string{"ord-3", "ord-4"}},
		{name: "offset", args: []string{"--from", "2", "-p", "0"}, wantKeys: []string{"ord-3"}},
		{name: "key regex", args: []string{"--key", "^ord-[24]$"}, wantKeys: []string{"ord-2", "ord-4"}},
		{name: "value regex", args: []string{"--value", `"total":\s*990`}, wantKeys: []string{"ord-4"}},
		{name: "header", args: []string{"--header", "source=^mqx$"}, wantKeys: []string{"ord-2"}},
		{name: "since", args: []string{"--since", "2026-10-08T09:02:00Z"}, wantKeys: []string{"ord-3", "ord-4"}},
		{name: "until", args: []string{"--until", "2026-10-08T09:01:00Z"}, wantKeys: []string{"ord-1"}},
		{name: "bad from", args: []string{"--from", "yesterday"}, wantErr: "--from"},
		{name: "bad to", args: []string{"--to", "latest"}, wantErr: "--to must be an offset or a time"},
		{name: "bad key regex", args: []string{"--key", "("}, wantErr: "--key"},
		{name: "bad since", args: []string{"--since", "5"}, wantErr: "--since"},
		{name: "negative limit", args: []string{"-n", "-1"}, wantErr: "--limit"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newFakeEnv(t, false)
			seedOrders(e)
			stdout, _, err := e.run(t, append([]string{"peek", "orders", "-o", "json"}, tt.args...)...)
			checkErr(t, err, tt.wantErr)
			if tt.wantErr != "" {
				return
			}
			var keys []string
			for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
				var m map[string]any
				if err := json.Unmarshal([]byte(line), &m); err != nil {
					t.Fatalf("line %q: %v", line, err)
				}
				keys = append(keys, m["key"].(string))
			}
			if !reflect.DeepEqual(keys, tt.wantKeys) {
				t.Errorf("keys = %v, want %v", keys, tt.wantKeys)
			}
		})
	}
}

func TestPeekJSONEncodings(t *testing.T) {
	e := newFakeEnv(t, false)
	seedOrders(e)
	stdout, _, err := e.run(t, "peek", "orders", "-p", "0", "-o", "json")
	checkErr(t, err, "")
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	want := []string{
		`"value":{"total":10}`,
		`"value":"plain text"`,
		`"value":"/wA=","value_encoding":"base64"`,
	}
	for i, w := range want {
		if !strings.Contains(lines[i], w) {
			t.Errorf("line %d = %s, want containing %s", i, lines[i], w)
		}
	}
	if !strings.Contains(lines[1], `"headers":{"source":"mqx"}`) || !strings.Contains(lines[0], `"partition":0`) {
		t.Errorf("headers or partition missing: %s", stdout)
	}
}

func TestPeekHuman(t *testing.T) {
	e := newFakeEnv(t, false)
	seedOrders(e)
	stdout, _, err := e.run(t, "peek", "orders", "-p", "0", "-n", "2")
	checkErr(t, err, "")
	for _, w := range []string{"--- orders[0] @0", "key=ord-1", "\"total\": 10", "source: mqx", "plain text"} {
		if !strings.Contains(stdout, w) {
			t.Errorf("output missing %q:\n%s", w, stdout)
		}
	}
	stdout, _, err = e.run(t, "peek", "orders", "-p", "1", "--raw")
	checkErr(t, err, "")
	if stdout != "{\"total\":990}\n" {
		t.Errorf("raw = %q", stdout)
	}
	_, stderr, err := e.run(t, "peek", "orders", "--key", "nope")
	checkErr(t, err, "")
	if !strings.Contains(stderr, "No messages matched") {
		t.Errorf("stderr = %q", stderr)
	}
}

func TestPublish(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		stdin     string
		wantCalls []string
		wantErr   string
		check     func(t *testing.T, msgs []broker.Message)
	}{
		{
			name:      "value with key and headers",
			args:      []string{"--key", "k1", "-H", "a=1", "-H", "b=2", "--value", `{"x":1}`},
			wantCalls: []string{`Publish orders 0 {"x":1}`},
			check: func(t *testing.T, msgs []broker.Message) {
				m := msgs[0]
				if string(m.Key) != "k1" || len(m.Headers) != 2 || m.Headers[1].Key != "b" || string(m.Headers[1].Value) != "2" {
					t.Errorf("message = %+v", m)
				}
			},
		},
		{name: "stdin payload", stdin: "from stdin", wantCalls: []string{"Publish orders 0 from stdin"}},
		{
			name:      "lines with key separator",
			args:      []string{"--lines", "--key-separator", ":"},
			stdin:     "a:1\n\nb:2\n",
			wantCalls: []string{"Publish orders 0 1", "Publish orders 0 2"},
			check: func(t *testing.T, msgs []broker.Message) {
				if string(msgs[0].Key) != "a" || string(msgs[1].Key) != "b" {
					t.Errorf("keys = %q %q", msgs[0].Key, msgs[1].Key)
				}
			},
		},
		{name: "explicit partition", args: []string{"--partition", "1", "--value", "x"}, wantCalls: []string{"Publish orders 1 x"}},
		{name: "line without separator", args: []string{"--lines", "--key-separator", ":"}, stdin: "nosep\n", wantErr: "no key separator"},
		{name: "separator needs lines", args: []string{"--key-separator", ":", "--value", "x"}, wantErr: "needs --lines"},
		{name: "value and file", args: []string{"--value", "x", "--file", "f"}, wantErr: "only one of"},
		{name: "bad header", args: []string{"-H", "nokv", "--value", "x"}, wantErr: "want key=value"},
		{name: "empty lines input", args: []string{"--lines"}, stdin: "", wantErr: "input was empty"},
		{name: "unknown topic", args: []string{"--value", "x"}, wantErr: "not found"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newFakeEnv(t, false)
			if tt.name != "unknown topic" {
				e.f.AddTopic("orders", 2)
			}
			args := append([]string{"--config", e.path, "publish", "orders", "--yes"}, tt.args...)
			_, _, err := runWith(t, nil, strings.NewReader(tt.stdin), args...)
			checkErr(t, err, tt.wantErr)
			if tt.wantErr != "" {
				return
			}
			if !reflect.DeepEqual(e.f.Calls, tt.wantCalls) {
				t.Errorf("calls = %q, want %q", e.f.Calls, tt.wantCalls)
			}
			if tt.check != nil {
				tt.check(t, append(e.f.Messages("orders", 0), e.f.Messages("orders", 1)...))
			}
		})
	}
}

func TestPublishRabbitFlags(t *testing.T) {
	pf := publishFlags{partition: broker.AnyPartition, value: "{}", exchange: "orders.x", routingKey: "order.created",
		properties: []string{"content_type=application/json"}, schema: "orders-value", schemaVer: 2}
	msgs, err := pf.messages(strings.NewReader(""))
	checkErr(t, err, "")
	m := msgs[0]
	if m.Exchange != "orders.x" || m.RoutingKey != "order.created" || m.Properties["content_type"] != "application/json" {
		t.Errorf("message = %+v", m)
	}
	if m.Schema == nil || m.Schema.Subject != "orders-value" || m.Schema.Version != 2 || m.Partition != broker.AnyPartition {
		t.Errorf("schema/partition = %+v %d", m.Schema, m.Partition)
	}
}

func TestPublishRefusedOnReadOnly(t *testing.T) {
	e := newFakeEnv(t, true)
	e.f.AddTopic("orders", 1)
	_, _, err := e.run(t, "publish", "orders", "--value", "x", "--yes")
	checkErr(t, err, "read_only")
	if len(e.f.Calls) != 0 {
		t.Errorf("calls = %v", e.f.Calls)
	}
}

func TestPublishRefusesInternalTopics(t *testing.T) {
	e := newFakeEnv(t, false)
	e.f.AddTopic("__consumer_offsets", 1)
	_, _, err := e.run(t, "publish", "__consumer_offsets", "--value", "x", "--yes")
	checkErr(t, err, "--allow-internal")
	if len(e.f.Calls) != 0 {
		t.Errorf("calls = %v", e.f.Calls)
	}
	_, _, err = e.run(t, "publish", "__consumer_offsets", "--value", "x", "--yes", "--allow-internal")
	checkErr(t, err, "")
	if len(e.f.Calls) != 1 {
		t.Errorf("calls = %v", e.f.Calls)
	}
}
