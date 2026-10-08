package tui

import (
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"

	"github.com/Max2535/mqx/internal/broker"
)

func TestRenderPayload(t *testing.T) {
	tests := []struct {
		name       string
		in         []byte
		raw        bool
		wantFormat string
		want       string
		colored    bool
	}{
		{"json pretty", []byte(`{"order_id":"o-1","total":12.5,"ok":true,"tags":[null]}`), false, formatJSON,
			"{\n  \"order_id\": \"o-1\",\n  \"total\": 12.5,\n  \"ok\": true,\n  \"tags\": [\n    null\n  ]\n}", true},
		{"json raw", []byte(`{"a":1}`), true, formatJSON, `{"a":1}`, false},
		{"text", []byte("hello\nworld"), false, formatText, "hello\nworld", false},
		{"binary hex", []byte{0x00, 0x01, 0xff}, false, formatHex, "00000000  00 01 ff", false},
		{"invalid json is text", []byte(`{"a":`), false, formatText, `{"a":`, false},
		{"empty", nil, false, formatEmpty, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, format := renderPayload(tt.in, tt.raw)
			if format != tt.wantFormat {
				t.Errorf("format = %q, want %q", format, tt.wantFormat)
			}
			if plain := ansi.Strip(got); !strings.HasPrefix(plain, tt.want) {
				t.Errorf("payload =\n%s\nwant prefix\n%s", plain, tt.want)
			}
			if colored := strings.Contains(got, "\x1b["); colored != tt.colored {
				t.Errorf("colored = %v, want %v", colored, tt.colored)
			}
		})
	}
}

func TestHighlightJSONColoursTokens(t *testing.T) {
	out := highlightJSON("{\n  \"k\": \"v\",\n  \"n\": -1.5e3,\n  \"b\": false\n}")
	for _, tok := range []string{`"k"`, `"v"`, "-1.5e3", "false"} {
		if !regexp.MustCompile(`\x1b\[[0-9;]*m` + regexp.QuoteMeta(tok)).MatchString(out) {
			t.Errorf("token %s not coloured in %q", tok, out)
		}
	}
	if ansi.Strip(out) != "{\n  \"k\": \"v\",\n  \"n\": -1.5e3,\n  \"b\": false\n}" {
		t.Errorf("highlighting changed the text: %q", ansi.Strip(out))
	}
}

func TestMessageBrowserViewer(t *testing.T) {
	f, c := fakeCtx(t, "local", false, kafkaCaps)
	seedKafka(f)
	h := newHarness(t, Options{Topic: "orders"}, c)
	h.wantView("Payload (json)", `"order_id": "o-1"`, `"total": 12.5`, "header", "source=web", "key", "o-1")

	h.keys("w")
	h.wantView("Payload (json) raw", `{"order_id":"o-1","total":12.5}`)
	h.keys("w", "down")
	h.wantView("Payload (text)", "plain text")
	h.keys("down")
	h.wantView("Payload (hex)", "00 01 ff")

	// Filter on key regex.
	h.keys("/")
	h.wantView("Filter orders", "Key regex")
	h.typeText("o-[12]")
	h.keys("ctrl+s")
	h.wantView("2 messages", "filter: key=o-[12]")
	h.wantNotView("o-3")

	// Invalid regex keeps the form open with the error.
	h.keys("/")
	h.typeText("(")
	h.keys("ctrl+s")
	h.wantView("key regex: error parsing regexp")
	h.keys("esc")

	// Follow: the fake's stream ends at once, which reports and stops following.
	h.keys("f")
	h.wantStatus("follow ended", false)
	if mb := h.top().(*messageBrowser); mb.following {
		t.Error("still following after the stream closed")
	}
}

func TestMessageBrowserRabbitColumns(t *testing.T) {
	f, c := fakeCtx(t, "local", false, rabbitCaps)
	f.AddTopic("jobs", 1)
	f.AddMessages("jobs", 0, broker.Message{Exchange: "work", RoutingKey: "jobs.new", Value: []byte("x"),
		Properties: map[string]string{"content_type": "text/plain"}, Redelivered: true})
	h := newHarness(t, Options{Topic: "jobs"}, c)
	h.wantView("queue jobs", "EXCHANGE", "ROUTING KEY", "jobs.new", "content_type=text/plain", "redelivered")
	h.wantNotView("follow") // follow is Kafka-only
}

func TestPeekOptions(t *testing.T) {
	now := time.Date(2026, 10, 8, 9, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		v       values
		check   func(t *testing.T, o broker.PeekOptions)
		wantErr string
	}{
		{"defaults", values{}, func(t *testing.T, o broker.PeekOptions) {
			if o.From.Kind != broker.Earliest || o.Limit != 0 || !o.Filter.IsZero() {
				t.Errorf("got %+v", o)
			}
		}, ""},
		{"tail and limit", values{"from": "-5", "limit": "7", "partitions": "0,2"}, func(t *testing.T, o broker.PeekOptions) {
			if o.From.Kind != broker.Tail || o.From.Offset != 5 || o.Limit != 7 || len(o.Partitions) != 2 {
				t.Errorf("got %+v", o)
			}
		}, ""},
		{"duration ago", values{"from": "15m"}, func(t *testing.T, o broker.PeekOptions) {
			if o.From.Kind != broker.AtTime || !o.From.Time.Equal(now.Add(-15*time.Minute)) {
				t.Errorf("got %+v", o.From)
			}
		}, ""},
		{"header match", values{"header": "trace=abc.*"}, func(t *testing.T, o broker.PeekOptions) {
			if len(o.Filter.Headers) != 1 || o.Filter.Headers[0].Key != "trace" {
				t.Errorf("got %+v", o.Filter)
			}
		}, ""},
		{"bad from", values{"from": "yesterday"}, nil, "invalid position"},
		{"bad limit", values{"limit": "-1"}, nil, "invalid limit"},
		{"bad value regex", values{"value": "["}, nil, "value regex"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o, err := peekOptions(tt.v, now)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			tt.check(t, o)
		})
	}
}

func TestBuildMessage(t *testing.T) {
	tests := []struct {
		name    string
		v       values
		want    broker.Message
		wantErr error
		errText string
	}{
		{"value only picks any partition", values{"payload": "hi"},
			broker.Message{Partition: broker.AnyPartition, Value: []byte("hi")}, nil, ""},
		{"kafka fields", values{"key": "k1", "partition": "2", "headers": "a=1\nb=two", "payload": "{}", "json": "y"},
			broker.Message{Partition: 2, Key: []byte("k1"), Value: []byte("{}"),
				Headers: []broker.Header{{Key: "a", Value: []byte("1")}, {Key: "b", Value: []byte("two")}}}, nil, ""},
		{"rabbit fields", values{"exchange": "work", "routing_key": "jobs.new", "properties": "content_type=text/plain", "payload": "x"},
			broker.Message{Partition: broker.AnyPartition, Value: []byte("x"), Exchange: "work", RoutingKey: "jobs.new",
				Properties: map[string]string{"content_type": "text/plain"}}, nil, ""},
		{"invalid json rejected when validating", values{"payload": "{", "json": "y"}, broker.Message{}, errInvalidJSON, ""},
		{"bad partition", values{"partition": "x"}, broker.Message{}, nil, "invalid partition"},
		{"bad header", values{"headers": "novalue"}, broker.Message{}, nil, "headers"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildMessage(tt.v)
			switch {
			case tt.wantErr != nil:
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				return
			case tt.errText != "":
				if err == nil || !strings.Contains(err.Error(), tt.errText) {
					t.Fatalf("err = %v, want %q", err, tt.errText)
				}
				return
			case err != nil:
				t.Fatal(err)
			}
			if got.Partition != tt.want.Partition || string(got.Key) != string(tt.want.Key) ||
				string(got.Value) != string(tt.want.Value) || got.Exchange != tt.want.Exchange ||
				got.RoutingKey != tt.want.RoutingKey || formatKV(got.Properties, ",") != formatKV(tt.want.Properties, ",") ||
				len(got.Headers) != len(tt.want.Headers) {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
			for i, h := range got.Headers {
				if h.Key != tt.want.Headers[i].Key || string(h.Value) != string(tt.want.Headers[i].Value) {
					t.Errorf("header %d = %s=%s", i, h.Key, h.Value)
				}
			}
		})
	}
}

func TestPublishFormEndToEnd(t *testing.T) {
	f, c := fakeCtx(t, "dev", false, kafkaCaps)
	seedKafka(f)
	h := newHarness(t, Options{}, c)

	h.keys("p")
	h.wantView("Publish to topic orders", "Key", "Partition", "Headers", "Validate JSON", "Payload")
	h.wantNotView("Exchange", "Routing key") // kafka-like: no RabbitMQ fields
	h.typeText("k-9")
	h.keys("tab")
	h.typeText("1")
	h.keys("tab")
	h.typeText("trace=abc")
	h.keys("tab")
	h.typeText("y")
	h.keys("tab")
	h.typeText(`{"n":`)
	h.keys("ctrl+s")
	h.wantView("payload is not valid JSON")
	wantCalls(t, f)

	h.typeText("1}")
	h.keys("ctrl+s")
	h.wantView(`Publish 7 bytes to topic orders on dev?`)
	h.keys("y")
	wantCalls(t, f, `Publish orders 1 {"n":1}`)
	msgs := f.Messages("orders", 1)
	last := msgs[len(msgs)-1]
	if string(last.Key) != "k-9" || len(last.Headers) != 1 || last.Headers[0].Key != "trace" || string(last.Headers[0].Value) != "abc" {
		t.Errorf("published %+v", last)
	}
	h.wantStatus("published", false)
}

func TestPublishFieldsFollowBroker(t *testing.T) {
	_, c := fakeCtx(t, "rabbit", false, rabbitCaps)
	h := newHarness(t, Options{}, c)
	keys := map[string]bool{}
	for _, fd := range publishFields(h.a.e) {
		keys[fd.key] = true
	}
	if !keys["exchange"] || !keys["routing_key"] || !keys["properties"] || keys["partition"] {
		t.Errorf("rabbit-like publish fields = %v", keys)
	}
}
