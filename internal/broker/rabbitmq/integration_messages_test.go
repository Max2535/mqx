//go:build integration

package rabbitmq_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Max2535/mqx/internal/broker"
)

func TestPing(t *testing.T) {
	e := newEnv(t)
	must(t, e.b.Ping(testCtx(t)))

	t.Setenv("MQX_IT_WRONG_PASSWORD", "wrong")
	c := srv.Context("bad", e.vhost)
	c.PasswordEnv = "MQX_IT_WRONG_PASSWORD"
	b, err := broker.Open(testCtx(t), c)
	must(t, err)
	defer b.Close()
	err = b.Ping(testCtx(t))
	if err == nil || !strings.Contains(err.Error(), "username_env/password_env") || strings.Contains(err.Error(), "wrong") {
		t.Errorf("Ping with a wrong password = %v", err)
	}
}

func TestPublishAndPeek(t *testing.T) {
	e := newEnv(t)
	ctx := testCtx(t)
	durableQueue(t, e, "orders", nil)
	for i := range 10 {
		m := broker.NewMessage([]byte(fmt.Sprintf("m%d", i)))
		m.Key = []byte(fmt.Sprintf("k%d", i))
		m.Headers = []broker.Header{{Key: "parity", Value: []byte(strconv.Itoa(i % 2))}}
		m.Properties = map[string]string{
			"content_type": "text/plain", "correlation_id": fmt.Sprintf("c%d", i), "priority": "1",
			"app_id": "mqx-it", "type": "order", "reply_to": "replies",
		}
		must(t, e.b.Publish(ctx, "orders", m))
	}
	conn := e.dial(t, "it-observer")

	all := e.peek(t, "orders", broker.PeekOptions{})
	want := []string{"m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9"}
	if got := values(all); !reflect.DeepEqual(got, want) {
		t.Fatalf("peek = %v, want %v", got, want)
	}
	m := all[3]
	wantProps := map[string]string{
		"content_type": "text/plain", "correlation_id": "c3", "priority": "1", "app_id": "mqx-it", "type": "order",
		"reply_to": "replies", "message_id": "k3", "delivery_mode": "2", "timestamp": m.Properties["timestamp"],
	}
	if string(m.Key) != "k3" || m.Offset != 3 || m.RoutingKey != "orders" || m.Exchange != "" ||
		m.Timestamp.IsZero() || !reflect.DeepEqual(m.Properties, wantProps) {
		t.Errorf("message = %+v", m)
	}
	if v, ok := m.HeaderValue("parity"); !ok || string(v) != "1" {
		t.Errorf("header parity = %q", v)
	}
	if d := e.depth(t, conn, "orders"); d != 10 {
		t.Fatalf("depth after peek = %d, want 10", d)
	}

	again := e.peek(t, "orders", broker.PeekOptions{})
	if got := values(again); !reflect.DeepEqual(got, want) || !again[0].Redelivered {
		t.Errorf("second peek = %v (redelivered %v), want same order", got, again[0].Redelivered)
	}

	tests := []struct {
		name string
		opts broker.PeekOptions
		want []string
	}{
		{"limit", broker.PeekOptions{Limit: 3}, []string{"m0", "m1", "m2"}},
		{"header filter and limit", broker.PeekOptions{Limit: 2, Filter: broker.Filter{
			Headers: []broker.HeaderMatch{{Key: "parity", Value: regexp.MustCompile("^1$")}},
		}}, []string{"m1", "m3"}},
		{"key filter", broker.PeekOptions{Filter: broker.Filter{Key: regexp.MustCompile("k[7-9]")}}, []string{"m7", "m8", "m9"}},
		{"value filter", broker.PeekOptions{Filter: broker.Filter{Value: regexp.MustCompile("^m5$")}}, []string{"m5"}},
		{"since in the future", broker.PeekOptions{Filter: broker.Filter{Since: time.Now().Add(time.Hour)}}, nil},
		{"until in the future", broker.PeekOptions{Limit: 1, Filter: broker.Filter{Until: time.Now().Add(time.Hour)}}, []string{"m0"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := values(e.peek(t, "orders", tt.opts))
			if len(got) == 0 {
				got = nil
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("peek = %v, want %v", got, tt.want)
			}
		})
	}
	if d := e.depth(t, conn, "orders"); d != 10 {
		t.Fatalf("depth after filtered peeks = %d, want 10", d)
	}

	for _, opts := range []broker.PeekOptions{
		{Follow: true}, {From: broker.Position{Kind: broker.Latest}}, {Partitions: []int32{0}},
	} {
		if _, err := e.b.Peek(ctx, "orders", opts); !errors.Is(err, broker.ErrUnsupported) {
			t.Errorf("Peek(%+v) = %v, want ErrUnsupported", opts, err)
		}
	}
	if _, err := e.b.Peek(ctx, "missing", broker.PeekOptions{}); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("Peek(missing) = %v, want ErrNotFound", err)
	}

	res, err := e.b.Purge(ctx, "orders", broker.PurgeOptions{})
	must(t, err)
	if res.Messages != 10 {
		t.Errorf("purged %d, want 10", res.Messages)
	}
	if got := e.peek(t, "orders", broker.PeekOptions{}); len(got) != 0 {
		t.Errorf("peek after purge = %v", values(got))
	}
}

func TestPublishErrors(t *testing.T) {
	e := newEnv(t)
	ctx := testCtx(t)
	err := e.b.Publish(ctx, "no-such-queue", broker.NewMessage([]byte("x")))
	if !errors.Is(err, broker.ErrNotFound) || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("publish to a missing queue = %v", err)
	}
	must(t, e.b.DeclareExchange(ctx, broker.Exchange{Name: "lonely", Type: "direct", Durable: true}))
	m := broker.NewMessage([]byte("x"))
	m.Exchange, m.RoutingKey = "lonely", "nowhere"
	err = e.b.Publish(ctx, "", m)
	if err == nil || !strings.Contains(err.Error(), `no queue is bound for routing key "nowhere"`) {
		t.Errorf("unroutable publish = %v", err)
	}
	m.Exchange = "no-such-exchange"
	if err := e.b.Publish(ctx, "", m); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("publish to a missing exchange = %v", err)
	}
	// The adapter recovers from the channel error above.
	durableQueue(t, e, "q", nil)
	must(t, e.b.Publish(ctx, "q", broker.NewMessage([]byte("ok"))))
	bad := broker.NewMessage(nil)
	bad.Properties = map[string]string{"colour": "red"}
	if err := e.b.Publish(ctx, "q", bad); err == nil || !strings.Contains(err.Error(), "unknown property") {
		t.Errorf("unknown property = %v", err)
	}
}

func TestPublishViaExchange(t *testing.T) {
	e := newEnv(t)
	ctx := testCtx(t)
	durableQueue(t, e, "q", nil)
	must(t, e.b.DeclareExchange(ctx, broker.Exchange{Name: "ex", Type: "topic", Durable: true}))
	must(t, e.b.Bind(ctx, broker.Binding{Source: "ex", Destination: "q", DestinationType: "queue", RoutingKey: "a.*"}))
	m := broker.NewMessage([]byte("routed"))
	m.Exchange, m.RoutingKey = "ex", "a.b"
	must(t, e.b.Publish(ctx, "", m))
	got := e.peek(t, "q", broker.PeekOptions{})
	if len(got) != 1 || got[0].Exchange != "ex" || got[0].RoutingKey != "a.b" {
		t.Errorf("peek = %+v", got)
	}
}

// Quorum queues dead-letter or drop a message after delivery-limit (20 by
// default) failed deliveries. Peek returns held messages with one explicit nack,
// which must not count, and waits until they are ready again.
func TestQuorumPeekIsRepeatable(t *testing.T) {
	e := newEnv(t)
	ctx := testCtx(t)
	durableQueue(t, e, "qq", map[string]any{"x-queue-type": "quorum"})
	must(t, e.b.Publish(ctx, "qq", broker.NewMessage([]byte("keep me"))))
	for i := range 25 {
		got := e.peek(t, "qq", broker.PeekOptions{})
		if len(got) != 1 {
			t.Fatalf("peek %d returned %d messages, want 1", i, len(got))
		}
	}
	if d := e.depth(t, e.dial(t, "obs"), "qq"); d != 1 {
		t.Errorf("depth = %d, want 1", d)
	}
}

func TestStreamPeek(t *testing.T) {
	e := newEnv(t)
	ctx := testCtx(t)
	durableQueue(t, e, "events", map[string]any{"x-queue-type": "stream"})
	start := time.Now()
	for i := range 20 {
		must(t, e.b.Publish(ctx, "events", broker.NewMessage([]byte(fmt.Sprintf("e%d", i)))))
	}
	offsets := func(ms []broker.Message) []int64 {
		out := make([]int64, len(ms))
		for i, m := range ms {
			out[i] = m.Offset
		}
		return out
	}
	got := e.peek(t, "events", broker.PeekOptions{Limit: 5})
	if !reflect.DeepEqual(offsets(got), []int64{0, 1, 2, 3, 4}) || string(got[0].Value) != "e0" {
		t.Errorf("earliest limit 5 = %v %v", offsets(got), values(got))
	}
	got = e.peek(t, "events", broker.PeekOptions{From: broker.Position{Kind: broker.AtOffset, Offset: 15}})
	if !reflect.DeepEqual(values(got), []string{"e15", "e16", "e17", "e18", "e19"}) {
		t.Errorf("from offset 15 = %v", values(got))
	}
	got = e.peek(t, "events", broker.PeekOptions{To: &broker.Position{Kind: broker.AtOffset, Offset: 3}})
	if !reflect.DeepEqual(values(got), []string{"e0", "e1", "e2"}) {
		t.Errorf("to offset 3 = %v", values(got))
	}
	got = e.peek(t, "events", broker.PeekOptions{From: broker.Position{Kind: broker.AtTime, Time: start.Add(-time.Minute)}})
	if len(got) != 20 {
		t.Errorf("from a time before the first message: %d messages, want 20", len(got))
	}
	got = e.peek(t, "events", broker.PeekOptions{From: broker.Position{Kind: broker.Latest}})
	if len(got) != 0 {
		t.Errorf("latest without follow = %v", values(got))
	}

	fctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	ch, err := e.b.Peek(fctx, "events", broker.PeekOptions{From: broker.Position{Kind: broker.Latest}, Follow: true, Limit: 1})
	must(t, err)
	time.Sleep(500 * time.Millisecond)
	must(t, e.b.Publish(ctx, "events", broker.NewMessage([]byte("live"))))
	got = collect(t, ch, nil)
	if !reflect.DeepEqual(values(got), []string{"live"}) || got[0].Offset != 20 {
		t.Errorf("follow = %v offsets %v", values(got), offsets(got))
	}
	if _, err := e.b.Peek(ctx, "events", broker.PeekOptions{From: broker.Position{Kind: broker.Tail, Offset: 3}}); !errors.Is(err, broker.ErrUnsupported) {
		t.Errorf("tail on a stream = %v", err)
	}
}
