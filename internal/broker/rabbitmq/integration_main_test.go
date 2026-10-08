//go:build integration

package rabbitmq_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/broker/rabbitmq"
	"github.com/Max2535/mqx/internal/testutil/rabbittest"
)

var srv *rabbittest.Server

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	var err error
	srv, err = rabbittest.Start(ctx)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, "rabbitmq integration tests:", err)
		os.Exit(1)
	}
	code := m.Run()
	if err := srv.Terminate(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "terminate rabbitmq container:", err)
	}
	os.Exit(code)
}

// env is an adapter on a fresh vhost of its own, deleted after the test.
type env struct {
	b     *rabbitmq.RabbitMQ
	vhost string
}

var unsafeChars = regexp.MustCompile(`[^A-Za-z0-9]+`)

func newEnv(t *testing.T) *env {
	t.Helper()
	// A slash and a space exercise percent-encoding of the vhost everywhere.
	vhost := "mqx it/" + unsafeChars.ReplaceAllString(t.Name(), "-")
	root := openVHost(t, "/")
	ctx := testCtx(t)
	must(t, root.PutVHost(ctx, broker.VHost{Name: vhost, Description: "mqx integration test"}))
	must(t, root.SetPermission(ctx, broker.Permission{User: srv.Username, VHost: vhost, Configure: ".*", Write: ".*", Read: ".*"}))
	t.Cleanup(func() {
		if err := root.DeleteVHost(context.Background(), vhost); err != nil {
			t.Logf("delete vhost %q: %v", vhost, err)
		}
	})
	return &env{b: openVHost(t, vhost), vhost: vhost}
}

func openVHost(t *testing.T, vhost string) *rabbitmq.RabbitMQ {
	t.Helper()
	b, err := broker.Open(testCtx(t), srv.Context("it", vhost))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b.(*rabbitmq.RabbitMQ)
}

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// dial opens a raw amqp091 connection to the env's vhost.
func (e *env) dial(t *testing.T, name string) *amqp.Connection {
	t.Helper()
	conn, err := amqp.DialConfig(srv.AMQPURL(e.vhost, true), amqp.Config{Properties: amqp.Table{"connection_name": name}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// depth reads a queue's message count directly over AMQP (management stats lag).
func (e *env) depth(t *testing.T, conn *amqp.Connection, queue string) int {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	q, err := ch.QueueDeclarePassive(queue, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("passive declare %q: %v", queue, err)
	}
	return q.Messages
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// eventually retries fn until it returns nil or the timeout passes.
func eventually(t *testing.T, timeout time.Duration, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := fn()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met after %s: %v", timeout, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// collect drains a peek stream, failing on a stream error.
func collect(t *testing.T, ch <-chan broker.Message, err error) []broker.Message {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	var out []broker.Message
	for m := range ch {
		if m.Err != nil {
			t.Fatalf("peek stream error: %v", m.Err)
		}
		out = append(out, m)
	}
	return out
}

func values(ms []broker.Message) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = string(m.Value)
	}
	return out
}

func durableQueue(t *testing.T, e *env, name string, args map[string]any) {
	t.Helper()
	must(t, e.b.CreateTopic(testCtx(t), broker.TopicSpec{Name: name, Durable: true, Arguments: args}))
}

var errNotYet = errors.New("not yet")

// peek runs Peek and drains it.
func (e *env) peek(t *testing.T, topic string, opts broker.PeekOptions) []broker.Message {
	t.Helper()
	ch, err := e.b.Peek(testCtx(t), topic, opts)
	return collect(t, ch, err)
}
