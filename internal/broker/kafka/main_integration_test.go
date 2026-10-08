//go:build integration

package kafka

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/config"
	"github.com/Max2535/mqx/internal/testutil/kafkatest"
)

// cluster is the broker shared by every integration test of the package.
var cluster *kafkatest.Cluster

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	c, err := kafkatest.Start(ctx)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start kafka:", err)
		os.Exit(1)
	}
	cluster = c
	code := m.Run()
	if err := c.Terminate(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "terminate kafka:", err)
	}
	os.Exit(code)
}

var seq atomic.Int64

// unique returns a name unique within the test run.
func unique(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano()%1e6, seq.Add(1))
}

// testContext returns the config context of the shared cluster.
func testContext() config.Context {
	return config.Context{Name: "it", Broker: Type, Brokers: []string{cluster.Brokers}}
}

// openKafka opens the adapter against the shared cluster, with optional endpoints set by mod.
func openKafka(t *testing.T, mod ...func(*config.Context)) *Kafka {
	t.Helper()
	c := testContext()
	for _, m := range mod {
		m(&c)
	}
	b, err := open(context.Background(), c, config.Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b.(*Kafka)
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// createTopic creates a topic with n partitions.
func createTopic(t *testing.T, k *Kafka, n int32) string {
	t.Helper()
	name := unique("t")
	if err := k.CreateTopic(ctxT(t), broker.TopicSpec{Name: name, Partitions: n, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	return name
}

// collect drains a peek.
func collect(t *testing.T, ch <-chan broker.Message) []broker.Message {
	t.Helper()
	var out []broker.Message
	timeout := time.After(45 * time.Second)
	for {
		select {
		case m, ok := <-ch:
			if !ok {
				return out
			}
			if m.Err != nil {
				t.Fatalf("peek error: %v", m.Err)
			}
			out = append(out, m)
		case <-timeout:
			t.Fatalf("peek did not finish; got %d messages", len(out))
		}
	}
}
