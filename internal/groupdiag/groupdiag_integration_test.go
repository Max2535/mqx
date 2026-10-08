//go:build integration

package groupdiag

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Max2535/mqx/internal/broker"
	_ "github.com/Max2535/mqx/internal/broker/kafka" // registers the kafka driver
	"github.com/Max2535/mqx/internal/config"
	"github.com/Max2535/mqx/internal/testutil/kafkatest"
)

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
	_ = c.Terminate(context.Background())
	os.Exit(code)
}

var seq atomic.Int64

func unique(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano()%1e6, seq.Add(1))
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func openBroker(t *testing.T) broker.Broker {
	t.Helper()
	b, err := broker.Open(ctxT(t), config.Context{Name: "it", Broker: "kafka", Brokers: []string{cluster.Brokers}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func createTopic(t *testing.T, b broker.Broker, partitions int32) string {
	t.Helper()
	name := unique("diag")
	admin := b.(broker.TopicAdmin)
	if err := admin.CreateTopic(ctxT(t), broker.TopicSpec{Name: name, Partitions: partitions, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	return name
}

func publish(t *testing.T, b broker.Broker, topic string, partition int32, n int) {
	t.Helper()
	for range n {
		if err := b.Publish(ctxT(t), topic, broker.Message{Partition: partition, Value: []byte("m")}); err != nil {
			t.Fatal(err)
		}
	}
}

func startMember(t *testing.T, group string, topics []string, opts ...kafkatest.MemberOption) *kafkatest.Member {
	t.Helper()
	m, err := kafkatest.StartMember(cluster.Brokers, group, topics, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)
	return m
}

// waitStable waits until the group is Stable with n members.
func waitStable(t *testing.T, b broker.Broker, group string, n int) {
	t.Helper()
	gi := b.(broker.GroupInspector)
	deadline := time.Now().Add(60 * time.Second)
	var d *broker.GroupDescription
	var err error
	for time.Now().Before(deadline) {
		d, err = gi.DescribeGroup(ctxT(t), group)
		if err == nil && d.State == "Stable" && len(d.Members) == n {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("group %s not Stable with %d members: %+v, %v", group, n, d, err)
}

// waitAssigned waits until the group's members hold n partitions in total.
// Under the consumer protocol (KIP-848) a group reports Stable before members
// have reconciled their assignments, so Stable alone is not enough.
func waitAssigned(t *testing.T, b broker.Broker, group string, n int) {
	t.Helper()
	gi := b.(broker.GroupInspector)
	deadline := time.Now().Add(60 * time.Second)
	var d *broker.GroupDescription
	var err error
	for time.Now().Before(deadline) {
		d, err = gi.DescribeGroup(ctxT(t), group)
		if err == nil {
			got := 0
			for _, m := range d.Members {
				for _, tp := range m.Assignment {
					got += len(tp.Partitions)
				}
			}
			if got == n {
				return
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("group %s members do not hold %d partitions: %+v, %v", group, n, d, err)
}

func findRule(fs []Finding, rule string) (Finding, bool) {
	i := slices.IndexFunc(fs, func(f Finding) bool { return f.Rule == rule })
	if i < 0 {
		return Finding{}, false
	}
	return fs[i], true
}

func collect(t *testing.T, b broker.Broker, group string) Snapshot {
	t.Helper()
	s, err := Collect(ctxT(t), b, group)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// A member that dies without leaving keeps its partitions until its session
// times out: its lag grows while its committed offsets stay put.
func TestDiagnoseStaleMember(t *testing.T) {
	for _, tt := range []struct {
		name   string
		static bool
	}{{name: "dynamic member"}, {name: "static member", static: true}} {
		t.Run(tt.name, func(t *testing.T) {
			b := openBroker(t)
			topic := createTopic(t, b, 2)
			publish(t, b, topic, 0, 3)
			publish(t, b, topic, 1, 3)
			group := unique("stale")
			startMember(t, group, []string{topic})
			opts := []kgo.Opt{kgo.SessionTimeout(60 * time.Second)} // keeps the dead member around for the test
			if tt.static {
				opts = append(opts, kgo.InstanceID("pod-dead"))
			}
			victim := startMember(t, group, []string{topic}, kafkatest.WithOpts(opts...))
			waitStable(t, b, group, 2)
			time.Sleep(2 * time.Second) // both consume and commit what exists
			victim.Kill()

			first := collect(t, b, group)
			publish(t, b, topic, 0, 5)
			publish(t, b, topic, 1, 5)
			time.Sleep(3 * time.Second) // the live member consumes and commits its partition
			last := collect(t, b, group)

			findings := Diagnose([]Snapshot{first, last})
			f, ok := findRule(findings, RuleStaleMember)
			if !ok {
				t.Fatalf("no stale-member finding: %+v", findings)
			}
			if len(f.Members) != 1 || f.Severity != SeverityError {
				t.Errorf("finding = %+v", f)
			}
			if tt.static && !slices.ContainsFunc(last.Group.Members, func(m broker.GroupMember) bool {
				return m.MemberID == f.Members[0] && m.InstanceID == "pod-dead"
			}) {
				t.Errorf("flagged member %v is not the killed static member", f.Members)
			}
			if len(findings) > 0 && findings[0].Rule != RuleStaleMember {
				t.Errorf("stale-member is not the first finding: %+v", findings)
			}
		})
	}
}

func TestDiagnoseMoreMembersThanPartitions(t *testing.T) {
	for _, tt := range []struct {
		name string
		opts []kafkatest.MemberOption
	}{{name: "classic"}, {name: "consumer protocol", opts: []kafkatest.MemberOption{kafkatest.ConsumerProtocol()}}} {
		t.Run(tt.name, func(t *testing.T) {
			b := openBroker(t)
			topic := createTopic(t, b, 1)
			group := unique("crowd")
			for range 3 {
				startMember(t, group, []string{topic}, tt.opts...)
			}
			waitStable(t, b, group, 3)
			waitAssigned(t, b, group, 1)
			f, ok := findRule(Diagnose([]Snapshot{collect(t, b, group)}), RuleMoreMembers)
			if !ok {
				t.Fatal("no more-members-than-partitions finding")
			}
			if len(f.Members) != 2 {
				t.Errorf("idle members = %v, want 2", f.Members)
			}
		})
	}
}

func TestDiagnoseSharedGroupID(t *testing.T) {
	for _, tt := range []struct {
		name string
		opts []kafkatest.MemberOption
	}{{name: "classic"}, {name: "consumer protocol", opts: []kafkatest.MemberOption{kafkatest.ConsumerProtocol()}}} {
		t.Run(tt.name, func(t *testing.T) {
			b := openBroker(t)
			orders, payments := createTopic(t, b, 1), createTopic(t, b, 1)
			group := unique("shared")
			startMember(t, group, []string{orders}, tt.opts...)
			startMember(t, group, []string{payments}, tt.opts...)
			waitStable(t, b, group, 2)
			f, ok := findRule(Diagnose([]Snapshot{collect(t, b, group)}), RuleSharedGroupID)
			if !ok {
				t.Fatal("no shared-group-id finding")
			}
			if len(f.Members) != 2 {
				t.Errorf("members = %v", f.Members)
			}
		})
	}
}

// The watcher sees a join, the rebalance and its completion on a real group.
func TestWatcherOnRealGroup(t *testing.T) {
	b := openBroker(t)
	topic := createTopic(t, b, 2)
	group := unique("watch")
	gi := b.(broker.GroupInspector)
	startMember(t, group, []string{topic})
	waitStable(t, b, group, 1)
	w := NewWatcher()
	d, err := gi.DescribeGroup(ctxT(t), group)
	if err != nil {
		t.Fatal(err)
	}
	w.Observe(d)
	startMember(t, group, []string{topic})
	var kinds []EventKind
	deadline := time.Now().Add(60 * time.Second)
	done := func() bool { return slices.Contains(kinds, EventJoin) && len(d.Members) == 2 && d.State == "Stable" }
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("events so far: %v", kinds)
		}
		time.Sleep(100 * time.Millisecond)
		if d, err = gi.DescribeGroup(ctxT(t), group); err != nil {
			t.Fatal(err)
		}
		for _, e := range w.Observe(d) {
			kinds = append(kinds, e.Kind)
		}
	}
	if !slices.Contains(kinds, EventJoin) || !slices.Contains(kinds, EventAssignment) {
		t.Errorf("events = %v, want a join and an assignment change", kinds)
	}
}
