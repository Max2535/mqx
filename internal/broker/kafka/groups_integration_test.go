//go:build integration

package kafka

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/testutil/kafkatest"
)

// commitGroup commits offsets for an empty group.
func commitGroup(t *testing.T, k *Kafka, group, topic string, offsets map[int32]int64) {
	t.Helper()
	os := kadm.Offsets{}
	for p, o := range offsets {
		os.Add(kadm.Offset{Topic: topic, Partition: p, At: o, LeaderEpoch: -1})
	}
	if err := k.adm.CommitAllOffsets(ctxT(t), group, os); err != nil {
		t.Fatal(err)
	}
}

func chg(topic string, p int32, old, cur int64) broker.OffsetChange {
	return broker.OffsetChange{Topic: topic, Partition: p, Old: old, New: cur}
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

// waitGroup polls DescribeGroup until ok accepts it.
func waitGroup(t *testing.T, k *Kafka, group string, ok func(*broker.GroupDescription) bool) *broker.GroupDescription {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	var last *broker.GroupDescription
	for time.Now().Before(deadline) {
		d, err := k.DescribeGroup(ctxT(t), group)
		if err == nil && ok(d) {
			return d
		}
		last = d
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("group %s never reached the expected state; last: %+v", group, last)
	return nil
}

func stableWith(n int) func(*broker.GroupDescription) bool {
	return func(d *broker.GroupDescription) bool {
		if d.State != "Stable" || len(d.Members) != n {
			return false
		}
		for _, m := range d.Members {
			if len(m.Assignment) == 0 {
				return false
			}
		}
		return true
	}
}

func publishN(t *testing.T, k *Kafka, topic string, n int) {
	t.Helper()
	for range n {
		if err := k.Publish(ctxT(t), topic, broker.NewMessage([]byte("m"))); err != nil {
			t.Fatal(err)
		}
	}
}

func TestClassicGroup(t *testing.T) {
	k := openKafka(t)
	topic := createTopic(t, k, 2)
	publishN(t, k, topic, 6)
	group := unique("classic")
	m := startMember(t, group, []string{topic})
	d := waitGroup(t, k, group, stableWith(1))
	if d.GroupProtocol != "classic" || d.ProtocolType != "consumer" || d.Assignor != "cooperative-sticky" || d.Epoch != -1 {
		t.Errorf("description = %+v", d)
	}
	if d.Coordinator.ID != "1" || d.Coordinator.Host == "" {
		t.Errorf("coordinator = %+v", d.Coordinator)
	}
	mem := d.Members[0]
	if !slices.Equal(mem.Subscriptions, []string{topic}) || len(mem.Assignment) != 1 ||
		!slices.Equal(mem.Assignment[0].Partitions, []int32{0, 1}) || !strings.HasPrefix(mem.ClientID, "mqx-test-") {
		t.Errorf("member = %+v", mem)
	}

	assertGroupViews(t, k, group, topic, mem.MemberID, m)

	// Reset refuses a group with members.
	if _, err := k.ResetOffsets(ctxT(t), group, broker.OffsetReset{Topic: topic, To: broker.Position{Kind: broker.Earliest}}); err == nil ||
		!strings.Contains(err.Error(), "active members") {
		t.Errorf("reset with members: err = %v", err)
	}
	if err := k.DeleteGroup(ctxT(t), group); err == nil {
		t.Error("deleting a group with members succeeded")
	}
}

// assertGroupViews checks ListGroups, Consumers and Lag once the member consumed everything.
func assertGroupViews(t *testing.T, k *Kafka, group, topic, memberID string, m *kafkatest.Member) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lag []broker.PartitionLag
	for time.Now().Before(deadline) {
		var err error
		if lag, err = k.Lag(ctxT(t), group); err != nil {
			t.Fatal(err)
		}
		if len(lag) == 2 && lag[0].Lag == 0 && lag[1].Lag == 0 && lag[0].Committed >= 0 && lag[1].Committed >= 0 {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if len(lag) != 2 || lag[0].End+lag[1].End != 6 || lag[0].MemberID != memberID || lag[0].Lag != 0 {
		t.Errorf("lag = %+v (consumed %d)", lag, m.Consumed())
	}

	groups, err := k.ListGroups(ctxT(t))
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(groups, func(g broker.GroupSummary) bool { return g.Name == group })
	if i < 0 || groups[i].Members != 1 || groups[i].State != "Stable" {
		t.Errorf("listed groups = %+v", groups)
	}

	consumers, err := k.Consumers(ctxT(t), topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(consumers) != 1 || consumers[0].Group != group || !slices.Equal(consumers[0].Partitions, []int32{0, 1}) {
		t.Errorf("consumers = %+v", consumers)
	}
}

func TestConsumerProtocolGroup(t *testing.T) {
	k := openKafka(t)
	topic := createTopic(t, k, 2)
	publishN(t, k, topic, 6)
	group := unique("kip848")
	m := startMember(t, group, []string{topic}, kafkatest.ConsumerProtocol())
	d := waitGroup(t, k, group, stableWith(1))
	if d.GroupProtocol != "consumer" || d.Epoch <= 0 || d.Assignor == "" {
		t.Errorf("description = %+v", d)
	}
	mem := d.Members[0]
	if mem.MemberEpoch <= 0 || len(mem.Target) != 1 || !slices.Equal(mem.Subscriptions, []string{topic}) {
		t.Errorf("member = %+v", mem)
	}
	assertGroupViews(t, k, group, topic, mem.MemberID, m)
}

func TestResetAndDeleteGroup(t *testing.T) {
	k := openKafka(t)
	topic := createTopic(t, k, 2)
	publishN(t, k, topic, 10) // spread by the sticky partitioner; ends vary per partition
	m, _ := k.watermarks(ctxT(t), topic)
	_, end0 := m.get(topic, 0)
	_, end1 := m.get(topic, 1)
	group := unique("reset")
	commitGroup(t, k, group, topic, map[int32]int64{0: 1})

	tests := []struct {
		name string
		req  broker.OffsetReset
		want []broker.OffsetChange
	}{
		{name: "dry run to earliest", req: broker.OffsetReset{Topic: topic, DryRun: true},
			want: []broker.OffsetChange{chg(topic, 0, 1, 0), chg(topic, 1, -1, 0)}},
		{name: "latest on one partition", req: broker.OffsetReset{Topic: topic, Partitions: []int32{1}, To: broker.Position{Kind: broker.Latest}},
			want: []broker.OffsetChange{chg(topic, 1, -1, end1)}},
		{name: "offset clamped to end", req: broker.OffsetReset{Topic: topic, Partitions: []int32{0}, To: broker.Position{Kind: broker.AtOffset, Offset: 999}},
			want: []broker.OffsetChange{chg(topic, 0, 1, end0)}},
		{name: "shift back", req: broker.OffsetReset{Topic: topic, Shift: -1},
			want: []broker.OffsetChange{chg(topic, 0, end0, end0-1), chg(topic, 1, end1, end1-1)}},
		{name: "time in the future means end", req: broker.OffsetReset{Topic: topic, To: broker.Position{Kind: broker.AtTime, Time: time.Now().Add(time.Hour)}},
			want: []broker.OffsetChange{chg(topic, 0, end0-1, end0), chg(topic, 1, end1-1, end1)}},
		{name: "time in the past means start", req: broker.OffsetReset{Topic: topic, To: broker.Position{Kind: broker.AtTime, Time: time.Now().Add(-time.Hour)}},
			want: []broker.OffsetChange{chg(topic, 0, end0, 0), chg(topic, 1, end1, 0)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := k.ResetOffsets(ctxT(t), group, tt.req)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("changes = %+v, want %+v", got, tt.want)
			}
		})
	}
	lag, err := k.Lag(ctxT(t), group)
	if err != nil || len(lag) != 2 || lag[0].Committed != 0 || lag[0].Lag != end0 {
		t.Errorf("lag after reset = %+v, %v", lag, err)
	}
	if _, err := k.ResetOffsets(ctxT(t), group, broker.OffsetReset{Topic: unique("missing")}); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("reset on missing topic: err = %v, want ErrNotFound", err)
	}
	if err := k.DeleteGroup(ctxT(t), group); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DescribeGroup(ctxT(t), group); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("describe deleted group: err = %v, want ErrNotFound", err)
	}
	if err := k.DeleteGroup(ctxT(t), group); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("delete missing group: err = %v, want ErrNotFound", err)
	}
	if _, err := k.Lag(ctxT(t), group); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("lag of missing group: err = %v, want ErrNotFound", err)
	}
}

func TestRemoveStaticMember(t *testing.T) {
	k := openKafka(t)
	topic := createTopic(t, k, 2)
	group := unique("static")
	m := startMember(t, group, []string{topic}, kafkatest.WithOpts(kgo.InstanceID("pod-1")))
	waitGroup(t, k, group, stableWith(1))
	m.Kill()
	time.Sleep(time.Second)
	d, err := k.DescribeGroup(ctxT(t), group)
	if err != nil || len(d.Members) != 1 || d.Members[0].InstanceID != "pod-1" {
		t.Fatalf("static member did not linger after an unclean shutdown: %+v, %v", d, err)
	}

	err = k.TerminateConsumer(ctxT(t), broker.ConsumerTarget{Group: group, InstanceID: "pod-2"})
	if !errors.Is(err, broker.ErrNotFound) || !strings.Contains(err.Error(), "pod-1") {
		t.Errorf("unknown instance: err = %v", err)
	}
	if err := k.TerminateConsumer(ctxT(t), broker.ConsumerTarget{Group: group, InstanceID: "pod-1", Reason: "mqx test"}); err != nil {
		t.Fatal(err)
	}
	waitGroup(t, k, group, func(d *broker.GroupDescription) bool { return len(d.Members) == 0 })
	if err := k.TerminateConsumer(ctxT(t), broker.ConsumerTarget{Connection: "x"}); !errors.Is(err, broker.ErrUnsupported) {
		t.Errorf("connection target: err = %v, want ErrUnsupported", err)
	}
}
