//go:build integration

package kafka

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Max2535/mqx/internal/broker"
)

func configValue(entries []broker.ConfigEntry, name string) (broker.ConfigEntry, bool) {
	for _, e := range entries {
		if e.Name == name {
			return e, true
		}
	}
	return broker.ConfigEntry{}, false
}

func TestTopicAdmin(t *testing.T) {
	k := openKafka(t)
	ctx := ctxT(t)
	if err := k.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	name := unique("admin")
	spec := broker.TopicSpec{Name: name, Partitions: 2, ReplicationFactor: 1, Configs: map[string]string{"retention.ms": "3600000"}}
	if err := k.CreateTopic(ctx, spec); err != nil {
		t.Fatal(err)
	}
	if err := k.CreateTopic(ctx, spec); err == nil {
		t.Error("creating a topic twice succeeded")
	}
	for i := range 6 {
		if err := k.Publish(ctx, name, broker.Message{Partition: int32(i % 2), Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}

	topics, err := k.ListTopics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(topics, func(tp broker.Topic) bool { return tp.Name == name })
	if i < 0 {
		t.Fatalf("topic %s not listed", name)
	}
	if got := topics[i]; got.Kind != "topic" || got.Partitions != 2 || got.Replicas != 1 || got.Messages != 6 || got.Consumers != 0 {
		t.Errorf("listed topic = %+v", got)
	}

	d, err := k.DescribeTopic(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Partitions) != 2 || d.Partitions[1].End != 3 || d.Partitions[0].Leader != 1 || len(d.Partitions[0].ISR) != 1 {
		t.Errorf("partitions = %+v", d.Partitions)
	}
	if e, ok := configValue(d.Configs, "retention.ms"); !ok || e.Value != "3600000" || e.Source != "DYNAMIC_TOPIC_CONFIG" || e.IsDefault() {
		t.Errorf("retention.ms = %+v", e)
	}

	if err := k.AlterTopicConfig(ctx, name, map[string]string{"retention.ms": "", "cleanup.policy": "delete", "max.message.bytes": "2048"}); err != nil {
		t.Fatal(err)
	}
	cfg, err := k.TopicConfig(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if e, _ := configValue(cfg, "retention.ms"); !e.IsDefault() {
		t.Errorf("retention.ms override not deleted: %+v", e)
	}
	if e, _ := configValue(cfg, "max.message.bytes"); e.Value != "2048" {
		t.Errorf("max.message.bytes = %+v", e)
	}
	if err := k.AlterTopicConfig(ctx, name, map[string]string{"no.such.config": "1"}); err == nil {
		t.Error("altering an unknown config succeeded")
	}

	if err := k.AddPartitions(ctx, name, 2); err == nil {
		t.Error("shrinking partitions succeeded")
	}
	if err := k.AddPartitions(ctx, name, 4); err != nil {
		t.Fatal(err)
	}
	d, _ = k.DescribeTopic(ctx, name)
	if len(d.Partitions) != 4 {
		t.Errorf("partitions after add = %d, want 4", len(d.Partitions))
	}

	res, err := k.Purge(ctx, name, broker.PurgeOptions{Partitions: []int32{0}, Before: &broker.Position{Kind: broker.AtOffset, Offset: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Messages != 2 || len(res.Partitions) != 1 || res.Partitions[0].LowMark != 2 {
		t.Errorf("purge before offset = %+v", res)
	}
	res, err = k.Purge(ctx, name, broker.PurgeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Messages != 4 || len(res.Partitions) != 4 || res.Partitions[1].LowMark != 3 {
		t.Errorf("purge all = %+v", res)
	}

	if err := k.DeleteTopic(ctx, name); err != nil {
		t.Fatal(err)
	}
	if err := k.DeleteTopic(ctx, name); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("delete missing topic: err = %v, want ErrNotFound", err)
	}
	if _, err := k.DescribeTopic(ctx, name); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("describe deleted topic: err = %v, want ErrNotFound", err)
	}
}

func TestClusterAndMetrics(t *testing.T) {
	k := openKafka(t)
	ctx := ctxT(t)
	nodes, err := k.Nodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].ID != "1" || !nodes[0].Controller || nodes[0].Port == 0 {
		t.Fatalf("nodes = %+v", nodes)
	}
	cfg, err := k.NodeConfig(ctx, "1")
	if err != nil {
		t.Fatal(err)
	}
	if e, ok := configValue(cfg, "node.id"); !ok || e.Value != "1" || !e.ReadOnly {
		t.Errorf("node.id = %+v", e)
	}
	if _, err := k.NodeConfig(ctx, "x"); err == nil {
		t.Error("NodeConfig(x) succeeded")
	}

	topic := createTopic(t, k, 2)
	before, err := k.Sample(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if err := k.Publish(ctx, topic, broker.NewMessage([]byte("m"))); err != nil {
			t.Fatal(err)
		}
	}
	commitGroup(t, k, unique("metrics"), topic, map[int32]int64{0: 0, 1: 0})
	after, err := k.Sample(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if d := after.Counters[broker.MetricMessagesIn] - before.Counters[broker.MetricMessagesIn]; d != 5 {
		t.Errorf("messages_in grew by %v, want 5", d)
	}
	if after.Gauges[broker.MetricLag] != 5 || after.Gauges["partitions"] != 2 {
		t.Errorf("gauges = %v", after.Gauges)
	}
	if _, err := k.Sample(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if !after.Time.After(time.Now().Add(-time.Minute)) {
		t.Errorf("sample time = %v", after.Time)
	}
}

func TestACLs(t *testing.T) {
	k := openKafka(t)
	ctx := ctxT(t)
	user := "User:" + unique("alice")
	acls := []broker.ACL{
		{Principal: user, ResourceType: "topic", ResourceName: "orders", Operation: "read", Permission: "allow"},
		{Principal: user, ResourceType: "group", ResourceName: "billing-", PatternType: "prefixed", Operation: "READ", Permission: "Allow"},
		{Principal: user, ResourceType: "cluster", Operation: "describe", Permission: "deny", Host: "10.0.0.1"},
	}
	for _, a := range acls {
		if err := k.CreateACL(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	var got []broker.ACL
	deadline := time.Now().Add(10 * time.Second)
	for len(got) < 3 && time.Now().Before(deadline) { // ACLs propagate through the metadata log
		var err error
		if got, err = k.ACLs(ctx, broker.ACL{Principal: user}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	want := []broker.ACL{
		{Principal: user, Host: "10.0.0.1", ResourceType: "cluster", ResourceName: "kafka-cluster", PatternType: "literal", Operation: "describe", Permission: "deny"},
		{Principal: user, Host: "*", ResourceType: "group", ResourceName: "billing-", PatternType: "prefixed", Operation: "read", Permission: "allow"},
		{Principal: user, Host: "*", ResourceType: "topic", ResourceName: "orders", PatternType: "literal", Operation: "read", Permission: "allow"},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("ACLs() =\n%+v\nwant\n%+v", got, want)
	}
	topicOnly, err := k.ACLs(ctx, broker.ACL{Principal: user, ResourceType: "TOPIC"})
	if err != nil || len(topicOnly) != 1 {
		t.Errorf("topic filter = %+v, %v", topicOnly, err)
	}
	deleted, err := k.DeleteACLs(ctx, broker.ACL{Principal: user, Permission: "allow"})
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 2 {
		t.Errorf("deleted = %+v, want the 2 allow ACLs", deleted)
	}
	if err := k.CreateACL(ctx, broker.ACL{Principal: user, ResourceType: "topic", ResourceName: "x", Operation: "any", Permission: "allow"}); err == nil {
		t.Error("creating an ACL with operation any succeeded")
	}
	_, _ = k.DeleteACLs(ctx, broker.ACL{Principal: user})
}
