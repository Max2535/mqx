package tui

import (
	"fmt"
	"strconv"
	"strings"
)

// Hints are notes about what a form is about to do, computed from its values
// and what the last listings showed about the cluster. The form shows them
// while the user types and the confirm dialog repeats them. They never block:
// what would certainly fail belongs in the action's check.

// createTopicHints advises on a new topic or queue.
func createTopicHints(e *env, v values) []string {
	var hints []string
	name := v["name"]
	if _, ok := e.topics[name]; ok && name != "" {
		hints = append(hints, fmt.Sprintf("%s %s already exists", e.kind(), name))
	}
	if e.kafkaLike() {
		hints = append(hints, kafkaTopicHints(e, v)...)
	}
	if e.rabbitLike() {
		hints = append(hints, queueHints(v)...)
	}
	return hints
}

func kafkaTopicHints(e *env, v values) []string {
	var hints []string
	name := v["name"]
	if strings.HasPrefix(name, "__") {
		hints = append(hints, "names starting with __ are used by Kafka's internal topics")
	}
	if strings.Contains(name, ".") && strings.Contains(name, "_") {
		hints = append(hints, "names mixing '.' and '_' can collide in metric names (a.b and a_b report as one)")
	}
	rf, rfErr := strconv.Atoi(v["replication"])
	switch {
	case rfErr != nil:
	case e.nodes > 0 && rf > e.nodes:
		hints = append(hints, fmt.Sprintf("replication factor %d is more than the cluster's broker count (%d), so not every copy can be placed", rf, e.nodes))
	case rf == 1 && e.nodes >= 3:
		hints = append(hints, "replication factor 1 keeps one copy: losing that broker makes its partitions unavailable (3 is usual)")
	}
	if configs, err := parseKVMap(v["configs"]); err == nil && rfErr == nil {
		if isr, err := strconv.Atoi(configs["min.insync.replicas"]); err == nil && isr > rf {
			hints = append(hints, fmt.Sprintf("min.insync.replicas=%d is more than the replication factor %d: producers with acks=all will always fail", isr, rf))
		}
	}
	return hints
}

func queueHints(v values) []string {
	var hints []string
	args, _ := parseArgs(v["arguments"])
	if kind, _ := args["x-queue-type"].(string); kind == "quorum" || kind == "stream" {
		if durable, err := parseYes(v["durable"], true); err == nil && !durable {
			hints = append(hints, kind+" queues are always durable; RabbitMQ refuses durable=no")
		}
		if auto, err := parseYes(v["auto_delete"], false); err == nil && auto {
			hints = append(hints, kind+" queues cannot be auto-delete; RabbitMQ refuses it")
		}
		return hints
	}
	if durable, err := parseYes(v["durable"], true); err == nil && !durable {
		hints = append(hints, "a non-durable queue is gone after a broker restart")
	}
	return hints
}

// addPartitionsHints advises on growing topic to the requested count.
func addPartitionsHints(e *env, topic string, v values) []string {
	n, err := strconv.Atoi(strings.TrimSpace(v["total"]))
	t, known := e.topics[topic]
	if err != nil || !known || t.Partitions <= 0 {
		return nil
	}
	if n <= t.Partitions {
		return []string{fmt.Sprintf("%s has %d partitions; Kafka can only add, so the count must be above %d", topic, t.Partitions, t.Partitions)}
	}
	return []string{fmt.Sprintf("going from %d to %d partitions moves keys to other partitions: "+
		"new messages for a key may land elsewhere than its older ones, so per-key order is not kept across the change", t.Partitions, n)}
}

// tooFewPartitions is the add-partitions check: Kafka only grows topics.
func tooFewPartitions(e *env, topic string, n int) error {
	if t, ok := e.topics[topic]; ok && t.Partitions > 0 && n <= t.Partitions {
		return fmt.Errorf("%s already has %d partitions; enter a larger count", topic, t.Partitions)
	}
	return nil
}

// tooManyReplicas is the create-topic check against the known broker count.
func tooManyReplicas(e *env, v values) error {
	if rf, err := strconv.Atoi(v["replication"]); err == nil && e.nodes > 0 && rf > e.nodes {
		return fmt.Errorf("replication factor %d is more than the cluster's broker count (%d)", rf, e.nodes)
	}
	return nil
}
