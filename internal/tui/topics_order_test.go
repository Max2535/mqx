package tui

import (
	"context"
	"reflect"
	"testing"

	"github.com/Max2535/mqx/internal/broker"
)

// internalTopics is a core-only broker whose listing includes internal topics.
type internalTopics struct{ broker.Broker }

func (internalTopics) Name() string { return "stub" }

func (internalTopics) ListTopics(context.Context) ([]broker.Topic, error) {
	return []broker.Topic{
		{Name: "__consumer_offsets", Internal: true},
		{Name: "orders"},
		{Name: "__transaction_state", Internal: true},
		{Name: "audit"},
	}, nil
}

func TestLoadTopicsPutsInternalLast(t *testing.T) {
	got, err := loadTopics(context.Background(), &env{b: internalTopics{}})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, r := range got.rows {
		names = append(names, r.key)
	}
	want := []string{"audit", "orders", "__consumer_offsets", "__transaction_state"}
	if !reflect.DeepEqual(names, want) {
		t.Errorf("order = %v, want %v", names, want)
	}
}
