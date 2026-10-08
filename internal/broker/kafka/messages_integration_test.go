//go:build integration

package kafka

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Max2535/mqx/internal/broker"
)

// seedMessages publishes 10 messages to partition 0 and 5 to partition 1 of a
// new 3-partition topic (partition 2 stays empty). Message i of a partition
// has key k<i>, value {"n":i}, header parity=even|odd and timestamp base+i min.
func seedMessages(t *testing.T, k *Kafka) (string, time.Time) {
	t.Helper()
	topic := createTopic(t, k, 3)
	base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	for p, n := range map[int32]int{0: 10, 1: 5} {
		for i := range n {
			parity := "even"
			if i%2 == 1 {
				parity = "odd"
			}
			m := broker.Message{
				Partition: p, Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte(fmt.Sprintf(`{"n":%d}`, i)),
				Headers:   []broker.Header{{Key: "parity", Value: []byte(parity)}, {Key: "dup", Value: []byte("1")}, {Key: "dup", Value: []byte("2")}},
				Timestamp: base.Add(time.Duration(i) * time.Minute),
			}
			if err := k.Publish(ctxT(t), topic, m); err != nil {
				t.Fatal(err)
			}
		}
	}
	return topic, base
}

type pk struct {
	p   int32
	off int64
}

func keys(msgs []broker.Message) []pk {
	out := make([]pk, len(msgs))
	for i, m := range msgs {
		out[i] = pk{m.Partition, m.Offset}
	}
	slices.SortFunc(out, func(a, b pk) int {
		if a.p != b.p {
			return int(a.p - b.p)
		}
		return int(a.off - b.off)
	})
	return out
}

func span(p int32, from, to int64) []pk {
	var out []pk
	for o := from; o < to; o++ {
		out = append(out, pk{p, o})
	}
	return out
}

func TestPeekPositionsAndFilters(t *testing.T) {
	k := openKafka(t)
	topic, base := seedMessages(t, k)
	at := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }
	all := append(span(0, 0, 10), span(1, 0, 5)...)
	tests := []struct {
		name string
		opts broker.PeekOptions
		want []pk
	}{
		{name: "earliest reads everything and stops", want: all},
		{name: "empty partition does not hang", opts: broker.PeekOptions{Partitions: []int32{2}}},
		{name: "latest without follow is empty", opts: broker.PeekOptions{From: broker.Position{Kind: broker.Latest}}},
		{name: "at offset", opts: broker.PeekOptions{Partitions: []int32{0}, From: broker.Position{Kind: broker.AtOffset, Offset: 7}}, want: span(0, 7, 10)},
		{name: "at offset beyond end is clamped", opts: broker.PeekOptions{Partitions: []int32{1}, From: broker.Position{Kind: broker.AtOffset, Offset: 99}}},
		{name: "at time", opts: broker.PeekOptions{From: broker.Position{Kind: broker.AtTime, Time: at(5)}}, want: span(0, 5, 10)},
		{name: "tail per partition", opts: broker.PeekOptions{From: broker.Position{Kind: broker.Tail, Offset: 2}},
			want: append(span(0, 8, 10), span(1, 3, 5)...)},
		{name: "to offset", opts: broker.PeekOptions{Partitions: []int32{0}, To: &broker.Position{Kind: broker.AtOffset, Offset: 3}}, want: span(0, 0, 3)},
		{name: "to time", opts: broker.PeekOptions{To: &broker.Position{Kind: broker.AtTime, Time: at(2)}},
			want: append(span(0, 0, 2), span(1, 0, 2)...)},
		{name: "from and to", opts: broker.PeekOptions{Partitions: []int32{0}, From: broker.Position{Kind: broker.AtOffset, Offset: 2},
			To: &broker.Position{Kind: broker.AtOffset, Offset: 4}}, want: span(0, 2, 4)},
		{name: "key filter", opts: broker.PeekOptions{Filter: broker.Filter{Key: regexp.MustCompile(`^k[0-2]$`)}},
			want: append(span(0, 0, 3), span(1, 0, 3)...)},
		{name: "value filter", opts: broker.PeekOptions{Filter: broker.Filter{Value: regexp.MustCompile(`"n":[89]`)}}, want: span(0, 8, 10)},
		{name: "header filter", opts: broker.PeekOptions{Partitions: []int32{1}, Filter: broker.Filter{Headers: []broker.HeaderMatch{
			{Key: "parity", Value: regexp.MustCompile("^odd$")}}}}, want: []pk{{1, 1}, {1, 3}}},
		{name: "time range filter", opts: broker.PeekOptions{Partitions: []int32{0}, Filter: broker.Filter{Since: at(3), Until: at(5)}}, want: span(0, 3, 5)},
		{name: "limit", opts: broker.PeekOptions{Partitions: []int32{0}, Limit: 4}, want: span(0, 0, 4)},
		{name: "filter applies before limit", opts: broker.PeekOptions{Partitions: []int32{0}, Limit: 2, Filter: broker.Filter{
			Headers: []broker.HeaderMatch{{Key: "parity", Value: regexp.MustCompile("odd")}}}}, want: []pk{{0, 1}, {0, 3}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch, err := k.Peek(ctxT(t), topic, tt.opts)
			if err != nil {
				t.Fatal(err)
			}
			got := keys(collect(t, ch))
			if !slices.Equal(got, tt.want) {
				t.Errorf("got %v\nwant %v", got, tt.want)
			}
		})
	}
}

func TestPeekMessageContent(t *testing.T) {
	k := openKafka(t)
	topic, base := seedMessages(t, k)
	ch, err := k.Peek(ctxT(t), topic, broker.PeekOptions{Partitions: []int32{0}, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	msgs := collect(t, ch)
	if len(msgs) != 1 {
		t.Fatalf("got %d messages", len(msgs))
	}
	m := msgs[0]
	if m.Topic != topic || string(m.Key) != "k0" || string(m.Value) != `{"n":0}` || !m.Timestamp.Equal(base) {
		t.Errorf("message = %+v", m)
	}
	if len(m.Headers) != 3 || string(m.Headers[2].Value) != "2" {
		t.Errorf("duplicate headers not preserved: %+v", m.Headers)
	}
}

func TestPeekFollow(t *testing.T) {
	k := openKafka(t)
	topic, _ := seedMessages(t, k)
	ch, err := k.Peek(ctxT(t), topic, broker.PeekOptions{From: broker.Position{Kind: broker.Latest}, Follow: true, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(time.Second) // let the consumer reach the end first
		for i := range 3 {
			_ = k.Publish(ctxT(t), topic, broker.Message{Partition: 2, Value: []byte(fmt.Sprintf("new-%d", i))})
		}
	}()
	got := collect(t, ch)
	if len(got) != 2 || string(got[0].Value) != "new-0" || string(got[1].Value) != "new-1" {
		t.Fatalf("follow got %+v", got)
	}

	// Follow with an end offset stops there even beyond the current end.
	ch, err = k.Peek(ctxT(t), topic, broker.PeekOptions{Partitions: []int32{2}, Follow: true,
		To: &broker.Position{Kind: broker.AtOffset, Offset: 4}})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(time.Second)
		_ = k.Publish(ctxT(t), topic, broker.Message{Partition: 2, Value: []byte("last")})
	}()
	if got := collect(t, ch); len(got) != 4 || string(got[3].Value) != "last" {
		t.Fatalf("follow to offset got %d messages", len(got))
	}
}

func TestPublishPartitioning(t *testing.T) {
	k := openKafka(t)
	topic := createTopic(t, k, 4)
	for range 3 {
		if err := k.Publish(ctxT(t), topic, broker.Message{Partition: broker.AnyPartition, Key: []byte("same-key"), Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := k.Publish(ctxT(t), topic, broker.NewMessage([]byte("unkeyed"))); err != nil {
		t.Fatal(err)
	}
	ch, err := k.Peek(ctxT(t), topic, broker.PeekOptions{Filter: broker.Filter{Key: regexp.MustCompile("same-key")}})
	if err != nil {
		t.Fatal(err)
	}
	msgs := collect(t, ch)
	if len(msgs) != 3 || msgs[0].Partition != msgs[1].Partition || msgs[1].Partition != msgs[2].Partition {
		t.Errorf("keyed messages spread over partitions: %v", keys(msgs))
	}
	if err := k.Publish(ctxT(t), topic, broker.Message{Partition: 9, Value: []byte("x")}); err == nil {
		t.Error("publish to partition 9 of a 4-partition topic succeeded")
	}
	if err := k.Publish(ctxT(t), unique("missing"), broker.NewMessage([]byte("x"))); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("publish to missing topic: err = %v, want ErrNotFound", err)
	}
	if _, err := k.Peek(ctxT(t), unique("missing"), broker.PeekOptions{}); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("peek missing topic: err = %v, want ErrNotFound", err)
	}
	if _, err := k.Peek(ctxT(t), topic, broker.PeekOptions{Partitions: []int32{7}}); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("peek missing partition: err = %v, want ErrNotFound", err)
	}
}

// A transaction commit marker is the last offset of the partition; the peek
// must still stop at the end instead of waiting for a record that never comes.
func TestPeekStopsAfterTransactionMarker(t *testing.T) {
	k := openKafka(t)
	topic := createTopic(t, k, 1)
	cl, err := kgo.NewClient(append(k.base, kgo.TransactionalID(unique("txn")), kgo.DefaultProduceTopic(topic))...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx := ctxT(t)
	if err := cl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := cl.ProduceSync(ctx, kgo.StringRecord("a"), kgo.StringRecord("b")).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
	ch, err := k.Peek(ctx, topic, broker.PeekOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := collect(t, ch); len(got) != 2 {
		t.Fatalf("got %d messages, want 2", len(got))
	}
}
