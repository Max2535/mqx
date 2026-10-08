package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Max2535/mqx/internal/broker"
)

// unbounded marks a partition range without an end (Follow).
const unbounded int64 = -1

// peekRange is the offset range read from one partition: [start, end).
type peekRange struct {
	start, end int64
	// until stops the partition at the first record at or after this time
	// (a time --to under Follow, where the end offset is not known yet).
	until time.Time
}

// Peek implements broker.Broker with a direct consumer: it never joins a
// group and never commits.
func (k *Kafka) Peek(ctx context.Context, topic string, opts broker.PeekOptions) (<-chan broker.Message, error) {
	ranges, err := k.planPeek(ctx, topic, opts)
	if err != nil {
		return nil, err
	}
	out := make(chan broker.Message)
	offsets := map[int32]kgo.Offset{}
	for p, r := range ranges {
		if r.end != unbounded && r.start >= r.end {
			delete(ranges, p) // empty in the requested range: nothing to wait for
			continue
		}
		offsets[p] = kgo.NewOffset().At(r.start)
	}
	if len(offsets) == 0 {
		close(out) // every selected partition is empty in the requested range
		return out, nil
	}
	cl, err := kgo.NewClient(append(k.base,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: offsets}),
		kgo.KeepControlRecords(), // see every offset, so a transaction marker at the end cannot hang the peek
		kgo.FetchMaxWait(500*time.Millisecond),
	)...)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	p := &peeker{k: k, cl: cl, topic: topic, opts: opts, ranges: ranges, out: out, open: len(offsets)}
	go p.run(ctx)
	return out, nil
}

// planPeek resolves the start and end offset of every selected partition.
func (k *Kafka) planPeek(ctx context.Context, topic string, opts broker.PeekOptions) (map[int32]peekRange, error) {
	parts, err := k.partitionsOf(ctx, topic, opts.Partitions)
	if err != nil {
		return nil, err
	}
	m, err := k.watermarks(ctx, topic)
	if err != nil {
		return nil, err
	}
	starts, err := k.resolvePositions(ctx, topic, parts, m, opts.From)
	if err != nil {
		return nil, err
	}
	var ends map[int32]int64
	if opts.To != nil && (!opts.Follow || opts.To.Kind != broker.AtTime) {
		if ends, err = k.resolvePositions(ctx, topic, parts, m, *opts.To); err != nil {
			return nil, err
		}
		if opts.Follow && opts.To.Kind == broker.AtOffset {
			for p := range ends {
				ends[p] = max(opts.To.Offset, starts[p]) // may lie beyond the current end
			}
		}
	}
	out := make(map[int32]peekRange, len(parts))
	for _, p := range parts {
		r := peekRange{start: starts[p]}
		switch {
		case ends != nil:
			r.end = ends[p]
		case opts.Follow:
			r.end = unbounded
			if opts.To != nil {
				r.until = opts.To.Time
			}
		default:
			_, r.end = m.get(topic, p)
		}
		out[p] = r
	}
	return out, nil
}

// peeker streams one Peek.
type peeker struct {
	k      *Kafka
	cl     *kgo.Client
	topic  string
	opts   broker.PeekOptions
	ranges map[int32]peekRange
	out    chan<- broker.Message
	open   int // partitions not finished yet
	sent   int
}

func (p *peeker) run(ctx context.Context) {
	defer close(p.out)
	defer p.cl.Close()
	for p.open > 0 {
		fetches := p.cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		if err := fetchErr(fetches); err != nil {
			p.send(ctx, broker.Message{Topic: p.topic, Err: wrapErr("peek "+p.topic, err)})
			return
		}
		for _, rec := range fetches.Records() {
			done, ok := p.handle(ctx, rec)
			if !ok {
				return
			}
			if done {
				p.finish(rec.Partition)
			}
		}
	}
}

// handle processes one record. It reports whether its partition is finished
// and false in ok when the peek must stop.
func (p *peeker) handle(ctx context.Context, rec *kgo.Record) (done, ok bool) {
	r, tracked := p.ranges[rec.Partition]
	if !tracked {
		return false, true // already finished
	}
	if r.end != unbounded && rec.Offset >= r.end {
		return true, true
	}
	if !r.until.IsZero() && !rec.Timestamp.Before(r.until) {
		return true, true
	}
	last := r.end != unbounded && rec.Offset >= r.end-1
	if rec.Attrs.IsControl() {
		return last, true
	}
	msg := toMessage(rec)
	if p.opts.Decode && p.k.serde != nil {
		p.k.serde.decodeMessage(ctx, &msg)
	}
	if !p.opts.Filter.Match(msg) {
		return last, true
	}
	if !p.send(ctx, msg) {
		return last, false
	}
	p.sent++
	if p.opts.Limit > 0 && p.sent >= p.opts.Limit {
		return last, false
	}
	return last, true
}

func (p *peeker) finish(partition int32) {
	if _, ok := p.ranges[partition]; !ok {
		return
	}
	delete(p.ranges, partition)
	p.open--
	p.cl.PauseFetchPartitions(map[string][]int32{p.topic: {partition}})
}

func (p *peeker) send(ctx context.Context, m broker.Message) bool {
	select {
	case p.out <- m:
		return true
	case <-ctx.Done():
		return false
	}
}

// fetchErr returns the first fatal fetch error. Data loss notices are not fatal.
func fetchErr(fs kgo.Fetches) error {
	for _, fe := range fs.Errors() {
		var loss *kgo.ErrDataLoss
		switch {
		case errors.As(fe.Err, &loss):
			continue
		case errors.Is(fe.Err, context.Canceled), errors.Is(fe.Err, context.DeadlineExceeded):
			return nil
		}
		return fe.Err
	}
	return nil
}

func toMessage(rec *kgo.Record) broker.Message {
	m := broker.Message{
		Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset,
		Key: rec.Key, Value: rec.Value, Timestamp: rec.Timestamp,
	}
	for _, h := range rec.Headers {
		m.Headers = append(m.Headers, broker.Header{Key: h.Key, Value: h.Value})
	}
	return m
}
