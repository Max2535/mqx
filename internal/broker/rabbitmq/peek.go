package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Max2535/mqx/internal/broker"
)

// Peek reads messages without consuming them.
//
// Classic and quorum queues are read from the head with basic.get and manual
// acks on a dedicated channel; one multiple nack then requeues everything
// held, so depth and order are unchanged (requeued messages are marked
// redelivered) and quorum queues do not count it as a failed delivery. A peek scans at most the queue depth at its start, capped
// by options.peek_max_scan.
//
// Stream queues are read with a consumer at x-stream-offset, which is
// non-destructive and supports From and To.
func (r *RabbitMQ) Peek(ctx context.Context, topic string, opts broker.PeekOptions) (<-chan broker.Message, error) {
	if len(opts.Partitions) > 0 {
		return nil, fmt.Errorf("partitions: RabbitMQ queues have no partitions: %w", broker.ErrUnsupported)
	}
	qtype := "classic"
	q, err := r.queue(ctx, topic)
	switch {
	case err == nil:
		qtype = q.queueType()
	case errors.Is(err, errNotFound):
		return nil, err
	} // other management errors: fall back to AMQP, which reports problems itself
	if qtype == "stream" {
		return r.peekStream(ctx, topic, opts)
	}
	if opts.From.Kind != broker.Earliest || opts.To != nil {
		return nil, fmt.Errorf("--from/--to: %s queue %q is read from its head only; positions work on stream queues: %w",
			qtype, topic, broker.ErrUnsupported)
	}
	if opts.Follow {
		return nil, fmt.Errorf("follow: a non-destructive peek cannot follow a %s queue without taking messages "+
			"from its consumers; peek again instead: %w", qtype, broker.ErrUnsupported)
	}
	ch, err := r.channel(ctx)
	if err != nil {
		return nil, err
	}
	info, err := ch.QueueDeclarePassive(topic, false, false, false, false, nil)
	if err != nil {
		ch.Close()
		return nil, amqpErr("peek queue", topic, err)
	}
	out := make(chan broker.Message)
	go func() {
		defer close(out)
		defer ch.Close()
		// Return every held message before the stream ends. An explicit nack
		// leaves a quorum queue's x-delivery-count unchanged; a channel close
		// or reject counts towards delivery-limit (RabbitMQ 4.x), so repeated
		// peeks would eventually drop or dead-letter the message.
		last, held := r.peekQueue(ctx, ch, topic, info.Messages, opts, out)
		if last == 0 {
			return
		}
		if err := ch.Nack(last, true, true); err == nil {
			awaitRequeue(ch, topic, held)
		}
	}()
	return out, nil
}

// peekQueue gets messages within the scan budget and returns the last delivery tag it holds.
func (r *RabbitMQ) peekQueue(ctx context.Context, ch *amqp.Channel, topic string, depth int, opts broker.PeekOptions,
	out chan<- broker.Message,
) (last uint64, held int) {
	scan, capped := depth, false
	if scan > r.settings.peekMaxScan {
		scan, capped = r.settings.peekMaxScan, true
	}
	if opts.Limit > 0 && opts.Filter.IsZero() && opts.Limit < scan {
		scan, capped = opts.Limit, false
	}
	sent := 0
	for i := 0; i < scan; i++ {
		if ctx.Err() != nil {
			return last, held
		}
		d, ok, err := ch.Get(topic, false)
		if err != nil {
			send(ctx, out, broker.Message{Topic: topic, Err: amqpErr("peek queue", topic, err)})
			return last, held
		}
		if !ok {
			return last, held // queue drained by other consumers meanwhile
		}
		last, held = d.DeliveryTag, held+1
		m := fromDelivery(d, topic, int64(i))
		if !opts.Filter.Match(m) {
			continue
		}
		if !send(ctx, out, m) {
			return last, held
		}
		sent++
		if opts.Limit > 0 && sent >= opts.Limit {
			return last, held
		}
	}
	if capped {
		send(ctx, out, broker.Message{Topic: topic, Err: fmt.Errorf(
			"stopped after scanning %d of %d messages in %q; raise options.%s on the context to scan further",
			scan, depth, topic, OptPeekMaxScan)})
	}
	return last, held
}

// requeueWait bounds how long a peek waits for its requeued messages to be ready again.
const requeueWait = 2 * time.Second

// awaitRequeue waits until the queue reports at least held ready messages.
// Requeueing is asynchronous (quorum queues apply it through Raft), so without
// this a peek right after another can find the queue momentarily empty.
// Consumers may take the messages meanwhile, so the wait is bounded, not an error.
func awaitRequeue(ch *amqp.Channel, topic string, held int) {
	deadline := time.Now().Add(requeueWait)
	for time.Now().Before(deadline) {
		info, err := ch.QueueDeclarePassive(topic, false, false, false, false, nil)
		if err != nil || info.Messages >= held {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// send delivers m unless ctx ended first.
func send(ctx context.Context, out chan<- broker.Message, m broker.Message) bool {
	select {
	case out <- m:
		return true
	case <-ctx.Done():
		return false
	}
}

const streamPrefetch = 100

func (r *RabbitMQ) peekStream(ctx context.Context, topic string, opts broker.PeekOptions) (<-chan broker.Message, error) {
	start, err := streamOffset(opts.From)
	if err != nil {
		return nil, err
	}
	if opts.To != nil && opts.To.Kind != broker.AtOffset && opts.To.Kind != broker.AtTime && opts.To.Kind != broker.Latest {
		return nil, fmt.Errorf("--to on a stream: use an offset or a time: %w", broker.ErrUnsupported)
	}
	ch, err := r.channel(ctx)
	if err != nil {
		return nil, err
	}
	if err := ch.Qos(streamPrefetch, 0, false); err != nil {
		ch.Close()
		return nil, fmt.Errorf("set prefetch for stream %q: %w", topic, err)
	}
	deliveries, err := ch.ConsumeWithContext(ctx, topic, "", false, false, false, false,
		amqp.Table{"x-stream-offset": start})
	if err != nil {
		ch.Close()
		return nil, amqpErr("consume stream", topic, err)
	}
	out := make(chan broker.Message)
	go func() {
		defer close(out)
		defer ch.Close()
		r.readStream(ctx, ch, deliveries, topic, opts, out)
	}()
	return out, nil
}

// streamOffset maps a position to an x-stream-offset value.
func streamOffset(p broker.Position) (any, error) {
	switch p.Kind {
	case broker.Earliest:
		return "first", nil
	case broker.Latest:
		return "next", nil // only messages published from now on
	case broker.AtOffset:
		return p.Offset, nil
	case broker.AtTime:
		return p.Time, nil
	default:
		return nil, fmt.Errorf("--from -N on a stream: use an offset, a time or earliest: %w", broker.ErrUnsupported)
	}
}

func (r *RabbitMQ) readStream(ctx context.Context, ch *amqp.Channel, deliveries <-chan amqp.Delivery, topic string,
	opts broker.PeekOptions, out chan<- broker.Message,
) {
	idle := time.NewTimer(r.settings.streamIdle)
	defer idle.Stop()
	closed := ch.NotifyClose(make(chan *amqp.Error, 1))
	sent := 0
	for {
		var d amqp.Delivery
		var ok bool
		select {
		case <-ctx.Done():
			return
		case e := <-closed:
			if e != nil {
				send(ctx, out, broker.Message{Topic: topic, Err: amqpErr("consume stream", topic, e)})
			}
			return
		case <-idle.C:
			if !opts.Follow {
				return // nothing new for a while: the end of the stream as of now
			}
			idle.Reset(r.settings.streamIdle)
			continue
		case d, ok = <-deliveries:
			if !ok {
				return
			}
		}
		idle.Reset(r.settings.streamIdle)
		if err := d.Ack(false); err != nil { // stream acks only grant credit; nothing is removed
			send(ctx, out, broker.Message{Topic: topic, Err: fmt.Errorf("ack stream delivery: %w", err)})
			return
		}
		off := streamDeliveryOffset(d)
		// Delivery starts at a chunk boundary, possibly before the requested offset.
		if opts.From.Kind == broker.AtOffset && off < opts.From.Offset {
			continue
		}
		m := fromDelivery(d, topic, off)
		if pastEnd(opts.To, m) {
			return
		}
		if !opts.Filter.Match(m) {
			continue
		}
		if !send(ctx, out, m) {
			return
		}
		sent++
		if opts.Limit > 0 && sent >= opts.Limit {
			return
		}
	}
}

func pastEnd(to *broker.Position, m broker.Message) bool {
	if to == nil {
		return false
	}
	switch to.Kind {
	case broker.AtOffset:
		return m.Offset >= to.Offset
	case broker.AtTime:
		return !m.Timestamp.IsZero() && !m.Timestamp.Before(to.Time)
	}
	return false
}

func streamDeliveryOffset(d amqp.Delivery) int64 {
	switch v := d.Headers["x-stream-offset"].(type) {
	case int64:
		return v
	case int32:
		return int64(v)
	case int:
		return int64(v)
	}
	return -1
}

// fromDelivery converts an AMQP delivery; message_id is reported as Key.
func fromDelivery(d amqp.Delivery, topic string, offset int64) broker.Message {
	m := broker.Message{
		Topic:       topic,
		Offset:      offset,
		Value:       d.Body,
		Timestamp:   d.Timestamp,
		Exchange:    d.Exchange,
		RoutingKey:  d.RoutingKey,
		Redelivered: d.Redelivered,
	}
	if d.MessageId != "" {
		m.Key = []byte(d.MessageId)
	}
	for _, k := range sortedKeys(d.Headers) {
		m.Headers = append(m.Headers, broker.Header{Key: k, Value: []byte(tableValue(d.Headers[k]))})
	}
	props := map[string]string{
		"content_type":     d.ContentType,
		"content_encoding": d.ContentEncoding,
		"correlation_id":   d.CorrelationId,
		"reply_to":         d.ReplyTo,
		"expiration":       d.Expiration,
		"message_id":       d.MessageId,
		"type":             d.Type,
		"app_id":           d.AppId,
		"user_id":          d.UserId,
	}
	if d.DeliveryMode != 0 {
		props["delivery_mode"] = strconv.Itoa(int(d.DeliveryMode))
	}
	if d.Priority != 0 {
		props["priority"] = strconv.Itoa(int(d.Priority))
	}
	if !d.Timestamp.IsZero() {
		props["timestamp"] = d.Timestamp.UTC().Format(time.RFC3339)
	}
	for k, v := range props {
		if v == "" {
			delete(props, k)
		}
	}
	if len(props) > 0 {
		m.Properties = props
	}
	return m
}

// tableValue renders an AMQP field value as text for headers and filters.
func tableValue(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	case time.Time:
		return x.UTC().Format(time.RFC3339)
	case amqp.Table, []any:
		data, err := json.Marshal(x)
		if err == nil {
			return string(data)
		}
	}
	return fmt.Sprint(v)
}
