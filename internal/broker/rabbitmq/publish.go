package rabbitmq

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Max2535/mqx/internal/broker"
)

// Publish sends msg with publisher confirms and the mandatory flag, so an
// unroutable message is an error rather than silently dropped.
//
// With msg.Exchange set the message goes to that exchange with msg.RoutingKey
// (or topic when RoutingKey is empty); otherwise it goes through the default
// exchange to the queue named topic. msg.Key becomes the message_id property
// unless Properties sets message_id; Peek reports message_id as Key, so keys
// round-trip. delivery_mode defaults to persistent and timestamp to now.
func (r *RabbitMQ) Publish(ctx context.Context, topic string, msg broker.Message) error {
	exchange, key := "", topic
	if msg.Exchange != "" {
		exchange = msg.Exchange
		if msg.RoutingKey != "" {
			key = msg.RoutingKey
		}
	} else if key == "" {
		key = msg.RoutingKey
	}
	pub, err := publishing(msg)
	if err != nil {
		return err
	}
	ch, err := r.channel(ctx)
	if err != nil {
		return err
	}
	defer ch.Close()
	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("enable publisher confirms: %w", err)
	}
	returns := ch.NotifyReturn(make(chan amqp.Return, 1))
	closed := ch.NotifyClose(make(chan *amqp.Error, 1))
	target := describeTarget(exchange, key)
	dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, true, false, pub)
	if err != nil {
		return amqpErr("publish to", target, err)
	}
	select {
	case <-dc.Done():
	case e := <-closed:
		if e == nil {
			return fmt.Errorf("publish to %s: channel closed before the broker confirmed", target)
		}
		return amqpErr("publish to", target, e)
	case <-ctx.Done():
		return fmt.Errorf("publish to %s: waiting for confirm: %w", target, ctx.Err())
	}
	// The broker sends basic.return before the ack, and amqp091 delivers it first.
	select {
	case ret := <-returns:
		if exchange == "" {
			return fmt.Errorf("publish to %s: message returned as unroutable (%d %s): queue %q does not exist; "+
				"list queues with `mqx topics`: %w", target, ret.ReplyCode, ret.ReplyText, key, errNotFound)
		}
		return fmt.Errorf("publish to %s: message returned as unroutable (%d %s): no queue is bound for routing key %q; "+
			"check bindings with `mqx bindings` or `mqx route`", target, ret.ReplyCode, ret.ReplyText, key)
	default:
	}
	if !dc.Acked() {
		return fmt.Errorf("publish to %s: broker nacked the message (queue full with reject-publish overflow, or an internal error)", target)
	}
	return nil
}

func describeTarget(exchange, key string) string {
	if exchange == "" {
		return fmt.Sprintf("queue %q via the default exchange", key)
	}
	return fmt.Sprintf("exchange %q with routing key %q", exchange, key)
}

// propertyNames lists the AMQP basic properties Publish accepts in msg.Properties.
var propertyNames = []string{
	"app_id", "content_encoding", "content_type", "correlation_id", "delivery_mode", "expiration",
	"message_id", "priority", "reply_to", "timestamp", "type", "user_id",
}

func publishing(msg broker.Message) (amqp.Publishing, error) {
	p := amqp.Publishing{Body: msg.Value, DeliveryMode: amqp.Persistent, Timestamp: msg.Timestamp}
	if p.Timestamp.IsZero() {
		p.Timestamp = time.Now()
	}
	if len(msg.Key) > 0 {
		p.MessageId = string(msg.Key)
	}
	if len(msg.Headers) > 0 {
		p.Headers = amqp.Table{}
		for _, h := range msg.Headers {
			p.Headers[h.Key] = string(h.Value)
		}
	}
	for k, v := range msg.Properties {
		if err := setProperty(&p, k, v); err != nil {
			return p, err
		}
	}
	return p, nil
}

func setProperty(p *amqp.Publishing, k, v string) error {
	switch k {
	case "content_type":
		p.ContentType = v
	case "content_encoding":
		p.ContentEncoding = v
	case "correlation_id":
		p.CorrelationId = v
	case "reply_to":
		p.ReplyTo = v
	case "expiration":
		if _, err := strconv.ParseUint(v, 10, 32); err != nil {
			return fmt.Errorf("property expiration %q: want a TTL in milliseconds, e.g. 60000", v)
		}
		p.Expiration = v
	case "message_id":
		p.MessageId = v
	case "type":
		p.Type = v
	case "app_id":
		p.AppId = v
	case "user_id":
		p.UserId = v
	case "priority":
		n, err := strconv.ParseUint(v, 10, 8)
		if err != nil {
			return fmt.Errorf("property priority %q: want 0-255", v)
		}
		p.Priority = uint8(n)
	case "delivery_mode":
		switch strings.ToLower(v) {
		case "1", "transient":
			p.DeliveryMode = amqp.Transient
		case "2", "persistent":
			p.DeliveryMode = amqp.Persistent
		default:
			return fmt.Errorf("property delivery_mode %q: want 1, 2, transient or persistent", v)
		}
	case "timestamp":
		t, err := parseTimestamp(v)
		if err != nil {
			return err
		}
		p.Timestamp = t
	default:
		return fmt.Errorf("unknown property %q; use one of %s, or a header for custom values", k,
			strings.Join(propertyNames, ", "))
	}
	return nil
}

func parseTimestamp(v string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t, nil
	}
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return time.Unix(n, 0), nil
	}
	return time.Time{}, fmt.Errorf("property timestamp %q: want RFC3339 or unix seconds", v)
}
