package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/Max2535/mqx/internal/broker"
)

type consumerJSON struct {
	Tag         string `json:"consumer_tag"`
	AckRequired bool   `json:"ack_required"`
	Prefetch    int    `json:"prefetch_count"`
	Active      *bool  `json:"active"`
	Queue       struct {
		Name string `json:"name"`
	} `json:"queue"`
	ChannelDetails struct {
		Name           string `json:"name"`
		ConnectionName string `json:"connection_name"`
		PeerHost       string `json:"peer_host"`
		PeerPort       any    `json:"peer_port"`
		User           string `json:"user"`
	} `json:"channel_details"`
}

// Consumers lists the consumers of a queue.
func (r *RabbitMQ) Consumers(ctx context.Context, topic string) ([]broker.Consumer, error) {
	if err := r.mgmt.get(ctx, withQuery(apiPath("queues", r.settings.vhost, topic), url.Values{"columns": {"name"}}), nil); err != nil {
		return nil, fmt.Errorf("queue %q in vhost %q: %w", topic, r.settings.vhost, err)
	}
	var cs []consumerJSON
	if err := r.mgmt.get(ctx, apiPath("consumers", r.settings.vhost), &cs); err != nil {
		return nil, fmt.Errorf("list consumers: %w", err)
	}
	out := []broker.Consumer{}
	for _, c := range cs {
		if c.Queue.Name != topic {
			continue
		}
		active := true // brokers before 3.8 do not report it; every consumer was active
		if c.Active != nil {
			active = *c.Active
		}
		out = append(out, broker.Consumer{
			Tag:         c.Tag,
			Connection:  c.ChannelDetails.ConnectionName,
			Channel:     c.ChannelDetails.Name,
			User:        c.ChannelDetails.User,
			Host:        c.ChannelDetails.PeerHost,
			AckRequired: c.AckRequired,
			Prefetch:    c.Prefetch,
			Active:      active,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Tag < out[j].Tag })
	return out, nil
}

type connectionJSON struct {
	Name             string         `json:"name"`
	User             string         `json:"user"`
	VHost            string         `json:"vhost"`
	PeerHost         string         `json:"peer_host"`
	PeerPort         int            `json:"peer_port"`
	State            string         `json:"state"`
	Channels         int            `json:"channels"`
	Protocol         string         `json:"protocol"`
	ClientProperties map[string]any `json:"client_properties"`
	ConnectedAt      int64          `json:"connected_at"`
	RecvOctDetails   rate           `json:"recv_oct_details"`
	SendOctDetails   rate           `json:"send_oct_details"`
}

type rate struct {
	Rate float64 `json:"rate"`
}

// Connections lists the client connections to the vhost.
func (r *RabbitMQ) Connections(ctx context.Context) ([]broker.Connection, error) {
	var cs []connectionJSON
	if err := r.mgmt.get(ctx, apiPath("vhosts", r.settings.vhost, "connections"), &cs); err != nil {
		return nil, fmt.Errorf("list connections: %w", err)
	}
	out := make([]broker.Connection, len(cs))
	for i, c := range cs {
		props := map[string]string{}
		for k, v := range c.ClientProperties {
			if _, nested := v.(map[string]any); !nested {
				props[k] = stringify(v)
			}
		}
		out[i] = broker.Connection{
			Name:        c.Name,
			User:        c.User,
			VHost:       c.VHost,
			Peer:        peer(c.PeerHost, c.PeerPort),
			State:       c.State,
			Channels:    c.Channels,
			Protocol:    c.Protocol,
			ClientProps: props,
			RecvRate:    c.RecvOctDetails.Rate,
			SendRate:    c.SendOctDetails.Rate,
		}
		if c.ConnectedAt > 0 {
			out[i].ConnectedAt = time.UnixMilli(c.ConnectedAt)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func peer(host string, port int) string {
	if host == "" {
		return ""
	}
	return host + ":" + strconv.Itoa(port)
}

type channelJSON struct {
	Name              string `json:"name"`
	Number            int    `json:"number"`
	User              string `json:"user"`
	VHost             string `json:"vhost"`
	State             string `json:"state"`
	Consumers         int    `json:"consumer_count"`
	Prefetch          int    `json:"prefetch_count"`
	Unacked           int    `json:"messages_unacknowledged"`
	Confirm           bool   `json:"confirm"`
	Transactional     bool   `json:"transactional"`
	ConnectionDetails struct {
		Name string `json:"name"`
	} `json:"connection_details"`
	MessageStats struct {
		PublishDetails    rate `json:"publish_details"`
		DeliverGetDetails rate `json:"deliver_get_details"`
	} `json:"message_stats"`
}

// Channels lists the AMQP channels in the vhost.
func (r *RabbitMQ) Channels(ctx context.Context) ([]broker.Channel, error) {
	var cs []channelJSON
	if err := r.mgmt.get(ctx, apiPath("vhosts", r.settings.vhost, "channels"), &cs); err != nil {
		return nil, fmt.Errorf("list channels: %w", err)
	}
	out := make([]broker.Channel, len(cs))
	for i, c := range cs {
		out[i] = broker.Channel{
			Name:          c.Name,
			Connection:    c.ConnectionDetails.Name,
			Number:        c.Number,
			User:          c.User,
			VHost:         c.VHost,
			State:         c.State,
			Consumers:     c.Consumers,
			Prefetch:      c.Prefetch,
			Unacked:       c.Unacked,
			Confirm:       c.Confirm,
			Transactional: c.Transactional,
			PublishRate:   c.MessageStats.PublishDetails.Rate,
			DeliverRate:   c.MessageStats.DeliverGetDetails.Rate,
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// TerminateConsumer closes the client connection named by target.Connection,
// which disconnects every consumer on it. The reason is shown to the client.
func (r *RabbitMQ) TerminateConsumer(ctx context.Context, target broker.ConsumerTarget) error {
	if target.Connection == "" {
		return errors.New("rabbitmq disconnects consumers by closing their connection; " +
			"name it (see `mqx connections` or `mqx consumers <queue>`)")
	}
	var c connectionJSON
	if err := r.mgmt.get(ctx, apiPath("connections", target.Connection), &c); err != nil {
		return fmt.Errorf("connection %q: %w", target.Connection, err)
	}
	if c.VHost != r.settings.vhost {
		return fmt.Errorf("connection %q belongs to vhost %q, not this context's vhost %q: %w",
			target.Connection, c.VHost, r.settings.vhost, errNotFound)
	}
	reason := target.Reason
	if reason == "" {
		reason = "closed by mqx"
	}
	if err := r.mgmt.delete(ctx, apiPath("connections", target.Connection), http.Header{"X-Reason": {reason}}); err != nil {
		return fmt.Errorf("close connection %q: %w", target.Connection, err)
	}
	return nil
}
