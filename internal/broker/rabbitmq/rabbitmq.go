// Package rabbitmq is the RabbitMQ adapter: amqp091-go for messages and the
// Management HTTP API for topology, stats and administration.
//
// Every operation is scoped to the virtual host named by the AMQP URL path
// ("/" when the path is empty). The AMQP connection is dialled lazily, only
// for publish, peek and purge, so listing and admin commands work through the
// management API alone.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/config"
)

// Type is the broker type name used in config files.
const Type = "rabbitmq"

func init() {
	broker.Register(Type, broker.Driver{Open: open, Validate: validate, Fields: []string{
		"url", "management_url",
		"tls.enabled", "tls.ca_file", "tls.cert_file", "tls.key_file", "tls.insecure_skip_verify",
		"options",
	}})
}

// Compile-time checks that the adapter implements its capabilities.
var (
	_ broker.Broker              = (*RabbitMQ)(nil)
	_ broker.TopicDescriber      = (*RabbitMQ)(nil)
	_ broker.ClusterInspector    = (*RabbitMQ)(nil)
	_ broker.ConsumerInspector   = (*RabbitMQ)(nil)
	_ broker.ConnectionInspector = (*RabbitMQ)(nil)
	_ broker.ConsumerTerminator  = (*RabbitMQ)(nil)
	_ broker.TopicAdmin          = (*RabbitMQ)(nil)
	_ broker.Purger              = (*RabbitMQ)(nil)
	_ broker.TopologyInspector   = (*RabbitMQ)(nil)
	_ broker.TopologyEditor      = (*RabbitMQ)(nil)
	_ broker.RouteSimulator      = (*RabbitMQ)(nil)
	_ broker.UserAdmin           = (*RabbitMQ)(nil)
	_ broker.PolicyAdmin         = (*RabbitMQ)(nil)
	_ broker.MetricsReporter     = (*RabbitMQ)(nil)
)

// RabbitMQ is a connection to one virtual host of a RabbitMQ cluster.
type RabbitMQ struct {
	settings settings
	mgmt     *mgmtClient

	mu   sync.Mutex
	conn *amqp.Connection // dialled lazily; guarded by mu
}

func open(_ context.Context, c config.Context, creds config.Credentials) (broker.Broker, error) {
	s, err := newSettings(c, creds)
	if err != nil {
		return nil, err
	}
	return &RabbitMQ{settings: s, mgmt: newMgmtClient(s)}, nil
}

// Name implements broker.Broker.
func (r *RabbitMQ) Name() string { return Type }

// Close implements broker.Broker.
func (r *RabbitMQ) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conn == nil || r.conn.IsClosed() {
		return nil
	}
	err := r.conn.Close()
	r.conn = nil
	if err != nil && !errors.Is(err, amqp.ErrClosed) {
		return fmt.Errorf("close amqp connection: %w", err)
	}
	return nil
}

// Ping checks both the management API (credentials and vhost) and the AMQP listener.
func (r *RabbitMQ) Ping(ctx context.Context) error {
	if err := r.mgmt.get(ctx, apiPath("vhosts", r.settings.vhost), nil); err != nil {
		return fmt.Errorf("management API: %w", err)
	}
	if _, err := r.connection(ctx); err != nil {
		return err
	}
	return nil
}
