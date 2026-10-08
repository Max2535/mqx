// Package rabbitmq is the RabbitMQ adapter: amqp091-go for messages and the
// Management HTTP API for topology, stats and administration.
package rabbitmq

import (
	"context"
	"errors"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/config"
)

// Type is the broker type name used in config files.
const Type = "rabbitmq"

func init() {
	broker.Register(Type, broker.Driver{Open: open, Validate: validate})
}

func validate(c config.Context) []error {
	if c.URL == "" {
		return []error{errors.New("rabbitmq needs url")}
	}
	return nil
}

func open(context.Context, config.Context, config.Credentials) (broker.Broker, error) {
	return nil, errors.New("rabbitmq adapter not implemented yet")
}
