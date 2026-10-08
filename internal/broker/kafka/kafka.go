// Package kafka is the Kafka adapter, built on franz-go.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/config"
)

// Type is the broker type name used in config files.
const Type = "kafka"

func init() {
	broker.Register(Type, broker.Driver{Open: open, Validate: validate})
}

func validate(c config.Context) []error {
	var errs []error
	if len(c.Brokers) == 0 {
		errs = append(errs, errors.New("kafka needs at least one entry in brokers"))
	}
	for i, b := range c.Brokers {
		// Static message: the entry may embed a credential and must not be echoed.
		if _, _, err := net.SplitHostPort(b); err != nil || strings.Contains(b, "@") {
			errs = append(errs, fmt.Errorf("brokers[%d] must be host:port; credentials go in username_env / password_env", i))
		}
	}
	switch strings.ToUpper(c.SASLMechanism) {
	case "", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
	default:
		errs = append(errs, fmt.Errorf("sasl_mechanism %q is not supported; use PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512", c.SASLMechanism))
	}
	return errs
}

func open(context.Context, config.Context, config.Credentials) (broker.Broker, error) {
	return nil, errors.New("kafka adapter not implemented yet")
}
