// Package kafka is the Kafka adapter, built on franz-go.
//
// It implements the broker core plus every Kafka capability. Schema Registry,
// Kafka Connect and ksqlDB are separate HTTP services; their capabilities are
// reported only when the context configures their endpoint.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

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

// Kafka is a connection to one Kafka cluster and its optional ecosystem services.
type Kafka struct {
	cl  *kgo.Client
	adm *kadm.Client
	// base are the connection options every client of this adapter shares
	// (seeds, SASL, TLS); Peek builds a dedicated consumer from them.
	base []kgo.Opt

	registry *registryClient // nil without schema_registry
	serde    *serde          // nil without schema_registry
	connect  *restClient     // nil without connect
	ksql     *restClient     // nil without ksqldb
}

// Compile-time checks that Kafka implements the core and every Kafka capability.
var (
	_ broker.Broker             = (*Kafka)(nil)
	_ broker.CapabilityChecker  = (*Kafka)(nil)
	_ broker.TopicDescriber     = (*Kafka)(nil)
	_ broker.ClusterInspector   = (*Kafka)(nil)
	_ broker.ConsumerInspector  = (*Kafka)(nil)
	_ broker.GroupInspector     = (*Kafka)(nil)
	_ broker.LagReporter        = (*Kafka)(nil)
	_ broker.OffsetManager      = (*Kafka)(nil)
	_ broker.ConsumerTerminator = (*Kafka)(nil)
	_ broker.TopicAdmin         = (*Kafka)(nil)
	_ broker.PartitionAdder     = (*Kafka)(nil)
	_ broker.Purger             = (*Kafka)(nil)
	_ broker.MetricsReporter    = (*Kafka)(nil)
	_ broker.SchemaRegistry     = (*Kafka)(nil)
	_ broker.ConnectManager     = (*Kafka)(nil)
	_ broker.KSQLRunner         = (*Kafka)(nil)
	_ broker.ACLAdmin           = (*Kafka)(nil)
)

func open(_ context.Context, c config.Context, creds config.Credentials) (broker.Broker, error) {
	base, err := clientOpts(c, creds)
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(append(base, kgo.RecordPartitioner(newPartitioner()))...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	k := &Kafka{cl: cl, adm: kadm.NewClient(cl), base: base}
	if err := k.openServices(c); err != nil {
		cl.Close()
		return nil, err
	}
	return k, nil
}

// openServices sets up the HTTP clients of the configured ecosystem services.
func (k *Kafka) openServices(c config.Context) error {
	label := func(key string) string { return fmt.Sprintf("context %q %s", c.Name, key) }
	if e := c.SchemaRegistry; e != nil {
		creds, err := e.Resolve(label("schema_registry"))
		if err != nil {
			return err
		}
		rc, err := newRegistryClient(e.URL, creds)
		if err != nil {
			return err
		}
		k.registry = rc
		k.serde = newSerde(rc)
	}
	if e := c.Connect; e != nil {
		creds, err := e.Resolve(label("connect"))
		if err != nil {
			return err
		}
		k.connect = newRESTClient("kafka connect", e.URL, creds)
	}
	if e := c.KSQLDB; e != nil {
		creds, err := e.Resolve(label("ksqldb"))
		if err != nil {
			return err
		}
		k.ksql = newRESTClient("ksqldb", e.URL, creds)
	}
	return nil
}

// Name implements broker.Broker.
func (k *Kafka) Name() string { return Type }

// HasCapability implements broker.CapabilityChecker: the ecosystem
// capabilities exist only when their endpoint is configured.
func (k *Kafka) HasCapability(name string) bool {
	switch name {
	case broker.CapSchemaRegistry:
		return k.registry != nil
	case broker.CapConnectManager:
		return k.connect != nil
	case broker.CapKSQLRunner:
		return k.ksql != nil
	}
	return true
}

// Close implements broker.Broker.
func (k *Kafka) Close() error {
	k.cl.Close()
	return nil
}
