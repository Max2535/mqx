package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/config"
)

// clientOpts builds the connection options shared by every client: seeds, SASL and TLS.
func clientOpts(c config.Context, creds config.Credentials) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID("mqx"),
		kgo.DialTimeout(10 * time.Second),
	}
	if c.SASLMechanism != "" {
		mech, err := saslMechanism(c.SASLMechanism, creds)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mech))
	}
	if c.TLS != nil && (c.TLS.Enabled || c.TLS.CAFile != "" || c.TLS.CertFile != "") {
		cfg, err := tlsConfig(c.TLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(cfg))
	}
	return opts, nil
}

func saslMechanism(name string, creds config.Credentials) (sasl.Mechanism, error) {
	if creds.Username == "" {
		return nil, fmt.Errorf("sasl_mechanism %s needs username_env and password_env", name)
	}
	switch strings.ToUpper(name) {
	case "PLAIN":
		return plain.Auth{User: creds.Username, Pass: creds.Password}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: creds.Username, Pass: creds.Password}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{User: creds.Username, Pass: creds.Password}.AsSha512Mechanism(), nil
	}
	return nil, fmt.Errorf("sasl_mechanism %q is not supported; use PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512", name)
}

// tlsConfig loads the CA and client certificate files of a context.
func tlsConfig(t *config.TLS) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: t.InsecureSkipVerify} //nolint:gosec // opt-in per context
	if t.CAFile != "" {
		pem, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("tls ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("tls ca_file %s holds no PEM certificates", t.CAFile)
		}
		cfg.RootCAs = pool
	}
	if t.CertFile != "" || t.KeyFile != "" {
		if t.CertFile == "" || t.KeyFile == "" {
			return nil, errors.New("tls needs both cert_file and key_file for client authentication")
		}
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("tls client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// partitioner honours an explicit record partition (>= 0) and otherwise
// delegates to the Java-compatible sticky key partitioner. Publish marks
// records that should be placed by the producer with partition -1.
type partitioner struct{ def kgo.Partitioner }

func newPartitioner() kgo.Partitioner { return partitioner{def: kgo.StickyKeyPartitioner(nil)} }

func (p partitioner) ForTopic(topic string) kgo.TopicPartitioner {
	return topicPartitioner{def: p.def.ForTopic(topic)}
}

type topicPartitioner struct{ def kgo.TopicPartitioner }

func (t topicPartitioner) RequiresConsistency(r *kgo.Record) bool {
	return r.Partition >= 0 || t.def.RequiresConsistency(r)
}

func (t topicPartitioner) Partition(r *kgo.Record, n int) int {
	if r.Partition >= 0 {
		return int(r.Partition)
	}
	return t.def.Partition(r, n)
}

// OnNewBatch keeps the sticky partitioner's batching behaviour.
func (t topicPartitioner) OnNewBatch() {
	if nb, ok := t.def.(kgo.TopicPartitionerOnNewBatch); ok {
		nb.OnNewBatch()
	}
}

// wrapErr maps Kafka error codes onto broker sentinel errors and adds a hint.
func wrapErr(what string, err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, kerr.UnknownTopicOrPartition), errors.Is(err, kerr.UnknownTopicID):
		return fmt.Errorf("%s: %w (%w)", what, broker.ErrNotFound, err)
	case errors.Is(err, kerr.GroupIDNotFound):
		return fmt.Errorf("%s: %w (%w)", what, broker.ErrNotFound, err)
	case errors.Is(err, kerr.TopicAuthorizationFailed), errors.Is(err, kerr.GroupAuthorizationFailed),
		errors.Is(err, kerr.ClusterAuthorizationFailed):
		return fmt.Errorf("%s: %w; check the ACLs of the configured user", what, err)
	case errors.Is(err, kerr.SecurityDisabled):
		return fmt.Errorf("%s: %w; the cluster has no authorizer configured", what, err)
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return fmt.Errorf("%s: %w; check that brokers are reachable and advertised listeners resolve from here", what, err)
	}
	return fmt.Errorf("%s: %w", what, err)
}
