// Package kafkatest starts Kafka and its ecosystem services in Docker for
// integration tests, with testcontainers-go.
//
// The broker is a single KRaft node (apache/kafka) that supports the KIP-848
// consumer protocol and has the StandardAuthorizer enabled with ANONYMOUS as a
// super user, so ACLs can be created and listed. It advertises a listener on
// a random host port for the tests and one on a shared Docker network for
// Schema Registry, Kafka Connect and ksqlDB.
package kafkatest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Images used by the helpers.
const (
	KafkaImage          = "apache/kafka:4.1.0"
	SchemaRegistryImage = "confluentinc/cp-schema-registry:7.9.0"
	ConnectImage        = "confluentinc/cp-kafka-connect:7.9.0"
	KSQLDBImage         = "confluentinc/cp-ksqldb-server:7.9.0"
)

const (
	kafkaAlias   = "kafka"
	internalAddr = kafkaAlias + ":19092"
	starterPath  = "/tmp/mqx-start.sh"
)

// Cluster is a running single-node Kafka.
type Cluster struct {
	// Brokers is the bootstrap address reachable from the test process.
	Brokers string
	// Internal is the bootstrap address reachable from containers on Network.
	Internal string

	network    *testcontainers.DockerNetwork
	containers []testcontainers.Container
}

// Start runs a Kafka broker. Call Terminate when done.
func Start(ctx context.Context) (*Cluster, error) {
	nw, err := tcnetwork.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create docker network: %w", err)
	}
	c := &Cluster{network: nw, Internal: internalAddr}
	env := map[string]string{
		"KAFKA_NODE_ID":                                  "1",
		"KAFKA_PROCESS_ROLES":                            "broker,controller",
		"KAFKA_LISTENERS":                                "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:19092,CONTROLLER://0.0.0.0:9093",
		"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "PLAINTEXT:PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT",
		"KAFKA_INTER_BROKER_LISTENER_NAME":               "BROKER",
		"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
		"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
		"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
		"KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS":             "1",
		"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
		"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
		"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
		"KAFKA_NUM_PARTITIONS":                           "1",
		"KAFKA_AUTHORIZER_CLASS_NAME":                    "org.apache.kafka.metadata.authorizer.StandardAuthorizer",
		"KAFKA_SUPER_USERS":                              "User:ANONYMOUS",
		// Short timeouts so stale classic members expire within a test.
		"KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS":             "1000",
		"KAFKA_GROUP_CONSUMER_MIN_SESSION_TIMEOUT_MS":    "1000",
		"KAFKA_GROUP_CONSUMER_SESSION_TIMEOUT_MS":        "6000",
		"KAFKA_GROUP_CONSUMER_MIN_HEARTBEAT_INTERVAL_MS": "500",
		"KAFKA_GROUP_CONSUMER_HEARTBEAT_INTERVAL_MS":     "1000",
	}
	req := testcontainers.GenericContainerRequest{
		Started: true,
		ContainerRequest: testcontainers.ContainerRequest{
			Image:          KafkaImage,
			Env:            env,
			ExposedPorts:   []string{"9092/tcp"},
			Networks:       []string{nw.Name},
			NetworkAliases: map[string][]string{nw.Name: {kafkaAlias}},
			// The advertised host port is only known after start: wait for
			// the starter script the PostStarts hook writes.
			Entrypoint: []string{"sh", "-c", "while [ ! -f " + starterPath + " ]; do sleep 0.1; done; exec sh " + starterPath},
			LifecycleHooks: []testcontainers.ContainerLifecycleHooks{{
				PostStarts: []testcontainers.ContainerHook{c.writeStarter},
			}},
			WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(2 * time.Minute),
		},
	}
	ctr, err := testcontainers.GenericContainer(ctx, req)
	if ctr != nil {
		c.containers = append(c.containers, ctr)
	}
	if err != nil {
		return nil, errors.Join(fmt.Errorf("start kafka: %w", err), c.Terminate(context.Background()))
	}
	return c, nil
}

func (c *Cluster) writeStarter(ctx context.Context, ctr testcontainers.Container) error {
	host, err := ctr.Host(ctx)
	if err != nil {
		return err
	}
	port, err := ctr.MappedPort(ctx, "9092/tcp")
	if err != nil {
		return err
	}
	c.Brokers = fmt.Sprintf("%s:%d", host, port.Num())
	script := fmt.Sprintf("export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://%s,BROKER://%s\nexec /__cacert_entrypoint.sh /etc/kafka/docker/run\n",
		c.Brokers, internalAddr)
	return ctr.CopyToContainer(ctx, []byte(script), starterPath, 0o755)
}

// Terminate stops every container started for the cluster and removes the network.
func (c *Cluster) Terminate(ctx context.Context) error {
	var errs []error
	for i := len(c.containers) - 1; i >= 0; i-- {
		errs = append(errs, testcontainers.TerminateContainer(c.containers[i]))
	}
	c.containers = nil
	if c.network != nil {
		errs = append(errs, c.network.Remove(ctx))
		c.network = nil
	}
	return errors.Join(errs...)
}

// service starts a container on the cluster network and returns its HTTP URL.
func (c *Cluster) service(ctx context.Context, name, image string, port string, env map[string]string, ready wait.Strategy) (string, error) {
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started: true,
		ContainerRequest: testcontainers.ContainerRequest{
			Image:          image,
			Env:            env,
			ExposedPorts:   []string{port + "/tcp"},
			Networks:       []string{c.network.Name},
			NetworkAliases: map[string][]string{c.network.Name: {name}},
			WaitingFor:     ready,
		},
	})
	if ctr != nil {
		c.containers = append(c.containers, ctr)
	}
	if err != nil {
		return "", fmt.Errorf("start %s: %w%s", name, err, tail(ctx, ctr))
	}
	host, err := ctr.Host(ctx)
	if err != nil {
		return "", err
	}
	mapped, err := ctr.MappedPort(ctx, port+"/tcp")
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("http://%s:%d", host, mapped.Num()), nil
}

// tail returns the last lines of a failed container's log for error messages.
func tail(ctx context.Context, ctr testcontainers.Container) string {
	if ctr == nil {
		return ""
	}
	rc, err := ctr.Logs(ctx)
	if err != nil {
		return ""
	}
	defer rc.Close()
	data, _ := io.ReadAll(rc)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) > 30 {
		lines = lines[len(lines)-30:]
	}
	return "\n" + strings.Join(lines, "\n")
}

// StartSchemaRegistry runs Confluent Schema Registry against the cluster and returns its URL.
func (c *Cluster) StartSchemaRegistry(ctx context.Context) (string, error) {
	return c.service(ctx, "schema-registry", SchemaRegistryImage, "8081", map[string]string{
		"SCHEMA_REGISTRY_HOST_NAME":                    "schema-registry",
		"SCHEMA_REGISTRY_LISTENERS":                    "http://0.0.0.0:8081",
		"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS": "PLAINTEXT://" + internalAddr,
	}, wait.ForHTTP("/subjects").WithPort("8081/tcp").WithStartupTimeout(3*time.Minute))
}

// StartConnect runs a Kafka Connect worker against the cluster and returns its URL.
func (c *Cluster) StartConnect(ctx context.Context) (string, error) {
	return c.service(ctx, "connect", ConnectImage, "8083", map[string]string{
		"CONNECT_BOOTSTRAP_SERVERS":                 internalAddr,
		"CONNECT_REST_ADVERTISED_HOST_NAME":         "connect",
		"CONNECT_REST_PORT":                         "8083",
		"CONNECT_GROUP_ID":                          "mqx-connect",
		"CONNECT_CONFIG_STORAGE_TOPIC":              "_connect-configs",
		"CONNECT_OFFSET_STORAGE_TOPIC":              "_connect-offsets",
		"CONNECT_STATUS_STORAGE_TOPIC":              "_connect-status",
		"CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR": "1",
		"CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR": "1",
		"CONNECT_STATUS_STORAGE_REPLICATION_FACTOR": "1",
		"CONNECT_KEY_CONVERTER":                     "org.apache.kafka.connect.storage.StringConverter",
		"CONNECT_VALUE_CONVERTER":                   "org.apache.kafka.connect.storage.StringConverter",
		"CONNECT_PLUGIN_PATH":                       "/usr/share/java",
	}, wait.ForHTTP("/connectors").WithPort("8083/tcp").WithStartupTimeout(4*time.Minute))
}

// StartKSQLDB runs a ksqlDB server against the cluster and returns its URL.
func (c *Cluster) StartKSQLDB(ctx context.Context) (string, error) {
	return c.service(ctx, "ksqldb", KSQLDBImage, "8088", map[string]string{
		"KSQL_BOOTSTRAP_SERVERS":                          internalAddr,
		"KSQL_LISTENERS":                                  "http://0.0.0.0:8088",
		"KSQL_KSQL_SERVICE_ID":                            "mqx_",
		"KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE": "false",
		"KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE":  "false",
		"KSQL_KSQL_INTERNAL_TOPIC_REPLICAS":               "1",
		"KSQL_KSQL_STREAMS_REPLICATION_FACTOR":            "1",
		"KSQL_KSQL_SINK_REPLICAS":                         "1",
	}, wait.ForHTTP("/info").WithPort("8088/tcp").WithStartupTimeout(4*time.Minute))
}
