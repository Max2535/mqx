package broker

import (
	"context"
	"time"
)

// Capabilities are optional. Callers type-assert:
//
//	if li, ok := b.(broker.LagReporter); ok { ... }
//
// Read-only capabilities come first, then mutating ones. Every method of a
// mutating capability must be called through the CLI/TUI mutation guard.

// ---------------------------------------------------------------------------
// Browse

// TopicDescriber returns partition-level detail and configs for one topic or queue.
type TopicDescriber interface {
	DescribeTopic(ctx context.Context, name string) (*TopicDetail, error)
}

// TopicDetail is the detail view of a topic or queue.
type TopicDetail struct {
	Topic      Topic           `json:"topic"`
	Partitions []PartitionInfo `json:"partitions,omitempty"`
	Configs    []ConfigEntry   `json:"configs,omitempty"`
}

// PartitionInfo describes one Kafka partition.
type PartitionInfo struct {
	ID       int32   `json:"id"`
	Leader   int32   `json:"leader"`
	Replicas []int32 `json:"replicas"`
	ISR      []int32 `json:"isr"`
	Start    int64   `json:"start_offset"`
	End      int64   `json:"end_offset"` // high watermark
}

// ClusterInspector lists the nodes of the cluster and their configs.
type ClusterInspector interface {
	Nodes(ctx context.Context) ([]Node, error)
	NodeConfig(ctx context.Context, id string) ([]ConfigEntry, error)
}

// Node is a Kafka broker or a RabbitMQ cluster node.
type Node struct {
	ID         string            `json:"id"`
	Host       string            `json:"host"`
	Port       int32             `json:"port,omitempty"`
	Rack       string            `json:"rack,omitempty"`
	Controller bool              `json:"controller,omitempty"`
	Running    bool              `json:"running"`
	Details    map[string]string `json:"details,omitempty"`
}

// ---------------------------------------------------------------------------
// Consumers

// ConsumerInspector reports who is attached to a topic or queue.
type ConsumerInspector interface {
	Consumers(ctx context.Context, topic string) ([]Consumer, error)
}

// Consumer is a Kafka group member assigned to the topic, or a RabbitMQ queue consumer.
type Consumer struct {
	Group      string  `json:"group,omitempty"` // kafka
	MemberID   string  `json:"member_id,omitempty"`
	InstanceID string  `json:"instance_id,omitempty"` // kafka static membership
	ClientID   string  `json:"client_id,omitempty"`
	Host       string  `json:"host,omitempty"`
	Partitions []int32 `json:"partitions,omitempty"`

	Tag         string `json:"tag,omitempty"` // rabbitmq consumer tag
	Connection  string `json:"connection,omitempty"`
	Channel     string `json:"channel,omitempty"`
	User        string `json:"user,omitempty"`
	AckRequired bool   `json:"ack_required,omitempty"`
	Prefetch    int    `json:"prefetch,omitempty"`
	Active      bool   `json:"active,omitempty"`
}

// GroupInspector lists and describes Kafka consumer groups.
type GroupInspector interface {
	ListGroups(ctx context.Context) ([]GroupSummary, error)
	DescribeGroup(ctx context.Context, group string) (*GroupDescription, error)
}

// GroupSummary is one row of a group listing.
type GroupSummary struct {
	Name          string `json:"name"`
	State         string `json:"state"`
	ProtocolType  string `json:"protocol_type"`
	GroupProtocol string `json:"group_protocol"` // "classic" or "consumer" (KIP-848)
	Members       int    `json:"members"`
}

// GroupDescription is everything needed to debug a group's rebalances.
type GroupDescription struct {
	Name          string        `json:"name"`
	State         string        `json:"state"` // Empty, PreparingRebalance, CompletingRebalance, Stable, Dead; KIP-848: Assigning, Reconciling
	ProtocolType  string        `json:"protocol_type"`
	GroupProtocol string        `json:"group_protocol"` // "classic" or "consumer"
	Assignor      string        `json:"assignor"`
	Coordinator   Node          `json:"coordinator"`
	Epoch         int32         `json:"epoch"` // classic generation or KIP-848 group epoch, -1 when unknown
	Members       []GroupMember `json:"members"`
	DescribedAt   time.Time     `json:"described_at"`
}

// GroupMember is one member of a group.
type GroupMember struct {
	MemberID      string            `json:"member_id"`
	InstanceID    string            `json:"instance_id,omitempty"`
	ClientID      string            `json:"client_id"`
	Host          string            `json:"host"`
	Subscriptions []string          `json:"subscriptions"`
	Pattern       string            `json:"pattern,omitempty"` // KIP-848 subscribed regex
	Assignment    []TopicPartitions `json:"assignment"`
	Target        []TopicPartitions `json:"target_assignment,omitempty"` // KIP-848
	MemberEpoch   int32             `json:"member_epoch,omitempty"`      // KIP-848
}

// TopicPartitions is a set of partitions of one topic.
type TopicPartitions struct {
	Topic      string  `json:"topic"`
	Partitions []int32 `json:"partitions"`
}

// LagReporter reports per-partition lag for a group.
type LagReporter interface {
	Lag(ctx context.Context, group string) ([]PartitionLag, error)
}

// PartitionLag is one partition's committed offset versus its end.
type PartitionLag struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Committed int64  `json:"committed"` // -1 when nothing committed
	End       int64  `json:"end"`
	Lag       int64  `json:"lag"`
	MemberID  string `json:"member_id,omitempty"`
	ClientID  string `json:"client_id,omitempty"`
	Host      string `json:"host,omitempty"`
}

// ConnectionInspector lists client connections and channels (RabbitMQ).
type ConnectionInspector interface {
	Connections(ctx context.Context) ([]Connection, error)
	Channels(ctx context.Context) ([]Channel, error)
}

// Connection is a client connection.
type Connection struct {
	Name        string            `json:"name"`
	User        string            `json:"user"`
	VHost       string            `json:"vhost"`
	Peer        string            `json:"peer"`
	State       string            `json:"state"`
	Channels    int               `json:"channels"`
	Protocol    string            `json:"protocol"`
	ClientProps map[string]string `json:"client_properties,omitempty"`
	ConnectedAt time.Time         `json:"connected_at"`
	RecvRate    float64           `json:"recv_bytes_per_sec"`
	SendRate    float64           `json:"send_bytes_per_sec"`
}

// Channel is an AMQP channel.
type Channel struct {
	Name          string  `json:"name"`
	Connection    string  `json:"connection"`
	Number        int     `json:"number"`
	User          string  `json:"user"`
	VHost         string  `json:"vhost"`
	State         string  `json:"state"`
	Consumers     int     `json:"consumers"`
	Prefetch      int     `json:"prefetch"`
	Unacked       int     `json:"unacked"`
	Confirm       bool    `json:"confirm"`
	PublishRate   float64 `json:"publish_rate"`
	DeliverRate   float64 `json:"deliver_rate"`
	Transactional bool    `json:"transactional"`
}

// ---------------------------------------------------------------------------
// Consumer management (mutating)

// OffsetManager changes committed offsets and removes groups.
type OffsetManager interface {
	// ResetOffsets moves a group's committed offsets. With DryRun it only
	// computes the changes. The group must have no active members.
	ResetOffsets(ctx context.Context, group string, req OffsetReset) ([]OffsetChange, error)
	DeleteGroup(ctx context.Context, group string) error
}

// OffsetReset says which partitions move where.
type OffsetReset struct {
	Topic      string
	Partitions []int32 // empty = all partitions of Topic
	To         Position
	// Shift moves relative to the current committed offset instead of To when non-zero.
	Shift  int64
	DryRun bool
}

// OffsetChange is one partition's committed offset before and after a reset.
type OffsetChange struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Old       int64  `json:"old"`
	New       int64  `json:"new"`
}

// ConsumerTerminator disconnects a consumer: a Kafka static member or a RabbitMQ connection.
type ConsumerTerminator interface {
	TerminateConsumer(ctx context.Context, target ConsumerTarget) error
}

// ConsumerTarget names the consumer to terminate.
type ConsumerTarget struct {
	Group      string // kafka
	InstanceID string // kafka static member (group.instance.id)
	Connection string // rabbitmq connection name
	Reason     string
}

// ---------------------------------------------------------------------------
// Admin (mutating)

// TopicAdmin creates, deletes and reconfigures topics or queues.
type TopicAdmin interface {
	CreateTopic(ctx context.Context, spec TopicSpec) error
	DeleteTopic(ctx context.Context, name string) error
	TopicConfig(ctx context.Context, name string) ([]ConfigEntry, error)
	// AlterTopicConfig sets keys to values; an empty value deletes the override.
	AlterTopicConfig(ctx context.Context, name string, changes map[string]string) error
}

// TopicSpec describes a topic or queue to create.
type TopicSpec struct {
	Name              string
	Partitions        int32             // kafka; 0 = broker default
	ReplicationFactor int16             // kafka; 0 = broker default
	Configs           map[string]string // kafka topic configs
	// RabbitMQ.
	Durable    bool
	AutoDelete bool
	Arguments  map[string]any // e.g. x-queue-type: quorum, x-message-ttl: 60000
}

// PartitionAdder grows a Kafka topic.
type PartitionAdder interface {
	// AddPartitions sets the topic's partition count to total.
	AddPartitions(ctx context.Context, topic string, total int) error
}

// Purger removes messages without deleting the topic or queue.
type Purger interface {
	Purge(ctx context.Context, topic string, opts PurgeOptions) (PurgeResult, error)
}

// PurgeOptions narrows a Kafka delete-records. RabbitMQ purges the whole queue
// and returns ErrUnsupported if any option is set.
type PurgeOptions struct {
	Partitions []int32
	// Before deletes records below this position; nil means everything (high watermark).
	Before *Position
}

// PurgeResult reports what was removed.
type PurgeResult struct {
	Messages   int64            `json:"messages"` // -1 when unknown
	Partitions []PartitionPurge `json:"partitions,omitempty"`
}

// PartitionPurge is one Kafka partition's new low watermark.
type PartitionPurge struct {
	Partition int32 `json:"partition"`
	LowMark   int64 `json:"low_watermark"`
}

// ---------------------------------------------------------------------------
// Routing (RabbitMQ)

// TopologyInspector lists vhosts, exchanges and bindings.
type TopologyInspector interface {
	VHosts(ctx context.Context) ([]VHost, error)
	Exchanges(ctx context.Context) ([]Exchange, error)
	Bindings(ctx context.Context) ([]Binding, error)
}

// VHost is a RabbitMQ virtual host.
type VHost struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Messages    int64  `json:"messages"`
	Tracing     bool   `json:"tracing,omitempty"`
}

// Exchange is a RabbitMQ exchange.
type Exchange struct {
	Name       string         `json:"name"`
	VHost      string         `json:"vhost,omitempty"`
	Type       string         `json:"type"` // direct, topic, fanout, headers, or a plugin type
	Durable    bool           `json:"durable"`
	AutoDelete bool           `json:"auto_delete"`
	Internal   bool           `json:"internal"`
	Arguments  map[string]any `json:"arguments,omitempty"`
}

// Binding connects an exchange to a queue or to another exchange.
type Binding struct {
	VHost           string         `json:"vhost,omitempty"`
	Source          string         `json:"source"` // "" = default exchange
	Destination     string         `json:"destination"`
	DestinationType string         `json:"destination_type"` // "queue" or "exchange"
	RoutingKey      string         `json:"routing_key"`
	Arguments       map[string]any `json:"arguments,omitempty"`
	PropertiesKey   string         `json:"properties_key,omitempty"` // management API id, needed to unbind
}

// TopologyEditor declares and removes exchanges and bindings (mutating).
type TopologyEditor interface {
	DeclareExchange(ctx context.Context, ex Exchange) error
	DeleteExchange(ctx context.Context, name string) error
	Bind(ctx context.Context, b Binding) error
	Unbind(ctx context.Context, b Binding) error
}

// RouteSimulator resolves a routing key through the binding graph without publishing.
type RouteSimulator interface {
	Route(ctx context.Context, exchange, routingKey string, headers map[string]string) (*RouteResult, error)
}

// RouteResult is where a message would land.
type RouteResult struct {
	Exchange   string     `json:"exchange"`
	RoutingKey string     `json:"routing_key"`
	Queues     []string   `json:"queues"` // sorted, de-duplicated
	Hops       []RouteHop `json:"hops"`   // every binding that matched, in traversal order
	// NotSimulated lists exchanges reached whose type the simulator cannot evaluate
	// (headers, x-consistent-hash, ...). Queues behind them are not in Queues.
	NotSimulated []string `json:"not_simulated,omitempty"`
	// AlternateExchanges used because nothing matched.
	Alternate []string `json:"alternate,omitempty"`
}

// RouteHop is one matched binding.
type RouteHop struct {
	Exchange        string `json:"exchange"`
	ExchangeType    string `json:"exchange_type"`
	BindingKey      string `json:"binding_key"`
	Destination     string `json:"destination"`
	DestinationType string `json:"destination_type"`
	Depth           int    `json:"depth"`
}

// ---------------------------------------------------------------------------
// Kafka ecosystem

// SchemaRegistry browses and manages Schema Registry subjects.
type SchemaRegistry interface {
	Subjects(ctx context.Context) ([]string, error)
	SchemaVersions(ctx context.Context, subject string) ([]int, error)
	// Schema returns one version; version -1 means latest.
	Schema(ctx context.Context, subject string, version int) (*Schema, error)
	SubjectCompatibility(ctx context.Context, subject string) (string, error)
	// Mutating.
	RegisterSchema(ctx context.Context, subject string, s Schema) (int, error)
	DeleteSubject(ctx context.Context, subject string, permanent bool) ([]int, error)
}

// Schema is one registered schema version.
type Schema struct {
	Subject    string      `json:"subject"`
	Version    int         `json:"version"`
	ID         int         `json:"id"`
	Type       string      `json:"type"` // AVRO, PROTOBUF, JSON
	Schema     string      `json:"schema"`
	References []SchemaRef `json:"references,omitempty"`
}

// ConnectManager manages Kafka Connect connectors.
type ConnectManager interface {
	Connectors(ctx context.Context) ([]Connector, error)
	Connector(ctx context.Context, name string) (*Connector, error)
	ConnectorPlugins(ctx context.Context) ([]ConnectorPlugin, error)
	// Mutating.
	PutConnector(ctx context.Context, name string, config map[string]string) error
	DeleteConnector(ctx context.Context, name string) error
	ConnectorAction(ctx context.Context, name string, action ConnectorAction) error
}

// ConnectorAction is a lifecycle operation on a connector.
type ConnectorAction string

// Connector actions.
const (
	ConnectorPause   ConnectorAction = "pause"
	ConnectorResume  ConnectorAction = "resume"
	ConnectorRestart ConnectorAction = "restart"
)

// Connector is a Kafka Connect connector with its status.
type Connector struct {
	Name   string            `json:"name"`
	Type   string            `json:"type"` // source or sink
	State  string            `json:"state"`
	Worker string            `json:"worker"`
	Config map[string]string `json:"config,omitempty"`
	Tasks  []ConnectorTask   `json:"tasks,omitempty"`
}

// ConnectorTask is one task of a connector.
type ConnectorTask struct {
	ID     int    `json:"id"`
	State  string `json:"state"`
	Worker string `json:"worker"`
	Trace  string `json:"trace,omitempty"`
}

// ConnectorPlugin is an installed connector class.
type ConnectorPlugin struct {
	Class   string `json:"class"`
	Type    string `json:"type"`
	Version string `json:"version"`
}

// KSQLRunner runs ksqlDB statements and queries.
type KSQLRunner interface {
	// RunKSQL runs one statement. SELECT returns rows (push queries need a LIMIT
	// or stop when ctx ends); other statements return a message or a listing.
	RunKSQL(ctx context.Context, statement string) (*KSQLResult, error)
}

// KSQLResult is a table of results or a status message.
type KSQLResult struct {
	Columns []string `json:"columns,omitempty"`
	Rows    [][]any  `json:"rows,omitempty"`
	Message string   `json:"message,omitempty"`
}

// ACLAdmin lists and edits Kafka ACLs. Create and Delete are mutating.
type ACLAdmin interface {
	ACLs(ctx context.Context, filter ACL) ([]ACL, error)
	CreateACL(ctx context.Context, acl ACL) error
	DeleteACLs(ctx context.Context, filter ACL) ([]ACL, error)
}

// ACL is one Kafka ACL binding. As a filter, empty fields match anything.
type ACL struct {
	Principal    string `json:"principal"`     // User:alice
	Host         string `json:"host"`          // * = any
	ResourceType string `json:"resource_type"` // topic, group, cluster, transactional_id, delegation_token
	ResourceName string `json:"resource_name"`
	PatternType  string `json:"pattern_type"` // literal, prefixed (filter also: any, match)
	Operation    string `json:"operation"`    // read, write, create, delete, alter, describe, all, ...
	Permission   string `json:"permission"`   // allow, deny
}

// ---------------------------------------------------------------------------
// RabbitMQ administration

// UserAdmin manages users, vhosts and permissions. All but the listings are mutating.
type UserAdmin interface {
	Users(ctx context.Context) ([]User, error)
	PutUser(ctx context.Context, u User, password string) error
	DeleteUser(ctx context.Context, name string) error
	PutVHost(ctx context.Context, v VHost) error
	DeleteVHost(ctx context.Context, name string) error
	Permissions(ctx context.Context) ([]Permission, error)
	SetPermission(ctx context.Context, p Permission) error
	ClearPermission(ctx context.Context, user, vhost string) error
}

// User is a RabbitMQ user. Password hashes are never exposed.
type User struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

// Permission is a user's configure/write/read regexes on a vhost.
type Permission struct {
	User      string `json:"user"`
	VHost     string `json:"vhost"`
	Configure string `json:"configure"`
	Write     string `json:"write"`
	Read      string `json:"read"`
}

// PolicyAdmin manages policies and runtime parameters (shovels, federation upstreams).
type PolicyAdmin interface {
	Policies(ctx context.Context) ([]Policy, error)
	PutPolicy(ctx context.Context, p Policy) error
	DeletePolicy(ctx context.Context, name string) error
	// Parameters lists runtime parameters of a component: "shovel" or "federation-upstream".
	Parameters(ctx context.Context, component string) ([]Parameter, error)
	PutParameter(ctx context.Context, p Parameter) error
	DeleteParameter(ctx context.Context, component, name string) error
	// LinkStatus reports running shovels ("shovel") or federation links ("federation").
	LinkStatus(ctx context.Context, kind string) ([]LinkStatus, error)
}

// Policy is a RabbitMQ policy or operator policy.
type Policy struct {
	Name       string         `json:"name"`
	VHost      string         `json:"vhost,omitempty"`
	Pattern    string         `json:"pattern"`
	ApplyTo    string         `json:"apply-to"` // queues, exchanges, all, classic_queues, quorum_queues, streams
	Priority   int            `json:"priority"`
	Definition map[string]any `json:"definition"`
}

// Parameter is a runtime parameter such as a dynamic shovel or a federation upstream.
type Parameter struct {
	Component string         `json:"component"`
	Name      string         `json:"name"`
	VHost     string         `json:"vhost,omitempty"`
	Value     map[string]any `json:"value"`
}

// LinkStatus is the runtime state of a shovel or federation link.
type LinkStatus struct {
	Name   string            `json:"name"`
	Kind   string            `json:"kind"`
	VHost  string            `json:"vhost,omitempty"`
	State  string            `json:"state"`
	Node   string            `json:"node,omitempty"`
	Error  string            `json:"error,omitempty"`
	Detail map[string]string `json:"detail,omitempty"`
}

// ---------------------------------------------------------------------------
// Metrics

// MetricsReporter samples counters so callers can derive rates over time.
type MetricsReporter interface {
	// Sample reads current values for a topic or queue, or cluster-wide when target is "".
	Sample(ctx context.Context, target string) (MetricSample, error)
}

// MetricSample is one point in time. Counters only ever grow, so the rate
// between two samples is their difference over the elapsed time. Gauges are
// instantaneous values.
type MetricSample struct {
	Time     time.Time          `json:"time"`
	Counters map[string]float64 `json:"counters"` // e.g. messages_in, messages_out, bytes_in
	Gauges   map[string]float64 `json:"gauges"`   // e.g. lag, depth, connections, consumers
}

// Well-known metric names. Adapters may add others.
const (
	MetricMessagesIn  = "messages_in"
	MetricMessagesOut = "messages_out"
	MetricLag         = "lag"
	MetricDepth       = "depth"
	MetricConnections = "connections"
	MetricConsumers   = "consumers"
)

// Rates returns per-second rates of every counter present in both samples.
func Rates(prev, cur MetricSample) map[string]float64 {
	secs := cur.Time.Sub(prev.Time).Seconds()
	out := make(map[string]float64, len(cur.Counters))
	if secs <= 0 {
		return out
	}
	for k, v := range cur.Counters {
		if p, ok := prev.Counters[k]; ok && v >= p {
			out[k] = (v - p) / secs
		}
	}
	return out
}
