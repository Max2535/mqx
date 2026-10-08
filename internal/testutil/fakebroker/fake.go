// Package fakebroker is an in-memory broker implementing every capability,
// for CLI and TUI tests. Register it with New, which returns the config
// context that opens it:
//
//	f, ctx := fakebroker.New(t)
//	f.AddTopic("orders", 3)
//
// Set Caps to limit which capabilities it reports (via broker.CapabilityChecker).
package fakebroker

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/config"
)

// Type is the broker type name the fake registers.
const Type = "fake"

var (
	instances sync.Map // id -> *Fake
	nextID    atomic.Int64
)

func init() {
	broker.Register(Type, broker.Driver{
		Open: func(_ context.Context, c config.Context, _ config.Credentials) (broker.Broker, error) {
			v, ok := instances.Load(c.Options["instance"])
			if !ok {
				return nil, fmt.Errorf("fake instance %q not registered", c.Options["instance"])
			}
			f := v.(*Fake)
			if f.OpenErr != nil {
				return nil, f.OpenErr
			}
			return f, nil
		},
		Validate: func(c config.Context) []error {
			if c.Options["instance"] == "" {
				return []error{fmt.Errorf("fake needs options.instance")}
			}
			return nil
		},
	})
}

// New registers a fresh fake and returns it with a context named "fake" that opens it.
func New(t testing.TB) (*Fake, config.Context) {
	t.Helper()
	id := fmt.Sprintf("fake-%d", nextID.Add(1))
	f := &Fake{
		topics:   map[string]*topic{},
		groups:   map[string]*group{},
		configs:  map[string]map[string]string{},
		subjects: map[string][]broker.Schema{},
		conns:    map[string]*broker.Connector{},
		users:    map[string]broker.User{},
		vhosts:   map[string]broker.VHost{"/": {Name: "/"}},
		policies: map[string]broker.Policy{},
		params:   map[string]broker.Parameter{},
		Now:      time.Now,
	}
	instances.Store(id, f)
	t.Cleanup(func() { instances.Delete(id) })
	return f, config.Context{Name: "fake", Broker: Type, Options: map[string]string{"instance": id}}
}

// Fake is the in-memory broker. All methods are safe for concurrent use.
type Fake struct {
	mu sync.Mutex

	// Caps, when non-nil, limits the capabilities reported to these names.
	Caps []string
	// OpenErr makes broker.Open fail.
	OpenErr error
	// Now is the clock used for timestamps and metric samples.
	Now func() time.Time
	// Calls records every mutating call as "Method arg..." for assertions.
	Calls []string

	topics    map[string]*topic
	groups    map[string]*group
	configs   map[string]map[string]string
	exchanges []broker.Exchange
	bindings  []broker.Binding
	nodes     []broker.Node
	conns     map[string]*broker.Connector
	cxns      []broker.Connection
	channels  []broker.Channel
	acls      []broker.ACL
	subjects  map[string][]broker.Schema
	users     map[string]broker.User
	vhosts    map[string]broker.VHost
	perms     []broker.Permission
	policies  map[string]broker.Policy
	params    map[string]broker.Parameter
	consumers map[string][]broker.Consumer
	closed    bool
}

type topic struct {
	partitions [][]broker.Message
	low        []int64
}

type group struct {
	desc      broker.GroupDescription
	committed map[string]map[int32]int64
}

func (f *Fake) record(format string, args ...any) {
	f.Calls = append(f.Calls, fmt.Sprintf(format, args...))
}

// HasCapability implements broker.CapabilityChecker.
func (f *Fake) HasCapability(name string) bool {
	return f.Caps == nil || slices.Contains(f.Caps, name)
}

// ---------------------------------------------------------------------------
// Setup helpers

// AddTopic creates a topic with n partitions (n < 1 means 1).
func (f *Fake) AddTopic(name string, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addTopicLocked(name, n)
}

func (f *Fake) addTopicLocked(name string, n int) {
	if n < 1 {
		n = 1
	}
	f.topics[name] = &topic{partitions: make([][]broker.Message, n), low: make([]int64, n)}
}

// AddMessages appends messages to a partition, assigning offsets.
func (f *Fake) AddMessages(topicName string, partition int32, msgs ...broker.Message) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := f.topics[topicName]
	for _, m := range msgs {
		m.Topic = topicName
		m.Partition = partition
		m.Offset = t.low[partition] + int64(len(t.partitions[partition]))
		if m.Timestamp.IsZero() {
			m.Timestamp = f.Now()
		}
		t.partitions[partition] = append(t.partitions[partition], m)
	}
}

// AddGroup adds a consumer group with committed offsets topic -> partition -> offset.
func (f *Fake) AddGroup(d broker.GroupDescription, committed map[string]map[int32]int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if committed == nil {
		committed = map[string]map[int32]int64{}
	}
	f.groups[d.Name] = &group{desc: d, committed: committed}
}

// SetGroupState changes a group's state and epoch, e.g. to simulate a rebalance.
func (f *Fake) SetGroupState(name, state string, epoch int32, members []broker.GroupMember) {
	f.mu.Lock()
	defer f.mu.Unlock()
	g := f.groups[name]
	g.desc.State, g.desc.Epoch, g.desc.Members = state, epoch, members
}

// AddExchange adds an exchange.
func (f *Fake) AddExchange(ex broker.Exchange) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exchanges = append(f.exchanges, ex)
}

// AddBinding adds a binding.
func (f *Fake) AddBinding(b broker.Binding) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bindings = append(f.bindings, b)
}

// SetNodes sets the cluster nodes.
func (f *Fake) SetNodes(nodes ...broker.Node) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodes = nodes
}

// SetConsumers sets the RabbitMQ-style consumers of a queue.
func (f *Fake) SetConsumers(topicName string, cs ...broker.Consumer) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.consumers == nil {
		f.consumers = map[string][]broker.Consumer{}
	}
	f.consumers[topicName] = cs
}

// SetConnections sets connections and channels.
func (f *Fake) SetConnections(cs []broker.Connection, chs []broker.Channel) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cxns, f.channels = cs, chs
}

// Messages returns a copy of a partition's messages.
func (f *Fake) Messages(topicName string, partition int32) []broker.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.topics[topicName].partitions[partition])
}

// Closed reports whether Close was called.
func (f *Fake) Closed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// ---------------------------------------------------------------------------
// Core

// Name implements broker.Broker.
func (f *Fake) Name() string { return Type }

// Ping implements broker.Broker.
func (f *Fake) Ping(context.Context) error { return nil }

// Close implements broker.Broker.
func (f *Fake) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

// ListTopics implements broker.Broker.
func (f *Fake) ListTopics(context.Context) ([]broker.Topic, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]broker.Topic, 0, len(f.topics))
	for _, name := range sortedKeys(f.topics) {
		out = append(out, f.topicInfoLocked(name))
	}
	return out, nil
}

func (f *Fake) topicInfoLocked(name string) broker.Topic {
	t := f.topics[name]
	var n int64
	for _, p := range t.partitions {
		n += int64(len(p))
	}
	return broker.Topic{Name: name, Kind: "topic", Partitions: len(t.partitions), Replicas: 1, Messages: n,
		Consumers: len(f.consumers[name]), Internal: strings.HasPrefix(name, "__")} // like Kafka's __consumer_offsets
}

// Publish implements broker.Broker.
func (f *Fake) Publish(_ context.Context, topicName string, msg broker.Message) error {
	f.mu.Lock()
	t, ok := f.topics[topicName]
	f.mu.Unlock()
	if !ok {
		return fmt.Errorf("topic %q: %w", topicName, broker.ErrNotFound)
	}
	p := msg.Partition
	if p == broker.AnyPartition {
		p = 0
	}
	if int(p) >= len(t.partitions) {
		return fmt.Errorf("partition %d out of range", p)
	}
	f.mu.Lock()
	f.record("Publish %s %d %s", topicName, p, msg.Value)
	f.mu.Unlock()
	f.AddMessages(topicName, p, msg)
	return nil
}

// Peek implements broker.Broker. It honours Partitions, From (Earliest, Latest,
// AtOffset, Tail), Filter and Limit; Follow is ignored.
func (f *Fake) Peek(ctx context.Context, topicName string, opts broker.PeekOptions) (<-chan broker.Message, error) {
	f.mu.Lock()
	t, ok := f.topics[topicName]
	if !ok {
		f.mu.Unlock()
		return nil, fmt.Errorf("topic %q: %w", topicName, broker.ErrNotFound)
	}
	var msgs []broker.Message
	for p, part := range t.partitions {
		if len(opts.Partitions) > 0 && !slices.Contains(opts.Partitions, int32(p)) {
			continue
		}
		start := 0
		switch opts.From.Kind {
		case broker.Latest:
			start = len(part)
		case broker.AtOffset:
			start = int(opts.From.Offset - t.low[p])
		case broker.Tail:
			start = len(part) - int(opts.From.Offset)
		}
		start = max(0, min(start, len(part)))
		msgs = append(msgs, part[start:]...)
	}
	f.mu.Unlock()
	ch := make(chan broker.Message)
	go func() {
		defer close(ch)
		sent := 0
		for _, m := range msgs {
			if !opts.Filter.Match(m) {
				continue
			}
			select {
			case ch <- m:
			case <-ctx.Done():
				return
			}
			sent++
			if opts.Limit > 0 && sent >= opts.Limit {
				return
			}
		}
	}()
	return ch, nil
}

// ---------------------------------------------------------------------------
// Browse

// DescribeTopic implements broker.TopicDescriber.
func (f *Fake) DescribeTopic(_ context.Context, name string) (*broker.TopicDetail, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.topics[name]
	if !ok {
		return nil, fmt.Errorf("topic %q: %w", name, broker.ErrNotFound)
	}
	d := &broker.TopicDetail{Topic: f.topicInfoLocked(name)}
	for p := range t.partitions {
		d.Partitions = append(d.Partitions, broker.PartitionInfo{ID: int32(p), Leader: 1, Replicas: []int32{1}, ISR: []int32{1},
			Start: t.low[p], End: t.low[p] + int64(len(t.partitions[p]))})
	}
	d.Configs = f.configEntriesLocked(name)
	return d, nil
}

func (f *Fake) configEntriesLocked(name string) []broker.ConfigEntry {
	var out []broker.ConfigEntry
	for _, k := range sortedKeys(f.configs[name]) {
		out = append(out, broker.ConfigEntry{Name: k, Value: f.configs[name][k], Source: "DYNAMIC_TOPIC_CONFIG"})
	}
	return out
}

// Nodes implements broker.ClusterInspector.
func (f *Fake) Nodes(context.Context) ([]broker.Node, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.nodes), nil
}

// NodeConfig implements broker.ClusterInspector.
func (f *Fake) NodeConfig(_ context.Context, id string) ([]broker.ConfigEntry, error) {
	return []broker.ConfigEntry{{Name: "node.id", Value: id, Source: "STATIC_BROKER_CONFIG", ReadOnly: true}}, nil
}

// ---------------------------------------------------------------------------
// Consumers

// Consumers implements broker.ConsumerInspector: queue consumers set with
// SetConsumers plus members of groups assigned partitions of the topic.
func (f *Fake) Consumers(_ context.Context, topicName string) ([]broker.Consumer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := slices.Clone(f.consumers[topicName])
	for _, name := range sortedKeys(f.groups) {
		for _, m := range f.groups[name].desc.Members {
			for _, a := range m.Assignment {
				if a.Topic == topicName {
					out = append(out, broker.Consumer{Group: name, MemberID: m.MemberID, InstanceID: m.InstanceID,
						ClientID: m.ClientID, Host: m.Host, Partitions: a.Partitions})
				}
			}
		}
	}
	return out, nil
}

// ListGroups implements broker.GroupInspector.
func (f *Fake) ListGroups(context.Context) ([]broker.GroupSummary, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []broker.GroupSummary
	for _, name := range sortedKeys(f.groups) {
		d := f.groups[name].desc
		out = append(out, broker.GroupSummary{Name: name, State: d.State, ProtocolType: d.ProtocolType,
			GroupProtocol: d.GroupProtocol, Members: len(d.Members)})
	}
	return out, nil
}

// DescribeGroup implements broker.GroupInspector.
func (f *Fake) DescribeGroup(_ context.Context, name string) (*broker.GroupDescription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	g, ok := f.groups[name]
	if !ok {
		return nil, fmt.Errorf("group %q: %w", name, broker.ErrNotFound)
	}
	d := g.desc
	d.Members = slices.Clone(d.Members)
	d.DescribedAt = f.Now()
	return &d, nil
}

// Lag implements broker.LagReporter.
func (f *Fake) Lag(_ context.Context, name string) ([]broker.PartitionLag, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	g, ok := f.groups[name]
	if !ok {
		return nil, fmt.Errorf("group %q: %w", name, broker.ErrNotFound)
	}
	var out []broker.PartitionLag
	for _, tn := range sortedKeys(g.committed) {
		t := f.topics[tn]
		for _, p := range sortedKeys(g.committed[tn]) {
			end := int64(0)
			if t != nil && int(p) < len(t.partitions) {
				end = t.low[p] + int64(len(t.partitions[p]))
			}
			c := g.committed[tn][p]
			pl := broker.PartitionLag{Topic: tn, Partition: p, Committed: c, End: end, Lag: max(0, end-c)}
			for _, m := range g.desc.Members {
				for _, a := range m.Assignment {
					if a.Topic == tn && slices.Contains(a.Partitions, p) {
						pl.MemberID, pl.ClientID, pl.Host = m.MemberID, m.ClientID, m.Host
					}
				}
			}
			out = append(out, pl)
		}
	}
	return out, nil
}

// Connections implements broker.ConnectionInspector.
func (f *Fake) Connections(context.Context) ([]broker.Connection, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.cxns), nil
}

// Channels implements broker.ConnectionInspector.
func (f *Fake) Channels(context.Context) ([]broker.Channel, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.channels), nil
}

// ---------------------------------------------------------------------------
// Consumer management

// ResetOffsets implements broker.OffsetManager for Earliest, Latest, AtOffset and Shift.
func (f *Fake) ResetOffsets(_ context.Context, name string, req broker.OffsetReset) ([]broker.OffsetChange, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	g, ok := f.groups[name]
	if !ok {
		return nil, fmt.Errorf("group %q: %w", name, broker.ErrNotFound)
	}
	if len(g.desc.Members) > 0 {
		return nil, fmt.Errorf("group %q has %d active members; stop them first", name, len(g.desc.Members))
	}
	t, ok := f.topics[req.Topic]
	if !ok {
		return nil, fmt.Errorf("topic %q: %w", req.Topic, broker.ErrNotFound)
	}
	var out []broker.OffsetChange
	for p := range t.partitions {
		pid := int32(p)
		if len(req.Partitions) > 0 && !slices.Contains(req.Partitions, pid) {
			continue
		}
		old, had := g.committed[req.Topic][pid]
		if !had {
			old = -1
		}
		end := t.low[p] + int64(len(t.partitions[p]))
		var target int64
		switch {
		case req.Shift != 0:
			target = max(t.low[p], min(end, old+req.Shift))
		case req.To.Kind == broker.Earliest:
			target = t.low[p]
		case req.To.Kind == broker.Latest:
			target = end
		case req.To.Kind == broker.AtOffset:
			target = max(t.low[p], min(end, req.To.Offset))
		default:
			return nil, fmt.Errorf("fake: position kind %d: %w", req.To.Kind, broker.ErrUnsupported)
		}
		out = append(out, broker.OffsetChange{Topic: req.Topic, Partition: pid, Old: old, New: target})
	}
	if !req.DryRun {
		f.record("ResetOffsets %s %s", name, req.Topic)
		if g.committed[req.Topic] == nil {
			g.committed[req.Topic] = map[int32]int64{}
		}
		for _, c := range out {
			g.committed[req.Topic][c.Partition] = c.New
		}
	}
	return out, nil
}

// DeleteGroup implements broker.OffsetManager.
func (f *Fake) DeleteGroup(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.groups[name]; !ok {
		return fmt.Errorf("group %q: %w", name, broker.ErrNotFound)
	}
	f.record("DeleteGroup %s", name)
	delete(f.groups, name)
	return nil
}

// TerminateConsumer implements broker.ConsumerTerminator.
func (f *Fake) TerminateConsumer(_ context.Context, t broker.ConsumerTarget) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if t.Connection != "" {
		f.record("CloseConnection %s", t.Connection)
		f.cxns = slices.DeleteFunc(f.cxns, func(c broker.Connection) bool { return c.Name == t.Connection })
		return nil
	}
	g, ok := f.groups[t.Group]
	if !ok {
		return fmt.Errorf("group %q: %w", t.Group, broker.ErrNotFound)
	}
	n := len(g.desc.Members)
	g.desc.Members = slices.DeleteFunc(g.desc.Members, func(m broker.GroupMember) bool { return m.InstanceID == t.InstanceID })
	if len(g.desc.Members) == n {
		return fmt.Errorf("static member %q in group %q: %w", t.InstanceID, t.Group, broker.ErrNotFound)
	}
	f.record("RemoveMember %s %s", t.Group, t.InstanceID)
	return nil
}

// ---------------------------------------------------------------------------
// Admin

// CreateTopic implements broker.TopicAdmin.
func (f *Fake) CreateTopic(_ context.Context, spec broker.TopicSpec) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.topics[spec.Name]; ok {
		return fmt.Errorf("topic %q already exists", spec.Name)
	}
	f.record("CreateTopic %s %d", spec.Name, spec.Partitions)
	f.addTopicLocked(spec.Name, int(spec.Partitions))
	if len(spec.Configs) > 0 {
		f.configs[spec.Name] = maps.Clone(spec.Configs)
	}
	return nil
}

// DeleteTopic implements broker.TopicAdmin.
func (f *Fake) DeleteTopic(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.topics[name]; !ok {
		return fmt.Errorf("topic %q: %w", name, broker.ErrNotFound)
	}
	f.record("DeleteTopic %s", name)
	delete(f.topics, name)
	return nil
}

// TopicConfig implements broker.TopicAdmin.
func (f *Fake) TopicConfig(_ context.Context, name string) ([]broker.ConfigEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.configEntriesLocked(name), nil
}

// AlterTopicConfig implements broker.TopicAdmin.
func (f *Fake) AlterTopicConfig(_ context.Context, name string, changes map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("AlterTopicConfig %s %s", name, kv(changes))
	if f.configs[name] == nil {
		f.configs[name] = map[string]string{}
	}
	for k, v := range changes {
		if v == "" {
			delete(f.configs[name], k)
		} else {
			f.configs[name][k] = v
		}
	}
	return nil
}

// AddPartitions implements broker.PartitionAdder.
func (f *Fake) AddPartitions(_ context.Context, name string, total int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.topics[name]
	if !ok {
		return fmt.Errorf("topic %q: %w", name, broker.ErrNotFound)
	}
	if total <= len(t.partitions) {
		return fmt.Errorf("topic %q already has %d partitions", name, len(t.partitions))
	}
	f.record("AddPartitions %s %d", name, total)
	for len(t.partitions) < total {
		t.partitions = append(t.partitions, nil)
		t.low = append(t.low, 0)
	}
	return nil
}

// Purge implements broker.Purger: deletes all messages, or those below Before (AtOffset).
func (f *Fake) Purge(_ context.Context, name string, opts broker.PurgeOptions) (broker.PurgeResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.topics[name]
	if !ok {
		return broker.PurgeResult{}, fmt.Errorf("topic %q: %w", name, broker.ErrNotFound)
	}
	f.record("Purge %s", name)
	var res broker.PurgeResult
	for p := range t.partitions {
		if len(opts.Partitions) > 0 && !slices.Contains(opts.Partitions, int32(p)) {
			continue
		}
		cut := len(t.partitions[p])
		if opts.Before != nil && opts.Before.Kind == broker.AtOffset {
			cut = max(0, min(cut, int(opts.Before.Offset-t.low[p])))
		}
		res.Messages += int64(cut)
		t.partitions[p] = t.partitions[p][cut:]
		t.low[p] += int64(cut)
		res.Partitions = append(res.Partitions, broker.PartitionPurge{Partition: int32(p), LowMark: t.low[p]})
	}
	return res, nil
}

// ---------------------------------------------------------------------------
// Topology

// VHosts implements broker.TopologyInspector.
func (f *Fake) VHosts(context.Context) ([]broker.VHost, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []broker.VHost
	for _, k := range sortedKeys(f.vhosts) {
		out = append(out, f.vhosts[k])
	}
	return out, nil
}

// Exchanges implements broker.TopologyInspector.
func (f *Fake) Exchanges(context.Context) ([]broker.Exchange, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.exchanges), nil
}

// Bindings implements broker.TopologyInspector.
func (f *Fake) Bindings(context.Context) ([]broker.Binding, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.bindings), nil
}

// DeclareExchange implements broker.TopologyEditor.
func (f *Fake) DeclareExchange(_ context.Context, ex broker.Exchange) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("DeclareExchange %s %s", ex.Name, ex.Type)
	f.exchanges = append(f.exchanges, ex)
	return nil
}

// DeleteExchange implements broker.TopologyEditor.
func (f *Fake) DeleteExchange(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("DeleteExchange %s", name)
	f.exchanges = slices.DeleteFunc(f.exchanges, func(e broker.Exchange) bool { return e.Name == name })
	return nil
}

// Bind implements broker.TopologyEditor.
func (f *Fake) Bind(_ context.Context, b broker.Binding) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("Bind %s %s %s %s", b.Source, b.DestinationType, b.Destination, b.RoutingKey)
	f.bindings = append(f.bindings, b)
	return nil
}

// Unbind implements broker.TopologyEditor.
func (f *Fake) Unbind(_ context.Context, b broker.Binding) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("Unbind %s %s %s %s", b.Source, b.DestinationType, b.Destination, b.RoutingKey)
	f.bindings = slices.DeleteFunc(f.bindings, func(x broker.Binding) bool {
		return x.Source == b.Source && x.Destination == b.Destination && x.DestinationType == b.DestinationType &&
			x.RoutingKey == b.RoutingKey
	})
	return nil
}

// Route implements broker.RouteSimulator with exact-match (direct) semantics only.
func (f *Fake) Route(_ context.Context, exchange, key string, _ map[string]string) (*broker.RouteResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	res := &broker.RouteResult{Exchange: exchange, RoutingKey: key, Queues: []string{}}
	for _, b := range f.bindings {
		if b.Source == exchange && b.RoutingKey == key && b.DestinationType == "queue" {
			res.Queues = append(res.Queues, b.Destination)
			res.Hops = append(res.Hops, broker.RouteHop{Exchange: exchange, ExchangeType: "direct", BindingKey: key,
				Destination: b.Destination, DestinationType: "queue"})
		}
	}
	sort.Strings(res.Queues)
	return res, nil
}

// ---------------------------------------------------------------------------
// Ecosystem

// Subjects implements broker.SchemaRegistry.
func (f *Fake) Subjects(context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return sortedKeys(f.subjects), nil
}

// SchemaVersions implements broker.SchemaRegistry.
func (f *Fake) SchemaVersions(_ context.Context, subject string) ([]int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []int
	for _, s := range f.subjects[subject] {
		out = append(out, s.Version)
	}
	if out == nil {
		return nil, fmt.Errorf("subject %q: %w", subject, broker.ErrNotFound)
	}
	return out, nil
}

// Schema implements broker.SchemaRegistry.
func (f *Fake) Schema(_ context.Context, subject string, version int) (*broker.Schema, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	vs := f.subjects[subject]
	if len(vs) == 0 {
		return nil, fmt.Errorf("subject %q: %w", subject, broker.ErrNotFound)
	}
	if version == -1 {
		s := vs[len(vs)-1]
		return &s, nil
	}
	for _, s := range vs {
		if s.Version == version {
			return &s, nil
		}
	}
	return nil, fmt.Errorf("subject %q version %d: %w", subject, version, broker.ErrNotFound)
}

// SubjectCompatibility implements broker.SchemaRegistry.
func (f *Fake) SubjectCompatibility(context.Context, string) (string, error) { return "BACKWARD", nil }

// RegisterSchema implements broker.SchemaRegistry.
func (f *Fake) RegisterSchema(_ context.Context, subject string, s broker.Schema) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("RegisterSchema %s", subject)
	s.Subject = subject
	s.Version = len(f.subjects[subject]) + 1
	s.ID = 100 + len(f.subjects)*10 + s.Version
	if s.Type == "" {
		s.Type = "AVRO"
	}
	f.subjects[subject] = append(f.subjects[subject], s)
	return s.ID, nil
}

// DeleteSubject implements broker.SchemaRegistry.
func (f *Fake) DeleteSubject(_ context.Context, subject string, _ bool) ([]int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("DeleteSubject %s", subject)
	var out []int
	for _, s := range f.subjects[subject] {
		out = append(out, s.Version)
	}
	delete(f.subjects, subject)
	return out, nil
}

// Connectors implements broker.ConnectManager.
func (f *Fake) Connectors(context.Context) ([]broker.Connector, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []broker.Connector
	for _, k := range sortedKeys(f.conns) {
		out = append(out, *f.conns[k])
	}
	return out, nil
}

// Connector implements broker.ConnectManager.
func (f *Fake) Connector(_ context.Context, name string) (*broker.Connector, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.conns[name]
	if !ok {
		return nil, fmt.Errorf("connector %q: %w", name, broker.ErrNotFound)
	}
	cp := *c
	return &cp, nil
}

// ConnectorPlugins implements broker.ConnectManager.
func (f *Fake) ConnectorPlugins(context.Context) ([]broker.ConnectorPlugin, error) {
	return []broker.ConnectorPlugin{{Class: "org.apache.kafka.connect.mirror.MirrorSourceConnector", Type: "source", Version: "4.1.0"}}, nil
}

// PutConnector implements broker.ConnectManager.
func (f *Fake) PutConnector(_ context.Context, name string, cfg map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("PutConnector %s", name)
	f.conns[name] = &broker.Connector{Name: name, Type: "source", State: "RUNNING", Config: maps.Clone(cfg)}
	return nil
}

// DeleteConnector implements broker.ConnectManager.
func (f *Fake) DeleteConnector(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("DeleteConnector %s", name)
	delete(f.conns, name)
	return nil
}

// ConnectorAction implements broker.ConnectManager.
func (f *Fake) ConnectorAction(_ context.Context, name string, a broker.ConnectorAction) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.conns[name]
	if !ok {
		return fmt.Errorf("connector %q: %w", name, broker.ErrNotFound)
	}
	f.record("ConnectorAction %s %s", name, a)
	switch a {
	case broker.ConnectorPause:
		c.State = "PAUSED"
	case broker.ConnectorResume, broker.ConnectorRestart:
		c.State = "RUNNING"
	}
	return nil
}

// RunKSQL implements broker.KSQLRunner by echoing the statement.
func (f *Fake) RunKSQL(_ context.Context, stmt string) (*broker.KSQLResult, error) {
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(stmt)), "SELECT") {
		return &broker.KSQLResult{Columns: []string{"ID", "TOTAL"}, Rows: [][]any{{"a", 1.0}, {"b", 2.0}}}, nil
	}
	return &broker.KSQLResult{Message: "ok: " + stmt}, nil
}

// ACLs implements broker.ACLAdmin.
func (f *Fake) ACLs(_ context.Context, filter broker.ACL) ([]broker.ACL, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []broker.ACL
	for _, a := range f.acls {
		if aclMatch(filter, a) {
			out = append(out, a)
		}
	}
	return out, nil
}

// CreateACL implements broker.ACLAdmin.
func (f *Fake) CreateACL(_ context.Context, a broker.ACL) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("CreateACL %s %s %s", a.Principal, a.ResourceName, a.Operation)
	f.acls = append(f.acls, a)
	return nil
}

// DeleteACLs implements broker.ACLAdmin.
func (f *Fake) DeleteACLs(_ context.Context, filter broker.ACL) ([]broker.ACL, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var deleted []broker.ACL
	f.acls = slices.DeleteFunc(f.acls, func(a broker.ACL) bool {
		if aclMatch(filter, a) {
			deleted = append(deleted, a)
			return true
		}
		return false
	})
	f.record("DeleteACLs %d", len(deleted))
	return deleted, nil
}

func aclMatch(f, a broker.ACL) bool {
	eq := func(want, got string) bool { return want == "" || want == "any" || strings.EqualFold(want, got) }
	return eq(f.Principal, a.Principal) && eq(f.Host, a.Host) && eq(f.ResourceType, a.ResourceType) &&
		eq(f.ResourceName, a.ResourceName) && eq(f.PatternType, a.PatternType) && eq(f.Operation, a.Operation) &&
		eq(f.Permission, a.Permission)
}

// Users implements broker.UserAdmin.
func (f *Fake) Users(context.Context) ([]broker.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []broker.User
	for _, k := range sortedKeys(f.users) {
		out = append(out, f.users[k])
	}
	return out, nil
}

// PutUser implements broker.UserAdmin.
func (f *Fake) PutUser(_ context.Context, u broker.User, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("PutUser %s", u.Name)
	f.users[u.Name] = u
	return nil
}

// DeleteUser implements broker.UserAdmin.
func (f *Fake) DeleteUser(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("DeleteUser %s", name)
	delete(f.users, name)
	return nil
}

// PutVHost implements broker.UserAdmin.
func (f *Fake) PutVHost(_ context.Context, v broker.VHost) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("PutVHost %s", v.Name)
	f.vhosts[v.Name] = v
	return nil
}

// DeleteVHost implements broker.UserAdmin.
func (f *Fake) DeleteVHost(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("DeleteVHost %s", name)
	delete(f.vhosts, name)
	return nil
}

// Permissions implements broker.UserAdmin.
func (f *Fake) Permissions(context.Context) ([]broker.Permission, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.perms), nil
}

// SetPermission implements broker.UserAdmin.
func (f *Fake) SetPermission(_ context.Context, p broker.Permission) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("SetPermission %s %s", p.User, p.VHost)
	f.perms = append(f.perms, p)
	return nil
}

// ClearPermission implements broker.UserAdmin.
func (f *Fake) ClearPermission(_ context.Context, user, vhost string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("ClearPermission %s %s", user, vhost)
	f.perms = slices.DeleteFunc(f.perms, func(p broker.Permission) bool { return p.User == user && p.VHost == vhost })
	return nil
}

// Policies implements broker.PolicyAdmin.
func (f *Fake) Policies(context.Context) ([]broker.Policy, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []broker.Policy
	for _, k := range sortedKeys(f.policies) {
		out = append(out, f.policies[k])
	}
	return out, nil
}

// PutPolicy implements broker.PolicyAdmin.
func (f *Fake) PutPolicy(_ context.Context, p broker.Policy) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("PutPolicy %s", p.Name)
	f.policies[p.Name] = p
	return nil
}

// DeletePolicy implements broker.PolicyAdmin.
func (f *Fake) DeletePolicy(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("DeletePolicy %s", name)
	delete(f.policies, name)
	return nil
}

// Parameters implements broker.PolicyAdmin.
func (f *Fake) Parameters(_ context.Context, component string) ([]broker.Parameter, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []broker.Parameter
	for _, k := range sortedKeys(f.params) {
		if p := f.params[k]; p.Component == component {
			out = append(out, p)
		}
	}
	return out, nil
}

// PutParameter implements broker.PolicyAdmin.
func (f *Fake) PutParameter(_ context.Context, p broker.Parameter) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("PutParameter %s %s", p.Component, p.Name)
	f.params[p.Component+"/"+p.Name] = p
	return nil
}

// DeleteParameter implements broker.PolicyAdmin.
func (f *Fake) DeleteParameter(_ context.Context, component, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("DeleteParameter %s %s", component, name)
	delete(f.params, component+"/"+name)
	return nil
}

// LinkStatus implements broker.PolicyAdmin: every shovel parameter reports running.
func (f *Fake) LinkStatus(_ context.Context, kind string) ([]broker.LinkStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []broker.LinkStatus
	for _, k := range sortedKeys(f.params) {
		if p := f.params[k]; p.Component == kind {
			out = append(out, broker.LinkStatus{Name: p.Name, Kind: kind, VHost: p.VHost, State: "running"})
		}
	}
	return out, nil
}

// Sample implements broker.MetricsReporter: messages_in is the total message count.
func (f *Fake) Sample(_ context.Context, target string) (broker.MetricSample, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var in float64
	for name, t := range f.topics {
		if target != "" && name != target {
			continue
		}
		for p := range t.partitions {
			in += float64(t.low[p] + int64(len(t.partitions[p])))
		}
	}
	return broker.MetricSample{
		Time:     f.Now(),
		Counters: map[string]float64{broker.MetricMessagesIn: in},
		Gauges:   map[string]float64{broker.MetricConnections: float64(len(f.cxns))},
	}, nil
}

func sortedKeys[K interface{ ~string | ~int32 }, V any](m map[K]V) []K {
	keys := slices.Collect(maps.Keys(m))
	slices.Sort(keys)
	return keys
}

func kv(m map[string]string) string {
	var parts []string
	for _, k := range sortedKeys(m) {
		parts = append(parts, k+"="+m[k])
	}
	return strings.Join(parts, ",")
}

var (
	_ broker.Broker              = (*Fake)(nil)
	_ broker.TopicDescriber      = (*Fake)(nil)
	_ broker.ClusterInspector    = (*Fake)(nil)
	_ broker.ConsumerInspector   = (*Fake)(nil)
	_ broker.GroupInspector      = (*Fake)(nil)
	_ broker.LagReporter         = (*Fake)(nil)
	_ broker.ConnectionInspector = (*Fake)(nil)
	_ broker.OffsetManager       = (*Fake)(nil)
	_ broker.ConsumerTerminator  = (*Fake)(nil)
	_ broker.TopicAdmin          = (*Fake)(nil)
	_ broker.PartitionAdder      = (*Fake)(nil)
	_ broker.Purger              = (*Fake)(nil)
	_ broker.TopologyInspector   = (*Fake)(nil)
	_ broker.TopologyEditor      = (*Fake)(nil)
	_ broker.RouteSimulator      = (*Fake)(nil)
	_ broker.SchemaRegistry      = (*Fake)(nil)
	_ broker.ConnectManager      = (*Fake)(nil)
	_ broker.KSQLRunner          = (*Fake)(nil)
	_ broker.ACLAdmin            = (*Fake)(nil)
	_ broker.UserAdmin           = (*Fake)(nil)
	_ broker.PolicyAdmin         = (*Fake)(nil)
	_ broker.MetricsReporter     = (*Fake)(nil)
	_ broker.CapabilityChecker   = (*Fake)(nil)
)
