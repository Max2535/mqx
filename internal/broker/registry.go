package broker

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/Max2535/mqx/internal/config"
)

// Driver is what an adapter registers in its init().
type Driver struct {
	// Open connects to the broker a context describes. Credentials are resolved by the caller.
	Open func(ctx context.Context, c config.Context, creds config.Credentials) (Broker, error)
	// Validate checks the broker-specific fields of a context without network access.
	// Returned errors are prefixed with the context label by the caller.
	Validate func(c config.Context) []error
	// Fields lists the config.Fields keys this broker uses besides config.CommonFields,
	// so editors show only those. Nil shows every field.
	Fields []string
}

// The registry is the only package-level mutable state in mqx. It is written
// by adapter init() functions and read afterwards.
var (
	mu      sync.RWMutex
	drivers = map[string]Driver{}
)

// Register makes a driver available under a broker type name. It panics on
// duplicates or a nil Open, which are programming errors.
func Register(name string, d Driver) {
	mu.Lock()
	defer mu.Unlock()
	if d.Open == nil {
		panic("broker: Register with nil Open for " + name)
	}
	if _, dup := drivers[name]; dup {
		panic("broker: Register called twice for " + name)
	}
	drivers[name] = d
}

// Types returns the registered broker types, sorted.
func Types() []string {
	mu.RLock()
	defer mu.RUnlock()
	names := make([]string, 0, len(drivers))
	for n := range drivers {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Open resolves the context's credentials and connects with its driver.
func Open(ctx context.Context, c config.Context) (Broker, error) {
	mu.RLock()
	d, ok := drivers[c.Broker]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("context %q: unknown broker %q (supported: %s)", c.Name, c.Broker, strings.Join(Types(), ", "))
	}
	creds, err := c.Resolve()
	if err != nil {
		return nil, err
	}
	b, err := d.Open(ctx, c, creds)
	if err != nil {
		return nil, fmt.Errorf("context %q: connect to %s: %w", c.Name, c.Broker, err)
	}
	return b, nil
}

// ContextFields returns the editable fields for a broker type: the common ones
// plus those its driver lists. An unknown type gets every field.
func ContextFields(brokerType string) []config.Field {
	mu.RLock()
	d, ok := drivers[brokerType]
	mu.RUnlock()
	if !ok {
		return config.Fields()
	}
	return config.FieldsFor(d.Fields)
}

// Validator adapts the registry to config.BrokerValidator.
func Validator() config.BrokerValidator { return registryValidator{} }

type registryValidator struct{}

func (registryValidator) Supported() []string { return Types() }

func (registryValidator) ValidateContext(c config.Context) ([]error, bool) {
	mu.RLock()
	d, ok := drivers[c.Broker]
	mu.RUnlock()
	if !ok {
		return nil, false
	}
	if d.Validate == nil {
		return nil, true
	}
	return d.Validate(c), true
}

// Capability names, as shown by `mqx ctx describe` and used by the TUI to pick panels.
const (
	CapTopicDescriber      = "topic-describe"
	CapClusterInspector    = "cluster"
	CapConsumerInspector   = "consumers"
	CapGroupInspector      = "groups"
	CapLagReporter         = "lag"
	CapConnectionInspector = "connections"
	CapOffsetManager       = "offsets"
	CapConsumerTerminator  = "terminate-consumer"
	CapTopicAdmin          = "topic-admin"
	CapPartitionAdder      = "add-partitions"
	CapPurger              = "purge"
	CapTopologyInspector   = "topology"
	CapTopologyEditor      = "topology-edit"
	CapRouteSimulator      = "route"
	CapSchemaRegistry      = "schema-registry"
	CapConnectManager      = "connect"
	CapKSQLRunner          = "ksql"
	CapACLAdmin            = "acl"
	CapUserAdmin           = "users"
	CapPolicyAdmin         = "policies"
	CapMetricsReporter     = "metrics"
)

// CapabilityChecker lets an adapter hide a capability its type implements but
// whose backing service is not configured (e.g. no schema_registry url).
type CapabilityChecker interface {
	HasCapability(name string) bool
}

// Capabilities lists the capability names b supports, in a stable order.
func Capabilities(b Broker) []string {
	checks := []struct {
		name string
		ok   bool
	}{
		{CapTopicDescriber, is[TopicDescriber](b)},
		{CapClusterInspector, is[ClusterInspector](b)},
		{CapConsumerInspector, is[ConsumerInspector](b)},
		{CapGroupInspector, is[GroupInspector](b)},
		{CapLagReporter, is[LagReporter](b)},
		{CapConnectionInspector, is[ConnectionInspector](b)},
		{CapOffsetManager, is[OffsetManager](b)},
		{CapConsumerTerminator, is[ConsumerTerminator](b)},
		{CapTopicAdmin, is[TopicAdmin](b)},
		{CapPartitionAdder, is[PartitionAdder](b)},
		{CapPurger, is[Purger](b)},
		{CapTopologyInspector, is[TopologyInspector](b)},
		{CapTopologyEditor, is[TopologyEditor](b)},
		{CapRouteSimulator, is[RouteSimulator](b)},
		{CapSchemaRegistry, is[SchemaRegistry](b)},
		{CapConnectManager, is[ConnectManager](b)},
		{CapKSQLRunner, is[KSQLRunner](b)},
		{CapACLAdmin, is[ACLAdmin](b)},
		{CapUserAdmin, is[UserAdmin](b)},
		{CapPolicyAdmin, is[PolicyAdmin](b)},
		{CapMetricsReporter, is[MetricsReporter](b)},
	}
	checker, _ := b.(CapabilityChecker)
	var out []string
	for _, c := range checks {
		if c.ok && (checker == nil || checker.HasCapability(c.name)) {
			out = append(out, c.name)
		}
	}
	return out
}

// Has reports whether b supports the named capability.
func Has(b Broker, name string) bool { return slices.Contains(Capabilities(b), name) }

func is[T any](b Broker) bool {
	_, ok := b.(T)
	return ok
}

// As returns b as capability T, honouring CapabilityChecker. It reports an
// actionable error naming the capability when b lacks it.
func As[T any](b Broker, name string) (T, error) {
	var zero T
	c, ok := b.(T)
	if !ok || (name != "" && !Has(b, name)) {
		return zero, fmt.Errorf("%s does not support %s: %w", b.Name(), name, ErrUnsupported)
	}
	return c, nil
}
