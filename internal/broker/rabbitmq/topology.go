package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/Max2535/mqx/internal/broker"
)

type vhostJSON struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Messages    int64  `json:"messages"`
	Tracing     bool   `json:"tracing"`
}

// VHosts lists every virtual host the user can see.
func (r *RabbitMQ) VHosts(ctx context.Context) ([]broker.VHost, error) {
	var vs []vhostJSON
	if err := r.mgmt.get(ctx, apiPath("vhosts"), &vs); err != nil {
		return nil, fmt.Errorf("list vhosts: %w", err)
	}
	out := make([]broker.VHost, len(vs))
	for i, v := range vs {
		out[i] = broker.VHost(v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

type exchangeJSON struct {
	Name       string         `json:"name"`
	VHost      string         `json:"vhost"`
	Type       string         `json:"type"`
	Durable    bool           `json:"durable"`
	AutoDelete bool           `json:"auto_delete"`
	Internal   bool           `json:"internal"`
	Arguments  map[string]any `json:"arguments"`
	Policy     string         `json:"policy"`
}

func (e exchangeJSON) exchange() broker.Exchange {
	ex := broker.Exchange{
		Name: e.Name, VHost: e.VHost, Type: e.Type, Durable: e.Durable, AutoDelete: e.AutoDelete,
		Internal: e.Internal,
	}
	if len(e.Arguments) > 0 {
		ex.Arguments = e.Arguments
	}
	return ex
}

func (r *RabbitMQ) exchanges(ctx context.Context) ([]exchangeJSON, error) {
	var es []exchangeJSON
	if err := r.mgmt.get(ctx, apiPath("exchanges", r.settings.vhost), &es); err != nil {
		return nil, fmt.Errorf("list exchanges: %w", err)
	}
	return es, nil
}

// Exchanges lists the exchanges of the vhost; the default exchange has Name "".
func (r *RabbitMQ) Exchanges(ctx context.Context) ([]broker.Exchange, error) {
	es, err := r.exchanges(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]broker.Exchange, len(es))
	for i, e := range es {
		out[i] = e.exchange()
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

type bindingJSON struct {
	VHost           string         `json:"vhost"`
	Source          string         `json:"source"`
	Destination     string         `json:"destination"`
	DestinationType string         `json:"destination_type"`
	RoutingKey      string         `json:"routing_key"`
	Arguments       map[string]any `json:"arguments"`
	PropertiesKey   string         `json:"properties_key"`
}

func (b bindingJSON) binding() broker.Binding {
	out := broker.Binding(b)
	if len(out.Arguments) == 0 {
		out.Arguments = nil
	}
	return out
}

func (r *RabbitMQ) bindings(ctx context.Context) ([]bindingJSON, error) {
	var bs []bindingJSON
	if err := r.mgmt.get(ctx, apiPath("bindings", r.settings.vhost), &bs); err != nil {
		return nil, fmt.Errorf("list bindings: %w", err)
	}
	return bs, nil
}

// Bindings lists every binding of the vhost, including the default exchange's
// implicit binding to each queue and exchange-to-exchange bindings.
func (r *RabbitMQ) Bindings(ctx context.Context) ([]broker.Binding, error) {
	bs, err := r.bindings(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]broker.Binding, len(bs))
	for i, b := range bs {
		out[i] = b.binding()
	}
	return out, nil
}

// DeclareExchange creates an exchange (idempotent for identical properties).
func (r *RabbitMQ) DeclareExchange(ctx context.Context, ex broker.Exchange) error {
	if ex.Name == "" {
		return errors.New("exchange name is required; the default exchange cannot be declared")
	}
	if ex.Type == "" {
		return errors.New("exchange type is required: direct, topic, fanout, headers or a plugin type")
	}
	body := map[string]any{
		"type": ex.Type, "durable": ex.Durable, "auto_delete": ex.AutoDelete, "internal": ex.Internal,
		"arguments": nonNil(ex.Arguments),
	}
	if err := r.mgmt.put(ctx, apiPath("exchanges", r.settings.vhost, ex.Name), body); err != nil {
		return fmt.Errorf("declare exchange %q: %w", ex.Name, err)
	}
	return nil
}

// DeleteExchange deletes an exchange and its bindings.
func (r *RabbitMQ) DeleteExchange(ctx context.Context, name string) error {
	if name == "" {
		return errors.New("the default exchange cannot be deleted")
	}
	if err := r.mgmt.delete(ctx, apiPath("exchanges", r.settings.vhost, name), nil); err != nil {
		return fmt.Errorf("delete exchange %q: %w", name, err)
	}
	return nil
}

// bindingPath is /api/bindings/{vhost}/e/{source}/{q|e}/{destination}.
func (r *RabbitMQ) bindingPath(b broker.Binding) (string, error) {
	if b.Source == "" {
		return "", errors.New("the default exchange's bindings are implicit and cannot be changed; bind from a named exchange")
	}
	if b.Destination == "" {
		return "", errors.New("binding destination is required")
	}
	kind := "q"
	switch b.DestinationType {
	case "", "queue":
	case "exchange":
		kind = "e"
	default:
		return "", fmt.Errorf("destination type %q: want queue or exchange", b.DestinationType)
	}
	return apiPath("bindings", r.settings.vhost, "e", b.Source, kind, b.Destination), nil
}

// Bind binds a queue or exchange to a source exchange.
func (r *RabbitMQ) Bind(ctx context.Context, b broker.Binding) error {
	path, err := r.bindingPath(b)
	if err != nil {
		return err
	}
	body := map[string]any{"routing_key": b.RoutingKey, "arguments": nonNil(b.Arguments)}
	if err := r.mgmt.post(ctx, path, body); err != nil {
		return fmt.Errorf("bind %s: %w", describeBinding(b), err)
	}
	return nil
}

// Unbind removes a binding. Without PropertiesKey it is looked up by routing
// key, and by arguments when Arguments is set.
func (r *RabbitMQ) Unbind(ctx context.Context, b broker.Binding) error {
	path, err := r.bindingPath(b)
	if err != nil {
		return err
	}
	props := b.PropertiesKey
	if props == "" {
		if props, err = r.findPropertiesKey(ctx, path, b); err != nil {
			return err
		}
	}
	if err := r.mgmt.delete(ctx, path+"/"+escapeSegment(props), nil); err != nil {
		return fmt.Errorf("unbind %s: %w", describeBinding(b), err)
	}
	return nil
}

func (r *RabbitMQ) findPropertiesKey(ctx context.Context, path string, b broker.Binding) (string, error) {
	var bs []bindingJSON
	if err := r.mgmt.get(ctx, path, &bs); err != nil {
		return "", fmt.Errorf("unbind %s: %w", describeBinding(b), err)
	}
	var matches []bindingJSON
	for _, x := range bs {
		if x.RoutingKey != b.RoutingKey {
			continue
		}
		if b.Arguments != nil && !sameArgs(x.Arguments, b.Arguments) {
			continue
		}
		matches = append(matches, x)
	}
	switch len(matches) {
	case 0:
		return "", fmt.Errorf("unbind %s: no such binding; list them with `mqx bindings`: %w", describeBinding(b), errNotFound)
	case 1:
		return matches[0].PropertiesKey, nil
	default:
		return "", fmt.Errorf("unbind %s: %d bindings differ only in arguments; pass the arguments to pick one",
			describeBinding(b), len(matches))
	}
}

// sameArgs compares binding arguments as decoded JSON (numbers as float64).
func sameArgs(have, want map[string]any) bool {
	if len(have) != len(want) {
		return false
	}
	for k, w := range want {
		h, ok := have[k]
		if !ok || stringify(h) != stringify(normalize(w)) {
			return false
		}
	}
	return true
}

// normalize converts Go integers to float64 so they compare with decoded JSON.
func normalize(v any) any {
	rv := reflect.ValueOf(v)
	switch rv.Kind() { //nolint:exhaustive // only numbers need converting
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint())
	}
	return v
}

func describeBinding(b broker.Binding) string {
	dt := b.DestinationType
	if dt == "" {
		dt = "queue"
	}
	return fmt.Sprintf("exchange %q -> %s %q (key %q)", b.Source, dt, b.Destination, b.RoutingKey)
}

// escapeSegment percent-encodes a properties key for the path. RabbitMQ
// decodes the path once and compares it with the key verbatim, so a key that
// already contains escapes (e.g. "a.%23") is escaped again.
func escapeSegment(s string) string {
	p := apiPath(s)
	return p[len("/api/"):]
}
