package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/Max2535/mqx/internal/broker"
)

// Parameter components and link kinds PolicyAdmin understands.
const (
	ComponentShovel     = "shovel"
	ComponentFederation = "federation-upstream"
	LinkShovel          = "shovel"
	LinkFederation      = "federation"
)

// Policies lists the vhost's policies.
func (r *RabbitMQ) Policies(ctx context.Context) ([]broker.Policy, error) {
	var ps []broker.Policy
	if err := r.mgmt.get(ctx, apiPath("policies", r.settings.vhost), &ps); err != nil {
		return nil, fmt.Errorf("list policies: %w", err)
	}
	sort.Slice(ps, func(i, j int) bool { return ps[i].Name < ps[j].Name })
	return ps, nil
}

// PutPolicy creates or replaces a policy in the context's vhost.
func (r *RabbitMQ) PutPolicy(ctx context.Context, p broker.Policy) error {
	if p.Name == "" || p.Pattern == "" {
		return errors.New("policy name and pattern are required")
	}
	if len(p.Definition) == 0 {
		return errors.New("policy definition is empty; set at least one key, e.g. max-length=1000")
	}
	applyTo := p.ApplyTo
	if applyTo == "" {
		applyTo = "all"
	}
	body := map[string]any{"pattern": p.Pattern, "apply-to": applyTo, "priority": p.Priority, "definition": p.Definition}
	if err := r.mgmt.put(ctx, apiPath("policies", r.settings.vhost, p.Name), body); err != nil {
		return fmt.Errorf("put policy %q: %w", p.Name, err)
	}
	return nil
}

// DeletePolicy deletes a policy from the context's vhost.
func (r *RabbitMQ) DeletePolicy(ctx context.Context, name string) error {
	if err := r.mgmt.delete(ctx, apiPath("policies", r.settings.vhost, name), nil); err != nil {
		return fmt.Errorf("delete policy %q: %w", name, err)
	}
	return nil
}

// pluginHint names the plugin a component or link kind needs.
func pluginHint(component string) string {
	if strings.HasPrefix(component, "federation") {
		return "enable the rabbitmq_federation and rabbitmq_federation_management plugins"
	}
	return "enable the rabbitmq_shovel and rabbitmq_shovel_management plugins"
}

// withPluginHint explains the errors plugin endpoints give when the plugin is
// off: 404, a bare 400 for an unknown path (RabbitMQ 4.x), or a 400 naming an
// unknown parameter component.
func withPluginHint(component string, err error) error {
	var ae *apiError
	if !errors.As(err, &ae) {
		return err
	}
	if ae.Status == http.StatusNotFound ||
		(ae.Status == http.StatusBadRequest && (ae.Reason == "" || strings.Contains(ae.Reason, "component"))) {
		return fmt.Errorf("%w (%s)", err, pluginHint(component))
	}
	return err
}

func checkComponent(component string) error {
	if component != ComponentShovel && component != ComponentFederation {
		return fmt.Errorf("component %q: use %q or %q", component, ComponentShovel, ComponentFederation)
	}
	return nil
}

// Parameters lists a component's runtime parameters in the vhost. Passwords
// in URIs are redacted.
func (r *RabbitMQ) Parameters(ctx context.Context, component string) ([]broker.Parameter, error) {
	if err := checkComponent(component); err != nil {
		return nil, err
	}
	var ps []broker.Parameter
	if err := r.mgmt.get(ctx, apiPath("parameters", component, r.settings.vhost), &ps); err != nil {
		if statusOf(err) == http.StatusNotFound {
			return nil, fmt.Errorf("list %s parameters: %w", component, withPluginHint(component, err))
		}
		return nil, fmt.Errorf("list %s parameters: %w", component, err)
	}
	for i := range ps {
		ps[i].Value = redactValue(ps[i].Value)
	}
	sort.Slice(ps, func(i, j int) bool { return ps[i].Name < ps[j].Name })
	return ps, nil
}

// redactValue returns a copy of v with URI passwords hidden, recursively.
func redactValue(v map[string]any) map[string]any {
	out := make(map[string]any, len(v))
	for k, x := range v {
		out[k] = redactAny(x)
	}
	return out
}

func redactAny(x any) any {
	switch t := x.(type) {
	case string:
		return redactURIs(t)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = redactAny(e)
		}
		return out
	case map[string]any:
		return redactValue(t)
	}
	return x
}

// PutParameter creates or replaces a runtime parameter in the context's vhost.
func (r *RabbitMQ) PutParameter(ctx context.Context, p broker.Parameter) error {
	if err := checkComponent(p.Component); err != nil {
		return err
	}
	if p.Name == "" {
		return errors.New("parameter name is required")
	}
	body := map[string]any{"component": p.Component, "vhost": r.settings.vhost, "name": p.Name, "value": p.Value}
	if err := r.mgmt.put(ctx, apiPath("parameters", p.Component, r.settings.vhost, p.Name), body); err != nil {
		return fmt.Errorf("put %s %q: %w", p.Component, p.Name, withPluginHint(p.Component, err))
	}
	return nil
}

// DeleteParameter deletes a runtime parameter from the context's vhost.
func (r *RabbitMQ) DeleteParameter(ctx context.Context, component, name string) error {
	if err := checkComponent(component); err != nil {
		return err
	}
	if err := r.mgmt.delete(ctx, apiPath("parameters", component, r.settings.vhost, name), nil); err != nil {
		return fmt.Errorf("delete %s %q: %w", component, name, err)
	}
	return nil
}

// LinkStatus reports running shovels ("shovel") or federation links ("federation").
func (r *RabbitMQ) LinkStatus(ctx context.Context, kind string) ([]broker.LinkStatus, error) {
	var path string
	switch kind {
	case LinkShovel:
		path = apiPath("shovels", r.settings.vhost)
	case LinkFederation:
		path = apiPath("federation-links", r.settings.vhost)
	default:
		return nil, fmt.Errorf("link kind %q: use %q or %q", kind, LinkShovel, LinkFederation)
	}
	var ls []map[string]any
	if err := r.mgmt.get(ctx, path, &ls); err != nil {
		return nil, fmt.Errorf("%s status: %w", kind, withPluginHint(kind, err))
	}
	out := make([]broker.LinkStatus, 0, len(ls))
	for _, l := range ls {
		out = append(out, linkStatus(kind, l))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func linkStatus(kind string, l map[string]any) broker.LinkStatus {
	str := func(k string) string { return redactURIs(stringify(l[k])) }
	s := broker.LinkStatus{Kind: kind, VHost: str("vhost"), Node: str("node"), Detail: map[string]string{}}
	used := map[string]bool{"vhost": true, "node": true}
	if kind == LinkShovel {
		s.Name, s.State, s.Error = str("name"), str("state"), str("reason")
		used["name"], used["state"], used["reason"] = true, true, true
	} else {
		s.Name, s.State, s.Error = str("upstream"), str("status"), str("error")
		used["upstream"], used["status"], used["error"] = true, true, true
	}
	for k, v := range l {
		if used[k] || v == nil {
			continue
		}
		switch v.(type) {
		case string, float64, bool:
			s.Detail[k] = redactURIs(stringify(v))
		}
	}
	return s
}
