package rabbitmq

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/Max2535/mqx/internal/broker"
)

// AlternateHopKey is the BindingKey of a RouteHop that follows an exchange's
// alternate-exchange because none of its bindings matched.
const AlternateHopKey = "<alternate-exchange>"

// Route resolves where a message published to exchange with routingKey would
// land, from the vhost's exchanges, bindings and policies, without publishing.
//
// It follows RabbitMQ's routing: direct (exact key), fanout (every binding),
// topic (* = one word, # = zero or more words), the default exchange,
// exchange-to-exchange bindings (each exchange visited once, so cycles end),
// and alternate exchanges (argument first, then policy) for exchanges whose
// own bindings matched nothing. Headers and plugin exchange types are listed
// in NotSimulated and not followed. CC/BCC headers are not simulated.
func (r *RabbitMQ) Route(ctx context.Context, exchange, routingKey string, _ map[string]string) (*broker.RouteResult, error) {
	es, err := r.exchanges(ctx)
	if err != nil {
		return nil, err
	}
	bs, err := r.bindings(ctx)
	if err != nil {
		return nil, err
	}
	var policies map[string]map[string]any
	for _, e := range es {
		if e.Policy != "" {
			if policies, err = r.policyDefinitions(ctx); err != nil {
				return nil, err
			}
			break
		}
	}
	return simulate(newRouteGraph(es, bs, policies), exchange, routingKey)
}

func (r *RabbitMQ) policyDefinitions(ctx context.Context) (map[string]map[string]any, error) {
	ps, err := r.Policies(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]map[string]any, len(ps))
	for _, p := range ps {
		out[p.Name] = p.Definition
	}
	return out, nil
}

// routeExchange is what routing needs to know about an exchange.
type routeExchange struct {
	Type      string
	Internal  bool
	Alternate string // alternate-exchange from the argument or the applied policy
}

// routeGraph is a vhost's exchanges and bindings indexed for routing.
type routeGraph struct {
	exchanges map[string]routeExchange
	bindings  map[string][]broker.Binding // by source exchange
}

func newRouteGraph(es []exchangeJSON, bs []bindingJSON, policies map[string]map[string]any) routeGraph {
	g := routeGraph{exchanges: map[string]routeExchange{}, bindings: map[string][]broker.Binding{}}
	for _, e := range es {
		x := routeExchange{Type: e.Type, Internal: e.Internal}
		if ae, ok := e.Arguments["alternate-exchange"].(string); ok {
			x.Alternate = ae // the argument wins over a policy, as in rabbit_policy:get_arg
		} else if ae, ok := policies[e.Policy]["alternate-exchange"].(string); ok && e.Policy != "" {
			x.Alternate = ae
		}
		g.exchanges[e.Name] = x
	}
	for _, b := range bs {
		g.bindings[b.Source] = append(g.bindings[b.Source], b.binding())
	}
	return g
}

// simulatedTypes are the exchange types whose routing mqx reproduces exactly.
var simulatedTypes = map[string]bool{"direct": true, "fanout": true, "topic": true}

func simulate(g routeGraph, start, key string) (*broker.RouteResult, error) {
	x, ok := g.exchanges[start]
	if !ok {
		return nil, fmt.Errorf("exchange %q does not exist; list them with `mqx exchanges`: %w", start, errNotFound)
	}
	if x.Internal {
		return nil, fmt.Errorf("exchange %q is internal: clients cannot publish to it, only other exchanges can route to it", start)
	}
	res := &broker.RouteResult{Exchange: start, RoutingKey: key, Queues: []string{}}
	type item struct {
		name  string
		depth int
	}
	// Like rabbit_exchange:route, a work stack plus a seen set: each exchange
	// routes once, which also ends exchange-to-exchange cycles.
	work := []item{{start, 0}}
	seen := map[string]bool{start: true}
	queues := map[string]bool{}
	for len(work) > 0 {
		it := work[len(work)-1]
		work = work[:len(work)-1]
		ex := g.exchanges[it.name]
		if !simulatedTypes[ex.Type] {
			res.NotSimulated = append(res.NotSimulated, it.name)
			continue
		}
		matched := matchBindings(ex.Type, g.bindings[it.name], key)
		if len(matched) == 0 && ex.Alternate != "" {
			if _, exists := g.exchanges[ex.Alternate]; exists {
				res.Alternate = append(res.Alternate, ex.Alternate)
				matched = []broker.Binding{{
					Source: it.name, Destination: ex.Alternate, DestinationType: "exchange", RoutingKey: AlternateHopKey,
				}}
			}
		}
		for _, b := range matched {
			res.Hops = append(res.Hops, broker.RouteHop{
				Exchange: it.name, ExchangeType: ex.Type, BindingKey: b.RoutingKey,
				Destination: b.Destination, DestinationType: b.DestinationType, Depth: it.depth,
			})
			if b.DestinationType == "exchange" {
				if _, exists := g.exchanges[b.Destination]; exists && !seen[b.Destination] {
					seen[b.Destination] = true
					work = append(work, item{b.Destination, it.depth + 1})
				}
				continue
			}
			queues[b.Destination] = true
		}
	}
	for q := range queues {
		res.Queues = append(res.Queues, q)
	}
	sort.Strings(res.Queues)
	return res, nil
}

func matchBindings(typ string, bs []broker.Binding, key string) []broker.Binding {
	var out []broker.Binding
	for _, b := range bs {
		var ok bool
		switch typ {
		case "fanout":
			ok = true
		case "direct":
			ok = b.RoutingKey == key
		case "topic":
			ok = topicMatch(b.RoutingKey, key)
		}
		if ok {
			out = append(out, b)
		}
	}
	return out
}

// topicWords splits a key on "." like RabbitMQ: "" has no words, while "a."
// is "a" followed by an empty word.
func topicWords(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ".")
}

// topicMatch reports whether a topic binding key matches a routing key:
// "*" matches exactly one word (which may be empty), "#" zero or more words,
// and any other word, including "a#" or "*b", only itself.
func topicMatch(bindingKey, routingKey string) bool {
	p, w := topicWords(bindingKey), topicWords(routingKey)
	// m[i][j]: pattern p[i:] matches words w[j:].
	m := make([][]bool, len(p)+1)
	for i := range m {
		m[i] = make([]bool, len(w)+1)
	}
	m[len(p)][len(w)] = true
	for i := len(p) - 1; i >= 0; i-- {
		for j := len(w); j >= 0; j-- {
			switch {
			case p[i] == "#":
				m[i][j] = m[i+1][j] || (j < len(w) && m[i][j+1])
			case j < len(w) && (p[i] == "*" || p[i] == w[j]):
				m[i][j] = m[i+1][j+1]
			}
		}
	}
	return m[0][0]
}
