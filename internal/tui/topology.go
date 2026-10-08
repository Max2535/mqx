package tui

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/charmbracelet/bubbles/key"

	"github.com/Max2535/mqx/internal/broker"
)

// newTopologyPanel shows exchanges with their bindings as a tree, plus vhosts
// ('v' switches), with exchange/binding editing and a route dry run.
func newTopologyPanel(e *env) panel {
	ti := as[broker.TopologyInspector](e)
	tree := resource{
		title: "Exchanges",
		cols:  []string{"EXCHANGE / BINDING", "TYPE / DESTINATION", "FLAGS"},
		load: func(ctx context.Context) (listing, error) {
			exs, err := ti.Exchanges(ctx)
			if err != nil {
				return listing{}, err
			}
			bs, err := ti.Bindings(ctx)
			if err != nil {
				return listing{}, err
			}
			return topologyListing(exs, bs), nil
		},
		actions: topologyActions(e),
	}
	vhosts := resource{
		title: "VHosts",
		cols:  []string{"NAME", "MESSAGES", "TRACING", "DESCRIPTION"},
		load: func(ctx context.Context) (listing, error) {
			vs, err := ti.VHosts(ctx)
			if err != nil {
				return listing{}, err
			}
			rows := make([]row, len(vs))
			for i, v := range vs {
				rows[i] = row{key: v.Name, data: v, cells: []string{v.Name, count(v.Messages), yesNo(v.Tracing), v.Description}}
			}
			return listing{rows: rows}, nil
		},
	}
	lv := newListView(e, tree, vhosts)
	lv.name = "Topology"
	return newStack(lv)
}

// topologyListing builds the tree: each exchange followed by its outgoing bindings.
func topologyListing(exs []broker.Exchange, bs []broker.Binding) listing {
	sort.Slice(exs, func(i, j int) bool { return exs[i].Name < exs[j].Name })
	bySource := map[string][]broker.Binding{}
	for _, b := range bs {
		bySource[b.Source] = append(bySource[b.Source], b)
	}
	seen := map[string]bool{}
	var rows []row
	addBindings := func(src string) {
		list := bySource[src]
		for i, b := range list {
			branch := "├─"
			if i == len(list)-1 {
				branch = "└─"
			}
			rows = append(rows, row{key: "binding:" + src + "/" + b.Destination + "/" + b.RoutingKey, data: b, cells: []string{
				fmt.Sprintf("  %s key %q", branch, b.RoutingKey),
				fmt.Sprintf("→ %s %s", b.DestinationType, b.Destination),
				formatKV(b.Arguments, " "),
			}})
		}
	}
	for _, ex := range exs {
		seen[ex.Name] = true
		var flags []string
		for _, f := range []struct {
			on   bool
			name string
		}{{ex.Durable, "durable"}, {ex.AutoDelete, "auto-delete"}, {ex.Internal, "internal"}} {
			if f.on {
				flags = append(flags, f.name)
			}
		}
		rows = append(rows, row{key: ex.Name, data: ex, cells: []string{orDefault(ex.Name), ex.Type, strings.Join(flags, ",")}})
		addBindings(ex.Name)
	}
	var orphans []string
	for src := range bySource {
		if !seen[src] {
			orphans = append(orphans, src)
		}
	}
	sort.Strings(orphans)
	for _, src := range orphans {
		rows = append(rows, row{key: src, data: broker.Exchange{Name: src}, cells: []string{orDefault(src), "?", ""}})
		addBindings(src)
	}
	return listing{header: fmt.Sprintf("%d exchanges, %d bindings", len(exs), len(bs)), rows: rows}
}

func isExchange(r *row) bool { _, ok := r.data.(broker.Exchange); return ok }
func isBinding(r *row) bool  { _, ok := r.data.(broker.Binding); return ok }

func selectedExchange(r *row) string {
	if r == nil {
		return ""
	}
	switch d := r.data.(type) {
	case broker.Exchange:
		return d.Name
	case broker.Binding:
		return d.Source
	}
	return ""
}

func topologyActions(e *env) []action {
	var actions []action
	if e.has(broker.CapRouteSimulator) {
		actions = append(actions, action{
			key: key.NewBinding(key.WithKeys("t"), key.WithHelp("t", "test route")),
			form: func(r *row) (string, []field) {
				return "Route dry run", []field{
					{key: "exchange", label: "Exchange", value: selectedExchange(r), hint: "empty = default exchange"},
					{key: "key", label: "Routing key"},
					{key: "headers", label: "Headers", hint: "k=v, k2=v2 (headers exchanges)"},
				}
			},
			check: func(_ *row, v values) error { _, err := parseKVMap(v["headers"]); return err },
			describe: func(_ *row, v values) string {
				return fmt.Sprintf("Route %q via %s", v["key"], orDefault(v["exchange"]))
			},
			run: func(ctx context.Context, _ *row, v values) (string, error) {
				h, _ := parseKVMap(v["headers"])
				res, err := as[broker.RouteSimulator](e).Route(ctx, v["exchange"], v["key"], h)
				if err != nil {
					return "", err
				}
				return routeResult(res), nil
			},
		})
	}
	if !e.has(broker.CapTopologyEditor) {
		return actions
	}
	te := as[broker.TopologyEditor](e)
	return append(actions, action{
		key:      key.NewBinding(key.WithKeys("n"), key.WithHelp("n", "declare exchange")),
		mutating: true,
		form: func(*row) (string, []field) {
			return "Declare exchange", []field{
				{key: "name", label: "Name"},
				{key: "type", label: "Type", hint: "direct, topic, fanout, headers", value: "direct"},
				{key: "durable", label: "Durable", hint: "Y/n"},
				{key: "auto_delete", label: "Auto-delete", hint: "y/N"},
				{key: "internal", label: "Internal", hint: "y/N"},
			}
		},
		check: func(_ *row, v values) error { _, err := exchangeSpec(v); return err },
		describe: func(_ *row, v values) string {
			return fmt.Sprintf("Declare %s exchange %s", v["type"], v["name"])
		},
		run: func(ctx context.Context, _ *row, v values) (string, error) {
			ex, err := exchangeSpec(v)
			if err != nil {
				return "", err
			}
			return "", te.DeclareExchange(ctx, ex)
		},
	}, action{
		key:      key.NewBinding(key.WithKeys("d"), key.WithHelp("d", "delete exchange")),
		mutating: true,
		needsRow: true,
		when:     func(r *row) bool { return isExchange(r) && r.key != "" && !strings.HasPrefix(r.key, "amq.") },
		describe: func(r *row, _ values) string { return "Delete exchange " + r.key },
		run:      func(ctx context.Context, r *row, _ values) (string, error) { return "", te.DeleteExchange(ctx, r.key) },
	}, action{
		key:      key.NewBinding(key.WithKeys("b"), key.WithHelp("b", "bind")),
		mutating: true,
		form: func(r *row) (string, []field) {
			return "Bind", []field{
				{key: "source", label: "Source exchange", value: selectedExchange(r)},
				{key: "dest_type", label: "Destination type", hint: "queue or exchange", value: "queue"},
				{key: "dest", label: "Destination"},
				{key: "key", label: "Routing key"},
				{key: "arguments", label: "Arguments", hint: "k=v (headers exchanges: x-match=all)"},
			}
		},
		check: func(_ *row, v values) error { _, err := bindingSpec(v); return err },
		describe: func(_ *row, v values) string {
			return fmt.Sprintf("Bind %s %s to exchange %s with key %q", v["dest_type"], v["dest"], v["source"], v["key"])
		},
		run: func(ctx context.Context, _ *row, v values) (string, error) {
			b, err := bindingSpec(v)
			if err != nil {
				return "", err
			}
			return "", te.Bind(ctx, b)
		},
	}, action{
		key:      key.NewBinding(key.WithKeys("u"), key.WithHelp("u", "unbind")),
		mutating: true,
		needsRow: true,
		when:     isBinding,
		describe: func(r *row, _ values) string {
			b := r.data.(broker.Binding)
			return fmt.Sprintf("Unbind %s %s from exchange %s (key %q)", b.DestinationType, b.Destination, orDefault(b.Source), b.RoutingKey)
		},
		run: func(ctx context.Context, r *row, _ values) (string, error) {
			return "", te.Unbind(ctx, r.data.(broker.Binding))
		},
	})
}

func exchangeSpec(v values) (broker.Exchange, error) {
	ex := broker.Exchange{Name: v["name"], Type: v["type"]}
	if ex.Name == "" || ex.Type == "" {
		return ex, fmt.Errorf("name and type are required")
	}
	var err error
	if ex.Durable, err = parseYes(v["durable"], true); err != nil {
		return ex, err
	}
	if ex.AutoDelete, err = parseYes(v["auto_delete"], false); err != nil {
		return ex, err
	}
	ex.Internal, err = parseYes(v["internal"], false)
	return ex, err
}

func bindingSpec(v values) (broker.Binding, error) {
	b := broker.Binding{Source: v["source"], Destination: v["dest"], DestinationType: v["dest_type"], RoutingKey: v["key"]}
	if b.Destination == "" {
		return b, fmt.Errorf("destination is required")
	}
	if b.DestinationType != "queue" && b.DestinationType != "exchange" {
		return b, fmt.Errorf("destination type must be queue or exchange")
	}
	var err error
	b.Arguments, err = parseArgs(v["arguments"])
	return b, err
}

// routeResult renders where a routing key lands.
func routeResult(r *broker.RouteResult) string {
	var b strings.Builder
	fmt.Fprintf(&b, "exchange %s, key %q → ", orDefault(r.Exchange), r.RoutingKey)
	if len(r.Queues) == 0 {
		b.WriteString("no queues (message would be dropped or returned)")
	} else {
		b.WriteString("queues: " + strings.Join(r.Queues, ", "))
	}
	for _, h := range r.Hops {
		fmt.Fprintf(&b, "\n%s%s (%s) --%q--> %s %s", strings.Repeat("  ", h.Depth+1), orDefault(h.Exchange),
			h.ExchangeType, h.BindingKey, h.DestinationType, h.Destination)
	}
	if len(r.Alternate) > 0 {
		b.WriteString("\nalternate exchanges used: " + strings.Join(r.Alternate, ", "))
	}
	if len(r.NotSimulated) > 0 {
		b.WriteString("\nnot simulated (headers or plugin exchange types): " + strings.Join(r.NotSimulated, ", "))
	}
	return b.String()
}
