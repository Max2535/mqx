package tui

import (
	"context"
	"fmt"
	"time"

	"github.com/charmbracelet/bubbles/key"

	"github.com/Max2535/mqx/internal/broker"
)

// newClusterPanel lists the nodes; enter shows a node's configs.
func newClusterPanel(e *env) panel {
	ci := as[broker.ClusterInspector](e)
	return newStack(newListView(e, resource{
		title: "Cluster",
		cols:  []string{"ID", "HOST", "RACK", "CONTROLLER", "RUNNING", "DETAILS"},
		load: func(ctx context.Context) (listing, error) {
			nodes, err := ci.Nodes(ctx)
			if err != nil {
				return listing{}, err
			}
			rows := make([]row, len(nodes))
			for i, n := range nodes {
				host := n.Host
				if n.Port > 0 {
					host = fmt.Sprintf("%s:%d", n.Host, n.Port)
				}
				rows[i] = row{key: n.ID, data: n, cells: []string{n.ID, host, n.Rack, yesNo(n.Controller), yesNo(n.Running),
					formatKV(n.Details, " ")}}
			}
			return listing{header: fmt.Sprintf("%d nodes", len(nodes)), rows: rows}, nil
		},
		open: func(r row) panel {
			return newListView(e, resource{
				title: "Node " + r.key,
				cols:  []string{"NAME", "VALUE", "SOURCE"},
				load: func(ctx context.Context) (listing, error) {
					entries, err := ci.NodeConfig(ctx, r.key)
					if err != nil {
						return listing{}, err
					}
					return configListing(entries), nil
				},
			})
		},
	}))
}

// configListing renders config entries, masking sensitive values.
func configListing(entries []broker.ConfigEntry) listing {
	rows := make([]row, len(entries))
	for i, c := range entries {
		v := c.Value
		if c.Sensitive {
			v = "******"
		}
		rows[i] = row{key: c.Name, data: c, cells: []string{c.Name, v, c.Source}}
	}
	return listing{rows: rows}
}

// newConnectionsPanel lists connections and channels ('v' switches) and closes connections.
func newConnectionsPanel(e *env) panel {
	ci := as[broker.ConnectionInspector](e)
	var actions []action
	if e.has(broker.CapConsumerTerminator) {
		actions = append(actions, action{
			key:      key.NewBinding(key.WithKeys("x"), key.WithHelp("x", "close connection")),
			mutating: true,
			needsRow: true,
			describe: func(r *row, _ values) string { return "Close connection " + r.key },
			run: func(ctx context.Context, r *row, _ values) (string, error) {
				return "", as[broker.ConsumerTerminator](e).TerminateConsumer(ctx,
					broker.ConsumerTarget{Connection: r.key, Reason: "closed from mqx TUI"})
			},
		})
	}
	conns := resource{
		title: "Connections",
		cols:  []string{"NAME", "USER", "VHOST", "PEER", "STATE", "CHANNELS", "PROTOCOL", "CONNECTED", "RECV/s", "SEND/s"},
		load: func(ctx context.Context) (listing, error) {
			cs, err := ci.Connections(ctx)
			if err != nil {
				return listing{}, err
			}
			rows := make([]row, len(cs))
			for i, c := range cs {
				connected := ""
				if !c.ConnectedAt.IsZero() {
					connected = c.ConnectedAt.Format(time.DateTime)
				}
				rows[i] = row{key: c.Name, data: c, cells: []string{c.Name, c.User, c.VHost, c.Peer, c.State, itoa(c.Channels),
					c.Protocol, connected, rate(c.RecvRate), rate(c.SendRate)}}
			}
			return listing{rows: rows}, nil
		},
		actions: actions,
	}
	chans := resource{
		title: "Channels",
		cols:  []string{"NAME", "USER", "VHOST", "STATE", "CONSUMERS", "PREFETCH", "UNACKED", "CONFIRM", "PUB/s", "DELIVER/s"},
		load: func(ctx context.Context) (listing, error) {
			cs, err := ci.Channels(ctx)
			if err != nil {
				return listing{}, err
			}
			rows := make([]row, len(cs))
			for i, c := range cs {
				rows[i] = row{key: c.Name, data: c, cells: []string{c.Name, c.User, c.VHost, c.State, itoa(c.Consumers),
					itoa(c.Prefetch), itoa(c.Unacked), yesNo(c.Confirm), rate(c.PublishRate), rate(c.DeliverRate)}}
			}
			return listing{rows: rows}, nil
		},
	}
	return newStack(newListView(e, conns, chans))
}

func rate(f float64) string { return fmt.Sprintf("%.1f", f) }
