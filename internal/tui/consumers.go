package tui

import (
	"context"
	"fmt"

	"github.com/charmbracelet/bubbles/key"

	"github.com/Max2535/mqx/internal/broker"
)

// newConsumersPanel shows who is attached to the topic or queue selected in
// the Topics panel, and terminates consumers when the broker can.
func newConsumersPanel(e *env) panel {
	shown := ""
	var actions []action
	if e.has(broker.CapConsumerTerminator) {
		actions = append(actions, action{
			key:      key.NewBinding(key.WithKeys("x"), key.WithHelp("x", "terminate consumer")),
			mutating: true,
			needsRow: true,
			when: func(r *row) bool {
				c := r.data.(broker.Consumer)
				return c.Connection != "" || (c.Group != "" && c.InstanceID != "")
			},
			describe: func(r *row, _ values) string {
				c := r.data.(broker.Consumer)
				if c.Connection != "" {
					return "Close connection " + c.Connection
				}
				return fmt.Sprintf("Remove static member %s from group %s", c.InstanceID, c.Group)
			},
			run: func(ctx context.Context, r *row, _ values) (string, error) {
				c := r.data.(broker.Consumer)
				t := broker.ConsumerTarget{Reason: "terminated from mqx TUI"}
				if c.Connection != "" {
					t.Connection = c.Connection
				} else {
					t.Group, t.InstanceID = c.Group, c.InstanceID
				}
				return "", as[broker.ConsumerTerminator](e).TerminateConsumer(ctx, t)
			},
		})
	}
	return newStack(newListView(e, resource{
		title: "Consumers",
		cols:  []string{"CONSUMER"},
		loader: func() func(ctx context.Context) (listing, error) {
			topic := e.topic
			shown = topic
			return func(ctx context.Context) (listing, error) {
				if topic == "" {
					return listing{header: st.muted.Render("Select a " + e.kind() + " in the Topics panel first.")}, nil
				}
				cs, err := as[broker.ConsumerInspector](e).Consumers(ctx, topic)
				if err != nil {
					return listing{}, err
				}
				return consumersListing(e, topic, cs), nil
			}
		},
		stale:   func() bool { return shown != e.topic },
		actions: actions,
	}))
}

func consumersListing(e *env, topic string, cs []broker.Consumer) listing {
	l := listing{header: fmt.Sprintf("%d consumers of %s %s", len(cs), e.kind(), topic)}
	rabbit := e.kind() == "queue"
	for _, c := range cs {
		if c.Tag != "" {
			rabbit = true
		}
	}
	if rabbit {
		l.cols = []string{"TAG", "CONNECTION", "CHANNEL", "USER", "ACK", "PREFETCH", "ACTIVE"}
	} else {
		l.cols = []string{"GROUP", "MEMBER", "INSTANCE", "CLIENT", "HOST", "PARTITIONS"}
	}
	for _, c := range cs {
		cells := []string{c.Group, c.MemberID, c.InstanceID, c.ClientID, c.Host, joinInts(c.Partitions)}
		if rabbit {
			cells = []string{c.Tag, c.Connection, c.Channel, c.User, yesNo(c.AckRequired), itoa(c.Prefetch), yesNo(c.Active)}
		}
		l.rows = append(l.rows, row{key: c.MemberID + c.Tag, cells: cells, data: c})
	}
	return l
}
