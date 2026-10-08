package cli

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Max2535/mqx/internal/broker"
)

func newRouteCmd(o *options) *cobra.Command {
	var (
		key     string
		headers []string
	)
	cmd := &cobra.Command{
		Use:   "route <exchange>",
		Short: "Show which queues a routing key reaches, without publishing",
		Long: `Resolve a routing key through the binding graph like RabbitMQ does: direct,
fanout and topic exchanges, the default exchange, exchange-to-exchange bindings
and alternate exchanges. Nothing is published. Headers exchanges and plugin
exchange types are reported as not simulated, and the queues behind them are
not listed. Use "(default)" or "amq.default" for the default exchange.`,
		Example: `  mqx route orders --key order.eu.created
  mqx route "(default)" --key billing
  mqx route events --key a.b -o json`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, pos []string) error {
			hs, err := parseKV("header", headers)
			if err != nil {
				return err
			}
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				rs, err := capability[broker.RouteSimulator](s, broker.CapRouteSimulator)
				if err != nil {
					return err
				}
				res, err := rs.Route(ctx, exchangeArg(pos[0]), key, hs)
				if err != nil {
					return err
				}
				res.Queues = nonNilList(res.Queues)
				res.Hops = nonNilList(res.Hops)
				if o.output != "table" && o.output != "" {
					return o.render(cmd, res, nil) // json, or an error for an unknown format
				}
				return printRoute(cmd.OutOrStdout(), res)
			})
		},
	}
	cmd.Flags().StringVar(&key, "key", "", "routing key")
	cmd.Flags().StringArrayVar(&headers, "header", nil, "message header key=value, repeatable (headers exchanges are not simulated)")
	return cmd
}

// printRoute prints the destination queues and the hop tree.
func printRoute(w io.Writer, res *broker.RouteResult) error {
	var b strings.Builder
	if len(res.Queues) == 0 {
		fmt.Fprintf(&b, "Queues: none; the message would be unroutable (dropped, or returned with mandatory)\n")
	} else {
		fmt.Fprintf(&b, "Queues: %s\n", strings.Join(res.Queues, ", "))
	}
	fmt.Fprintf(&b, "\nRoute of key %q:\n", res.RoutingKey)
	byExchange := map[string][]broker.RouteHop{}
	for _, h := range res.Hops {
		byExchange[h.Exchange] = append(byExchange[h.Exchange], h)
	}
	shown := map[string]bool{}
	var walk func(exchange, typ string, indent int)
	walk = func(exchange, typ string, indent int) {
		pad := strings.Repeat("  ", indent)
		label := displayExchange(exchange)
		if typ != "" {
			label += " [" + typ + "]"
		}
		switch {
		case shown[exchange]:
			fmt.Fprintf(&b, "%s%s (already routed above)\n", pad, label)
			return
		case slices.Contains(res.NotSimulated, exchange):
			fmt.Fprintf(&b, "%s%s (not simulated)\n", pad, label)
			return
		}
		shown[exchange] = true
		hops := byExchange[exchange]
		if len(hops) == 0 {
			fmt.Fprintf(&b, "%s%s (no binding matches)\n", pad, label)
			return
		}
		fmt.Fprintf(&b, "%s%s\n", pad, label)
		for _, h := range hops {
			bk := fmt.Sprintf("%q", h.BindingKey)
			if strings.HasPrefix(h.BindingKey, "<") {
				bk = h.BindingKey // e.g. <alternate-exchange>
			}
			if h.DestinationType == "exchange" {
				fmt.Fprintf(&b, "%s  %s -> exchange\n", pad, bk)
				walk(h.Destination, exchangeType(res, h.Destination), indent+2)
				continue
			}
			fmt.Fprintf(&b, "%s  %s -> queue %s\n", pad, bk, h.Destination)
		}
	}
	walk(res.Exchange, exchangeType(res, res.Exchange), 1)
	if len(res.Alternate) > 0 {
		fmt.Fprintf(&b, "\nAlternate exchanges used: %s\n", strings.Join(res.Alternate, ", "))
	}
	if len(res.NotSimulated) > 0 {
		fmt.Fprintf(&b, "\nNot simulated: %s (headers or plugin exchange types; queues behind them are not listed)\n",
			strings.Join(res.NotSimulated, ", "))
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// exchangeType finds an exchange's type from the hops leaving it.
func exchangeType(res *broker.RouteResult, exchange string) string {
	for _, h := range res.Hops {
		if h.Exchange == exchange {
			return h.ExchangeType
		}
	}
	return ""
}
