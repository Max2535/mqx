package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/Max2535/mqx/internal/broker"
)

func TestRouteCmd(t *testing.T) {
	e := newRMQEnv(t, false)
	e.f.AddBinding(broker.Binding{Source: "orders", Destination: "billing", DestinationType: "queue", RoutingKey: "eu"})
	e.f.AddBinding(broker.Binding{Source: "orders", Destination: "audit", DestinationType: "queue", RoutingKey: "eu"})

	out, _, err := e.exec(t, "route", "orders", "--key", "eu", "--header", "h=1")
	checkErr(t, err, "")
	for _, want := range []string{"Queues: audit, billing", `"eu" -> queue billing`, "orders [direct]"} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
	out, _, err = e.exec(t, "route", "orders", "--key", "us")
	checkErr(t, err, "")
	if !strings.Contains(out, "Queues: none") || !strings.Contains(out, "no binding matches") {
		t.Errorf("unroutable output:\n%s", out)
	}
	out, _, err = e.exec(t, "route", "orders", "--key", "eu", "-o", "json")
	checkErr(t, err, "")
	res := decodeJSON[broker.RouteResult](t, out)
	if res.Exchange != "orders" || strings.Join(res.Queues, ",") != "audit,billing" || len(res.Hops) != 2 {
		t.Errorf("json = %+v", res)
	}
	out, _, err = e.exec(t, "route", "orders", "--key", "us", "-o", "json")
	checkErr(t, err, "")
	if !strings.Contains(out, `"queues": []`) {
		t.Errorf("empty queues must be [] in json: %s", out)
	}
	_, _, err = e.exec(t, "route", "orders", "--header", "novalue")
	checkErr(t, err, "want key=value")
	_, _, err = e.exec(t, "route")
	checkErr(t, err, "accepts 1 arg")
	_, _, err = e.exec(t, "route", "x", "-o", "yaml")
	checkErr(t, err, "unknown --output")
}

func TestPrintRoute(t *testing.T) {
	res := &broker.RouteResult{
		Exchange: "", RoutingKey: "a.b", Queues: []string{"q1", "q2", "q-ae"},
		Hops: []broker.RouteHop{
			{Exchange: "", ExchangeType: "direct", BindingKey: "a.b", Destination: "x", DestinationType: "exchange"},
			{Exchange: "", ExchangeType: "direct", BindingKey: "a.b", Destination: "q1", DestinationType: "queue"},
			{Exchange: "x", ExchangeType: "topic", BindingKey: "a.*", Destination: "q2", DestinationType: "queue", Depth: 1},
			{Exchange: "x", ExchangeType: "topic", BindingKey: "#", Destination: "hdr", DestinationType: "exchange", Depth: 1},
			{Exchange: "x", ExchangeType: "topic", BindingKey: "a.#", Destination: "f", DestinationType: "exchange", Depth: 1},
			{Exchange: "f", ExchangeType: "fanout", BindingKey: "", Destination: "x", DestinationType: "exchange", Depth: 2},
			{Exchange: "f", ExchangeType: "fanout", BindingKey: "", Destination: "nomatch", DestinationType: "exchange", Depth: 2},
			{Exchange: "nomatch", ExchangeType: "direct", BindingKey: "<alternate-exchange>", Destination: "ae",
				DestinationType: "exchange", Depth: 3},
			{Exchange: "ae", ExchangeType: "fanout", BindingKey: "", Destination: "q-ae", DestinationType: "queue", Depth: 4},
		},
		NotSimulated: []string{"hdr"},
		Alternate:    []string{"ae"},
	}
	var buf bytes.Buffer
	if err := printRoute(&buf, res); err != nil {
		t.Fatal(err)
	}
	want := `Queues: q1, q2, q-ae

Route of key "a.b":
  (default) [direct]
    "a.b" -> exchange
      x [topic]
        "a.*" -> queue q2
        "#" -> exchange
          hdr (not simulated)
        "a.#" -> exchange
          f [fanout]
            "" -> exchange
              x [topic] (already routed above)
            "" -> exchange
              nomatch [direct]
                <alternate-exchange> -> exchange
                  ae [fanout]
                    "" -> queue q-ae
    "a.b" -> queue q1

Alternate exchanges used: ae

Not simulated: hdr (headers or plugin exchange types; queues behind them are not listed)
`
	if got := buf.String(); got != want {
		t.Errorf("printRoute =\n%s\nwant\n%s", got, want)
	}
}
