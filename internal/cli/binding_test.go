package cli

import (
	"slices"
	"testing"

	"github.com/Max2535/mqx/internal/broker"
)

func TestBindings(t *testing.T) {
	e := newRMQEnv(t, false)
	e.f.AddBinding(broker.Binding{Source: "", Destination: "q1", DestinationType: "queue", RoutingKey: "q1"})
	e.f.AddBinding(broker.Binding{Source: "orders", Destination: "q1", DestinationType: "queue", RoutingKey: "a.*",
		Arguments: map[string]any{"x-n": 1}})
	e.f.AddBinding(broker.Binding{Source: "orders", Destination: "audit", DestinationType: "exchange", RoutingKey: "#"})

	tests := []struct {
		args []string
		want [][]string
	}{
		{[]string{"bindings"}, [][]string{
			{"SOURCE", "DESTINATION", "TYPE", "ROUTING", "KEY", "ARGUMENTS"},
			{"(default)", "q1", "queue", "q1", "-"},
			{"orders", "q1", "queue", "a.*", "x-n=1"},
			{"orders", "audit", "exchange", "#", "-"},
		}},
		{[]string{"bindings", "--source", "orders", "--destination", "audit"}, [][]string{
			{"SOURCE", "DESTINATION", "TYPE", "ROUTING", "KEY", "ARGUMENTS"},
			{"orders", "audit", "exchange", "#", "-"},
		}},
		{[]string{"bindings", "--source", "(default)"}, [][]string{
			{"SOURCE", "DESTINATION", "TYPE", "ROUTING", "KEY", "ARGUMENTS"},
			{"(default)", "q1", "queue", "q1", "-"},
		}},
	}
	for _, tt := range tests {
		out, _, err := e.exec(t, tt.args...)
		checkErr(t, err, "")
		if got := lines(out); !slices.EqualFunc(got, tt.want, slices.Equal) {
			t.Errorf("%v: table = %q", tt.args, got)
		}
	}
	out, _, err := e.exec(t, "bindings", "--destination", "q1", "-o", "json")
	checkErr(t, err, "")
	bs := decodeJSON[[]broker.Binding](t, out)
	if len(bs) != 2 || bs[1].Arguments["x-n"] != float64(1) {
		t.Errorf("json = %+v", bs)
	}
}

func TestBindValidation(t *testing.T) {
	e := newRMQEnv(t, false)
	_, _, err := e.exec(t, "bind", "--source", "x", "--yes")
	checkErr(t, err, `"destination" not set`)
	_, _, err = e.exec(t, "bind", "--source", "x", "--destination", "q", "--destination-type", "topic", "--yes")
	checkErr(t, err, "want queue or exchange")
	_, _, err = e.exec(t, "unbind", "--source", "x", "--destination", "q", "--arg", "bad", "--yes")
	checkErr(t, err, "want key=value")
	if len(e.f.Calls) != 0 {
		t.Errorf("calls = %v", e.f.Calls)
	}
}
