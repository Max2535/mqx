package cli

import (
	"slices"
	"testing"
	"time"

	"github.com/Max2535/mqx/internal/broker"
)

func TestConnectionsAndChannels(t *testing.T) {
	e := newRMQEnv(t, false)
	e.f.SetConnections(
		[]broker.Connection{{Name: "c1", User: "app", Peer: "10.0.0.1:5000", State: "running", Channels: 2,
			Protocol: "AMQP 0-9-1", ClientProps: map[string]string{"connection_name": "billing"},
			ConnectedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), RecvRate: 10.5}},
		[]broker.Channel{{Name: "c1 (1)", Connection: "c1", Number: 1, User: "app", State: "running", Consumers: 1,
			Prefetch: 10, Unacked: 3, Confirm: true, PublishRate: 2}},
	)
	out, _, err := e.exec(t, "connections")
	checkErr(t, err, "")
	got := lines(out)
	if len(got) != 2 || got[0][0] != "NAME" || !slices.Contains(got[1], "billing") || !slices.Contains(got[1], "10.50") {
		t.Errorf("connections = %q", got)
	}
	out, _, err = e.exec(t, "connections", "-o", "json")
	checkErr(t, err, "")
	if cs := decodeJSON[[]broker.Connection](t, out); len(cs) != 1 || cs[0].ClientProps["connection_name"] != "billing" {
		t.Errorf("json = %+v", cs)
	}
	out, _, err = e.exec(t, "channels")
	checkErr(t, err, "")
	want := [][]string{
		{"NAME", "USER", "STATE", "CONSUMERS", "PREFETCH", "UNACKED", "CONFIRM", "PUBLISH/S", "DELIVER/S"},
		{"c1", "(1)", "app", "running", "1", "10", "3", "yes", "2", "0"},
	}
	if got := lines(out); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("channels = %q", got)
	}
	out, _, err = e.exec(t, "connections", "close", "c1", "--yes", "-o", "json")
	checkErr(t, err, "")
	if !slices.Equal(e.f.Calls, []string{"CloseConnection c1"}) || decodeJSON[map[string]any](t, out)["ok"] != true {
		t.Errorf("close: calls %v, out %s", e.f.Calls, out)
	}
	_, _, err = e.exec(t, "connections", "close", "--yes")
	checkErr(t, err, "accepts 1 arg")
}
