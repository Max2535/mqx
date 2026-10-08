package cli

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/Max2535/mqx/internal/broker"
)

func TestConnectCommands(t *testing.T) {
	e := newKafkaCLIEnv(t, false)
	cfgFile := writeTemp(t, "sink.json", `{"name":"sink","config":{"connector.class":"FileStreamSink","tasks.max":2,"topics":"orders"}}`)

	steps := []struct {
		args    []string
		wantErr string
		want    []string
	}{
		{args: []string{"connect", "create", "sink", "--file", cfgFile, "--set", "file=/tmp/out", "--yes"}, want: []string{"Created connector sink."}},
		{args: []string{"connect", "create", "sink", "--set", "connector.class=X", "--yes"}, wantErr: "already exists; use `mqx connect update sink`"},
		{args: []string{"connect", "update", "nope", "--set", "connector.class=X", "--yes"}, wantErr: "use `mqx connect create nope`"},
		{args: []string{"connect", "update", "sink", "--file", cfgFile, "--set", "tasks.max=4", "--yes"}, want: []string{"Updated connector sink."}},
		{args: []string{"connect", "list"}, want: []string{"NAME TYPE STATE TASKS WORKER CLASS", "sink source RUNNING 0 - FileStreamSink"}},
		{args: []string{"connect", "get", "sink"}, want: []string{"Name: sink", "Type: source", "State: RUNNING", "Worker: -",
			"CONFIG VALUE", "connector.class FileStreamSink", "tasks.max 4", "topics orders"}},
		{args: []string{"connect", "plugins"}, want: []string{"CLASS TYPE VERSION",
			"org.apache.kafka.connect.mirror.MirrorSourceConnector source 4.1.0"}},
		{args: []string{"connect", "pause", "sink", "--yes"}, want: []string{"Paused connector sink."}},
		{args: []string{"connect", "resume", "sink", "--yes"}, want: []string{"Resumed connector sink."}},
		{args: []string{"connect", "restart", "sink", "--yes"}, want: []string{"Restarted connector sink."}},
		{args: []string{"connect", "delete", "sink", "--yes"}, want: []string{"Deleted connector sink."}},
		{args: []string{"connect", "create", "x", "--yes"}, wantErr: "give the connector config"},
		{args: []string{"connect", "create", "x", "--set", "novalue", "--yes"}, wantErr: "want key=value"},
		{args: []string{"connect", "get", "sink"}, wantErr: "not found"},
	}
	for _, s := range steps {
		out, _, err := e.mqx(t, s.args...)
		checkErr(t, err, s.wantErr)
		if s.want != nil && !slices.Equal(outLines(out), s.want) {
			t.Errorf("%v:\n got %q\nwant %q", s.args, outLines(out), s.want)
		}
	}
	want := []string{"PutConnector sink", "PutConnector sink", "ConnectorAction sink pause", "ConnectorAction sink resume",
		"ConnectorAction sink restart", "DeleteConnector sink"}
	if !slices.Equal(e.f.Calls, want) {
		t.Errorf("calls = %q\nwant %q", e.f.Calls, want)
	}
}

func TestConnectJSONAndGuard(t *testing.T) {
	e := newKafkaCLIEnv(t, true)
	_, _, err := e.mqx(t, "connect", "create", "c", "--set", "connector.class=X", "--yes")
	checkErr(t, err, "read_only")
	for _, a := range []string{"pause", "resume", "restart", "delete"} {
		_, _, err := e.mqx(t, "connect", a, "c", "--yes")
		checkErr(t, err, "read_only")
	}
	if len(e.f.Calls) != 0 {
		t.Errorf("calls on read-only = %q", e.f.Calls)
	}
	w := newKafkaCLIEnv(t, false)
	_, _, err = w.mqx(t, "connect", "delete", "c")
	checkErr(t, err, "without confirmation")
	_, _, err = w.mqx(t, "connect", "create", "c", "--set", "connector.class=X", "--set", "topics=a", "--yes")
	checkErr(t, err, "")
	out, _, err := w.mqx(t, "connect", "get", "c", "-o", "json")
	checkErr(t, err, "")
	var c broker.Connector
	if json.Unmarshal([]byte(out), &c) != nil || c.Config["topics"] != "a" {
		t.Errorf("get json = %s", out)
	}
	out, _, err = w.mqx(t, "connect", "list", "-o", "json")
	checkErr(t, err, "")
	if !strings.HasPrefix(strings.TrimSpace(out), "[") {
		t.Errorf("list json = %s", out)
	}
}

func TestConnectTasks(t *testing.T) {
	got := connectTasks([]broker.ConnectorTask{{State: "RUNNING"}, {State: "FAILED"}, {State: "RUNNING"}})
	if got != "3: 1 FAILED, 2 RUNNING" {
		t.Errorf("connectTasks = %q", got)
	}
}
