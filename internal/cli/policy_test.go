package cli

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/Max2535/mqx/internal/broker"
)

func TestPolicyCommands(t *testing.T) {
	e := newRMQEnv(t, false)
	_, _, err := e.exec(t, "policy", "set", "limits", "--pattern", `^orders\.`, "--apply-to", "queues", "--priority", "2",
		"--definition", "max-length=1000", "--definition", "overflow=reject-publish", "--yes")
	checkErr(t, err, "")
	_, _, err = e.exec(t, "policy", "set", "ae", "--pattern", "^ev$", "--apply-to", "exchanges",
		"--definition-json", `{"alternate-exchange":"unrouted"}`, "--yes")
	checkErr(t, err, "")
	out, _, err := e.exec(t, "policy", "list")
	checkErr(t, err, "")
	want := [][]string{
		{"NAME", "PATTERN", "APPLY-TO", "PRIORITY", "DEFINITION"},
		{"ae", "^ev$", "exchanges", "0", "alternate-exchange=unrouted"},
		{"limits", `^orders\.`, "queues", "2", "max-length=1000", "overflow=reject-publish"},
	}
	if got := lines(out); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("policies = %q", got)
	}
	out, _, err = e.exec(t, "policy", "list", "-o", "json")
	checkErr(t, err, "")
	ps := decodeJSON[[]broker.Policy](t, out)
	if len(ps) != 2 || ps[1].Definition["max-length"] != float64(1000) {
		t.Errorf("json = %+v", ps)
	}

	for _, tt := range []struct {
		args []string
		want string
	}{
		{[]string{"policy", "set", "p", "--definition", "a=1"}, "--pattern is required"},
		{[]string{"policy", "set", "p", "--pattern", "x"}, "a definition is required"},
		{[]string{"policy", "set", "p", "--pattern", "x", "--definition", "a=1", "--definition-json", "{}"}, "not both"},
		{[]string{"policy", "set", "p", "--pattern", "x", "--definition-json", "[1]"}, "want a JSON object"},
	} {
		_, _, err := e.exec(t, append(tt.args, "--yes")...)
		checkErr(t, err, tt.want)
	}
}

func TestShovelCommands(t *testing.T) {
	e := newRMQEnv(t, false)
	t.Setenv("MQX_CLI_SRC", "amqp://user:src-secret@old-host/v")
	_, _, err := e.exec(t, "shovel", "create", "migrate", "--src-uri-env", "MQX_CLI_SRC", "--src-exchange", "orders",
		"--dest-uri", "amqp://", "--dest-queue", "orders", "--set", "prefetch-count=500", "--yes")
	checkErr(t, err, "")
	ps, _ := e.f.Parameters(context.Background(), componentShovel)
	if len(ps) != 1 {
		t.Fatalf("parameters = %+v", ps)
	}
	v := ps[0].Value
	if v["src-uri"] != "amqp://user:src-secret@old-host/v" || v["src-exchange"] != "orders" || v["src-exchange-key"] != "#" ||
		v["dest-queue"] != "orders" || v["prefetch-count"] != int64(500) || v["ack-mode"] != "on-confirm" {
		t.Errorf("shovel value = %v", v)
	}

	out, _, err := e.exec(t, "shovel", "list")
	checkErr(t, err, "")
	if strings.Contains(out, "src-secret") || !strings.Contains(out, "amqp://user:xxxxx@old-host/v exchange orders key #") {
		t.Errorf("shovel list = %q", out)
	}
	out, _, err = e.exec(t, "shovel", "list", "-o", "json")
	checkErr(t, err, "")
	if strings.Contains(out, "src-secret") || !strings.Contains(out, "xxxxx") {
		t.Errorf("shovel list json = %s", out)
	}
	out, _, err = e.exec(t, "shovel", "status")
	checkErr(t, err, "")
	if got := lines(out); len(got) != 2 || got[1][0] != "migrate" || got[1][1] != "running" {
		t.Errorf("shovel status = %q", got)
	}

	for _, tt := range []struct {
		args []string
		want string
	}{
		{[]string{"--src-uri", "amqp://u:pw-literal@h", "--src-queue", "a", "--dest-uri", "amqp://", "--dest-queue", "b"},
			"--src-uri contains a password"},
		{[]string{"--src-queue", "a", "--dest-uri", "amqp://", "--dest-queue", "b"}, "--src-uri-env VAR"},
		{[]string{"--src-uri", "amqp://", "--dest-uri", "amqp://", "--dest-queue", "b"}, "exactly one of --src-queue or --src-exchange"},
		{[]string{"--src-uri", "amqp://", "--src-queue", "a", "--dest-uri", "amqp://", "--dest-queue", "b", "--dest-exchange", "x"},
			"at most one"},
		{[]string{"--src-uri-env", "MQX_CLI_UNSET", "--src-queue", "a", "--dest-uri", "amqp://"}, "MQX_CLI_UNSET is not set"},
		{[]string{"--src-uri", "amqp://", "--src-uri-env", "MQX_CLI_SRC", "--src-queue", "a", "--dest-uri", "amqp://"}, "not both"},
	} {
		_, _, err := e.exec(t, append([]string{"shovel", "create", "bad", "--yes"}, tt.args...)...)
		checkErr(t, err, tt.want)
		if strings.Contains(err.Error(), "pw-literal") {
			t.Errorf("error echoes the password: %v", err)
		}
	}
}

func TestFederationCommands(t *testing.T) {
	e := newRMQEnv(t, false)
	t.Setenv("MQX_CLI_UP", "amqps://fed:up-secret@dc1:5671")
	_, _, err := e.exec(t, "federation", "set-upstream", "dc1", "--uri-env", "MQX_CLI_UP", "--set", "expires=3600000",
		"--set", "max-hops=1", "--yes")
	checkErr(t, err, "")
	out, _, err := e.exec(t, "federation", "upstreams")
	checkErr(t, err, "")
	want := [][]string{{"NAME", "URI", "SETTINGS"}, {"dc1", "amqps://fed:xxxxx@dc1:5671", "expires=3600000", "max-hops=1"}}
	if got := lines(out); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("upstreams = %q", got)
	}
	out, _, err = e.exec(t, "federation", "upstreams", "-o", "json")
	checkErr(t, err, "")
	if strings.Contains(out, "up-secret") {
		t.Errorf("json leaks: %s", out)
	}
	out, _, err = e.exec(t, "federation", "links", "-o", "json")
	checkErr(t, err, "")
	if strings.TrimSpace(out) != "[]" {
		t.Errorf("links = %s", out)
	}
	_, _, err = e.exec(t, "federation", "set-upstream", "dc2", "--yes")
	checkErr(t, err, "--uri-env VAR")
}

func TestRedactURI(t *testing.T) {
	for in, want := range map[string]string{
		"amqp://u:p@h/v":                "amqp://u:xxxxx@h/v",
		"amqp://u@h":                    "amqp://u@h",
		"amqp://":                       "amqp://",
		"a amqp://x:y@h b amqps://:z@i": "a amqp://x:xxxxx@h b amqps://:xxxxx@i",
	} {
		if got := redactURI(in); got != want {
			t.Errorf("redactURI(%q) = %q, want %q", in, got, want)
		}
	}
}
