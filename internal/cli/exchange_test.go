package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/config"
	"github.com/Max2535/mqx/internal/testutil/fakebroker"
)

// rmqEnv is a config file whose current context opens a fake broker.
type rmqEnv struct {
	f    *fakebroker.Fake
	path string
}

func newRMQEnv(t *testing.T, readOnly bool) *rmqEnv {
	t.Helper()
	f, c := fakebroker.New(t)
	c.ReadOnly = readOnly
	cfg := config.Config{CurrentContext: c.Name, Contexts: []config.Context{c}}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := cfg.Save(path); err != nil {
		t.Fatal(err)
	}
	return &rmqEnv{f: f, path: path}
}

func (e *rmqEnv) exec(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	return run(t, append([]string{"--config", e.path}, args...)...)
}

// execTTY runs as if on a terminal, reading stdin from in.
func (e *rmqEnv) execTTY(t *testing.T, in string, args ...string) (string, string, error) {
	t.Helper()
	cmd := NewRootCmd(withTerminal(true))
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetIn(strings.NewReader(in))
	cmd.SetArgs(append([]string{"--config", e.path}, args...))
	err := cmd.Execute()
	return out.String(), errOut.String(), err
}

// lines splits output into whitespace-separated fields per line.
func lines(s string) [][]string {
	var out [][]string
	for _, l := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		out = append(out, strings.Fields(l))
	}
	return out
}

func decodeJSON[T any](t *testing.T, s string) T {
	t.Helper()
	var v T
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		t.Fatalf("decode %q: %v", s, err)
	}
	return v
}

// mutations lists every mutating RabbitMQ command with the fake call it makes.
var mutations = []struct {
	args []string
	call string
}{
	{[]string{"exchange", "declare", "orders", "--type", "topic", "--arg", "alternate-exchange=ae"}, "DeclareExchange orders topic"},
	{[]string{"exchange", "delete", "orders"}, "DeleteExchange orders"},
	{[]string{"bind", "--source", "ex", "--destination", "q", "--key", "a.*"}, "Bind ex queue q a.*"},
	{[]string{"bind", "--source", "ex", "--destination", "ex2", "--destination-type", "exchange", "--key", "#"}, "Bind ex exchange ex2 #"},
	{[]string{"unbind", "--source", "ex", "--destination", "q", "--key", "a.*"}, "Unbind ex queue q a.*"},
	{[]string{"connections", "close", "conn-1", "--reason", "bye"}, "CloseConnection conn-1"},
	{[]string{"vhost", "create", "staging", "--description", "d"}, "PutVHost staging"},
	{[]string{"vhost", "delete", "staging"}, "DeleteVHost staging"},
	{[]string{"user", "create", "alice", "--password-env", "MQX_CLI_TEST_PW", "--tags", "monitoring"}, "PutUser alice"},
	{[]string{"user", "delete", "alice"}, "DeleteUser alice"},
	{[]string{"user", "set-tags", "alice", "administrator"}, "PutUser alice"},
	{[]string{"permission", "set", "alice", "--vhost", "/", "--read", ".*"}, "SetPermission alice /"},
	{[]string{"permission", "clear", "alice", "--vhost", "/"}, "ClearPermission alice /"},
	{[]string{"policy", "set", "ttl", "--pattern", "^o", "--definition", "message-ttl=1000"}, "PutPolicy ttl"},
	{[]string{"policy", "delete", "ttl"}, "DeletePolicy ttl"},
	{[]string{"shovel", "create", "mv", "--src-uri", "amqp://", "--src-queue", "a", "--dest-uri-env", "MQX_CLI_TEST_URI",
		"--dest-queue", "b"}, "PutParameter shovel mv"},
	{[]string{"shovel", "delete", "mv"}, "DeleteParameter shovel mv"},
	{[]string{"federation", "set-upstream", "up", "--uri-env", "MQX_CLI_TEST_URI", "--set", "max-hops=1"}, "PutParameter federation-upstream up"},
	{[]string{"federation", "delete-upstream", "up"}, "DeleteParameter federation-upstream up"},
}

func TestRabbitMutationsAreGuarded(t *testing.T) {
	t.Setenv("MQX_CLI_TEST_PW", "s3cret-pw")
	t.Setenv("MQX_CLI_TEST_URI", "amqp://u:s3cret-uri@remote/v")
	for _, m := range mutations {
		name := strings.Join(m.args[:2], " ")
		t.Run(name+"/read_only", func(t *testing.T) {
			e := newRMQEnv(t, true)
			_, _, err := e.exec(t, append(m.args, "--yes")...)
			if !errors.Is(err, ErrReadOnly) {
				t.Fatalf("error = %v, want ErrReadOnly", err)
			}
			if len(e.f.Calls) != 0 {
				t.Errorf("calls on a read_only context: %v", e.f.Calls)
			}
		})
		t.Run(name+"/unconfirmed", func(t *testing.T) {
			e := newRMQEnv(t, false)
			_, _, err := e.exec(t, m.args...)
			if !errors.Is(err, ErrNotConfirmed) {
				t.Fatalf("error = %v, want ErrNotConfirmed", err)
			}
			if len(e.f.Calls) != 0 {
				t.Errorf("calls without confirmation: %v", e.f.Calls)
			}
		})
		t.Run(name+"/yes", func(t *testing.T) {
			e := newRMQEnv(t, false)
			out, errOut, err := e.exec(t, append(m.args, "--yes")...)
			checkErr(t, err, "")
			if !slices.Equal(e.f.Calls, []string{m.call}) {
				t.Errorf("calls = %v, want [%s]", e.f.Calls, m.call)
			}
			if strings.Contains(out+errOut, "s3cret") {
				t.Errorf("output leaks a secret: %q %q", out, errOut)
			}
			if out == "" {
				t.Error("no confirmation message")
			}
		})
	}
}

func TestRabbitMutationsConfirmOnTTY(t *testing.T) {
	e := newRMQEnv(t, false)
	_, errOut, err := e.execTTY(t, "n\n", "exchange", "delete", "orders")
	if !errors.Is(err, ErrNotConfirmed) || !strings.Contains(errOut, `delete exchange "orders"`) {
		t.Fatalf("declined: err %v, prompt %q", err, errOut)
	}
	_, _, err = e.execTTY(t, "y\n", "exchange", "delete", "orders")
	checkErr(t, err, "")
	if !slices.Equal(e.f.Calls, []string{"DeleteExchange orders"}) {
		t.Errorf("calls = %v", e.f.Calls)
	}
}

func TestExchanges(t *testing.T) {
	e := newRMQEnv(t, false)
	e.f.AddExchange(broker.Exchange{Name: "", Type: "direct", Durable: true})
	e.f.AddExchange(broker.Exchange{Name: "orders", Type: "topic", Durable: true, Internal: true,
		Arguments: map[string]any{"alternate-exchange": "ae"}})
	out, _, err := e.exec(t, "exchanges")
	checkErr(t, err, "")
	want := [][]string{
		{"NAME", "TYPE", "DURABLE", "AUTO-DELETE", "INTERNAL", "ARGUMENTS"},
		{"(default)", "direct", "yes", "no", "no", "-"},
		{"orders", "topic", "yes", "no", "yes", "alternate-exchange=ae"},
	}
	if got := lines(out); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("table = %q", got)
	}
	out, _, err = e.exec(t, "exchanges", "-o", "json")
	checkErr(t, err, "")
	es := decodeJSON[[]broker.Exchange](t, out)
	if len(es) != 2 || es[1].Name != "orders" || es[1].Arguments["alternate-exchange"] != "ae" {
		t.Errorf("json = %+v", es)
	}

	empty := newRMQEnv(t, false)
	out, _, err = empty.exec(t, "exchanges", "-o", "json")
	checkErr(t, err, "")
	if strings.TrimSpace(out) != "[]" {
		t.Errorf("empty json = %q", out)
	}
}

func TestExchangeDeclareValidation(t *testing.T) {
	e := newRMQEnv(t, false)
	_, _, err := e.exec(t, "exchange", "declare", "x", "--yes")
	checkErr(t, err, "--type is required")
	_, _, err = e.exec(t, "exchange", "declare", "x", "--type", "direct", "--arg", "novalue", "--yes")
	checkErr(t, err, "want key=value")
	_, _, err = e.exec(t, "exchange", "declare", "--type", "direct", "--yes")
	checkErr(t, err, "accepts 1 arg")
	if len(e.f.Calls) != 0 {
		t.Errorf("calls = %v", e.f.Calls)
	}
}

func TestCapabilityMissing(t *testing.T) {
	e := newRMQEnv(t, false)
	e.f.Caps = []string{broker.CapTopicDescriber}
	for _, args := range [][]string{
		{"exchanges"}, {"bindings"}, {"route", "x"}, {"connections"}, {"channels"}, {"vhosts"}, {"user", "list"},
		{"permission", "list"}, {"policy", "list"}, {"shovel", "list"}, {"federation", "links"},
		{"exchange", "delete", "x", "--yes"}, {"connections", "close", "c", "--yes"},
	} {
		_, _, err := e.exec(t, args...)
		if !errors.Is(err, broker.ErrUnsupported) || !strings.Contains(err.Error(), "mqx ctx describe") {
			t.Errorf("%v: error = %v", args, err)
		}
	}
}
