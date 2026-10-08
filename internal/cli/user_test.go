package cli

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/Max2535/mqx/internal/broker"
)

func TestUserCommands(t *testing.T) {
	e := newRMQEnv(t, false)
	t.Setenv("MQX_CLI_TEST_PW", "pw-from-env")
	_, _, err := e.exec(t, "user", "create", "alice", "--password-env", "MQX_CLI_TEST_PW", "--tags", "administrator,monitoring", "--yes")
	checkErr(t, err, "")
	_, _, err = e.exec(t, "user", "create", "bob", "--yes")
	checkErr(t, err, "pass --password-env")
	_, _, err = e.exec(t, "user", "create", "bob", "--password-env", "MQX_CLI_TEST_UNSET", "--yes")
	checkErr(t, err, "MQX_CLI_TEST_UNSET is not set")

	// On a terminal the password is prompted for; the confirmation line follows it.
	_, errOut, err := e.execTTY(t, "typed-pw\ny\n", "user", "create", "bob")
	checkErr(t, err, "")
	if !strings.Contains(errOut, `Password for user "bob"`) || strings.Contains(errOut, "typed-pw") {
		t.Errorf("prompt = %q", errOut)
	}
	_, _, err = e.execTTY(t, "\n", "user", "create", "carol", "--yes")
	checkErr(t, err, "empty password")

	out, _, err := e.exec(t, "user", "list")
	checkErr(t, err, "")
	want := [][]string{{"NAME", "TAGS"}, {"alice", "administrator,monitoring"}, {"bob", "-"}}
	if got := lines(out); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("users = %q", got)
	}
	_, _, err = e.exec(t, "user", "set-tags", "bob", "management", "monitoring", "--yes")
	checkErr(t, err, "")
	out, _, err = e.exec(t, "user", "list", "-o", "json")
	checkErr(t, err, "")
	us := decodeJSON[[]broker.User](t, out)
	if len(us) != 2 || !slices.Equal(us[1].Tags, []string{"management", "monitoring"}) || us[0].Tags == nil {
		t.Errorf("json = %+v", us)
	}
	if strings.Contains(out, "pw") {
		t.Errorf("listing leaks a password: %s", out)
	}
	if !slices.Equal(e.f.Calls, []string{"PutUser alice", "PutUser bob", "PutUser bob"}) {
		t.Errorf("calls = %v", e.f.Calls)
	}
}

func TestPermissions(t *testing.T) {
	e := newRMQEnv(t, false)
	ctx := context.Background()
	_ = e.f.SetPermission(ctx, broker.Permission{User: "alice", VHost: "/", Configure: ".*", Write: ".*", Read: ".*"})
	_ = e.f.SetPermission(ctx, broker.Permission{User: "bob", VHost: "prod", Configure: "", Write: "", Read: "^r"})
	out, _, err := e.exec(t, "permission", "list")
	checkErr(t, err, "")
	want := [][]string{
		{"USER", "VHOST", "CONFIGURE", "WRITE", "READ"},
		{"alice", "/", ".*", ".*", ".*"},
		{"bob", "prod", `""`, `""`, "^r"},
	}
	if got := lines(out); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("permissions = %q", got)
	}
	out, _, err = e.exec(t, "permission", "list", "--vhost", "prod", "-o", "json")
	checkErr(t, err, "")
	if ps := decodeJSON[[]broker.Permission](t, out); len(ps) != 1 || ps[0].User != "bob" {
		t.Errorf("json = %+v", ps)
	}
	out, _, err = e.exec(t, "permission", "list", "--user", "nobody", "-o", "json")
	checkErr(t, err, "")
	if strings.TrimSpace(out) != "[]" {
		t.Errorf("empty json = %q", out)
	}
}
