package cli

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/Max2535/mqx/internal/broker"
)

func TestACLCommands(t *testing.T) {
	e := newKafkaCLIEnv(t, false)
	steps := []struct {
		args    []string
		wantErr string
		want    []string
	}{
		{args: []string{"acl", "create", "--principal", "User:alice", "--resource-type", "topic", "--resource-name", "orders",
			"--pattern", "literal", "--host", "*", "--operation", "read", "--permission", "allow", "--yes"},
			want: []string{"Created ACL: allow User:alice read on topic orders."}},
		{args: []string{"acl", "create", "--principal", "User:bob", "--resource-type", "group", "--resource-name", "b-",
			"--pattern", "prefixed", "--host", "*", "--operation", "read", "--permission", "allow", "--yes"},
			want: []string{"Created ACL: allow User:bob read on group b-."}},
		{args: []string{"acl", "list"}, want: []string{"PRINCIPAL HOST RESOURCE NAME PATTERN OPERATION PERMISSION",
			"User:alice * topic orders literal read allow", "User:bob * group b- prefixed read allow"}},
		{args: []string{"acl", "list", "--principal", "User:bob"}, want: []string{"PRINCIPAL HOST RESOURCE NAME PATTERN OPERATION PERMISSION",
			"User:bob * group b- prefixed read allow"}},
		{args: []string{"acl", "create", "--principal", "User:x", "--yes"}, wantErr: "are required"},
		{args: []string{"acl", "delete", "--yes"}, wantErr: "refusing to delete every ACL"},
		{args: []string{"acl", "delete", "--principal", "User:alice"}, wantErr: "without confirmation"},
		{args: []string{"acl", "delete", "--principal", "User:alice", "--yes"}, want: []string{
			"PRINCIPAL HOST RESOURCE NAME PATTERN OPERATION PERMISSION", "User:alice * topic orders literal read allow"}},
	}
	for _, s := range steps {
		out, _, err := e.mqx(t, s.args...)
		checkErr(t, err, s.wantErr)
		if s.want != nil && !slices.Equal(outLines(out), s.want) {
			t.Errorf("%v:\n got %q\nwant %q", s.args, outLines(out), s.want)
		}
	}
	want := []string{"CreateACL User:alice orders read", "CreateACL User:bob b- read", "DeleteACLs 1"}
	if !slices.Equal(e.f.Calls, want) {
		t.Errorf("calls = %q, want %q", e.f.Calls, want)
	}
	out, _, err := e.mqx(t, "acl", "list", "-o", "json")
	checkErr(t, err, "")
	var acls []broker.ACL
	if json.Unmarshal([]byte(out), &acls) != nil || len(acls) != 1 || acls[0].Principal != "User:bob" {
		t.Errorf("list json = %s", out)
	}
}

func TestACLGuard(t *testing.T) {
	e := newKafkaCLIEnv(t, true)
	_, _, err := e.mqx(t, "acl", "create", "--principal", "User:a", "--resource-type", "topic", "--resource-name", "t",
		"--operation", "read", "--permission", "allow", "--yes")
	checkErr(t, err, "read_only")
	_, _, err = e.mqx(t, "acl", "delete", "--principal", "User:a", "--yes")
	checkErr(t, err, "read_only")
	_, _, err = e.mqx(t, "acl", "list")
	checkErr(t, err, "")
	if len(e.f.Calls) != 0 {
		t.Errorf("calls = %q", e.f.Calls)
	}
}
