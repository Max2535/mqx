package cli

import (
	"slices"
	"testing"

	"github.com/Max2535/mqx/internal/broker"
)

func TestVHosts(t *testing.T) {
	e := newRMQEnv(t, false)
	_, _, err := e.exec(t, "vhost", "create", "staging", "--description", "for tests", "--tracing", "--yes")
	checkErr(t, err, "")
	out, _, err := e.exec(t, "vhosts")
	checkErr(t, err, "")
	want := [][]string{
		{"NAME", "MESSAGES", "TRACING", "DESCRIPTION"},
		{"/", "0", "no", "-"},
		{"staging", "0", "yes", "for", "tests"},
	}
	if got := lines(out); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("vhosts = %q", got)
	}
	out, _, err = e.exec(t, "vhosts", "-o", "json")
	checkErr(t, err, "")
	if vs := decodeJSON[[]broker.VHost](t, out); len(vs) != 2 || vs[1].Description != "for tests" || !vs[1].Tracing {
		t.Errorf("json = %+v", vs)
	}
	_, _, err = e.exec(t, "vhost", "delete", "--yes")
	checkErr(t, err, "accepts 1 arg")
}
