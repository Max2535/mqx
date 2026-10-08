package cli

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/Max2535/mqx/internal/broker"
)

func TestKSQL(t *testing.T) {
	tests := []struct {
		name     string
		readOnly bool
		args     []string
		wantErr  string
		want     []string
	}{
		{name: "select is read-only", readOnly: true, args: []string{"ksql", "select * from orders emit changes limit 2;"},
			want: []string{"ID TOTAL", "a 1", "b 2"}},
		{name: "show is read-only", readOnly: true, args: []string{"ksql", "SHOW STREAMS"}, want: []string{"ok: SHOW STREAMS"}},
		{name: "create refused on read-only", readOnly: true, args: []string{"ksql", "CREATE STREAM s (id INT) WITH (kafka_topic='s')", "--yes"},
			wantErr: "read_only"},
		{name: "drop needs confirmation", args: []string{"ksql", "DROP STREAM s"}, wantErr: "without confirmation"},
		{name: "insert with --yes", args: []string{"ksql", "INSERT INTO s (id) VALUES (1)", "--yes"}, want: []string{"ok: INSERT INTO s (id) VALUES (1)"}},
		{name: "empty statement", args: []string{"ksql", " "}, wantErr: "empty statement"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newKafkaCLIEnv(t, tt.readOnly)
			out, _, err := e.mqx(t, tt.args...)
			checkErr(t, err, tt.wantErr)
			if tt.want != nil && !slices.Equal(outLines(out), tt.want) {
				t.Errorf("got %q, want %q", outLines(out), tt.want)
			}
		})
	}
	e := newKafkaCLIEnv(t, true)
	out, _, err := e.mqx(t, "ksql", "SELECT * FROM t;", "-o", "json")
	checkErr(t, err, "")
	var res broker.KSQLResult
	if json.Unmarshal([]byte(out), &res) != nil || len(res.Rows) != 2 {
		t.Errorf("json = %s", out)
	}
}

func TestKSQLClassification(t *testing.T) {
	for stmt, mutating := range map[string]bool{
		"select 1;": false, "Show tables": false, "LIST TOPICS;": false, "describe s": false, "EXPLAIN q1": false,
		"PRINT 'orders';": false, "CREATE TABLE t AS SELECT 1": true, "terminate q1": true, "SET 'x'='y'": true, "": true,
	} {
		if got := isKSQLMutating(stmt); got != mutating {
			t.Errorf("isKSQLMutating(%q) = %v, want %v", stmt, got, mutating)
		}
	}
	if got := ksqlCell(map[string]any{"a": 1.0}); got != `{"a":1}` {
		t.Errorf("ksqlCell(map) = %v", got)
	}
}
