package cli

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Max2535/mqx/internal/broker"
)

// outLines normalises output into lines of single-space-separated fields.
func outLines(s string) []string {
	var out []string
	for _, line := range strings.Split(strings.TrimSpace(s), "\n") {
		if line = strings.Join(strings.Fields(line), " "); line != "" {
			out = append(out, line)
		}
	}
	return out
}

func writeTemp(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

const testAvro = `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`

func TestSchemaCommands(t *testing.T) {
	e := newKafkaCLIEnv(t, false)
	avsc := writeTemp(t, "order.avsc", testAvro)
	proto := writeTemp(t, "order.proto", `syntax = "proto3"; message Order { string id = 1; }`)

	out, _, err := e.mqx(t, "schema", "register", "orders-value", "--file", avsc, "--yes")
	checkErr(t, err, "")
	if !strings.HasPrefix(out, "Registered schema id 101 under subject orders-value.") {
		t.Errorf("register output = %q", out)
	}
	_, _, err = e.mqx(t, "schema", "register", "orders-value", "--file", avsc, "--yes")
	checkErr(t, err, "")
	out, _, err = e.mqx(t, "schema", "register", "porders-value", "--file", proto, "--yes", "-o", "json")
	checkErr(t, err, "")
	var reg map[string]any
	if json.Unmarshal([]byte(out), &reg) != nil || reg["subject"] != "porders-value" {
		t.Errorf("register json = %s", out)
	}
	if !slices.Equal(e.f.Calls, []string{"RegisterSchema orders-value", "RegisterSchema orders-value", "RegisterSchema porders-value"}) {
		t.Errorf("calls = %q", e.f.Calls)
	}

	tests := []struct {
		args []string
		want []string
	}{
		{args: []string{"schema", "subjects"}, want: []string{"SUBJECT", "orders-value", "porders-value"}},
		{args: []string{"schema", "versions", "orders-value"}, want: []string{"VERSION", "1", "2"}},
		{args: []string{"schema", "get", "porders-value"}, want: []string{
			"Subject: porders-value", "Version: 1", "ID: 111", "Type: PROTOBUF", "Compatibility: BACKWARD",
			`syntax = "proto3"; message Order { string id = 1; }`}},
		{args: []string{"schema", "get", "orders-value", "--version", "1"}, want: []string{
			"Subject: orders-value", "Version: 1", "ID: 101", "Type: AVRO", "Compatibility: BACKWARD",
			"{", `"type": "record",`, `"name": "Order",`, `"fields": [`, "{", `"name": "id",`, `"type": "string"`, "}", "]", "}"}},
	}
	for _, tt := range tests {
		out, _, err := e.mqx(t, tt.args...)
		checkErr(t, err, "")
		if got := outLines(out); !slices.Equal(got, tt.want) {
			t.Errorf("%v:\n got %q\nwant %q", tt.args, got, tt.want)
		}
	}
	out, _, err = e.mqx(t, "schema", "get", "orders-value", "-o", "json")
	checkErr(t, err, "")
	var view struct {
		Version       int    `json:"version"`
		Compatibility string `json:"compatibility"`
	}
	if json.Unmarshal([]byte(out), &view) != nil || view.Version != 2 || view.Compatibility != "BACKWARD" {
		t.Errorf("get json = %s", out)
	}

	out, _, err = e.mqx(t, "schema", "delete", "orders-value", "--permanent", "--yes")
	checkErr(t, err, "")
	if strings.TrimSpace(out) != "Deleted subject orders-value (versions 1,2)." {
		t.Errorf("delete output = %q", out)
	}
	_, _, err = e.mqx(t, "schema", "get", "orders-value")
	if !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("get deleted: err = %v", err)
	}
}

func TestSchemaValidationAndGuard(t *testing.T) {
	avsc := writeTemp(t, "order.avsc", testAvro)
	tests := []struct {
		name     string
		readOnly bool
		args     []string
		wantErr  string
	}{
		{name: "register refused on read-only", readOnly: true, args: []string{"schema", "register", "s", "--file", avsc, "--yes"}, wantErr: "read_only"},
		{name: "delete refused on read-only", readOnly: true, args: []string{"schema", "delete", "s", "--yes"}, wantErr: "read_only"},
		{name: "register needs confirmation", args: []string{"schema", "register", "s", "--file", avsc}, wantErr: "without confirmation"},
		{name: "register needs a file", args: []string{"schema", "register", "s", "--yes"}, wantErr: "--file is required"},
		{name: "unreadable file", args: []string{"schema", "register", "s", "--file", "/no/such.avsc", "--yes"}, wantErr: "read schema"},
		{name: "bad type", args: []string{"schema", "register", "s", "--file", avsc, "--type", "XML", "--yes"}, wantErr: "use AVRO, PROTOBUF or JSON"},
		{name: "bad version", args: []string{"schema", "get", "s", "--version", "x"}, wantErr: "want a positive number or latest"},
		{name: "missing subject", args: []string{"schema", "versions", "nope"}, wantErr: "not found"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newKafkaCLIEnv(t, tt.readOnly)
			_, _, err := e.mqx(t, tt.args...)
			checkErr(t, err, tt.wantErr)
			if len(e.f.Calls) != 0 {
				t.Errorf("calls = %q", e.f.Calls)
			}
		})
	}
	e := newKafkaCLIEnv(t, false)
	e.f.Caps = []string{broker.CapGroupInspector}
	_, _, err := e.mqx(t, "schema", "subjects")
	checkErr(t, err, "does not support schema-registry")
}

func TestSchemaTypeOf(t *testing.T) {
	for file, want := range map[string]string{"a.avsc": "AVRO", "a.PROTO": "PROTOBUF", "a.schema.json": "JSON", "a": "AVRO"} {
		if got := schemaTypeOf(file); got != want {
			t.Errorf("schemaTypeOf(%q) = %s, want %s", file, got, want)
		}
	}
}
