package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

// table is a header plus rows, printed aligned with tabwriter.
type table struct {
	header []string
	rows   [][]string
}

func newTable(header ...string) *table { return &table{header: header} }

func (t *table) add(cells ...any) {
	row := make([]string, len(cells))
	for i, c := range cells {
		row[i] = cell(c)
	}
	t.rows = append(t.rows, row)
}

func (t *table) write(w io.Writer) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, strings.Join(t.header, "\t"))
	for _, r := range t.rows {
		fmt.Fprintln(tw, strings.Join(r, "\t"))
	}
	return tw.Flush()
}

// cell formats one table value; empty strings print as "-" to keep columns aligned.
func cell(v any) string {
	var s string
	switch x := v.(type) {
	case string:
		s = x
	case bool:
		if x {
			s = "yes"
		} else {
			s = "no"
		}
	case []string:
		s = strings.Join(x, ",")
	case []int32:
		s = int32s(x)
	case time.Time:
		if !x.IsZero() {
			s = x.Local().Format("2006-01-02 15:04:05")
		}
	case time.Duration:
		s = x.Round(time.Millisecond).String()
	case float64:
		s = strconv.FormatFloat(x, 'f', -1, 64)
		if strings.Contains(s, ".") {
			s = strconv.FormatFloat(x, 'f', 2, 64)
		}
	case nil:
	default:
		s = fmt.Sprint(x)
	}
	s = strings.ReplaceAll(s, "\t", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	if s == "" {
		return "-"
	}
	return s
}

func int32s(xs []int32) string {
	parts := make([]string, len(xs))
	for i, x := range xs {
		parts[i] = strconv.Itoa(int(x))
	}
	return strings.Join(parts, ",")
}

// render prints v as JSON with -o json, else the table built by mk.
func (o *options) render(cmd *cobra.Command, v any, mk func() *table) error {
	switch o.output {
	case "json":
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(v)
	case "table", "":
		return mk().write(cmd.OutOrStdout())
	default:
		return fmt.Errorf("unknown --output %q; use table or json", o.output)
	}
}

// done prints a one-line success message, or {"ok":true,...} with -o json.
func (o *options) done(cmd *cobra.Command, format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	if o.output == "json" {
		return json.NewEncoder(cmd.OutOrStdout()).Encode(map[string]any{"ok": true, "message": msg})
	}
	_, err := fmt.Fprintln(cmd.OutOrStdout(), msg)
	return err
}

// kv renders a map as sorted key=value pairs.
func kv[V any](m map[string]V) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%v", k, m[k])
	}
	return strings.Join(parts, " ")
}

// parseKV parses repeated key=value flags.
func parseKV(flag string, pairs []string) (map[string]string, error) {
	out := make(map[string]string, len(pairs))
	for _, p := range pairs {
		k, v, ok := strings.Cut(p, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("--%s %q: want key=value", flag, p)
		}
		out[k] = v
	}
	return out, nil
}

// parseArgs parses key=value pairs into typed values: integers, booleans and
// JSON objects/arrays are decoded, everything else stays a string. Used for
// RabbitMQ arguments (x-max-length=1000) and definitions.
func parseArgs(flag string, pairs []string) (map[string]any, error) {
	raw, err := parseKV(flag, pairs)
	if err != nil {
		return nil, err
	}
	out := make(map[string]any, len(raw))
	for k, v := range raw {
		out[k] = typedValue(v)
	}
	return out, nil
}

func typedValue(v string) any {
	if i, err := strconv.ParseInt(v, 10, 64); err == nil {
		return i
	}
	if b, err := strconv.ParseBool(v); err == nil {
		return b
	}
	if strings.HasPrefix(v, "{") || strings.HasPrefix(v, "[") {
		var x any
		if json.Unmarshal([]byte(v), &x) == nil {
			return x
		}
	}
	return v
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
