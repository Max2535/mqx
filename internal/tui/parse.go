package tui

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Max2535/mqx/internal/broker"
)

const positionHint = "earliest, latest, offset, -N (last N), RFC3339 time or duration ago (15m)"

// parsePosition parses the same position syntax as the CLI's --from flag.
func parsePosition(s string, now time.Time) (broker.Position, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "earliest", "beginning", "start":
		return broker.Position{Kind: broker.Earliest}, nil
	case "latest", "end":
		return broker.Position{Kind: broker.Latest}, nil
	}
	if strings.HasPrefix(s, "-") {
		if n, err := strconv.ParseInt(s[1:], 10, 64); err == nil && n > 0 {
			return broker.Position{Kind: broker.Tail, Offset: n}, nil
		}
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil && n >= 0 {
		return broker.Position{Kind: broker.AtOffset, Offset: n}, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return broker.Position{Kind: broker.AtTime, Time: t}, nil
	}
	if d, err := time.ParseDuration(s); err == nil && d > 0 {
		return broker.Position{Kind: broker.AtTime, Time: now.Add(-d)}, nil
	}
	return broker.Position{}, fmt.Errorf("invalid position %q; use %s", s, positionHint)
}

// parsePartitions parses "0,2,5"; empty means all.
func parsePartitions(s string) ([]int32, error) {
	var out []int32
	for _, f := range strings.Split(s, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		n, err := strconv.ParseInt(f, 10, 32)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("invalid partition %q; want a non-negative integer", f)
		}
		out = append(out, int32(n))
	}
	return out, nil
}

// parseKV parses "k=v" entries separated by newlines or commas into ordered pairs.
func parseKV(s string) ([][2]string, error) {
	var out [][2]string
	for _, line := range strings.FieldsFunc(s, func(r rune) bool { return r == '\n' || r == ',' }) {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			return nil, fmt.Errorf("invalid entry %q; want key=value", line)
		}
		out = append(out, [2]string{k, strings.TrimSpace(v)})
	}
	return out, nil
}

// parseKVMap is parseKV into a map; nil when empty.
func parseKVMap(s string) (map[string]string, error) {
	kvs, err := parseKV(s)
	if err != nil || len(kvs) == 0 {
		return nil, err
	}
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		m[kv[0]] = kv[1]
	}
	return m, nil
}

// parseArgs parses "k=v" entries into typed values: integers and booleans are
// converted so x-message-ttl=60000 reaches RabbitMQ as a number.
func parseArgs(s string) (map[string]any, error) {
	m, err := parseKVMap(s)
	if err != nil || m == nil {
		return nil, err
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			out[k] = n
		} else if b, err := strconv.ParseBool(v); err == nil {
			out[k] = b
		} else {
			out[k] = v
		}
	}
	return out, nil
}

// parseYes parses a y/n field with a default.
func parseYes(s string, def bool) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "":
		return def, nil
	case "y", "yes", "true", "1":
		return true, nil
	case "n", "no", "false", "0":
		return false, nil
	}
	return false, fmt.Errorf("invalid answer %q; want y or n", s)
}

// formatKV renders a map as sorted "k=v" pairs.
func formatKV[V any](m map[string]V, sep string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%v", k, m[k])
	}
	return strings.Join(parts, sep)
}

func itoa[T ~int | ~int32 | ~int64](n T) string { return strconv.FormatInt(int64(n), 10) }

func joinInts[T ~int | ~int32](xs []T) string {
	parts := make([]string, len(xs))
	for i, x := range xs {
		parts[i] = itoa(x)
	}
	return strings.Join(parts, ",")
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}
