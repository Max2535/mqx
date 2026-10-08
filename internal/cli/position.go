package cli

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Max2535/mqx/internal/broker"
)

const positionHelp = "earliest, latest, an offset (1234), -N for the last N per partition, " +
	"an RFC3339 time (2026-10-08T09:00:00Z), or a duration ago (15m)"

// parsePosition parses the --from / --to / --to-offset style values.
func parsePosition(s string, now time.Time) (broker.Position, error) {
	switch strings.ToLower(s) {
	case "", "earliest", "beginning", "start":
		return broker.Position{Kind: broker.Earliest}, nil
	case "latest", "end":
		return broker.Position{Kind: broker.Latest}, nil
	}
	if strings.HasPrefix(s, "-") {
		n, err := strconv.ParseInt(s[1:], 10, 64)
		if err == nil && n > 0 {
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
	return broker.Position{}, fmt.Errorf("invalid position %q; use %s", s, positionHelp)
}

// parsePartitions parses "0,2,5".
func parsePartitions(s []string) ([]int32, error) {
	var out []int32
	for _, part := range s {
		for _, f := range strings.Split(part, ",") {
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
	}
	return out, nil
}
