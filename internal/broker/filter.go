package broker

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Filter selects messages during Peek. The zero Filter matches everything.
// Adapters apply it before counting towards PeekOptions.Limit.
type Filter struct {
	Key     *regexp.Regexp
	Value   *regexp.Regexp
	Headers []HeaderMatch
	Since   time.Time // inclusive; zero = unbounded
	Until   time.Time // exclusive; zero = unbounded
}

// HeaderMatch requires a header to be present, and to match Value when set.
type HeaderMatch struct {
	Key   string
	Value *regexp.Regexp
}

// Match reports whether m passes every condition.
func (f Filter) Match(m Message) bool {
	if f.Key != nil && !f.Key.Match(m.Key) {
		return false
	}
	if f.Value != nil && !f.Value.Match(m.Value) {
		return false
	}
	if !f.Since.IsZero() && m.Timestamp.Before(f.Since) {
		return false
	}
	if !f.Until.IsZero() && !m.Timestamp.Before(f.Until) {
		return false
	}
	for _, hm := range f.Headers {
		if !matchHeader(m.Headers, hm) {
			return false
		}
	}
	return true
}

func matchHeader(headers []Header, hm HeaderMatch) bool {
	for _, h := range headers {
		if h.Key == hm.Key && (hm.Value == nil || hm.Value.Match(h.Value)) {
			return true
		}
	}
	return false
}

// ParseHeaderMatch parses "key" (present) or "key=regex".
func ParseHeaderMatch(s string) (HeaderMatch, error) {
	key, pattern, hasValue := strings.Cut(s, "=")
	if key == "" {
		return HeaderMatch{}, fmt.Errorf("header filter %q: want key or key=regex", s)
	}
	hm := HeaderMatch{Key: key}
	if hasValue {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return HeaderMatch{}, fmt.Errorf("header filter %q: %w", s, err)
		}
		hm.Value = re
	}
	return hm, nil
}

// IsZero reports whether the filter matches everything.
func (f Filter) IsZero() bool {
	return f.Key == nil && f.Value == nil && len(f.Headers) == 0 && f.Since.IsZero() && f.Until.IsZero()
}
