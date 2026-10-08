package broker

import (
	"regexp"
	"testing"
	"time"
)

func TestFilterMatch(t *testing.T) {
	t0 := time.Date(2026, 10, 8, 9, 0, 0, 0, time.UTC)
	msg := Message{
		Key:       []byte("ord-42"),
		Value:     []byte(`{"total": 990}`),
		Headers:   []Header{{Key: "source", Value: []byte("mqx")}, {Key: "trace", Value: []byte("abc")}},
		Timestamp: t0,
	}
	re := regexp.MustCompile
	tests := []struct {
		name string
		f    Filter
		want bool
	}{
		{name: "zero filter matches", f: Filter{}, want: true},
		{name: "key regex", f: Filter{Key: re(`^ord-\d+$`)}, want: true},
		{name: "key mismatch", f: Filter{Key: re(`^usr-`)}, want: false},
		{name: "value regex", f: Filter{Value: re(`"total":\s*990`)}, want: true},
		{name: "header present", f: Filter{Headers: []HeaderMatch{{Key: "trace"}}}, want: true},
		{name: "header absent", f: Filter{Headers: []HeaderMatch{{Key: "missing"}}}, want: false},
		{name: "header value", f: Filter{Headers: []HeaderMatch{{Key: "source", Value: re("^mqx$")}}}, want: true},
		{name: "header value mismatch", f: Filter{Headers: []HeaderMatch{{Key: "source", Value: re("^app$")}}}, want: false},
		{name: "since inclusive", f: Filter{Since: t0}, want: true},
		{name: "until exclusive", f: Filter{Until: t0}, want: false},
		{name: "inside range", f: Filter{Since: t0.Add(-time.Minute), Until: t0.Add(time.Minute)}, want: true},
		{name: "all conditions must hold", f: Filter{Key: re("ord"), Value: re("nope")}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.f.Match(msg); got != tt.want {
				t.Errorf("Match() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseHeaderMatch(t *testing.T) {
	tests := []struct {
		in        string
		wantKey   string
		wantRegex string
		wantErr   bool
	}{
		{in: "trace", wantKey: "trace"},
		{in: "source=^mqx$", wantKey: "source", wantRegex: "^mqx$"},
		{in: "k=a=b", wantKey: "k", wantRegex: "a=b"},
		{in: "=x", wantErr: true},
		{in: "k=(", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := ParseHeaderMatch(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if got.Key != tt.wantKey {
				t.Errorf("Key = %q, want %q", got.Key, tt.wantKey)
			}
			gotRe := ""
			if got.Value != nil {
				gotRe = got.Value.String()
			}
			if gotRe != tt.wantRegex {
				t.Errorf("Value = %q, want %q", gotRe, tt.wantRegex)
			}
		})
	}
}

func TestRates(t *testing.T) {
	t0 := time.Unix(1000, 0)
	prev := MetricSample{Time: t0, Counters: map[string]float64{"in": 100, "out": 50, "gone": 1}}
	cur := MetricSample{Time: t0.Add(2 * time.Second), Counters: map[string]float64{"in": 300, "out": 40, "new": 9}}
	got := Rates(prev, cur)
	if got["in"] != 100 {
		t.Errorf("in rate = %v, want 100", got["in"])
	}
	if _, ok := got["out"]; ok {
		t.Errorf("counter reset must not produce a rate, got %v", got["out"])
	}
	if _, ok := got["new"]; ok {
		t.Errorf("counter missing from prev must not produce a rate")
	}
	if len(Rates(cur, cur)) != 0 {
		t.Errorf("zero elapsed time must produce no rates")
	}
}
