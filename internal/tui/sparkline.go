package tui

import (
	"math"
	"strings"
)

var sparkRunes = []rune("▁▂▃▄▅▆▇█")

// sparkline renders the last width values as block characters scaled between
// 0 (or the minimum, if negative) and the maximum. Empty input renders as "".
func sparkline(values []float64, width int) string {
	if width <= 0 || len(values) == 0 {
		return ""
	}
	if len(values) > width {
		values = values[len(values)-width:]
	}
	lo, hi := 0.0, 0.0
	for _, v := range values {
		lo, hi = math.Min(lo, v), math.Max(hi, v)
	}
	var b strings.Builder
	for _, v := range values {
		i := 0
		if hi > lo {
			i = int(math.Round((v - lo) / (hi - lo) * float64(len(sparkRunes)-1)))
		}
		b.WriteRune(sparkRunes[max(0, min(len(sparkRunes)-1, i))])
	}
	return b.String()
}

// bar renders v as a horizontal bar of width cells relative to maxV.
func bar(v, maxV float64, width int) string {
	if width <= 0 {
		return ""
	}
	n := 0
	if maxV > 0 && v > 0 {
		n = int(math.Round(v / maxV * float64(width)))
	}
	n = max(0, min(width, n))
	return strings.Repeat("█", n) + strings.Repeat("░", width-n)
}

// ring is a fixed-capacity buffer keeping the newest items.
type ring[T any] struct {
	items []T
	start int
	size  int
}

func newRing[T any](capacity int) *ring[T] { return &ring[T]{items: make([]T, capacity)} }

func (r *ring[T]) push(v T) {
	if len(r.items) == 0 {
		return
	}
	if r.size < len(r.items) {
		r.items[(r.start+r.size)%len(r.items)] = v
		r.size++
		return
	}
	r.items[r.start] = v
	r.start = (r.start + 1) % len(r.items)
}

// slice returns the items oldest first.
func (r *ring[T]) slice() []T {
	out := make([]T, r.size)
	for i := range out {
		out[i] = r.items[(r.start+i)%len(r.items)]
	}
	return out
}
