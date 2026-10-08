package tui

import (
	"strings"
	"unicode"
)

// fuzzyScore reports whether every space-separated word of query matches s
// as an in-order subsequence, ignoring case, and how well. Runs of
// consecutive letters, matches at word starts and exact substrings score
// higher, so "grp" ranks "Groups" above "Go to report".
func fuzzyScore(query, s string) (int, bool) {
	total := 0
	for _, word := range strings.Fields(strings.ToLower(query)) {
		n, ok := wordScore(word, strings.ToLower(s))
		if !ok {
			return 0, false
		}
		total += n
	}
	return total, true
}

func wordScore(word, s string) (int, bool) {
	q, r := []rune(word), []rune(s)
	score, qi, prev := 0, 0, -2
	for i := 0; i < len(r) && qi < len(q); i++ {
		if r[i] != q[qi] {
			continue
		}
		score++
		if i == prev+1 {
			score += 4
		}
		if i == 0 || !unicode.IsLetter(r[i-1]) && !unicode.IsDigit(r[i-1]) {
			score += 6
		}
		prev = i
		qi++
	}
	if qi < len(q) {
		return 0, false
	}
	if strings.Contains(s, word) {
		score += 10
	}
	return score, true
}
