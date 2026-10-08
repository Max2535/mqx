package tui

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Max2535/mqx/internal/broker"
)

// Payload formats detected by renderPayload.
const (
	formatJSON  = "json"
	formatText  = "text"
	formatHex   = "hex"
	formatEmpty = "empty"
)

// prettyJSON indents b when it is valid JSON.
func prettyJSON(b []byte) (string, bool) {
	trimmed := bytes.TrimSpace(b)
	if len(trimmed) == 0 || !json.Valid(trimmed) {
		return "", false
	}
	var out bytes.Buffer
	if err := json.Indent(&out, trimmed, "", "  "); err != nil {
		return "", false
	}
	return out.String(), true
}

// isText reports whether b is printable UTF-8 text.
func isText(b []byte) bool {
	if !utf8.Valid(b) {
		return false
	}
	for _, r := range string(b) {
		if !unicode.IsPrint(r) && !unicode.IsSpace(r) {
			return false
		}
	}
	return true
}

// renderPayload returns b as pretty, syntax-coloured JSON when valid, else
// as text, else as a hex dump, with the detected format. raw disables
// colouring and indentation so the output can be copied as-is.
func renderPayload(b []byte, raw bool) (string, string) {
	if len(b) == 0 {
		return "", formatEmpty
	}
	if s, ok := prettyJSON(b); ok {
		if raw {
			return string(b), formatJSON
		}
		return highlightJSON(s), formatJSON
	}
	if isText(b) {
		return string(b), formatText
	}
	return strings.TrimRight(hex.Dump(b), "\n"), formatHex
}

// highlightJSON colours keys, strings, numbers and literals of indented JSON.
func highlightJSON(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == '"':
			j := i + 1
			for j < len(s) && s[j] != '"' {
				if s[j] == '\\' {
					j++
				}
				j++
			}
			j = min(j+1, len(s))
			tok := s[i:j]
			k := j
			for k < len(s) && (s[k] == ' ' || s[k] == '\t') {
				k++
			}
			if k < len(s) && s[k] == ':' {
				b.WriteString(st.jsonKey.Render(tok))
			} else {
				b.WriteString(st.jsonString.Render(tok))
			}
			i = j
		case c == '-' || (c >= '0' && c <= '9'):
			j := i + 1
			for j < len(s) && strings.IndexByte("0123456789.eE+-", s[j]) >= 0 {
				j++
			}
			b.WriteString(st.jsonNumber.Render(s[i:j]))
			i = j
		case c >= 'a' && c <= 'z':
			j := i
			for j < len(s) && s[j] >= 'a' && s[j] <= 'z' {
				j++
			}
			b.WriteString(st.jsonLiteral.Render(s[i:j]))
			i = j
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}

// describeMessage renders the metadata block of a message: position, key,
// headers, RabbitMQ routing and properties, schema.
func describeMessage(m broker.Message) string {
	var lines []string
	add := func(k, v string) { lines = append(lines, st.muted.Render(pad(k, 12))+" "+v) }
	if m.Exchange != "" || m.RoutingKey != "" {
		add("exchange", orDefault(m.Exchange))
		add("routing key", m.RoutingKey)
	} else {
		add("partition", fmt.Sprintf("%d  offset %d", m.Partition, m.Offset))
	}
	if !m.Timestamp.IsZero() {
		add("timestamp", m.Timestamp.Format("2006-01-02 15:04:05.000 MST"))
	}
	if m.Key != nil {
		add("key", printable(m.Key))
	}
	add("size", fmt.Sprintf("%d bytes", len(m.Value)))
	if m.Redelivered {
		add("redelivered", "yes")
	}
	for _, h := range m.Headers {
		add("header", h.Key+"="+printable(h.Value))
	}
	keys := make([]string, 0, len(m.Properties))
	for k := range m.Properties {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		add("property", k+"="+m.Properties[k])
	}
	if s := m.Schema; s != nil {
		add("schema", fmt.Sprintf("%s id=%d subject=%s v%d", s.Format, s.ID, s.Subject, s.Version))
	}
	return strings.Join(lines, "\n")
}

// printable shows text as-is and binary as hex.
func printable(b []byte) string {
	if isText(b) {
		return string(b)
	}
	return "0x" + hex.EncodeToString(b)
}

func orDefault(exchange string) string {
	if exchange == "" {
		return "(default)"
	}
	return exchange
}
