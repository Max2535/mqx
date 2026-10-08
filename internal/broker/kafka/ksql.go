package kafka

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/Max2535/mqx/internal/broker"
)

// maxKSQLLine bounds one row of a streamed query result.
const maxKSQLLine = 16 << 20

// RunKSQL implements broker.KSQLRunner. SELECT goes to /query-stream and
// returns rows; push queries end at their LIMIT or when ctx ends, keeping the
// rows read so far. Other statements go to /ksql.
func (k *Kafka) RunKSQL(ctx context.Context, statement string) (*broker.KSQLResult, error) {
	if k.ksql == nil {
		return nil, fmt.Errorf("ksqldb is not configured; add ksqldb.url to the context: %w", broker.ErrUnsupported)
	}
	stmt := strings.TrimSpace(statement)
	if stmt == "" {
		return nil, fmt.Errorf("empty ksql statement")
	}
	if !strings.HasSuffix(stmt, ";") {
		stmt += ";"
	}
	if IsKSQLQuery(stmt) {
		return k.ksqlQuery(ctx, stmt)
	}
	var entities []map[string]any
	body := map[string]any{"ksql": stmt, "streamsProperties": map[string]any{}}
	if err := k.ksql.do(ctx, http.MethodPost, "/ksql", body, &entities); err != nil {
		return nil, err
	}
	return ksqlEntities(entities), nil
}

// IsKSQLQuery reports whether a statement is a SELECT (a pull or push query).
func IsKSQLQuery(stmt string) bool {
	f := strings.Fields(stmt)
	return len(f) > 0 && strings.EqualFold(strings.TrimSuffix(f[0], ";"), "SELECT")
}

// ksqlQuery runs a SELECT through /query-stream, whose delimited response is
// a header object followed by one JSON array per row.
func (k *Kafka) ksqlQuery(ctx context.Context, stmt string) (*broker.KSQLResult, error) {
	c := k.ksql
	// Read from the earliest offset, like a peek, so existing rows show up.
	props := map[string]any{"ksql.streams.auto.offset.reset": "earliest"}
	data, err := json.Marshal(map[string]any{"sql": stmt, "properties": props})
	if err != nil {
		return nil, fmt.Errorf("encode ksql query: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/query-stream", bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("ksqldb request: %w", err)
	}
	c.prepare(req, true)
	req.Header.Set("Accept", "application/vnd.ksqlapi.delimited.v1")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ksqldb at %s is unreachable; check the url in the context: %w", c.base, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody*4))
		return nil, c.statusErr(http.MethodPost, "/query-stream", resp.StatusCode, body)
	}
	return readKSQLRows(ctx, resp.Body)
}

func readKSQLRows(ctx context.Context, r io.Reader) (*broker.KSQLResult, error) {
	res := &broker.KSQLResult{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), maxKSQLLine)
	header := true
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		line = bytes.TrimSuffix(bytes.TrimPrefix(line, []byte("[")), []byte(",")) // tolerate the JSON-array framing
		if len(line) == 0 || bytes.Equal(line, []byte("]")) {
			continue
		}
		if header {
			var h struct {
				ColumnNames []string `json:"columnNames"`
			}
			if err := json.Unmarshal(line, &h); err != nil {
				return nil, fmt.Errorf("decode ksqldb query header: %w", err)
			}
			res.Columns, header = h.ColumnNames, false
			continue
		}
		if line[0] == '{' {
			var e struct {
				Message string `json:"message"`
			}
			if json.Unmarshal(line, &e) == nil && e.Message != "" {
				return res, fmt.Errorf("ksqldb query failed: %s", e.Message)
			}
			continue
		}
		var row []any
		if err := json.Unmarshal(append([]byte("["), append(bytes.TrimSuffix(line, []byte("]")), ']')...), &row); err != nil {
			return res, fmt.Errorf("decode ksqldb row: %w", err)
		}
		res.Rows = append(res.Rows, row)
	}
	if err := sc.Err(); err != nil && ctx.Err() == nil {
		return res, fmt.Errorf("read ksqldb query result: %w", err)
	}
	return res, nil
}

// ksqlEntities formats /ksql results: listings become a table, command
// results a message.
func ksqlEntities(entities []map[string]any) *broker.KSQLResult {
	res := &broker.KSQLResult{}
	var msgs []string
	for _, e := range entities {
		if status, ok := e["commandStatus"].(map[string]any); ok {
			msgs = append(msgs, fmt.Sprint(status["message"]))
			continue
		}
		if src, ok := e["sourceDescription"].(map[string]any); ok {
			if fields, ok := src["fields"].([]any); ok {
				res.Columns = []string{"field", "type"}
				for _, f := range fields {
					fm, _ := f.(map[string]any)
					typ, _ := fm["schema"].(map[string]any)
					res.Rows = append(res.Rows, []any{fm["name"], typ["type"]})
				}
				continue
			}
		}
		if cols, rows, ok := listing(e); ok {
			res.Columns, res.Rows = cols, append(res.Rows, rows...)
			continue
		}
		data, _ := json.Marshal(e)
		msgs = append(msgs, string(data))
	}
	res.Message = strings.Join(msgs, "\n")
	if res.Message == "" && len(res.Columns) == 0 {
		res.Message = "ok"
	}
	return res
}

// listing finds the array of objects in an entity (streams, tables, topics,
// queries, properties, ...) and turns it into columns and rows.
func listing(e map[string]any) ([]string, [][]any, bool) {
	keys := make([]string, 0, len(e))
	for k := range e {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		arr, ok := e[k].([]any)
		if !ok {
			continue
		}
		var cols []string
		seen := map[string]bool{}
		for _, item := range arr {
			m, ok := item.(map[string]any)
			if !ok {
				return nil, nil, false
			}
			names := make([]string, 0, len(m))
			for c := range m {
				if c != "@type" && !seen[c] {
					names = append(names, c)
				}
			}
			sort.Strings(names)
			for _, c := range names {
				seen[c] = true
				cols = append(cols, c)
			}
		}
		if cols == nil {
			cols = []string{k}
		}
		rows := make([][]any, 0, len(arr))
		for _, item := range arr {
			m := item.(map[string]any)
			row := make([]any, len(cols))
			for i, c := range cols {
				row[i] = m[c]
			}
			rows = append(rows, row)
		}
		return cols, rows, true
	}
	return nil, nil, false
}
