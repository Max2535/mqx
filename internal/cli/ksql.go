package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Max2535/mqx/internal/broker"
)

// ksqlReadOnly are the statement keywords that never change anything.
var ksqlReadOnly = map[string]bool{"SELECT": true, "SHOW": true, "LIST": true, "DESCRIBE": true, "EXPLAIN": true, "PRINT": true}

func newKSQLCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:   "ksql <statement>",
		Short: "Run a ksqlDB statement or query",
		Long: `Run one ksqlDB statement. Needs ksqldb.url in the context.

SELECT, SHOW, LIST, DESCRIBE, EXPLAIN and PRINT are read-only; every other
statement (CREATE, DROP, INSERT, TERMINATE, ...) is mutating and needs
confirmation. Push queries (EMIT CHANGES) run until their LIMIT or --timeout.`,
		Example: `  mqx ksql "SHOW STREAMS"
  mqx ksql "SELECT * FROM orders EMIT CHANGES LIMIT 10"
  mqx ksql "CREATE STREAM orders (id INT) WITH (kafka_topic='orders', value_format='JSON')" --yes`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			stmt := strings.TrimSpace(args[0])
			if stmt == "" || stmt == ";" {
				return fmt.Errorf("empty statement; e.g. mqx ksql \"SHOW STREAMS\"")
			}
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				kr, err := capability[broker.KSQLRunner](s, broker.CapKSQLRunner)
				if err != nil {
					return err
				}
				if isKSQLMutating(stmt) {
					if err := s.guard(cmd, "run ksql statement: "+ksqlSummary(stmt)); err != nil {
						return err
					}
				}
				res, err := kr.RunKSQL(ctx, stmt)
				if err != nil {
					return err
				}
				if o.output == "json" {
					return o.render(cmd, res, nil)
				}
				if len(res.Columns) > 0 {
					t := newTable(res.Columns...)
					for _, row := range res.Rows {
						cells := make([]any, len(res.Columns))
						for i := range cells {
							if i < len(row) {
								cells[i] = ksqlCell(row[i])
							}
						}
						t.add(cells...)
					}
					if err := t.write(cmd.OutOrStdout()); err != nil {
						return err
					}
				}
				if res.Message != "" {
					_, err = fmt.Fprintln(cmd.OutOrStdout(), res.Message)
				}
				return err
			})
		},
	}
}

func isKSQLMutating(stmt string) bool {
	f := strings.Fields(stmt)
	return len(f) == 0 || !ksqlReadOnly[strings.ToUpper(strings.TrimSuffix(f[0], ";"))]
}

// ksqlSummary shortens a statement for the confirmation prompt.
func ksqlSummary(stmt string) string {
	stmt = strings.Join(strings.Fields(stmt), " ")
	if len(stmt) > 80 {
		return stmt[:77] + "..."
	}
	return stmt
}

// ksqlCell renders nested values (structs, arrays, maps) as compact JSON.
func ksqlCell(v any) any {
	switch v.(type) {
	case map[string]any, []any:
		b, err := json.Marshal(v)
		if err == nil {
			return string(b)
		}
	}
	return v
}
