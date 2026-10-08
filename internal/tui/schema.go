package tui

import (
	"context"
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/key"

	"github.com/Max2535/mqx/internal/broker"
)

// newSchemaPanel browses Schema Registry: subjects → versions → schema text.
func newSchemaPanel(e *env) panel {
	sr := as[broker.SchemaRegistry](e)
	actions := []action{{
		key:      key.NewBinding(key.WithKeys("n"), key.WithHelp("n", "register schema")),
		mutating: true,
		form: func(r *row) (string, []field) {
			subject := ""
			if r != nil {
				subject = r.key
			}
			return "Register schema", []field{
				{key: "subject", label: "Subject", value: subject},
				{key: "type", label: "Type", hint: "AVRO, PROTOBUF or JSON", value: "AVRO"},
				{key: "schema", label: "Schema", kind: fieldArea},
			}
		},
		check: func(_ *row, v values) error {
			if v["subject"] == "" || strings.TrimSpace(v["schema"]) == "" {
				return fmt.Errorf("subject and schema are required")
			}
			return nil
		},
		describe: func(_ *row, v values) string { return "Register a new schema version for subject " + v["subject"] },
		run: func(ctx context.Context, _ *row, v values) (string, error) {
			id, err := sr.RegisterSchema(ctx, v["subject"], broker.Schema{Type: strings.ToUpper(v["type"]), Schema: v["schema"]})
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("registered schema id %d", id), nil
		},
	}, {
		key:      key.NewBinding(key.WithKeys("d"), key.WithHelp("d", "delete subject")),
		mutating: true,
		needsRow: true,
		describe: func(r *row, _ values) string { return "Delete subject " + r.key },
		run: func(ctx context.Context, r *row, _ values) (string, error) {
			vs, err := sr.DeleteSubject(ctx, r.key, false)
			if err != nil {
				return "", err
			}
			return "deleted versions " + joinInts(vs), nil
		},
	}}
	return newStack(newListView(e, resource{
		title: "Schemas",
		cols:  []string{"SUBJECT"},
		load: func(ctx context.Context) (listing, error) {
			subjects, err := sr.Subjects(ctx)
			if err != nil {
				return listing{}, err
			}
			rows := make([]row, len(subjects))
			for i, s := range subjects {
				rows[i] = row{key: s, cells: []string{s}}
			}
			return listing{rows: rows}, nil
		},
		open:    func(r row) panel { return newSchemaVersions(e, sr, r.key) },
		actions: actions,
	}))
}

func newSchemaVersions(e *env, sr broker.SchemaRegistry, subject string) panel {
	return newListView(e, resource{
		title: subject,
		cols:  []string{"VERSION"},
		load: func(ctx context.Context) (listing, error) {
			vs, err := sr.SchemaVersions(ctx, subject)
			if err != nil {
				return listing{}, err
			}
			compat, err := sr.SubjectCompatibility(ctx, subject)
			if err != nil {
				compat = "unknown (" + err.Error() + ")"
			}
			rows := make([]row, len(vs))
			for i, v := range vs {
				rows[i] = row{key: itoa(v), cells: []string{itoa(v)}, data: v}
			}
			return listing{header: "compatibility " + compat, rows: rows}, nil
		},
		open: func(r row) panel {
			version := r.data.(int)
			return newTextView(e, fmt.Sprintf("%s v%d", subject, version), func(ctx context.Context) (string, error) {
				s, err := sr.Schema(ctx, subject, version)
				if err != nil {
					return "", err
				}
				body := s.Schema
				if pretty, ok := prettyJSON([]byte(s.Schema)); ok {
					body = highlightJSON(pretty)
				}
				return fmt.Sprintf("id %d  type %s  version %d\n\n%s", s.ID, s.Type, s.Version, body), nil
			})
		},
	})
}
