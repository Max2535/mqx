package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Max2535/mqx/internal/broker"
)

func newSchemaCmd(o *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "schema",
		Aliases: []string{"schemas"},
		Short:   "Browse and manage Schema Registry subjects",
		Long: `Browse and manage Schema Registry subjects. Needs schema_registry.url in the
context. peek --decode and publish --schema use the registry for serde.`,
	}
	cmd.AddCommand(newSchemaSubjectsCmd(o), newSchemaVersionsCmd(o), newSchemaGetCmd(o),
		newSchemaRegisterCmd(o), newSchemaDeleteCmd(o))
	return cmd
}

// withRegistry runs fn with the session's SchemaRegistry capability.
func (o *options) withRegistry(cmd *cobra.Command, fn func(ctx context.Context, s *session, sr broker.SchemaRegistry) error) error {
	return o.withSession(cmd, func(ctx context.Context, s *session) error {
		sr, err := capability[broker.SchemaRegistry](s, broker.CapSchemaRegistry)
		if err != nil {
			return err
		}
		return fn(ctx, s, sr)
	})
}

func newSchemaSubjectsCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:     "subjects",
		Short:   "List subjects",
		Example: "  mqx schema subjects",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.withRegistry(cmd, func(ctx context.Context, _ *session, sr broker.SchemaRegistry) error {
				subjects, err := sr.Subjects(ctx)
				if err != nil {
					return err
				}
				if subjects == nil {
					subjects = []string{}
				}
				return o.render(cmd, subjects, func() *table {
					t := newTable("SUBJECT")
					for _, s := range subjects {
						t.add(s)
					}
					return t
				})
			})
		},
	}
}

func newSchemaVersionsCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:     "versions <subject>",
		Short:   "List the versions of a subject",
		Example: "  mqx schema versions orders-value",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.withRegistry(cmd, func(ctx context.Context, _ *session, sr broker.SchemaRegistry) error {
				versions, err := sr.SchemaVersions(ctx, args[0])
				if err != nil {
					return err
				}
				if versions == nil {
					versions = []int{}
				}
				return o.render(cmd, versions, func() *table {
					t := newTable("VERSION")
					for _, v := range versions {
						t.add(v)
					}
					return t
				})
			})
		},
	}
}

// schemaView is the JSON form of `mqx schema get`.
type schemaView struct {
	*broker.Schema
	Compatibility string `json:"compatibility,omitempty"`
}

func newSchemaGetCmd(o *options) *cobra.Command {
	var version string
	cmd := &cobra.Command{
		Use:     "get <subject>",
		Short:   "Show one version of a subject (default latest)",
		Example: "  mqx schema get orders-value\n  mqx schema get orders-value --version 2 -o json",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			v := -1
			if version != "" && version != "latest" {
				n, err := strconv.Atoi(version)
				if err != nil || n < 1 {
					return fmt.Errorf("--version %q: want a positive number or latest", version)
				}
				v = n
			}
			return o.withRegistry(cmd, func(ctx context.Context, _ *session, sr broker.SchemaRegistry) error {
				s, err := sr.Schema(ctx, args[0], v)
				if err != nil {
					return err
				}
				view := schemaView{Schema: s}
				view.Compatibility, _ = sr.SubjectCompatibility(ctx, args[0]) // informational
				if o.output == "json" {
					return o.render(cmd, view, nil)
				}
				w := cmd.OutOrStdout()
				fmt.Fprintf(w, "Subject:       %s\nVersion:       %d\nID:            %d\nType:          %s\n", s.Subject, s.Version, s.ID, s.Type)
				if view.Compatibility != "" {
					fmt.Fprintf(w, "Compatibility: %s\n", view.Compatibility)
				}
				for _, r := range s.References {
					fmt.Fprintf(w, "Reference:     %s version %d\n", r.Subject, r.Version)
				}
				_, err = fmt.Fprintf(w, "\n%s\n", schemaText(s.Schema))
				return err
			})
		},
	}
	cmd.Flags().StringVar(&version, "version", "latest", "version number or latest")
	return cmd
}

// schemaText pretty-prints JSON schemas (Avro, JSON Schema) and leaves Protobuf as is.
func schemaText(s string) string {
	var buf bytes.Buffer
	if json.Indent(&buf, []byte(s), "", "  ") == nil {
		return buf.String()
	}
	return strings.TrimRight(s, "\n")
}

func newSchemaRegisterCmd(o *options) *cobra.Command {
	var file, typ string
	cmd := &cobra.Command{
		Use:   "register <subject>",
		Short: "Register a schema version under a subject",
		Long: `Register the schema in --file under a subject. The type defaults from the file
extension: .avsc is AVRO, .proto is PROTOBUF, .json is JSON (JSON Schema).`,
		Example: `  mqx schema register orders-value --file order.avsc --yes
  mqx schema register orders-value --file order.proto
  mqx schema register orders-value --file order.schema.json --type JSON`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if file == "" {
				return errors.New("--file is required")
			}
			data, err := os.ReadFile(file)
			if err != nil {
				return fmt.Errorf("read schema: %w", err)
			}
			if typ == "" {
				typ = schemaTypeOf(file)
			}
			switch strings.ToUpper(typ) {
			case "AVRO", "PROTOBUF", "JSON":
			default:
				return fmt.Errorf("--type %q: use AVRO, PROTOBUF or JSON", typ)
			}
			return o.withRegistry(cmd, func(ctx context.Context, s *session, sr broker.SchemaRegistry) error {
				if err := s.guard(cmd, "register a schema under subject "+args[0]); err != nil {
					return err
				}
				id, err := sr.RegisterSchema(ctx, args[0], broker.Schema{Type: strings.ToUpper(typ), Schema: string(data)})
				if err != nil {
					return err
				}
				if o.output == "json" {
					return o.render(cmd, map[string]any{"subject": args[0], "id": id}, nil)
				}
				return o.done(cmd, "Registered schema id %d under subject %s.", id, args[0])
			})
		},
	}
	cmd.Flags().StringVar(&file, "file", "", "schema file (required)")
	cmd.Flags().StringVar(&typ, "type", "", "AVRO, PROTOBUF or JSON (default from the file extension, else AVRO)")
	return cmd
}

func schemaTypeOf(file string) string {
	switch strings.ToLower(filepath.Ext(file)) {
	case ".proto":
		return "PROTOBUF"
	case ".json":
		return "JSON"
	}
	return "AVRO"
}

func newSchemaDeleteCmd(o *options) *cobra.Command {
	var permanent bool
	cmd := &cobra.Command{
		Use:   "delete <subject>",
		Short: "Delete a subject (soft by default)",
		Long: `Soft-delete every version of a subject; --permanent also hard-deletes it, which
frees the schema ids and cannot be undone.`,
		Example: "  mqx schema delete orders-value --yes\n  mqx schema delete orders-value --permanent --yes",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.withRegistry(cmd, func(ctx context.Context, s *session, sr broker.SchemaRegistry) error {
				action := "delete subject " + args[0]
				if permanent {
					action = "permanently delete subject " + args[0]
				}
				if err := s.guard(cmd, action); err != nil {
					return err
				}
				versions, err := sr.DeleteSubject(ctx, args[0], permanent)
				if err != nil {
					return err
				}
				if o.output == "json" {
					if versions == nil {
						versions = []int{}
					}
					return o.render(cmd, map[string]any{"subject": args[0], "versions": versions, "permanent": permanent}, nil)
				}
				vs := make([]string, len(versions))
				for i, v := range versions {
					vs[i] = strconv.Itoa(v)
				}
				return o.done(cmd, "Deleted subject %s (versions %s).", args[0], cell(strings.Join(vs, ",")))
			})
		},
	}
	cmd.Flags().BoolVar(&permanent, "permanent", false, "hard-delete the subject (irreversible)")
	return cmd
}
