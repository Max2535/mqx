package tui

import (
	"context"
	"fmt"

	"github.com/charmbracelet/bubbles/key"

	"github.com/Max2535/mqx/internal/broker"
)

// newACLPanel lists, creates and deletes Kafka ACLs.
func newACLPanel(e *env) panel {
	aa := as[broker.ACLAdmin](e)
	actions := []action{{
		key:      key.NewBinding(key.WithKeys("n"), key.WithHelp("n", "new ACL")),
		mutating: true,
		form: func(*row) (string, []field) {
			return "New ACL", []field{
				{key: "principal", label: "Principal", hint: "User:alice"},
				{key: "host", label: "Host", value: "*"},
				{key: "resource_type", label: "Resource type", hint: "topic, group, cluster, transactional_id", value: "topic"},
				{key: "resource_name", label: "Resource name"},
				{key: "pattern_type", label: "Pattern type", hint: "literal or prefixed", value: "literal"},
				{key: "operation", label: "Operation", hint: "read, write, create, delete, alter, describe, all", value: "read"},
				{key: "permission", label: "Permission", hint: "allow or deny", value: "allow"},
			}
		},
		check: func(_ *row, v values) error {
			for _, k := range []string{"principal", "resource_type", "resource_name", "operation", "permission"} {
				if v[k] == "" {
					return fmt.Errorf("%s is required", k)
				}
			}
			return nil
		},
		describe: func(_ *row, v values) string {
			a := aclFrom(v)
			return fmt.Sprintf("Create ACL %s %s %s on %s %s", a.Permission, a.Principal, a.Operation, a.ResourceType, a.ResourceName)
		},
		run: func(ctx context.Context, _ *row, v values) (string, error) { return "", aa.CreateACL(ctx, aclFrom(v)) },
	}, {
		key:      key.NewBinding(key.WithKeys("d"), key.WithHelp("d", "delete ACL")),
		mutating: true,
		needsRow: true,
		describe: func(r *row, _ values) string {
			a := r.data.(broker.ACL)
			return fmt.Sprintf("Delete ACL %s %s %s on %s %s", a.Permission, a.Principal, a.Operation, a.ResourceType, a.ResourceName)
		},
		run: func(ctx context.Context, r *row, _ values) (string, error) {
			deleted, err := aa.DeleteACLs(ctx, r.data.(broker.ACL))
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("deleted %d ACLs", len(deleted)), nil
		},
	}}
	return newStack(newListView(e, resource{
		title: "ACLs",
		cols:  []string{"PRINCIPAL", "HOST", "RESOURCE", "NAME", "PATTERN", "OPERATION", "PERMISSION"},
		load: func(ctx context.Context) (listing, error) {
			acls, err := aa.ACLs(ctx, broker.ACL{})
			if err != nil {
				return listing{}, err
			}
			rows := make([]row, len(acls))
			for i, a := range acls {
				rows[i] = row{key: fmt.Sprint(a), data: a, cells: []string{a.Principal, a.Host, a.ResourceType, a.ResourceName,
					a.PatternType, a.Operation, a.Permission}}
			}
			return listing{rows: rows}, nil
		},
		actions: actions,
	}))
}

func aclFrom(v values) broker.ACL {
	return broker.ACL{Principal: v["principal"], Host: v["host"], ResourceType: v["resource_type"],
		ResourceName: v["resource_name"], PatternType: v["pattern_type"], Operation: v["operation"], Permission: v["permission"]}
}
