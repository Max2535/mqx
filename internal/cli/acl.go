package cli

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/Max2535/mqx/internal/broker"
)

func newACLCmd(o *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "acl",
		Aliases: []string{"acls"},
		Short:   "List, create and delete Kafka ACLs",
		Long: `List, create and delete Kafka ACLs. Values are case-insensitive: resource types
topic, group, cluster, transactional_id, delegation_token; patterns literal,
prefixed (filters also any, match); operations read, write, create, delete,
alter, describe, cluster_action, describe_configs, alter_configs,
idempotent_write, all; permissions allow, deny. Empty filter fields match any.`,
	}
	cmd.AddCommand(newACLListCmd(o), newACLCreateCmd(o), newACLDeleteCmd(o))
	return cmd
}

// addACLFlags binds the ACL fields to flags.
func addACLFlags(f *pflag.FlagSet, a *broker.ACL, filter bool) {
	suffix := ""
	if filter {
		suffix = " (empty = any)"
	}
	f.StringVar(&a.Principal, "principal", "", "principal, e.g. User:alice"+suffix)
	f.StringVar(&a.Host, "host", "", "client host; * for every host"+suffix)
	f.StringVar(&a.ResourceType, "resource-type", "", "topic, group, cluster, transactional_id or delegation_token"+suffix)
	f.StringVar(&a.ResourceName, "resource-name", "", "resource name or prefix"+suffix)
	f.StringVar(&a.PatternType, "pattern", "", "literal or prefixed"+suffix)
	f.StringVar(&a.Operation, "operation", "", "read, write, create, delete, alter, describe, all, ..."+suffix)
	f.StringVar(&a.Permission, "permission", "", "allow or deny"+suffix)
}

func aclTable(acls []broker.ACL) func() *table {
	return func() *table {
		t := newTable("PRINCIPAL", "HOST", "RESOURCE", "NAME", "PATTERN", "OPERATION", "PERMISSION")
		for _, a := range acls {
			t.add(a.Principal, a.Host, a.ResourceType, a.ResourceName, a.PatternType, a.Operation, a.Permission)
		}
		return t
	}
}

// withACLAdmin runs fn with the session's ACLAdmin capability.
func (o *options) withACLAdmin(cmd *cobra.Command, fn func(ctx context.Context, s *session, aa broker.ACLAdmin) error) error {
	return o.withSession(cmd, func(ctx context.Context, s *session) error {
		aa, err := capability[broker.ACLAdmin](s, broker.CapACLAdmin)
		if err != nil {
			return err
		}
		return fn(ctx, s, aa)
	})
}

func newACLListCmd(o *options) *cobra.Command {
	var filter broker.ACL
	cmd := &cobra.Command{
		Use:     "list",
		Short:   "List ACLs matching a filter",
		Example: "  mqx acl list\n  mqx acl list --principal User:alice --resource-type topic",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.withACLAdmin(cmd, func(ctx context.Context, _ *session, aa broker.ACLAdmin) error {
				acls, err := aa.ACLs(ctx, filter)
				if err != nil {
					return err
				}
				if acls == nil {
					acls = []broker.ACL{}
				}
				return o.render(cmd, acls, aclTable(acls))
			})
		},
	}
	addACLFlags(cmd.Flags(), &filter, true)
	return cmd
}

func newACLCreateCmd(o *options) *cobra.Command {
	var acl broker.ACL
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create an ACL",
		Example: `  mqx acl create --principal User:alice --resource-type topic --resource-name orders --operation read --permission allow --yes
  mqx acl create --principal User:billing --resource-type group --resource-name billing- --pattern prefixed --operation read --permission allow`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if acl.Principal == "" || acl.ResourceType == "" || acl.Operation == "" || acl.Permission == "" {
				return errors.New("--principal, --resource-type, --operation and --permission are required")
			}
			return o.withACLAdmin(cmd, func(ctx context.Context, s *session, aa broker.ACLAdmin) error {
				action := fmt.Sprintf("%s %s %s on %s %s", acl.Permission, acl.Principal, acl.Operation, acl.ResourceType, acl.ResourceName)
				if err := s.guard(cmd, "create ACL: "+action); err != nil {
					return err
				}
				if err := aa.CreateACL(ctx, acl); err != nil {
					return err
				}
				return o.done(cmd, "Created ACL: %s.", action)
			})
		},
	}
	addACLFlags(cmd.Flags(), &acl, false)
	return cmd
}

func newACLDeleteCmd(o *options) *cobra.Command {
	var filter broker.ACL
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete every ACL matching a filter",
		Long: `Delete every ACL matching the filter flags. At least one filter flag is
required; preview the matches with ` + "`mqx acl list`" + ` and the same flags.`,
		Example: "  mqx acl delete --principal User:alice --yes\n  mqx acl delete --resource-type topic --resource-name orders --operation write",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if filter == (broker.ACL{}) {
				return errors.New("refusing to delete every ACL; give at least one filter flag (see `mqx acl list`)")
			}
			return o.withACLAdmin(cmd, func(ctx context.Context, s *session, aa broker.ACLAdmin) error {
				if err := s.guard(cmd, "delete the ACLs matching the filter"); err != nil {
					return err
				}
				deleted, err := aa.DeleteACLs(ctx, filter)
				if err != nil {
					return err
				}
				if deleted == nil {
					deleted = []broker.ACL{}
				}
				if o.output != "json" {
					fmt.Fprintf(cmd.ErrOrStderr(), "Deleted %d ACLs.\n", len(deleted))
					if len(deleted) == 0 {
						return nil
					}
				}
				return o.render(cmd, deleted, aclTable(deleted))
			})
		},
	}
	addACLFlags(cmd.Flags(), &filter, true)
	return cmd
}
