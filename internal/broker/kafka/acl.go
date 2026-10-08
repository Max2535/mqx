package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/Max2535/mqx/internal/broker"
)

// aclEnums is an ACL's enum fields parsed into their Kafka codes.
type aclEnums struct {
	resource   kmsg.ACLResourceType
	pattern    kmsg.ACLResourcePatternType
	operation  kmsg.ACLOperation
	permission kmsg.ACLPermissionType
}

// parseACLEnums parses the string fields of a; empty fields become "any".
// Names are matched case-insensitively, ignoring '_', '-' and '.'.
func parseACLEnums(a broker.ACL) (aclEnums, error) {
	or := func(s, def string) string {
		if s == "" {
			return def
		}
		return s
	}
	var e aclEnums
	var err error
	if e.resource, err = kmsg.ParseACLResourceType(or(a.ResourceType, "any")); err != nil {
		return e, fmt.Errorf("resource type %q: use topic, group, cluster, transactional_id or delegation_token", a.ResourceType)
	}
	if e.pattern, err = kmsg.ParseACLResourcePatternType(or(a.PatternType, "any")); err != nil {
		return e, fmt.Errorf("pattern type %q: use literal or prefixed (filters also take any or match)", a.PatternType)
	}
	if e.operation, err = kmsg.ParseACLOperation(or(a.Operation, "any")); err != nil {
		return e, fmt.Errorf("operation %q: use read, write, create, delete, alter, describe, cluster_action, "+
			"describe_configs, alter_configs, idempotent_write or all", a.Operation)
	}
	if e.permission, err = kmsg.ParseACLPermissionType(or(a.Permission, "any")); err != nil {
		return e, fmt.Errorf("permission %q: use allow or deny", a.Permission)
	}
	return e, nil
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func lower(s fmt.Stringer) string { return strings.ToLower(s.String()) }

// ACLs implements broker.ACLAdmin.
func (k *Kafka) ACLs(ctx context.Context, filter broker.ACL) ([]broker.ACL, error) {
	e, err := parseACLEnums(filter)
	if err != nil {
		return nil, err
	}
	req := kmsg.NewPtrDescribeACLsRequest()
	req.ResourceType, req.ResourcePatternType = e.resource, e.pattern
	req.Operation, req.PermissionType = e.operation, e.permission
	req.ResourceName, req.Principal, req.Host = optional(filter.ResourceName), optional(filter.Principal), optional(filter.Host)
	resp, err := req.RequestWith(ctx, k.cl)
	if err != nil {
		return nil, wrapErr("describe acls", err)
	}
	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		return nil, aclErr("describe acls", err, deref(resp.ErrorMessage))
	}
	var out []broker.ACL
	for _, r := range resp.Resources {
		for _, a := range r.ACLs {
			out = append(out, broker.ACL{
				Principal: a.Principal, Host: a.Host, ResourceType: lower(r.ResourceType), ResourceName: r.ResourceName,
				PatternType: lower(r.ResourcePatternType), Operation: lower(a.Operation), Permission: lower(a.PermissionType),
			})
		}
	}
	sortACLs(out)
	return out, nil
}

// CreateACL implements broker.ACLAdmin. Host defaults to "*", the pattern to
// literal and a cluster resource's name to kafka-cluster.
func (k *Kafka) CreateACL(ctx context.Context, acl broker.ACL) error {
	if acl.Principal == "" || acl.ResourceType == "" || acl.Operation == "" || acl.Permission == "" {
		return errors.New("an ACL needs a principal (User:name), resource type, operation and permission")
	}
	if acl.Host == "" {
		acl.Host = "*"
	}
	if acl.PatternType == "" {
		acl.PatternType = "literal"
	}
	e, err := parseACLEnums(acl)
	if err != nil {
		return err
	}
	if e.resource == kmsg.ACLResourceTypeCluster && acl.ResourceName == "" {
		acl.ResourceName = "kafka-cluster"
	}
	if acl.ResourceName == "" {
		return errors.New("an ACL needs a resource name")
	}
	if e.resource == kmsg.ACLResourceTypeAny || e.pattern == kmsg.ACLResourcePatternTypeAny ||
		e.pattern == kmsg.ACLResourcePatternTypeMatch || e.operation == kmsg.ACLOperationAny || e.permission == kmsg.ACLPermissionTypeAny {
		return errors.New(`"any" and "match" are only valid in filters; give a concrete resource type, pattern, operation and permission`)
	}
	req := kmsg.NewPtrCreateACLsRequest()
	c := kmsg.NewCreateACLsRequestCreation()
	c.ResourceType, c.ResourceName, c.ResourcePatternType = e.resource, acl.ResourceName, e.pattern
	c.Principal, c.Host, c.Operation, c.PermissionType = acl.Principal, acl.Host, e.operation, e.permission
	req.Creations = append(req.Creations, c)
	resp, err := req.RequestWith(ctx, k.cl)
	if err != nil {
		return wrapErr("create acl", err)
	}
	for _, r := range resp.Results {
		if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
			return aclErr("create acl", err, deref(r.ErrorMessage))
		}
	}
	return nil
}

// DeleteACLs implements broker.ACLAdmin and returns the ACLs it removed.
func (k *Kafka) DeleteACLs(ctx context.Context, filter broker.ACL) ([]broker.ACL, error) {
	e, err := parseACLEnums(filter)
	if err != nil {
		return nil, err
	}
	req := kmsg.NewPtrDeleteACLsRequest()
	f := kmsg.NewDeleteACLsRequestFilter()
	f.ResourceType, f.ResourcePatternType, f.Operation, f.PermissionType = e.resource, e.pattern, e.operation, e.permission
	f.ResourceName, f.Principal, f.Host = optional(filter.ResourceName), optional(filter.Principal), optional(filter.Host)
	req.Filters = append(req.Filters, f)
	resp, err := req.RequestWith(ctx, k.cl)
	if err != nil {
		return nil, wrapErr("delete acls", err)
	}
	var out []broker.ACL
	for _, r := range resp.Results {
		if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
			return nil, aclErr("delete acls", err, deref(r.ErrorMessage))
		}
		for _, m := range r.MatchingACLs {
			if err := kerr.ErrorForCode(m.ErrorCode); err != nil {
				return out, aclErr("delete acls", err, deref(m.ErrorMessage))
			}
			out = append(out, broker.ACL{
				Principal: m.Principal, Host: m.Host, ResourceType: lower(m.ResourceType), ResourceName: m.ResourceName,
				PatternType: lower(m.ResourcePatternType), Operation: lower(m.Operation), Permission: lower(m.PermissionType),
			})
		}
	}
	sortACLs(out)
	return out, nil
}

func aclErr(what string, err error, msg string) error {
	if msg != "" {
		what = fmt.Sprintf("%s (%s)", what, msg)
	}
	return wrapErr(what, err)
}

func sortACLs(acls []broker.ACL) {
	sort.Slice(acls, func(i, j int) bool {
		a, b := acls[i], acls[j]
		if a.ResourceType != b.ResourceType {
			return a.ResourceType < b.ResourceType
		}
		if a.ResourceName != b.ResourceName {
			return a.ResourceName < b.ResourceName
		}
		if a.Principal != b.Principal {
			return a.Principal < b.Principal
		}
		return a.Operation < b.Operation
	})
}
