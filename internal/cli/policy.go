package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Max2535/mqx/internal/broker"
)

// Runtime parameter components and link kinds (see broker.PolicyAdmin).
const (
	componentShovel     = "shovel"
	componentFederation = "federation-upstream"
	linkShovel          = "shovel"
	linkFederation      = "federation"
)

func newPolicyCmd(o *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "policy",
		Short: "Manage RabbitMQ policies in the context's vhost",
	}
	cmd.AddCommand(newPolicyListCmd(o), newPolicySetCmd(o), newPolicyDeleteCmd(o))
	return cmd
}

func newPolicyListCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Short:   "List policies",
		Example: `  mqx policy list -o json`,
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				pa, err := capability[broker.PolicyAdmin](s, broker.CapPolicyAdmin)
				if err != nil {
					return err
				}
				ps, err := pa.Policies(ctx)
				if err != nil {
					return err
				}
				return o.render(cmd, nonNilList(ps), func() *table {
					t := newTable("NAME", "PATTERN", "APPLY-TO", "PRIORITY", "DEFINITION")
					for _, p := range ps {
						t.add(p.Name, p.Pattern, p.ApplyTo, p.Priority, kv(p.Definition))
					}
					return t
				})
			})
		},
	}
}

func newPolicySetCmd(o *options) *cobra.Command {
	var (
		p        broker.Policy
		defs     []string
		defsJSON string
	)
	cmd := &cobra.Command{
		Use:   "set <name>",
		Short: "Create or replace a policy",
		Example: `  mqx policy set ttl --pattern '^orders\.' --apply-to queues --definition message-ttl=60000
  mqx policy set ha --pattern '.*' --priority 1 --definition-json '{"max-length":1000,"overflow":"reject-publish"}'
  mqx policy set ae --pattern '^events$' --apply-to exchanges --definition alternate-exchange=unrouted`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, pos []string) error {
			p.Name = pos[0]
			def, err := policyDefinition(defs, defsJSON)
			if err != nil {
				return err
			}
			p.Definition = def
			if p.Pattern == "" {
				return errors.New("--pattern is required, e.g. '^orders\\.' or '.*'")
			}
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				pa, err := capability[broker.PolicyAdmin](s, broker.CapPolicyAdmin)
				if err != nil {
					return err
				}
				if err := s.guard(cmd, fmt.Sprintf("set policy %q on %s matching %q", p.Name, p.ApplyTo, p.Pattern)); err != nil {
					return err
				}
				if err := pa.PutPolicy(ctx, p); err != nil {
					return err
				}
				return o.done(cmd, "set policy %q", p.Name)
			})
		},
	}
	f := cmd.Flags()
	f.StringVar(&p.Pattern, "pattern", "", "regex of queue/exchange names the policy applies to (required)")
	f.StringVar(&p.ApplyTo, "apply-to", "all", "queues, exchanges, all, classic_queues, quorum_queues or streams")
	f.IntVar(&p.Priority, "priority", 0, "priority; the highest matching policy wins")
	f.StringArrayVar(&defs, "definition", nil, "definition key=value, repeatable (numbers and booleans are typed)")
	f.StringVar(&defsJSON, "definition-json", "", "whole definition as a JSON object")
	return cmd
}

func policyDefinition(defs []string, defsJSON string) (map[string]any, error) {
	switch {
	case len(defs) > 0 && defsJSON != "":
		return nil, errors.New("use either --definition or --definition-json, not both")
	case defsJSON != "":
		var def map[string]any
		if err := json.Unmarshal([]byte(defsJSON), &def); err != nil {
			return nil, fmt.Errorf("--definition-json: want a JSON object: %w", err)
		}
		if len(def) == 0 {
			return nil, errors.New("--definition-json: the definition is empty")
		}
		return def, nil
	case len(defs) > 0:
		return parseArgs("definition", defs)
	}
	return nil, errors.New("a definition is required: --definition key=value (repeatable) or --definition-json")
}

func newPolicyDeleteCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:     "delete <name>",
		Short:   "Delete a policy",
		Example: `  mqx policy delete ttl --yes`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, pos []string) error {
			name := pos[0]
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				pa, err := capability[broker.PolicyAdmin](s, broker.CapPolicyAdmin)
				if err != nil {
					return err
				}
				if err := s.guard(cmd, fmt.Sprintf("delete policy %q", name)); err != nil {
					return err
				}
				if err := pa.DeletePolicy(ctx, name); err != nil {
					return err
				}
				return o.done(cmd, "deleted policy %q", name)
			})
		},
	}
}

// ---------------------------------------------------------------------------
// URIs with credentials

var uriPassword = regexp.MustCompile(`(://[^:/@\s"']*):[^@\s"']*@`)

// redactURI hides the password of every user:password@ URI in s.
func redactURI(s string) string { return uriPassword.ReplaceAllString(s, "$1:xxxxx@") }

// redactParam returns v with URI passwords hidden, recursively.
func redactParam(v any) any {
	switch t := v.(type) {
	case string:
		return redactURI(t)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = redactParam(e)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = redactParam(e)
		}
		return out
	}
	return v
}

func redactParams(ps []broker.Parameter) []broker.Parameter {
	out := make([]broker.Parameter, len(ps))
	for i, p := range ps {
		p.Value, _ = redactParam(p.Value).(map[string]any)
		out[i] = p
	}
	return out
}

// uriFrom returns the URI held by env var envName, or literal when it carries
// no password. Literal URIs with passwords are refused so they never land in
// shell history.
func uriFrom(flag, literal, envName string) (string, error) {
	switch {
	case literal != "" && envName != "":
		return "", fmt.Errorf("use either --%s or --%s-env, not both", flag, flag)
	case envName != "":
		v, ok := os.LookupEnv(envName)
		if !ok || v == "" {
			return "", fmt.Errorf("--%s-env: environment variable %s is not set or empty", flag, envName)
		}
		return v, nil
	case literal != "":
		if redactURI(literal) != literal {
			return "", fmt.Errorf("--%s contains a password; put the URI in an environment variable and pass --%s-env VAR",
				flag, flag)
		}
		return literal, nil
	}
	return "", fmt.Errorf("--%s-env VAR (or --%s for a URI without a password, such as amqp://) is required", flag, flag)
}

// ---------------------------------------------------------------------------
// Shovels

func newShovelCmd(o *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "shovel",
		Short: "Manage dynamic shovels (needs the rabbitmq_shovel_management plugin)",
	}
	cmd.AddCommand(newShovelListCmd(o), newShovelStatusCmd(o), newShovelCreateCmd(o),
		newParamDeleteCmd(o, componentShovel, "shovel"))
	return cmd
}

func listParams(ctx context.Context, s *session, component string) (broker.PolicyAdmin, []broker.Parameter, error) {
	pa, err := capability[broker.PolicyAdmin](s, broker.CapPolicyAdmin)
	if err != nil {
		return nil, nil, err
	}
	ps, err := pa.Parameters(ctx, component)
	if err != nil {
		return nil, nil, err
	}
	return pa, redactParams(nonNilList(ps)), nil
}

func newShovelListCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Short:   "List shovel definitions (passwords in URIs are redacted)",
		Example: `  mqx shovel list -o json`,
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				_, ps, err := listParams(ctx, s, componentShovel)
				if err != nil {
					return err
				}
				return o.render(cmd, ps, func() *table {
					t := newTable("NAME", "SOURCE", "DESTINATION")
					for _, p := range ps {
						t.add(p.Name, shovelEnd(p.Value, "src"), shovelEnd(p.Value, "dest"))
					}
					return t
				})
			})
		},
	}
}

// shovelEnd summarises one side of a shovel: uri plus queue or exchange.
func shovelEnd(v map[string]any, side string) string {
	str := func(k string) string { return cell(v[side+"-"+k]) }
	target := "queue " + str("queue")
	if _, ok := v[side+"-exchange"]; ok {
		target = fmt.Sprintf("exchange %s key %s", str("exchange"), str("exchange-key"))
	}
	uri := v[side+"-uri"]
	if list, ok := uri.([]any); ok && len(list) > 0 {
		uri = list[0]
	}
	return fmt.Sprintf("%s %s", redactURI(cell(uri)), target)
}

func newShovelStatusCmd(o *options) *cobra.Command {
	return newLinkStatusCmd(o, "status", linkShovel, "Show the state of running shovels")
}

func newLinkStatusCmd(o *options, use, kind, short string) *cobra.Command {
	return &cobra.Command{
		Use:     use,
		Short:   short,
		Example: fmt.Sprintf("  mqx %s %s", map[string]string{linkShovel: "shovel", linkFederation: "federation"}[kind], use),
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				pa, err := capability[broker.PolicyAdmin](s, broker.CapPolicyAdmin)
				if err != nil {
					return err
				}
				ls, err := pa.LinkStatus(ctx, kind)
				if err != nil {
					return err
				}
				for i := range ls {
					ls[i].Error = redactURI(ls[i].Error)
					for k, v := range ls[i].Detail {
						ls[i].Detail[k] = redactURI(v)
					}
				}
				ls = nonNilList(ls)
				return o.render(cmd, ls, func() *table {
					t := newTable("NAME", "STATE", "TARGET", "NODE", "ERROR")
					for _, l := range ls {
						target := l.Detail["exchange"]
						if target == "" {
							target = l.Detail["queue"]
						}
						if target == "" {
							target = l.Detail["src_queue"]
						}
						t.add(l.Name, l.State, target, l.Node, l.Error)
					}
					return t
				})
			})
		},
	}
}

type shovelFlags struct {
	srcURI, srcURIEnv, srcQueue, srcExchange, srcKey      string
	destURI, destURIEnv, destQueue, destExchange, destKey string
	ackMode, deleteAfter                                  string
	extra                                                 []string
}

func (sf *shovelFlags) value() (map[string]any, error) {
	src, err := uriFrom("src-uri", sf.srcURI, sf.srcURIEnv)
	if err != nil {
		return nil, err
	}
	dest, err := uriFrom("dest-uri", sf.destURI, sf.destURIEnv)
	if err != nil {
		return nil, err
	}
	v := map[string]any{"src-protocol": "amqp091", "src-uri": src, "dest-protocol": "amqp091", "dest-uri": dest,
		"ack-mode": sf.ackMode, "src-delete-after": sf.deleteAfter}
	switch {
	case (sf.srcQueue == "") == (sf.srcExchange == ""):
		return nil, errors.New("set exactly one of --src-queue or --src-exchange")
	case sf.srcQueue != "":
		v["src-queue"] = sf.srcQueue
	default:
		v["src-exchange"], v["src-exchange-key"] = sf.srcExchange, sf.srcKey
	}
	switch {
	case sf.destQueue != "" && sf.destExchange != "":
		return nil, errors.New("set at most one of --dest-queue or --dest-exchange")
	case sf.destQueue != "":
		v["dest-queue"] = sf.destQueue
	case sf.destExchange != "":
		v["dest-exchange"] = sf.destExchange
		if sf.destKey != "" {
			v["dest-exchange-key"] = sf.destKey
		}
	}
	extra, err := parseArgs("set", sf.extra)
	if err != nil {
		return nil, err
	}
	for k, x := range extra {
		v[k] = x
	}
	return v, nil
}

func newShovelCreateCmd(o *options) *cobra.Command {
	var sf shovelFlags
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create or replace a dynamic shovel",
		Long: `Create or replace a dynamic shovel that moves messages from a source queue or
exchange to a destination. URIs usually carry credentials, so pass them in
environment variables with --src-uri-env / --dest-uri-env; --src-uri and
--dest-uri accept only URIs without a password, such as amqp:// (the local
broker). URIs are never printed back.`,
		Example: `  SRC=amqp://user:pw@old-host DEST=amqp://user:pw@new-host \
    mqx shovel create migrate --src-uri-env SRC --src-queue orders --dest-uri-env DEST --dest-queue orders
  mqx shovel create local-move --src-uri amqp:// --src-queue a --dest-uri amqp:// --dest-queue b`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, pos []string) error {
			value, err := sf.value()
			if err != nil {
				return err
			}
			return putParam(cmd, o, broker.Parameter{Component: componentShovel, Name: pos[0], Value: value}, "shovel")
		},
	}
	f := cmd.Flags()
	f.StringVar(&sf.srcURIEnv, "src-uri-env", "", "environment variable holding the source URI")
	f.StringVar(&sf.srcURI, "src-uri", "", "source URI without a password (e.g. amqp:// for this broker)")
	f.StringVar(&sf.srcQueue, "src-queue", "", "source queue")
	f.StringVar(&sf.srcExchange, "src-exchange", "", "source exchange")
	f.StringVar(&sf.srcKey, "src-exchange-key", "#", "routing key to bind the source exchange with")
	f.StringVar(&sf.destURIEnv, "dest-uri-env", "", "environment variable holding the destination URI")
	f.StringVar(&sf.destURI, "dest-uri", "", "destination URI without a password")
	f.StringVar(&sf.destQueue, "dest-queue", "", "destination queue")
	f.StringVar(&sf.destExchange, "dest-exchange", "", "destination exchange")
	f.StringVar(&sf.destKey, "dest-exchange-key", "", "routing key for the destination exchange (default: the original key)")
	f.StringVar(&sf.ackMode, "ack-mode", "on-confirm", "on-confirm, on-publish or no-ack")
	f.StringVar(&sf.deleteAfter, "delete-after", "never", "never, queue-length (stop when the source is drained) or a count")
	f.StringArrayVar(&sf.extra, "set", nil, "extra shovel key=value, repeatable (e.g. prefetch-count=500)")
	return cmd
}

// putParam guards and stores a runtime parameter without echoing its value.
func putParam(cmd *cobra.Command, o *options, p broker.Parameter, what string) error {
	return o.withSession(cmd, func(ctx context.Context, s *session) error {
		pa, err := capability[broker.PolicyAdmin](s, broker.CapPolicyAdmin)
		if err != nil {
			return err
		}
		if err := s.guard(cmd, fmt.Sprintf("create or replace %s %q", what, p.Name)); err != nil {
			return err
		}
		if err := pa.PutParameter(ctx, p); err != nil {
			return errors.New(redactURI(err.Error())) // the broker may quote a URI back
		}
		return o.done(cmd, "set %s %q", what, p.Name)
	})
}

func newParamDeleteCmd(o *options, component, what string) *cobra.Command {
	use := "delete <name>"
	if component == componentFederation {
		use = "delete-upstream <name>"
	}
	return &cobra.Command{
		Use:     use,
		Short:   "Delete a " + what,
		Example: fmt.Sprintf("  mqx %s %s old --yes", strings.Fields(what)[0], strings.Fields(use)[0]),
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, pos []string) error {
			name := pos[0]
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				pa, err := capability[broker.PolicyAdmin](s, broker.CapPolicyAdmin)
				if err != nil {
					return err
				}
				if err := s.guard(cmd, fmt.Sprintf("delete %s %q", what, name)); err != nil {
					return err
				}
				if err := pa.DeleteParameter(ctx, component, name); err != nil {
					return err
				}
				return o.done(cmd, "deleted %s %q", what, name)
			})
		},
	}
}

// ---------------------------------------------------------------------------
// Federation

func newFederationCmd(o *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "federation",
		Short: "Manage federation upstreams and links (needs the rabbitmq_federation_management plugin)",
		Long: `Manage federation upstreams and inspect links. An upstream only takes effect
for exchanges or queues matched by a policy with federation-upstream-set or
federation-upstream, e.g.:

  mqx policy set fed --pattern '^federated\.' --apply-to exchanges --definition federation-upstream-set=all`,
	}
	cmd.AddCommand(newFederationUpstreamsCmd(o),
		newLinkStatusCmd(o, "links", linkFederation, "Show the state of federation links"),
		newFederationSetUpstreamCmd(o),
		newParamDeleteCmd(o, componentFederation, "federation upstream"))
	return cmd
}

func newFederationUpstreamsCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:     "upstreams",
		Short:   "List federation upstreams (passwords in URIs are redacted)",
		Example: `  mqx federation upstreams -o json`,
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				_, ps, err := listParams(ctx, s, componentFederation)
				if err != nil {
					return err
				}
				return o.render(cmd, ps, func() *table {
					t := newTable("NAME", "URI", "SETTINGS")
					for _, p := range ps {
						rest := make(map[string]any, len(p.Value))
						for k, v := range p.Value {
							if k != "uri" {
								rest[k] = v
							}
						}
						uri := p.Value["uri"]
						if list, ok := uri.([]any); ok {
							parts := make([]string, len(list))
							for i, u := range list {
								parts[i] = cell(u)
							}
							uri = strings.Join(parts, ",")
						}
						t.add(p.Name, redactURI(cell(uri)), kv(rest))
					}
					return t
				})
			})
		},
	}
}

func newFederationSetUpstreamCmd(o *options) *cobra.Command {
	var (
		uri, uriEnv string
		extra       []string
	)
	cmd := &cobra.Command{
		Use:   "set-upstream <name>",
		Short: "Create or replace a federation upstream",
		Long: `Create or replace a federation upstream. The URI usually carries credentials,
so pass it in an environment variable with --uri-env; --uri accepts only a URI
without a password. Other upstream settings go in --set key=value.`,
		Example: `  UP=amqp://fed:pw@upstream-host mqx federation set-upstream dc1 --uri-env UP --set expires=3600000 --set max-hops=1`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, pos []string) error {
			u, err := uriFrom("uri", uri, uriEnv)
			if err != nil {
				return err
			}
			value, err := parseArgs("set", extra)
			if err != nil {
				return err
			}
			value["uri"] = u
			return putParam(cmd, o, broker.Parameter{Component: componentFederation, Name: pos[0], Value: value},
				"federation upstream")
		},
	}
	cmd.Flags().StringVar(&uriEnv, "uri-env", "", "environment variable holding the upstream URI")
	cmd.Flags().StringVar(&uri, "uri", "", "upstream URI without a password")
	cmd.Flags().StringArrayVar(&extra, "set", nil, "upstream setting key=value, repeatable (expires, message-ttl, max-hops, ack-mode, ...)")
	return cmd
}
