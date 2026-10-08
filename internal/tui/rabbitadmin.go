package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/charmbracelet/bubbles/key"

	"github.com/Max2535/mqx/internal/broker"
)

func newKey(k, help string) key.Binding {
	return key.NewBinding(key.WithKeys(k), key.WithHelp(k, help))
}

// deleteAction deletes the selected row's object after confirmation.
func deleteAction(what string, del func(ctx context.Context, r *row) error) action {
	return action{
		key:      newKey("d", "delete "+what),
		mutating: true,
		needsRow: true,
		describe: func(r *row, _ values) string { return fmt.Sprintf("Delete %s %s", what, r.key) },
		run:      func(ctx context.Context, r *row, _ values) (string, error) { return "", del(ctx, r) },
	}
}

// newUsersPanel manages RabbitMQ users, vhosts and permissions ('v' switches).
func newUsersPanel(e *env) panel {
	ua := as[broker.UserAdmin](e)
	users := resource{
		title: "Users",
		cols:  []string{"NAME", "TAGS"},
		load: func(ctx context.Context) (listing, error) {
			us, err := ua.Users(ctx)
			if err != nil {
				return listing{}, err
			}
			rows := make([]row, len(us))
			for i, u := range us {
				rows[i] = row{key: u.Name, data: u, cells: []string{u.Name, strings.Join(u.Tags, ",")}}
			}
			return listing{rows: rows}, nil
		},
		actions: []action{{
			key:      newKey("n", "new user"),
			mutating: true,
			form: func(*row) (string, []field) {
				return "New user", []field{
					{key: "name", label: "Name"},
					{key: "tags", label: "Tags", hint: "administrator, monitoring, management"},
					{key: "password", label: "Password", kind: fieldSecret},
				}
			},
			check: func(_ *row, v values) error {
				if v["name"] == "" {
					return fmt.Errorf("name is required")
				}
				return nil
			},
			describe: func(_ *row, v values) string { return "Create or update user " + v["name"] },
			run: func(ctx context.Context, _ *row, v values) (string, error) {
				return "", ua.PutUser(ctx, broker.User{Name: v["name"], Tags: splitList(v["tags"])}, v["password"])
			},
		}, deleteAction("user", func(ctx context.Context, r *row) error { return ua.DeleteUser(ctx, r.key) })},
	}
	vhosts := resource{
		title: "VHosts",
		cols:  []string{"NAME", "MESSAGES", "DESCRIPTION"},
		load: func(ctx context.Context) (listing, error) {
			if !e.has(broker.CapTopologyInspector) {
				return listing{header: st.muted.Render("Listing vhosts needs the topology capability.")}, nil
			}
			vs, err := as[broker.TopologyInspector](e).VHosts(ctx)
			if err != nil {
				return listing{}, err
			}
			rows := make([]row, len(vs))
			for i, v := range vs {
				rows[i] = row{key: v.Name, data: v, cells: []string{v.Name, count(v.Messages), v.Description}}
			}
			return listing{rows: rows}, nil
		},
		actions: []action{{
			key:      newKey("n", "new vhost"),
			mutating: true,
			form: func(*row) (string, []field) {
				return "New vhost", []field{{key: "name", label: "Name"}, {key: "description", label: "Description"}}
			},
			check:    requireName,
			describe: func(_ *row, v values) string { return "Create vhost " + v["name"] },
			run: func(ctx context.Context, _ *row, v values) (string, error) {
				return "", ua.PutVHost(ctx, broker.VHost{Name: v["name"], Description: v["description"]})
			},
		}, deleteAction("vhost", func(ctx context.Context, r *row) error { return ua.DeleteVHost(ctx, r.key) })},
	}
	perms := resource{
		title: "Permissions",
		cols:  []string{"USER", "VHOST", "CONFIGURE", "WRITE", "READ"},
		load: func(ctx context.Context) (listing, error) {
			ps, err := ua.Permissions(ctx)
			if err != nil {
				return listing{}, err
			}
			rows := make([]row, len(ps))
			for i, p := range ps {
				rows[i] = row{key: p.User + "@" + p.VHost, data: p, cells: []string{p.User, p.VHost, p.Configure, p.Write, p.Read}}
			}
			return listing{rows: rows}, nil
		},
		actions: []action{{
			key:      newKey("n", "set permission"),
			mutating: true,
			form: func(*row) (string, []field) {
				return "Set permission", []field{
					{key: "user", label: "User"}, {key: "vhost", label: "VHost", value: "/"},
					{key: "configure", label: "Configure", value: ".*"}, {key: "write", label: "Write", value: ".*"},
					{key: "read", label: "Read", value: ".*"},
				}
			},
			check: func(_ *row, v values) error {
				if v["user"] == "" || v["vhost"] == "" {
					return fmt.Errorf("user and vhost are required")
				}
				return nil
			},
			describe: func(_ *row, v values) string {
				return fmt.Sprintf("Set permissions of %s on vhost %s", v["user"], v["vhost"])
			},
			run: func(ctx context.Context, _ *row, v values) (string, error) {
				return "", ua.SetPermission(ctx, broker.Permission{User: v["user"], VHost: v["vhost"],
					Configure: v["configure"], Write: v["write"], Read: v["read"]})
			},
		}, {
			key:      newKey("d", "clear permission"),
			mutating: true,
			needsRow: true,
			describe: func(r *row, _ values) string {
				p := r.data.(broker.Permission)
				return fmt.Sprintf("Clear permissions of %s on vhost %s", p.User, p.VHost)
			},
			run: func(ctx context.Context, r *row, _ values) (string, error) {
				p := r.data.(broker.Permission)
				return "", ua.ClearPermission(ctx, p.User, p.VHost)
			},
		}},
	}
	return newStack(newListView(e, users, vhosts, perms))
}

// newPoliciesPanel manages policies, shovels and federation upstreams, and
// shows link status ('v' switches).
func newPoliciesPanel(e *env) panel {
	pa := as[broker.PolicyAdmin](e)
	policies := resource{
		title: "Policies",
		cols:  []string{"NAME", "VHOST", "PATTERN", "APPLY TO", "PRIORITY", "DEFINITION"},
		load: func(ctx context.Context) (listing, error) {
			ps, err := pa.Policies(ctx)
			if err != nil {
				return listing{}, err
			}
			rows := make([]row, len(ps))
			for i, p := range ps {
				rows[i] = row{key: p.Name, data: p, cells: []string{p.Name, p.VHost, p.Pattern, p.ApplyTo, itoa(p.Priority), jsonString(p.Definition)}}
			}
			return listing{rows: rows}, nil
		},
		actions: []action{{
			key:      newKey("n", "new policy"),
			mutating: true,
			form: func(*row) (string, []field) {
				return "New policy", []field{
					{key: "name", label: "Name"}, {key: "vhost", label: "VHost", value: "/"},
					{key: "pattern", label: "Pattern", value: ".*"},
					{key: "apply_to", label: "Apply to", hint: "queues, exchanges, all, quorum_queues, …", value: "queues"},
					{key: "priority", label: "Priority", value: "0"},
					{key: "definition", label: "Definition (JSON)", kind: fieldArea, value: "{}"},
				}
			},
			check:    func(_ *row, v values) error { _, err := policySpec(v); return err },
			describe: func(_ *row, v values) string { return "Create or update policy " + v["name"] },
			run: func(ctx context.Context, _ *row, v values) (string, error) {
				p, err := policySpec(v)
				if err != nil {
					return "", err
				}
				return "", pa.PutPolicy(ctx, p)
			},
		}, deleteAction("policy", func(ctx context.Context, r *row) error { return pa.DeletePolicy(ctx, r.key) })},
	}
	param := func(title, component, statusKind string) resource {
		return resource{
			title: title,
			cols:  []string{"NAME", "VHOST", "STATE", "VALUE"},
			load: func(ctx context.Context) (listing, error) {
				ps, err := pa.Parameters(ctx, component)
				if err != nil {
					return listing{}, err
				}
				states := map[string]string{}
				if links, err := pa.LinkStatus(ctx, statusKind); err == nil {
					for _, l := range links {
						states[l.Name] = l.State
						if l.Error != "" {
							states[l.Name] += ": " + l.Error
						}
					}
				}
				rows := make([]row, len(ps))
				for i, p := range ps {
					rows[i] = row{key: p.Name, data: p, cells: []string{p.Name, p.VHost, states[p.Name], jsonString(p.Value)}}
				}
				return listing{rows: rows}, nil
			},
			actions: []action{{
				key:      newKey("n", "new "+component),
				mutating: true,
				form: func(*row) (string, []field) {
					return "New " + component, []field{
						{key: "name", label: "Name"}, {key: "vhost", label: "VHost", value: "/"},
						{key: "value", label: "Value (JSON)", kind: fieldArea, value: "{}"},
					}
				},
				check:    func(_ *row, v values) error { _, err := paramSpec(component, v); return err },
				describe: func(_ *row, v values) string { return fmt.Sprintf("Create or update %s %s", component, v["name"]) },
				run: func(ctx context.Context, _ *row, v values) (string, error) {
					p, err := paramSpec(component, v)
					if err != nil {
						return "", err
					}
					return "", pa.PutParameter(ctx, p)
				},
			}, deleteAction(component, func(ctx context.Context, r *row) error {
				return pa.DeleteParameter(ctx, component, r.key)
			})},
		}
	}
	links := resource{
		title: "Federation links",
		cols:  []string{"NAME", "VHOST", "STATE", "NODE", "ERROR"},
		load: func(ctx context.Context) (listing, error) {
			ls, err := pa.LinkStatus(ctx, "federation")
			if err != nil {
				return listing{}, err
			}
			rows := make([]row, len(ls))
			for i, l := range ls {
				rows[i] = row{key: l.Name, data: l, cells: []string{l.Name, l.VHost, l.State, l.Node, l.Error}}
			}
			return listing{rows: rows}, nil
		},
	}
	return newStack(newListView(e, policies, param("Shovels", "shovel", "shovel"),
		param("Federation upstreams", "federation-upstream", "federation"), links))
}

func requireName(_ *row, v values) error {
	if v["name"] == "" {
		return fmt.Errorf("name is required")
	}
	return nil
}

func policySpec(v values) (broker.Policy, error) {
	p := broker.Policy{Name: v["name"], VHost: v["vhost"], Pattern: v["pattern"], ApplyTo: v["apply_to"]}
	if p.Name == "" || p.Pattern == "" {
		return p, fmt.Errorf("name and pattern are required")
	}
	n, err := strconv.Atoi(strings.TrimSpace(v["priority"]))
	if err != nil {
		return p, fmt.Errorf("invalid priority %q", v["priority"])
	}
	p.Priority = n
	if err := json.Unmarshal([]byte(v["definition"]), &p.Definition); err != nil {
		return p, fmt.Errorf("definition: %w", err)
	}
	return p, nil
}

func paramSpec(component string, v values) (broker.Parameter, error) {
	p := broker.Parameter{Component: component, Name: v["name"], VHost: v["vhost"]}
	if p.Name == "" {
		return p, fmt.Errorf("name is required")
	}
	if err := json.Unmarshal([]byte(v["value"]), &p.Value); err != nil {
		return p, fmt.Errorf("value: %w", err)
	}
	return p, nil
}

// jsonString renders v compactly. Parameter values may contain URIs with
// credentials, so userinfo passwords are masked.
func jsonString(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return redactURIs(string(b))
}

func splitList(s string) []string {
	var out []string
	for _, f := range strings.Split(s, ",") {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	return out
}
