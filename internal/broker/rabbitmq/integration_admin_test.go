//go:build integration

package rabbitmq_test

import (
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/broker/rabbitmq"
)

func TestUsersVHostsPermissions(t *testing.T) {
	e := newEnv(t)
	ctx := testCtx(t)
	user := "mqx-it-" + strings.ToLower(fmt.Sprint(time.Now().UnixNano()))
	t.Cleanup(func() { _ = e.b.DeleteUser(ctx, user) })
	must(t, e.b.PutUser(ctx, broker.User{Name: user, Tags: []string{"monitoring"}}, "first-pw"))
	must(t, e.b.SetPermission(ctx, broker.Permission{User: user, Configure: "", Write: "^w", Read: ".*"}))

	us, err := e.b.Users(ctx)
	must(t, err)
	i := slices.IndexFunc(us, func(u broker.User) bool { return u.Name == user })
	if i < 0 || !slices.Equal(us[i].Tags, []string{"monitoring"}) {
		t.Fatalf("users = %+v", us)
	}
	// Changing tags without a password keeps the password.
	must(t, e.b.PutUser(ctx, broker.User{Name: user, Tags: []string{"management", "monitoring"}}, ""))
	u := url.URL{Scheme: "amqp", User: url.UserPassword(user, "first-pw"), Host: srv.AMQPHost, Path: "/" + e.vhost,
		RawPath: "/" + url.PathEscape(e.vhost)}
	conn, err := amqp.Dial(u.String())
	if err != nil {
		t.Fatalf("login after tag change: %v", err)
	}
	conn.Close()

	ps, err := e.b.Permissions(ctx)
	must(t, err)
	if !slices.Contains(ps, broker.Permission{User: user, VHost: e.vhost, Configure: "", Write: "^w", Read: ".*"}) {
		t.Errorf("permissions = %+v", ps)
	}
	must(t, e.b.ClearPermission(ctx, user, e.vhost))
	ps, err = e.b.Permissions(ctx)
	must(t, err)
	if slices.ContainsFunc(ps, func(p broker.Permission) bool { return p.User == user }) {
		t.Error("permission not cleared")
	}
	must(t, e.b.DeleteUser(ctx, user))

	extra := e.vhost + "-extra"
	must(t, e.b.PutVHost(ctx, broker.VHost{Name: extra, Description: "d"}))
	vs, err := e.b.VHosts(ctx)
	must(t, err)
	if !slices.ContainsFunc(vs, func(v broker.VHost) bool { return v.Name == extra && v.Description == "d" }) {
		t.Errorf("vhosts = %+v", vs)
	}
	must(t, e.b.DeleteVHost(ctx, extra))
}

func TestPolicies(t *testing.T) {
	e := newEnv(t)
	ctx := testCtx(t)
	p := broker.Policy{Name: "limits", Pattern: "^lim\\.", ApplyTo: "queues", Priority: 3,
		Definition: map[string]any{"max-length": 10, "overflow": "reject-publish"}}
	must(t, e.b.PutPolicy(ctx, p))
	ps, err := e.b.Policies(ctx)
	must(t, err)
	if len(ps) != 1 || ps[0].Name != "limits" || ps[0].Pattern != p.Pattern || ps[0].ApplyTo != "queues" ||
		ps[0].Priority != 3 || ps[0].VHost != e.vhost || fmt.Sprint(ps[0].Definition["max-length"]) != "10" {
		t.Fatalf("policies = %+v", ps)
	}
	must(t, e.b.DeletePolicy(ctx, "limits"))
	ps, err = e.b.Policies(ctx)
	must(t, err)
	if len(ps) != 0 {
		t.Errorf("policies after delete = %+v", ps)
	}
}

// localURI points a shovel or upstream at this broker from inside the container,
// with credentials that must never be echoed back.
func localURI(vhost string) string {
	u := url.URL{Scheme: "amqp", User: url.UserPassword(srv.Username, srv.Password), Host: "localhost",
		Path: "/" + vhost, RawPath: "/" + url.PathEscape(vhost)}
	return u.String()
}

func noSecret(t *testing.T, what string, v any) {
	t.Helper()
	data, _ := json.Marshal(v)
	if strings.Contains(string(data), srv.Password) {
		t.Errorf("%s leaks the password: %s", what, data)
	}
}

func TestShovel(t *testing.T) {
	e := newEnv(t)
	ctx := testCtx(t)
	durableQueue(t, e, "src", nil)
	durableQueue(t, e, "dst", nil)
	must(t, e.b.PutParameter(ctx, broker.Parameter{Component: rabbitmq.ComponentShovel, Name: "move", Value: map[string]any{
		"src-protocol": "amqp091", "src-uri": localURI(e.vhost), "src-queue": "src",
		"dest-protocol": "amqp091", "dest-uri": localURI(e.vhost), "dest-queue": "dst",
	}}))
	params, err := e.b.Parameters(ctx, rabbitmq.ComponentShovel)
	must(t, err)
	if len(params) != 1 || params[0].Name != "move" || params[0].Value["src-queue"] != "src" {
		t.Fatalf("parameters = %+v", params)
	}
	noSecret(t, "shovel parameters", params)
	eventually(t, 30*time.Second, func() error {
		ls, err := e.b.LinkStatus(ctx, rabbitmq.LinkShovel)
		if err != nil {
			return err
		}
		noSecret(t, "shovel status", ls)
		if len(ls) != 1 || ls[0].Name != "move" || ls[0].State != "running" {
			return fmt.Errorf("shovel status = %+v", ls)
		}
		return nil
	})
	must(t, e.b.Publish(ctx, "src", broker.NewMessage([]byte("shovelled"))))
	eventually(t, 15*time.Second, func() error {
		if got := values(e.peek(t, "dst", broker.PeekOptions{})); !slices.Equal(got, []string{"shovelled"}) {
			return fmt.Errorf("dst = %v", got)
		}
		return nil
	})
	must(t, e.b.DeleteParameter(ctx, rabbitmq.ComponentShovel, "move"))
	params, err = e.b.Parameters(ctx, rabbitmq.ComponentShovel)
	must(t, err)
	if len(params) != 0 {
		t.Errorf("parameters after delete = %+v", params)
	}
}

func TestFederation(t *testing.T) {
	e := newEnv(t)
	ctx := testCtx(t)
	must(t, e.b.PutParameter(ctx, broker.Parameter{Component: rabbitmq.ComponentFederation, Name: "up",
		Value: map[string]any{"uri": localURI(e.vhost), "expires": 3600000}}))
	params, err := e.b.Parameters(ctx, rabbitmq.ComponentFederation)
	must(t, err)
	if len(params) != 1 || params[0].Name != "up" {
		t.Fatalf("upstreams = %+v", params)
	}
	noSecret(t, "federation upstreams", params)
	must(t, e.b.PutPolicy(ctx, broker.Policy{Name: "fed", Pattern: "^fed\\.", ApplyTo: "exchanges",
		Definition: map[string]any{"federation-upstream-set": "all"}}))
	must(t, e.b.DeclareExchange(ctx, broker.Exchange{Name: "fed.x", Type: "topic", Durable: true}))
	eventually(t, 30*time.Second, func() error {
		ls, err := e.b.LinkStatus(ctx, rabbitmq.LinkFederation)
		if err != nil {
			return err
		}
		noSecret(t, "federation links", ls)
		if len(ls) != 1 || ls[0].Name != "up" || ls[0].State == "" || ls[0].Detail["exchange"] != "fed.x" {
			return fmt.Errorf("links = %+v", ls)
		}
		return nil
	})
	must(t, e.b.DeleteParameter(ctx, rabbitmq.ComponentFederation, "up"))
}

func TestMetrics(t *testing.T) {
	e := newEnv(t)
	ctx := testCtx(t)
	durableQueue(t, e, "m", nil)
	for range 5 {
		must(t, e.b.Publish(ctx, "m", broker.NewMessage([]byte("x"))))
	}
	eventually(t, 15*time.Second, func() error {
		s, err := e.b.Sample(ctx, "m")
		if err != nil {
			return err
		}
		if s.Counters[broker.MetricMessagesIn] != 5 || s.Gauges[broker.MetricDepth] != 5 || s.Time.IsZero() {
			return fmt.Errorf("sample = %+v", s)
		}
		return nil
	})
	s, err := e.b.Sample(ctx, "")
	must(t, err)
	if _, ok := s.Gauges[broker.MetricConnections]; !ok || s.Counters[broker.MetricMessagesIn] < 5 {
		t.Errorf("overview sample = %+v", s)
	}
}
