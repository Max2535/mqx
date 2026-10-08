package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/config"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name string
		ctx  config.Context
		want string
	}{
		{name: "ok", ctx: config.Context{URL: "amqp://localhost/"}},
		{name: "amqps with mgmt", ctx: config.Context{URL: "amqps://h/v", ManagementURL: "https://h:15671"}},
		{name: "missing url", ctx: config.Context{}, want: "needs url"},
		{name: "bad scheme", ctx: config.Context{URL: "http://h/"}, want: "use amqp:// or amqps://"},
		{name: "bad mgmt scheme", ctx: config.Context{URL: "amqp://h/", ManagementURL: "ftp://h"}, want: "management_url scheme"},
		{name: "tls needs amqps", ctx: config.Context{URL: "amqp://h/", TLS: &config.TLS{Enabled: true}}, want: "amqps"},
		{name: "bad scan option", ctx: config.Context{URL: "amqp://h/", Options: map[string]string{OptPeekMaxScan: "0"}},
			want: OptPeekMaxScan},
		{name: "bad idle option", ctx: config.Context{URL: "amqp://h/", Options: map[string]string{OptStreamIdle: "x"}},
			want: OptStreamIdle},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.Join(validate(tt.ctx)...)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestNewSettings(t *testing.T) {
	tests := []struct {
		name               string
		ctx                config.Context
		creds              config.Credentials
		vhost, user, pass  string
		mgmt, host, scheme string
	}{
		{
			name: "defaults", ctx: config.Context{URL: "amqp://rabbit"},
			vhost: "/", user: "guest", pass: "guest", mgmt: "http://rabbit:15672", host: "rabbit:5672", scheme: "amqp",
		},
		{
			name: "vhost and creds", ctx: config.Context{URL: "amqp://u@rabbit:5673/prod"},
			creds: config.Credentials{Password: "pw"},
			vhost: "prod", user: "u", pass: "pw", mgmt: "http://rabbit:15672", host: "rabbit:5673", scheme: "amqp",
		},
		{
			name: "encoded slash vhost", ctx: config.Context{URL: "amqp://rabbit/%2F"},
			creds: config.Credentials{Username: "a", Password: "b"},
			vhost: "/", user: "a", pass: "b", mgmt: "http://rabbit:15672", host: "rabbit:5672", scheme: "amqp",
		},
		{
			name: "amqps", ctx: config.Context{URL: "amqps://rabbit/v", ManagementURL: "https://m.example/rabbit/"},
			vhost: "v", user: "guest", pass: "guest", mgmt: "https://m.example/rabbit", host: "rabbit:5671", scheme: "amqps",
		},
		{
			name: "amqps default mgmt", ctx: config.Context{URL: "amqps://rabbit/"},
			vhost: "/", user: "guest", pass: "guest", mgmt: "https://rabbit:15671", host: "rabbit:5671", scheme: "amqps",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := newSettings(tt.ctx, tt.creds)
			if err != nil {
				t.Fatal(err)
			}
			got := []string{s.vhost, s.username, s.password, s.mgmtURL.String(), s.host, s.scheme}
			want := []string{tt.vhost, tt.user, tt.pass, tt.mgmt, tt.host, tt.scheme}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("got %q, want %q", got, want)
			}
		})
	}
}

func TestRedactURIs(t *testing.T) {
	tests := map[string]string{
		"amqp://user:s3cret@host/v":            "amqp://user:xxxxx@host/v",
		`uri "amqps://a:b@h:5671" is invalid`:  `uri "amqps://a:xxxxx@h:5671" is invalid`,
		"amqp://user@host":                     "amqp://user@host",
		"amqp://host":                          "amqp://host",
		"x amqp://a:1@h y amqp://b:2@i z":      "x amqp://a:xxxxx@h y amqp://b:xxxxx@i z",
		"amqp://:onlypass@host":                "amqp://:xxxxx@host",
		"no uri here: just a colon:and an @at": "no uri here: just a colon:and an @at",
	}
	for in, want := range tests {
		if got := redactURIs(in); got != want {
			t.Errorf("redactURIs(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTagList(t *testing.T) {
	for in, want := range map[string][]string{
		`["administrator","monitoring"]`: {"administrator", "monitoring"},
		`"administrator, monitoring"`:    {"administrator", "monitoring"},
		`""`:                             nil,
		`[]`:                             {},
	} {
		var tl tagList
		if err := json.Unmarshal([]byte(in), &tl); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual([]string(tl), want) {
			t.Errorf("%s: got %#v, want %#v", in, tl, want)
		}
	}
}

func TestPublishing(t *testing.T) {
	ts := time.Date(2026, 10, 8, 9, 0, 0, 0, time.UTC)
	msg := broker.Message{
		Key: []byte("id-1"), Value: []byte("v"),
		Headers: []broker.Header{{Key: "h", Value: []byte("x")}},
		Properties: map[string]string{
			"content_type": "application/json", "priority": "5", "delivery_mode": "transient",
			"timestamp": ts.Format(time.RFC3339), "correlation_id": "c", "expiration": "1000",
		},
	}
	p, err := publishing(msg)
	if err != nil {
		t.Fatal(err)
	}
	if p.MessageId != "id-1" || p.ContentType != "application/json" || p.Priority != 5 ||
		p.DeliveryMode != amqp.Transient || !p.Timestamp.Equal(ts) || p.CorrelationId != "c" ||
		p.Expiration != "1000" || p.Headers["h"] != "x" {
		t.Errorf("publishing = %+v", p)
	}
	p, err = publishing(broker.Message{Key: []byte("k"), Properties: map[string]string{"message_id": "explicit"}})
	if err != nil || p.MessageId != "explicit" || p.DeliveryMode != amqp.Persistent || p.Timestamp.IsZero() {
		t.Errorf("explicit message_id / defaults: %+v, %v", p, err)
	}
	for _, bad := range []map[string]string{
		{"priority": "300"}, {"delivery_mode": "3"}, {"expiration": "soon"}, {"timestamp": "yesterday"}, {"colour": "red"},
	} {
		if _, err := publishing(broker.Message{Properties: bad}); err == nil {
			t.Errorf("publishing(%v): want error", bad)
		}
	}
}

func TestFromDelivery(t *testing.T) {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	d := amqp.Delivery{
		MessageId: "m1", Body: []byte("body"), Timestamp: ts, Exchange: "ex", RoutingKey: "rk", Redelivered: true,
		ContentType: "text/plain", DeliveryMode: 2, Priority: 3,
		Headers: amqp.Table{"s": "str", "n": int32(7), "b": []byte("raw"), "t": amqp.Table{"x": "y"}},
	}
	m := fromDelivery(d, "q", 4)
	if string(m.Key) != "m1" || m.Offset != 4 || m.Exchange != "ex" || m.RoutingKey != "rk" || !m.Redelivered ||
		!m.Timestamp.Equal(ts) {
		t.Errorf("message = %+v", m)
	}
	wantHeaders := []broker.Header{
		{Key: "b", Value: []byte("raw")}, {Key: "n", Value: []byte("7")}, {Key: "s", Value: []byte("str")},
		{Key: "t", Value: []byte(`{"x":"y"}`)},
	}
	if !reflect.DeepEqual(m.Headers, wantHeaders) {
		t.Errorf("headers = %q", m.Headers)
	}
	wantProps := map[string]string{
		"content_type": "text/plain", "delivery_mode": "2", "priority": "3", "message_id": "m1",
		"timestamp": "2026-01-02T03:04:05Z",
	}
	if !reflect.DeepEqual(m.Properties, wantProps) {
		t.Errorf("properties = %v", m.Properties)
	}
}

// fakeAPI is a scripted management API recording requests.
type fakeAPI struct {
	mu       sync.Mutex
	routes   map[string]func(w http.ResponseWriter, r *http.Request) // "METHOD escaped-path"
	requests []string
	bodies   []string
	headers  []http.Header
}

func newFakeAPI(t *testing.T) (*fakeAPI, *RabbitMQ) {
	t.Helper()
	f := &fakeAPI{routes: map[string]func(http.ResponseWriter, *http.Request){}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		key := r.Method + " " + r.URL.EscapedPath()
		body, _ := io.ReadAll(r.Body)
		f.requests = append(f.requests, key)
		f.bodies = append(f.bodies, string(body))
		f.headers = append(f.headers, r.Header.Clone())
		h := f.routes[key]
		f.mu.Unlock()
		if u, p, _ := r.BasicAuth(); u != "user" || p != "pw" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if h == nil {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"error":"Object Not Found","reason":"Not Found"}`)
			return
		}
		h(w, r)
	}))
	t.Cleanup(srv.Close)
	b, err := open(context.Background(), config.Context{URL: "amqp://localhost/", ManagementURL: srv.URL},
		config.Credentials{Username: "user", Password: "pw"})
	if err != nil {
		t.Fatal(err)
	}
	return f, b.(*RabbitMQ)
}

func (f *fakeAPI) json(key, body string) {
	f.routes[key] = func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}
}

func (f *fakeAPI) status(key string, code int, body string) {
	f.routes[key] = func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(code)
		_, _ = io.WriteString(w, body)
	}
}

func TestListTopicsEscapesVHost(t *testing.T) {
	f, r := newFakeAPI(t)
	f.json("GET /api/queues/%2F", `[
		{"name":"b","type":"quorum","state":"running","durable":true,"messages":3,"consumers":1,"node":"rabbit@n1"},
		{"name":"a","arguments":{"x-queue-type":"stream"},"messages_ready":2,"messages_unacknowledged":1},
		{"name":"c"}
	]`)
	ts, err := r.ListTopics(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(ts) != 3 || ts[0].Name != "a" || ts[0].Messages != 3 || ts[0].Details["type"] != "stream" ||
		ts[1].Messages != 3 || ts[1].Consumers != 1 || ts[1].Details["type"] != "quorum" ||
		ts[2].Messages != -1 || ts[2].Consumers != -1 || ts[2].Details["type"] != "classic" {
		t.Errorf("topics = %+v", ts)
	}
}

func TestMgmtErrors(t *testing.T) {
	f, r := newFakeAPI(t)
	ctx := context.Background()
	if _, err := r.DescribeTopic(ctx, "missing"); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("404 = %v, want ErrNotFound", err)
	}
	f.status("PUT /api/queues/%2F/q", http.StatusBadRequest,
		`{"error":"bad_request","reason":"inequivalent arg 'durable'"}`)
	err := r.CreateTopic(ctx, broker.TopicSpec{Name: "q"})
	if err == nil || !strings.Contains(err.Error(), "400") || !strings.Contains(err.Error(), "inequivalent arg") {
		t.Errorf("400 = %v", err)
	}
	r.mgmt.password = "wrong"
	err = r.DeleteTopic(ctx, "q")
	if err == nil || !strings.Contains(err.Error(), "username_env/password_env") {
		t.Errorf("401 = %v", err)
	}
	r.mgmt.password = "pw"
	f.status("PUT /api/parameters/shovel/%2F/s", http.StatusBadRequest,
		`{"error":"bad_request","reason":"invalid uri amqp://u:secret@h"}`)
	err = r.PutParameter(ctx, broker.Parameter{Component: ComponentShovel, Name: "s"})
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Errorf("reason not redacted: %v", err)
	}
	err = r.PutParameter(ctx, broker.Parameter{Component: ComponentFederation, Name: "missing-plugin"})
	if err == nil || !strings.Contains(err.Error(), "rabbitmq_federation") {
		t.Errorf("plugin hint missing: %v", err)
	}
	if _, err := r.LinkStatus(ctx, LinkShovel); err == nil || !strings.Contains(err.Error(), "rabbitmq_shovel_management") {
		t.Errorf("shovel 404 hint missing: %v", err)
	}
	if err := r.AlterTopicConfig(ctx, "q", nil); !errors.Is(err, broker.ErrUnsupported) {
		t.Errorf("AlterTopicConfig = %v", err)
	}
	if _, err := r.Purge(ctx, "q", broker.PurgeOptions{Partitions: []int32{0}}); !errors.Is(err, broker.ErrUnsupported) {
		t.Errorf("Purge with options = %v", err)
	}
}

func TestUnbindLooksUpPropertiesKey(t *testing.T) {
	f, r := newFakeAPI(t)
	ctx := context.Background()
	path := "/api/bindings/%2F/e/ex%2Fone/q/my%20q"
	f.json("GET "+path, `[
		{"source":"ex/one","destination":"my q","destination_type":"queue","routing_key":"a.#","properties_key":"a.%23"},
		{"source":"ex/one","destination":"my q","destination_type":"queue","routing_key":"k","arguments":{"x":1},"properties_key":"k~abc"},
		{"source":"ex/one","destination":"my q","destination_type":"queue","routing_key":"k","arguments":{"x":2},"properties_key":"k~def"}
	]`)
	f.status("DELETE "+path+"/a.%2523", http.StatusNoContent, "")
	f.status("DELETE "+path+"/k~def", http.StatusNoContent, "")
	if err := r.Unbind(ctx, broker.Binding{Source: "ex/one", Destination: "my q", RoutingKey: "a.#"}); err != nil {
		t.Fatal(err)
	}
	err := r.Unbind(ctx, broker.Binding{Source: "ex/one", Destination: "my q", RoutingKey: "k"})
	if err == nil || !strings.Contains(err.Error(), "differ only in arguments") {
		t.Errorf("ambiguous unbind = %v", err)
	}
	if err := r.Unbind(ctx, broker.Binding{Source: "ex/one", Destination: "my q", RoutingKey: "k",
		Arguments: map[string]any{"x": int64(2)}}); err != nil {
		t.Fatal(err)
	}
	if err := r.Unbind(ctx, broker.Binding{Source: "ex/one", Destination: "my q", RoutingKey: "zz"}); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("missing binding = %v", err)
	}
	if err := r.Bind(ctx, broker.Binding{Source: "", Destination: "q"}); err == nil {
		t.Error("binding from the default exchange must fail")
	}
}

func TestTerminateConsumerSendsReason(t *testing.T) {
	f, r := newFakeAPI(t)
	name := "127.0.0.1:5000 -> 127.0.0.1:5672"
	esc := "/api/connections/127.0.0.1:5000%20-%3E%20127.0.0.1:5672"
	f.json("GET "+esc, `{"name":"x","vhost":"/"}`)
	f.status("DELETE "+esc, http.StatusNoContent, "")
	if err := r.TerminateConsumer(context.Background(), broker.ConsumerTarget{Connection: name, Reason: "bye"}); err != nil {
		t.Fatal(err)
	}
	last := len(f.headers) - 1
	if f.requests[last] != "DELETE "+esc || f.headers[last].Get("X-Reason") != "bye" {
		t.Errorf("request %q with X-Reason %q", f.requests[last], f.headers[last].Get("X-Reason"))
	}
	f.json("GET "+esc, `{"name":"x","vhost":"other"}`)
	if err := r.TerminateConsumer(context.Background(), broker.ConsumerTarget{Connection: name}); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("other vhost = %v", err)
	}
	if err := r.TerminateConsumer(context.Background(), broker.ConsumerTarget{Group: "g"}); err == nil {
		t.Error("want error without a connection")
	}
}

func TestLinkStatusAndParametersRedact(t *testing.T) {
	f, r := newFakeAPI(t)
	f.json("GET /api/shovels/%2F", `[{"name":"s1","vhost":"/","state":"terminated","reason":"conn refused amqp://u:pw1@h",
		"src_uri":"amqp://u:pw2@src","node":"rabbit@n"}]`)
	f.json("GET /api/federation-links/%2F", `[{"upstream":"up","exchange":"x","status":"running","uri":"amqp://u:pw3@up"}]`)
	f.json("GET /api/parameters/shovel/%2F", `[{"name":"s1","component":"shovel","vhost":"/",
		"value":{"src-uri":"amqp://u:pw4@src","dest-uri":["amqp://u:pw5@d"],"src-queue":"q"}}]`)
	ctx := context.Background()
	sh, err := r.LinkStatus(ctx, LinkShovel)
	if err != nil {
		t.Fatal(err)
	}
	fed, err := r.LinkStatus(ctx, LinkFederation)
	if err != nil {
		t.Fatal(err)
	}
	ps, err := r.Parameters(ctx, ComponentShovel)
	if err != nil {
		t.Fatal(err)
	}
	all, _ := json.Marshal([]any{sh, fed, ps})
	if strings.Contains(string(all), "pw") {
		t.Errorf("secrets leaked: %s", all)
	}
	if sh[0].Name != "s1" || sh[0].State != "terminated" || sh[0].Error == "" || fed[0].Name != "up" ||
		fed[0].State != "running" || fed[0].Detail["exchange"] != "x" {
		t.Errorf("links = %+v %+v", sh, fed)
	}
	if _, err := r.LinkStatus(ctx, "bogus"); err == nil {
		t.Error("want error for unknown kind")
	}
}

func TestPutUserKeepsPasswordWhenEmpty(t *testing.T) {
	f, r := newFakeAPI(t)
	f.status("PUT /api/users/alice", http.StatusNoContent, "")
	ctx := context.Background()
	if err := r.PutUser(ctx, broker.User{Name: "alice", Tags: []string{"a", "b"}}, ""); err != nil {
		t.Fatal(err)
	}
	if err := r.PutUser(ctx, broker.User{Name: "alice"}, "pw!"); err != nil {
		t.Fatal(err)
	}
	if f.bodies[0] != `{"tags":"a,b"}` || f.bodies[1] != `{"password":"pw!","tags":""}` {
		t.Errorf("bodies = %q", f.bodies)
	}
}

func TestSample(t *testing.T) {
	f, r := newFakeAPI(t)
	f.json("GET /api/queues/%2F/q", `{"messages":5,"messages_ready":4,"messages_unacknowledged":1,"consumers":2,
		"message_stats":{"publish":10,"deliver_get":6,"ack":5,"redeliver":1}}`)
	f.json("GET /api/overview", `{"message_stats":{"publish":100},"queue_totals":{"messages":7},
		"object_totals":{"connections":3,"consumers":4}}`)
	s, err := r.Sample(context.Background(), "q")
	if err != nil {
		t.Fatal(err)
	}
	if s.Counters[broker.MetricMessagesIn] != 10 || s.Counters[broker.MetricMessagesOut] != 6 ||
		s.Gauges[broker.MetricDepth] != 5 || s.Gauges[broker.MetricConsumers] != 2 || s.Gauges[MetricUnacked] != 1 {
		t.Errorf("queue sample = %+v", s)
	}
	s, err = r.Sample(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if s.Counters[broker.MetricMessagesIn] != 100 || s.Gauges[broker.MetricConnections] != 3 || s.Gauges[broker.MetricDepth] != 7 {
		t.Errorf("overview sample = %+v", s)
	}
}
