package kafka

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

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/config"
)

// recorder is an httptest server that answers by "METHOD path" and records requests.
type recorder struct {
	mu       sync.Mutex
	requests []string
	bodies   []string
	auth     []string
}

func newServer(t *testing.T, routes map[string]func(w http.ResponseWriter)) (*httptest.Server, *recorder) {
	t.Helper()
	rec := &recorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		key := r.Method + " " + r.URL.RequestURI()
		user, pass, _ := r.BasicAuth()
		rec.mu.Lock()
		rec.requests = append(rec.requests, key)
		rec.bodies = append(rec.bodies, string(body))
		rec.auth = append(rec.auth, user+":"+pass)
		rec.mu.Unlock()
		if h, ok := routes[key]; ok {
			h(w)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"error_code":404,"message":"Connector nope not found"}`)
	}))
	t.Cleanup(srv.Close)
	return srv, rec
}

func reply(status int, body string) func(http.ResponseWriter) {
	return func(w http.ResponseWriter) {
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

const expandedConnectors = `{
 "sink-b": {"status": {"name":"sink-b","type":"sink","connector":{"state":"PAUSED","worker_id":"w2:8083"},"tasks":[]},
            "info": {"name":"sink-b","type":"sink","config":{"connector.class":"S"}}},
 "src-a": {"status": {"name":"src-a","type":"source","connector":{"state":"RUNNING","worker_id":"w1:8083"},
            "tasks":[{"id":1,"state":"FAILED","worker_id":"w1:8083","trace":"boom"},{"id":0,"state":"RUNNING","worker_id":"w1:8083"}]},
           "info": {"name":"src-a","type":"source","config":{"connector.class":"A","tasks.max":"2"}}}
}`

func TestConnectors(t *testing.T) {
	srv, rec := newServer(t, map[string]func(http.ResponseWriter){
		"GET /connectors?expand=status&expand=info": reply(200, expandedConnectors),
		"GET /connectors/src-a":                     reply(200, `{"name":"src-a","type":"source","config":{"connector.class":"A"}}`),
		"GET /connectors/src-a/status":              reply(200, `{"name":"src-a","connector":{"state":"RUNNING","worker_id":"w1"},"tasks":[{"id":0,"state":"RUNNING","worker_id":"w1"}]}`),
		"GET /connector-plugins":                    reply(200, `[{"class":"z.Sink","type":"sink","version":"1"},{"class":"a.Source","type":"source","version":"2"}]`),
	})
	k := &Kafka{connect: newRESTClient("kafka connect", srv.URL+"/", config.Credentials{Username: "u", Password: "p"})}
	ctx := context.Background()

	list, err := k.Connectors(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []broker.Connector{
		{Name: "sink-b", Type: "sink", State: "PAUSED", Worker: "w2:8083", Config: map[string]string{"connector.class": "S"}},
		{Name: "src-a", Type: "source", State: "RUNNING", Worker: "w1:8083", Config: map[string]string{"connector.class": "A", "tasks.max": "2"},
			Tasks: []broker.ConnectorTask{{ID: 0, State: "RUNNING", Worker: "w1:8083"}, {ID: 1, State: "FAILED", Worker: "w1:8083", Trace: "boom"}}},
	}
	if !reflect.DeepEqual(list, want) {
		t.Errorf("Connectors() =\n%+v\nwant\n%+v", list, want)
	}
	c, err := k.Connector(ctx, "src-a")
	if err != nil || c.State != "RUNNING" || c.Type != "source" || len(c.Tasks) != 1 {
		t.Errorf("Connector() = %+v, %v", c, err)
	}
	plugins, err := k.ConnectorPlugins(ctx)
	if err != nil || len(plugins) != 2 || plugins[0].Class != "a.Source" {
		t.Errorf("ConnectorPlugins() = %+v, %v", plugins, err)
	}
	if _, err := k.Connector(ctx, "nope"); !errors.Is(err, broker.ErrNotFound) || !strings.Contains(err.Error(), "Connector nope not found") {
		t.Errorf("Connector(nope) error = %v, want ErrNotFound with the server message", err)
	}
	for _, a := range rec.auth {
		if a != "u:p" {
			t.Errorf("basic auth = %q, want u:p", a)
		}
	}
}

func TestConnectorMutations(t *testing.T) {
	srv, rec := newServer(t, map[string]func(http.ResponseWriter){
		"PUT /connectors/my%20conn/config":             reply(201, `{"name":"my conn"}`),
		"DELETE /connectors/c":                         reply(204, ""),
		"PUT /connectors/c/pause":                      reply(202, ""),
		"PUT /connectors/c/resume":                     reply(202, ""),
		"POST /connectors/c/restart?includeTasks=true": reply(202, `{}`),
		"PUT /connectors/bad/config":                   reply(400, `{"error_code":400,"message":"Connector configuration is invalid"}`),
	})
	k := &Kafka{connect: newRESTClient("kafka connect", srv.URL, config.Credentials{})}
	ctx := context.Background()
	steps := []struct {
		name string
		do   func() error
		want string
	}{
		{name: "put", do: func() error { return k.PutConnector(ctx, "my conn", map[string]string{"connector.class": "X"}) }},
		{name: "put without class", do: func() error { return k.PutConnector(ctx, "x", map[string]string{}) }, want: "needs connector.class"},
		{name: "put invalid", do: func() error { return k.PutConnector(ctx, "bad", map[string]string{"connector.class": "X"}) },
			want: "HTTP 400: Connector configuration is invalid"},
		{name: "delete", do: func() error { return k.DeleteConnector(ctx, "c") }},
		{name: "pause", do: func() error { return k.ConnectorAction(ctx, "c", broker.ConnectorPause) }},
		{name: "resume", do: func() error { return k.ConnectorAction(ctx, "c", broker.ConnectorResume) }},
		{name: "restart", do: func() error { return k.ConnectorAction(ctx, "c", broker.ConnectorRestart) }},
		{name: "unknown action", do: func() error { return k.ConnectorAction(ctx, "c", "explode") }, want: "use pause, resume or restart"},
		{name: "missing", do: func() error { return k.DeleteConnector(ctx, "gone") }, want: "mqx connect list"},
	}
	for _, s := range steps {
		err := s.do()
		if s.want == "" && err != nil {
			t.Errorf("%s: %v", s.name, err)
		}
		if s.want != "" && (err == nil || !strings.Contains(err.Error(), s.want)) {
			t.Errorf("%s: err = %v, want containing %q", s.name, err, s.want)
		}
	}
	if rec.bodies[0] != `{"connector.class":"X"}` {
		t.Errorf("put body = %s", rec.bodies[0])
	}
}

func TestConnectNotConfigured(t *testing.T) {
	k := &Kafka{}
	if _, err := k.Connectors(context.Background()); !errors.Is(err, broker.ErrUnsupported) {
		t.Errorf("err = %v, want ErrUnsupported", err)
	}
	if _, err := k.RunKSQL(context.Background(), "SHOW STREAMS"); !errors.Is(err, broker.ErrUnsupported) {
		t.Errorf("err = %v, want ErrUnsupported", err)
	}
	if k.HasCapability(broker.CapSchemaRegistry) || k.HasCapability(broker.CapConnectManager) || k.HasCapability(broker.CapKSQLRunner) {
		t.Error("ecosystem capabilities reported without endpoints")
	}
	if !k.HasCapability(broker.CapLagReporter) {
		t.Error("core capability hidden")
	}
}

func TestRunKSQL(t *testing.T) {
	tests := []struct {
		name     string
		stmt     string
		path     string
		response string
		status   int
		want     broker.KSQLResult
		wantErr  string
	}{
		{
			name: "show streams is a table", stmt: "SHOW STREAMS", path: "/ksql",
			response: `[{"@type":"streams","statementText":"SHOW STREAMS;","streams":[` +
				`{"type":"STREAM","name":"ORDERS","topic":"orders","valueFormat":"JSON"},` +
				`{"type":"STREAM","name":"KSQL_PROCESSING_LOG","topic":"log","valueFormat":"JSON"}]}]`,
			want: broker.KSQLResult{Columns: []string{"name", "topic", "type", "valueFormat"}, Rows: [][]any{
				{"ORDERS", "orders", "STREAM", "JSON"}, {"KSQL_PROCESSING_LOG", "log", "STREAM", "JSON"}}},
		},
		{
			name: "create returns the command message", stmt: "CREATE STREAM s (id INT) WITH (kafka_topic='s', value_format='JSON', partitions=1);",
			path:     "/ksql",
			response: `[{"@type":"currentStatus","commandStatus":{"status":"SUCCESS","message":"Stream created"}}]`,
			want:     broker.KSQLResult{Message: "Stream created"},
		},
		{
			name: "describe lists fields", stmt: "DESCRIBE ORDERS", path: "/ksql",
			response: `[{"@type":"sourceDescription","sourceDescription":{"name":"ORDERS","fields":[` +
				`{"name":"ID","schema":{"type":"INTEGER"}},{"name":"NAME","schema":{"type":"STRING"}}]}}]`,
			want: broker.KSQLResult{Columns: []string{"field", "type"}, Rows: [][]any{{"ID", "INTEGER"}, {"NAME", "STRING"}}},
		},
		{
			name: "select streams rows", stmt: "select * from orders emit changes limit 2;", path: "/query-stream",
			response: `{"queryId":"q1","columnNames":["ID","NAME"],"columnTypes":["INTEGER","STRING"]}` + "\n" +
				`[1,"a"]` + "\n" + `[2,"b"]` + "\n",
			want: broker.KSQLResult{Columns: []string{"ID", "NAME"}, Rows: [][]any{{1.0, "a"}, {2.0, "b"}}},
		},
		{name: "print points at peek", stmt: "print 'orders';", path: "/ksql", wantErr: "use `mqx peek <topic>` instead"},
		{
			name: "statement error carries the server message", stmt: "SHOW NOPE", path: "/ksql", status: 400,
			response: `{"@type":"statement_error","error_code":40001,"message":"line 1:6: Syntax Error"}`,
			wantErr:  "HTTP 400: line 1:6: Syntax Error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := tt.status
			if status == 0 {
				status = 200
			}
			srv, rec2 := newServer(t, map[string]func(http.ResponseWriter){"POST " + tt.path: reply(status, tt.response)})
			k := &Kafka{ksql: newRESTClient("ksqldb", srv.URL, config.Credentials{})}
			got, err := k.RunKSQL(context.Background(), tt.stmt)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(*got, tt.want) {
				t.Errorf("RunKSQL() = %+v, want %+v", *got, tt.want)
			}
			var body map[string]any
			_ = json.Unmarshal([]byte(rec2.bodies[0]), &body)
			text, _ := body["ksql"].(string)
			if text == "" {
				text, _ = body["sql"].(string)
			}
			if !strings.HasSuffix(text, ";") {
				t.Errorf("statement sent without terminating semicolon: %q", text)
			}
		})
	}
}
