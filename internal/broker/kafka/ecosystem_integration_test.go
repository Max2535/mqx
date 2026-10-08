//go:build integration

package kafka

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/config"
)

// startService starts an ecosystem container and returns its URL.
func startService(t *testing.T, start func(context.Context) (string, error)) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	url, err := start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return url
}

const avroUser = `{"type":"record","name":"User","namespace":"it","fields":[
 {"name":"name","type":"string"},{"name":"age","type":"int"},{"name":"email","type":["null","string"],"default":null}]}`

const protoUser = `syntax = "proto3";
package it;
message User { string name = 1; int32 age = 2; repeated string roles = 3; }
`

func TestSchemaRegistry(t *testing.T) {
	t.Parallel()
	url := startService(t, cluster.StartSchemaRegistry)
	k := openKafka(t, func(c *config.Context) { c.SchemaRegistry = &config.Endpoint{URL: url} })
	ctx := ctxT(t)
	if !broker.Has(k, broker.CapSchemaRegistry) || broker.Has(k, broker.CapConnectManager) {
		t.Errorf("capabilities = %v", broker.Capabilities(k))
	}
	tests := []struct {
		name, typ, schema string
		in, want          string
	}{
		{name: "avro", typ: "AVRO", schema: avroUser, in: `{"name":"ann","age":41,"email":{"string":"a@x"}}`,
			want: `{"name":"ann","age":41,"email":"a@x"}`},
		{name: "protobuf", typ: "PROTOBUF", schema: protoUser, in: `{"name":"bob","age":7,"roles":["admin"]}`},
		{name: "json", typ: "JSON", schema: `{"type":"object"}`, in: `{"free":"form"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic := createTopic(t, k, 1)
			subject := topic + "-value"
			id, err := k.RegisterSchema(ctx, subject, broker.Schema{Type: tt.typ, Schema: tt.schema})
			if err != nil {
				t.Fatal(err)
			}
			s, err := k.Schema(ctx, subject, -1)
			if err != nil || s.ID != id || s.Version != 1 || s.Type != tt.typ {
				t.Fatalf("Schema() = %+v, %v", s, err)
			}
			if err := k.Publish(ctx, topic, broker.Message{Partition: broker.AnyPartition, Value: []byte(tt.in), Schema: &broker.SchemaRef{}}); err != nil {
				t.Fatal(err)
			}
			if err := k.Publish(ctx, topic, broker.NewMessage([]byte("plain"))); err != nil {
				t.Fatal(err)
			}
			ch, err := k.Peek(ctx, topic, broker.PeekOptions{Decode: true})
			if err != nil {
				t.Fatal(err)
			}
			msgs := collect(t, ch)
			if len(msgs) != 2 {
				t.Fatalf("got %d messages", len(msgs))
			}
			want := tt.want
			if want == "" {
				want = tt.in
			}
			assertJSONEqual(t, msgs[0].Value, want)
			if ref := msgs[0].Schema; ref == nil || ref.ID != id || ref.Format != tt.typ || ref.Subject != subject {
				t.Errorf("schema ref = %+v", ref)
			}
			if string(msgs[1].Value) != "plain" || msgs[1].Schema != nil {
				t.Errorf("plain message = %+v", msgs[1])
			}
			// Without Decode the raw wire format comes back.
			ch, _ = k.Peek(ctx, topic, broker.PeekOptions{Limit: 1})
			if raw := collect(t, ch); len(raw) != 1 || raw[0].Value[0] != 0 {
				t.Errorf("raw = %+v", raw)
			}
		})
	}

	subjects, err := k.Subjects(ctx)
	if err != nil || len(subjects) < 3 {
		t.Fatalf("Subjects() = %v, %v", subjects, err)
	}
	subject := subjects[0]
	if versions, err := k.SchemaVersions(ctx, subject); err != nil || !slices.Equal(versions, []int{1}) {
		t.Errorf("SchemaVersions() = %v, %v", versions, err)
	}
	if level, err := k.SubjectCompatibility(ctx, subject); err != nil || level != "BACKWARD" {
		t.Errorf("SubjectCompatibility() = %q, %v", level, err)
	}
	if _, err := k.Schema(ctx, "no-such-subject", -1); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("missing subject: err = %v, want ErrNotFound", err)
	}
	if _, err := k.RegisterSchema(ctx, "bad", broker.Schema{Type: "AVRO", Schema: `{"type":"nope"}`}); err == nil {
		t.Error("registering an invalid schema succeeded")
	}
	if versions, err := k.DeleteSubject(ctx, subject, false); err != nil || !slices.Equal(versions, []int{1}) {
		t.Errorf("soft delete = %v, %v", versions, err)
	}
	if _, err := k.DeleteSubject(ctx, subject, true); err != nil {
		t.Errorf("permanent delete after soft delete: %v", err)
	}
	if _, err := k.DeleteSubject(ctx, subjects[1], true); err != nil {
		t.Errorf("permanent delete without soft delete: %v", err)
	}
}

func TestKafkaConnect(t *testing.T) {
	t.Parallel()
	url := startService(t, cluster.StartConnect)
	k := openKafka(t, func(c *config.Context) { c.Connect = &config.Endpoint{URL: url} })
	ctx := ctxT(t)
	plugins, err := k.ConnectorPlugins(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const class = "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector"
	if !slices.ContainsFunc(plugins, func(p broker.ConnectorPlugin) bool { return p.Class == class }) {
		t.Fatalf("plugins = %+v, want %s", plugins, class)
	}
	cfg := map[string]string{
		"connector.class": class, "tasks.max": "1",
		"source.cluster.alias": "a", "target.cluster.alias": "b",
		"source.cluster.bootstrap.servers": cluster.Internal, "target.cluster.bootstrap.servers": cluster.Internal,
		"heartbeats.topic.replication.factor": "1",
	}
	if err := k.PutConnector(ctx, "hb", cfg); err != nil {
		t.Fatal(err)
	}
	waitConnector(t, k, "hb", "RUNNING")
	list, err := k.Connectors(ctx)
	if err != nil || len(list) != 1 || list[0].Name != "hb" || list[0].Type != "source" || list[0].Config["connector.class"] != class {
		t.Errorf("Connectors() = %+v, %v", list, err)
	}
	if err := k.ConnectorAction(ctx, "hb", broker.ConnectorPause); err != nil {
		t.Fatal(err)
	}
	waitConnector(t, k, "hb", "PAUSED")
	if err := k.ConnectorAction(ctx, "hb", broker.ConnectorResume); err != nil {
		t.Fatal(err)
	}
	waitConnector(t, k, "hb", "RUNNING")
	if err := k.ConnectorAction(ctx, "hb", broker.ConnectorRestart); err != nil {
		t.Fatal(err)
	}
	cfg["tasks.max"] = "2"
	if err := k.PutConnector(ctx, "hb", cfg); err != nil {
		t.Fatal(err)
	}
	if c, err := k.Connector(ctx, "hb"); err != nil || c.Config["tasks.max"] != "2" {
		t.Errorf("updated connector = %+v, %v", c, err)
	}
	if err := k.PutConnector(ctx, "bad", map[string]string{"connector.class": "no.such.Class"}); err == nil ||
		!strings.Contains(err.Error(), "HTTP") {
		t.Errorf("invalid connector: err = %v", err)
	}
	if err := k.DeleteConnector(ctx, "hb"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.Connector(ctx, "hb"); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("deleted connector: err = %v, want ErrNotFound", err)
	}
}

func waitConnector(t *testing.T, k *Kafka, name, state string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var c *broker.Connector
	var err error
	for time.Now().Before(deadline) {
		if c, err = k.Connector(ctxT(t), name); err == nil && c.State == state {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("connector %s never reached %s: %+v, %v", name, state, c, err)
}

func TestKSQLDB(t *testing.T) {
	t.Parallel()
	url := startService(t, cluster.StartKSQLDB)
	k := openKafka(t, func(c *config.Context) { c.KSQLDB = &config.Endpoint{URL: url} })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	topic := unique("ksql")
	stream := strings.ToUpper(strings.ReplaceAll(topic, "-", "_"))
	res, err := k.RunKSQL(ctx, "CREATE STREAM "+stream+" (id INT, name STRING) WITH (kafka_topic='"+topic+"', value_format='JSON', partitions=1)")
	if err != nil || res.Message == "" {
		t.Fatalf("create stream = %+v, %v", res, err)
	}
	for _, row := range []string{"(1, 'a')", "(2, 'b')"} {
		if _, err := k.RunKSQL(ctx, "INSERT INTO "+stream+" (id, name) VALUES "+row); err != nil {
			t.Fatal(err)
		}
	}
	res, err = k.RunKSQL(ctx, "SHOW STREAMS")
	if err != nil {
		t.Fatal(err)
	}
	ni := slices.Index(res.Columns, "name")
	if ni < 0 || !slices.ContainsFunc(res.Rows, func(r []any) bool { return r[ni] == stream }) {
		t.Errorf("SHOW STREAMS = %+v", res)
	}
	res, err = k.RunKSQL(ctx, "DESCRIBE "+stream)
	if err != nil || !slices.Equal(res.Columns, []string{"field", "type"}) || len(res.Rows) != 2 {
		t.Errorf("DESCRIBE = %+v, %v", res, err)
	}
	res, err = k.RunKSQL(ctx, "SELECT id, name FROM "+stream+" EMIT CHANGES LIMIT 2;")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Columns, []string{"ID", "NAME"}) || len(res.Rows) != 2 || res.Rows[1][1] != "b" {
		t.Errorf("SELECT = %+v", res)
	}
	if _, err := k.RunKSQL(ctx, "SHOW NOTHING"); err == nil {
		t.Error("invalid statement succeeded")
	}
	if _, err := k.RunKSQL(ctx, "DROP STREAM "+stream); err != nil {
		t.Error(err)
	}
}
