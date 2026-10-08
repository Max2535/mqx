package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/Max2535/mqx/internal/broker"
)

// memLookup is an in-memory schemaLookup.
type memLookup struct{ schemas []regSchema }

func (m *memLookup) byID(_ context.Context, id int) (regSchema, error) {
	for _, s := range m.schemas {
		if s.ID == id {
			return s, nil
		}
	}
	return regSchema{}, fmt.Errorf("schema id %d: %w", id, broker.ErrNotFound)
}

func (m *memLookup) bySubject(_ context.Context, subject string, version int) (regSchema, error) {
	var found *regSchema
	for i, s := range m.schemas {
		if s.Subject == subject && (version < 0 || s.Version == version) {
			found = &m.schemas[i]
		}
	}
	if found == nil {
		return regSchema{}, fmt.Errorf("subject %q: %w", subject, broker.ErrNotFound)
	}
	return *found, nil
}

const avroOrder = `{
  "type": "record", "name": "Order", "namespace": "shop",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "qty", "type": "int"},
    {"name": "total", "type": "double"},
    {"name": "ts", "type": "long"},
    {"name": "paid", "type": "boolean"},
    {"name": "note", "type": ["null", "string"], "default": null},
    {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["NEW", "DONE"]}},
    {"name": "tags", "type": {"type": "array", "items": "string"}},
    {"name": "attrs", "type": {"type": "map", "values": "long"}},
    {"name": "customer", "type": "shop.Customer"},
    {"name": "code", "type": {"type": "fixed", "name": "Code", "size": 2}}
  ]
}`

const avroCustomer = `{"type": "record", "name": "Customer", "namespace": "shop",
  "fields": [{"name": "name", "type": "string"}, {"name": "vip", "type": ["null", "boolean"], "default": null}]}`

const protoCommon = `syntax = "proto3";
package shop;
message Money { string currency = 1; int64 cents = 2; }
`

const protoOrder = `syntax = "proto3";
package shop;
import "common.proto";
import "google/protobuf/timestamp.proto";
message Order {
  string id = 1;
  int32 qty = 2;
  Money total = 3;
  repeated string tags = 4;
  google.protobuf.Timestamp at = 5;
  message Line { string sku = 1; }
}
`

func testLookup() *memLookup {
	return &memLookup{schemas: []regSchema{
		{ID: 1, Subject: "customer", Version: 1, Type: "AVRO", Text: avroCustomer},
		{ID: 2, Subject: "orders-value", Version: 1, Type: "AVRO", Text: avroOrder,
			Refs: []sr.SchemaReference{{Name: "shop.Customer", Subject: "customer", Version: 1}}},
		{ID: 3, Subject: "common", Version: 1, Type: "PROTOBUF", Text: protoCommon},
		{ID: 4, Subject: "porders-value", Version: 1, Type: "PROTOBUF", Text: protoOrder,
			Refs: []sr.SchemaReference{{Name: "common.proto", Subject: "common", Version: 1}}},
		{ID: 5, Subject: "jorders-value", Version: 1, Type: "JSON", Text: `{"type":"object"}`},
	}}
}

func TestSerdeRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		ref    broker.SchemaRef
		in     string
		want   string // JSON after decode; empty = same as in
		wantID int
		format string
	}{
		{
			name: "avro with reference, union, enum, array, map, fixed",
			ref:  broker.SchemaRef{Subject: "orders-value"},
			in: `{"id":"o-1","qty":3,"total":9.5,"ts":1700000000000,"paid":true,"note":{"string":"gift"},` +
				`"status":"NEW","tags":["a","b"],"attrs":{"x":1},"customer":{"name":"ann","vip":{"boolean":true}},"code":"AB"}`,
			// Decoded unions are bare values.
			want: `{"id":"o-1","qty":3,"total":9.5,"ts":1700000000000,"paid":true,"note":"gift",` +
				`"status":"NEW","tags":["a","b"],"attrs":{"x":1},"customer":{"name":"ann","vip":true},"code":"AB"}`,
			wantID: 2, format: "AVRO",
		},
		{
			name: "avro bare union value and null",
			ref:  broker.SchemaRef{ID: 2},
			in: `{"id":"o-2","qty":1,"total":1,"ts":1,"paid":false,"note":"plain",` +
				`"status":"DONE","tags":[],"attrs":{},"customer":{"name":"bob","vip":null},"code":"ZZ"}`,
			wantID: 2, format: "AVRO",
		},
		{
			name:   "protobuf with import and well-known type",
			ref:    broker.SchemaRef{Subject: "porders-value"},
			in:     `{"id":"p-1","qty":2,"total":{"currency":"EUR","cents":"1250"},"tags":["x"],"at":"2026-10-08T09:00:00Z"}`,
			wantID: 4, format: "PROTOBUF",
		},
		{
			name: "topic name strategy", ref: broker.SchemaRef{}, in: `{"a":1}`, wantID: 5, format: "JSON",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newSerde(testLookup())
			ctx := context.Background()
			topic := strings.TrimSuffix(testLookup().schemas[tt.wantID-1].Subject, "-value")
			ref := tt.ref
			framed, err := s.encode(ctx, topic, &ref, []byte(tt.in))
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if framed[0] != 0 || int(framed[4]) != tt.wantID {
				t.Fatalf("header = %v, want magic 0 and id %d", framed[:5], tt.wantID)
			}
			out, got, err := s.decodeErr(ctx, framed)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got.ID != tt.wantID || got.Format != tt.format {
				t.Errorf("ref = %+v, want id %d format %s", got, tt.wantID, tt.format)
			}
			want := tt.want
			if want == "" {
				want = tt.in
			}
			assertJSONEqual(t, out, want)
		})
	}
}

func TestSerdeEncodeErrors(t *testing.T) {
	tests := []struct {
		name string
		ref  broker.SchemaRef
		in   string
		want string
	}{
		{name: "missing field", ref: broker.SchemaRef{ID: 1}, in: `{}`, want: "field name: missing required field"},
		{name: "wrong type", ref: broker.SchemaRef{ID: 1}, in: `{"name": 5}`, want: "does not match Avro type string"},
		{name: "not json", ref: broker.SchemaRef{ID: 1}, in: `nope`, want: "not JSON"},
		{name: "unknown subject", ref: broker.SchemaRef{Subject: "nope"}, in: `{}`, want: "not found"},
		{name: "protobuf unknown field", ref: broker.SchemaRef{ID: 4}, in: `{"bogus":1}`, want: "does not match message shop.Order"},
		{name: "json schema invalid", ref: broker.SchemaRef{ID: 5}, in: `{`, want: "not valid JSON"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref := tt.ref
			_, err := newSerde(testLookup()).encode(context.Background(), "t", &ref, []byte(tt.in))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestDecodeMessage(t *testing.T) {
	s := newSerde(testLookup())
	ctx := context.Background()
	key, err := s.encode(ctx, "customer", &broker.SchemaRef{ID: 1}, []byte(`{"name":"k"}`))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name      string
		msg       broker.Message
		wantKey   string
		wantValue string
		wantRef   *broker.SchemaRef
	}{
		{name: "plain payload untouched", msg: broker.Message{Key: []byte("k"), Value: []byte("hello")}, wantKey: "k", wantValue: "hello"},
		{name: "unknown schema id untouched", msg: broker.Message{Value: []byte{0, 0, 0, 0, 99, 1}}, wantValue: "\x00\x00\x00\x00c\x01"},
		{
			name: "framed key only", msg: broker.Message{Key: key, Value: []byte("raw")},
			wantKey: `{"name":"k","vip":null}`, wantValue: "raw",
			wantRef: &broker.SchemaRef{ID: 1, Subject: "customer", Version: 1, Format: "AVRO", KeySide: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := tt.msg
			s.decodeMessage(ctx, &m)
			if string(m.Key) != tt.wantKey || string(m.Value) != tt.wantValue {
				t.Errorf("key, value = %q, %q; want %q, %q", m.Key, m.Value, tt.wantKey, tt.wantValue)
			}
			if !reflect.DeepEqual(m.Schema, tt.wantRef) {
				t.Errorf("schema = %+v, want %+v", m.Schema, tt.wantRef)
			}
		})
	}
}

func TestProtoMessageIndex(t *testing.T) {
	c, err := newProtoCodec(context.Background(), testLookup(), testLookup().schemas[3])
	if err != nil {
		t.Fatal(err)
	}
	pc := c.(protoCodec)
	md, err := pc.message([]int{0, 0})
	if err != nil || md.FullName() != "shop.Order.Line" {
		t.Fatalf("message([0 0]) = %v, %v; want shop.Order.Line", md, err)
	}
	if _, err := pc.message([]int{3}); err == nil {
		t.Fatal("message([3]) succeeded, want error")
	}
}

func assertJSONEqual(t *testing.T, got []byte, want string) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("got invalid JSON %s: %v", got, err)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("want invalid JSON %s: %v", want, err)
	}
	if !reflect.DeepEqual(g, w) {
		t.Errorf("JSON mismatch\n got: %s\nwant: %s", got, want)
	}
}
