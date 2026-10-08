package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/Max2535/mqx/internal/broker"
)

// schemaLookup fetches registered schemas; the registry client implements it
// and tests inject an in-memory one.
type schemaLookup interface {
	byID(ctx context.Context, id int) (regSchema, error)
	// bySubject returns one version of a subject; -1 is the latest.
	bySubject(ctx context.Context, subject string, version int) (regSchema, error)
}

// codec converts between JSON text and one schema's binary encoding.
type codec interface {
	// encode returns the payload and, for Protobuf, the message index path.
	encode(jsonText []byte) (payload []byte, index []int, err error)
	decode(payload []byte, index []int) (jsonText []byte, err error)
}

// serde encodes and decodes Confluent wire format payloads: magic byte 0, a
// big-endian schema id, for Protobuf the message index varints, then the data.
// Compiled codecs are cached by schema id.
type serde struct {
	lookup schemaLookup
	header sr.ConfluentHeader

	mu     sync.Mutex
	codecs map[int]codecEntry
}

type codecEntry struct {
	schema regSchema
	codec  codec
}

func newSerde(l schemaLookup) *serde { return &serde{lookup: l, codecs: map[int]codecEntry{}} }

// encode encodes JSON text with the schema ref names. Without an id or
// subject it uses the topic name strategy: <topic>-value or <topic>-key.
func (s *serde) encode(ctx context.Context, topic string, ref *broker.SchemaRef, data []byte) ([]byte, error) {
	var (
		rs  regSchema
		err error
	)
	switch {
	case ref.ID > 0:
		rs, err = s.lookup.byID(ctx, ref.ID)
	default:
		subject := ref.Subject
		if subject == "" {
			subject = topic + "-value"
			if ref.KeySide {
				subject = topic + "-key"
			}
		}
		version := ref.Version
		if version <= 0 {
			version = -1
		}
		rs, err = s.lookup.bySubject(ctx, subject, version)
	}
	if err != nil {
		return nil, err
	}
	e, err := s.codecFor(ctx, rs)
	if err != nil {
		return nil, err
	}
	payload, index, err := e.codec.encode(data)
	if err != nil {
		return nil, fmt.Errorf("encode with %s schema %d (%s): %w", rs.Type, rs.ID, rs.Subject, err)
	}
	out, _ := s.header.AppendEncode(nil, rs.ID, index)
	return append(out, payload...), nil
}

// decodeMessage replaces a framed key and value with JSON text and records
// the value's schema (or the key's, marked KeySide, when only the key is framed).
// Payloads that are not framed or fail to decode are left untouched.
func (s *serde) decodeMessage(ctx context.Context, m *broker.Message) {
	if v, ref, ok := s.decode(ctx, m.Value); ok {
		m.Value, m.Schema = v, &ref
	}
	if v, ref, ok := s.decode(ctx, m.Key); ok {
		m.Key = v
		if m.Schema == nil {
			ref.KeySide = true
			m.Schema = &ref
		}
	}
}

func (s *serde) decode(ctx context.Context, data []byte) ([]byte, broker.SchemaRef, bool) {
	out, ref, err := s.decodeErr(ctx, data)
	return out, ref, err == nil
}

func (s *serde) decodeErr(ctx context.Context, data []byte) ([]byte, broker.SchemaRef, error) {
	id, rest, err := s.header.DecodeID(data)
	if err != nil {
		return nil, broker.SchemaRef{}, err
	}
	rs, err := s.schemaByID(ctx, id)
	if err != nil {
		return nil, broker.SchemaRef{}, err
	}
	e, err := s.codecFor(ctx, rs)
	if err != nil {
		return nil, broker.SchemaRef{}, err
	}
	var index []int
	if rs.Type == "PROTOBUF" {
		if index, rest, err = s.header.DecodeIndex(rest, 0); err != nil {
			return nil, broker.SchemaRef{}, err
		}
	}
	out, err := e.codec.decode(rest, index)
	if err != nil {
		return nil, broker.SchemaRef{}, err
	}
	return out, broker.SchemaRef{ID: id, Subject: rs.Subject, Version: rs.Version, Format: rs.Type}, nil
}

func (s *serde) schemaByID(ctx context.Context, id int) (regSchema, error) {
	s.mu.Lock()
	e, ok := s.codecs[id]
	s.mu.Unlock()
	if ok {
		return e.schema, nil
	}
	return s.lookup.byID(ctx, id)
}

// codecFor returns the cached codec of a schema, compiling it on first use.
func (s *serde) codecFor(ctx context.Context, rs regSchema) (codecEntry, error) {
	s.mu.Lock()
	e, ok := s.codecs[rs.ID]
	s.mu.Unlock()
	if ok {
		return e, nil
	}
	var (
		c   codec
		err error
	)
	switch rs.Type {
	case "AVRO":
		c, err = newAvroCodec(ctx, s.lookup, rs)
	case "PROTOBUF":
		c, err = newProtoCodec(ctx, s.lookup, rs)
	case "JSON":
		c = jsonCodec{}
	default:
		err = fmt.Errorf("schema type %q: %w", rs.Type, broker.ErrUnsupported)
	}
	if err != nil {
		return codecEntry{}, fmt.Errorf("compile %s schema %d: %w", rs.Type, rs.ID, err)
	}
	e = codecEntry{schema: rs, codec: c}
	s.mu.Lock()
	s.codecs[rs.ID] = e
	s.mu.Unlock()
	return e, nil
}

// jsonCodec handles JSON Schema payloads, which are JSON text already.
type jsonCodec struct{}

var errNotJSON = errors.New("payload is not valid JSON")

func (jsonCodec) encode(data []byte) ([]byte, []int, error) {
	if !json.Valid(data) {
		return nil, nil, errNotJSON
	}
	return data, nil, nil
}

func (jsonCodec) decode(data []byte, _ []int) ([]byte, error) {
	if !json.Valid(data) {
		return nil, errNotJSON
	}
	return data, nil
}

// referenced fetches every schema a schema references, depth first, so
// dependencies come before their dependents. Visited subjects are skipped.
func referenced(ctx context.Context, l schemaLookup, refs []sr.SchemaReference, seen map[string]bool, visit func(name string, rs regSchema) error) error {
	for _, ref := range refs {
		key := fmt.Sprintf("%s@%d", ref.Subject, ref.Version)
		if seen[key] {
			continue
		}
		seen[key] = true
		rs, err := l.bySubject(ctx, ref.Subject, ref.Version)
		if err != nil {
			return fmt.Errorf("reference %q: %w", ref.Name, err)
		}
		if err := referenced(ctx, l, rs.Refs, seen, visit); err != nil {
			return err
		}
		if err := visit(ref.Name, rs); err != nil {
			return err
		}
	}
	return nil
}
