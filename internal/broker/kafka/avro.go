package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"time"

	"github.com/hamba/avro/v2"
)

// avroCodec encodes Avro JSON (unions as {"type": value} or a bare value) to
// Avro binary and decodes binary back to Avro JSON.
type avroCodec struct{ schema avro.Schema }

func newAvroCodec(ctx context.Context, l schemaLookup, rs regSchema) (codec, error) {
	cache := &avro.SchemaCache{}
	err := referenced(ctx, l, rs.Refs, map[string]bool{}, func(_ string, ref regSchema) error {
		_, err := avro.ParseWithCache(ref.Text, "", cache)
		return err
	})
	if err != nil {
		return nil, err
	}
	s, err := avro.ParseWithCache(rs.Text, "", cache)
	if err != nil {
		return nil, err
	}
	return avroCodec{schema: s}, nil
}

func (c avroCodec) encode(data []byte) ([]byte, []int, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, nil, fmt.Errorf("value is not JSON: %w", err)
	}
	native, err := fromJSON(c.schema, v, "")
	if err != nil {
		return nil, nil, err
	}
	out, err := avro.Marshal(c.schema, native)
	return out, nil, err
}

func (c avroCodec) decode(data []byte, _ []int) ([]byte, error) {
	var v any
	if err := avro.Unmarshal(c.schema, data, &v); err != nil {
		return nil, err
	}
	return json.Marshal(toJSON(v))
}

// fromJSON converts a JSON-decoded value (numbers as json.Number) into the Go
// types the Avro encoder expects for schema s. path names the field in errors.
func fromJSON(s avro.Schema, v any, path string) (any, error) {
	if ref, ok := s.(*avro.RefSchema); ok {
		s = ref.Schema()
	}
	bad := func() error { return fmt.Errorf("%s: %T does not match Avro type %s", pathLabel(path), v, s.Type()) }
	switch s := s.(type) {
	case *avro.NullSchema:
		if v != nil {
			return nil, bad()
		}
		return nil, nil
	case *avro.RecordSchema:
		obj, ok := v.(map[string]any)
		if !ok {
			return nil, bad()
		}
		out := make(map[string]any, len(s.Fields()))
		for _, f := range s.Fields() {
			fv, present := obj[f.Name()]
			if !present {
				if f.HasDefault() {
					continue
				}
				if _, isUnion := f.Type().(*avro.UnionSchema); !isUnion {
					return nil, fmt.Errorf("%s: missing required field", pathLabel(path+"."+f.Name()))
				}
			}
			nv, err := fromJSON(f.Type(), fv, path+"."+f.Name())
			if err != nil {
				return nil, err
			}
			out[f.Name()] = nv
		}
		return out, nil
	case *avro.ArraySchema:
		arr, ok := v.([]any)
		if !ok {
			return nil, bad()
		}
		out := make([]any, len(arr))
		for i, item := range arr {
			nv, err := fromJSON(s.Items(), item, fmt.Sprintf("%s[%d]", path, i))
			if err != nil {
				return nil, err
			}
			out[i] = nv
		}
		return out, nil
	case *avro.MapSchema:
		obj, ok := v.(map[string]any)
		if !ok {
			return nil, bad()
		}
		out := make(map[string]any, len(obj))
		for k, item := range obj {
			nv, err := fromJSON(s.Values(), item, path+"."+k)
			if err != nil {
				return nil, err
			}
			out[k] = nv
		}
		return out, nil
	case *avro.UnionSchema:
		return unionFromJSON(s, v, path)
	case *avro.EnumSchema:
		str, ok := v.(string)
		if !ok {
			return nil, bad()
		}
		return str, nil
	case *avro.FixedSchema:
		str, ok := v.(string)
		if !ok || len(str) != s.Size() {
			return nil, fmt.Errorf("%s: fixed(%d) needs a string of %d bytes", pathLabel(path), s.Size(), s.Size())
		}
		// The encoder wants a [size]byte array.
		arr := reflect.New(reflect.ArrayOf(s.Size(), reflect.TypeFor[byte]())).Elem()
		reflect.Copy(arr, reflect.ValueOf([]byte(str)))
		return arr.Interface(), nil
	case *avro.PrimitiveSchema:
		return primitiveFromJSON(s, v, bad)
	}
	return nil, fmt.Errorf("%s: Avro type %s is not supported", pathLabel(path), s.Type())
}

// unionFromJSON accepts the Avro JSON encoding {"branch": value} or a bare
// value, trying each branch in order.
func unionFromJSON(s *avro.UnionSchema, v any, path string) (any, error) {
	if v == nil {
		if s.Nullable() {
			return nil, nil
		}
		return nil, fmt.Errorf("%s: null is not allowed by the union", pathLabel(path))
	}
	if obj, ok := v.(map[string]any); ok && len(obj) == 1 {
		for name, inner := range obj {
			if branch, _ := s.Types().Get(name); branch != nil {
				nv, err := fromJSON(branch, inner, path)
				if err != nil {
					return nil, err
				}
				return map[string]any{name: nv}, nil
			}
		}
	}
	for _, branch := range s.Types() {
		if branch.Type() == avro.Null {
			continue
		}
		if nv, err := fromJSON(branch, v, path); err == nil {
			return map[string]any{unionName(branch): nv}, nil
		}
	}
	return nil, fmt.Errorf("%s: value matches no branch of the union", pathLabel(path))
}

// unionName is the name hamba uses to pick a union branch.
func unionName(s avro.Schema) string {
	if ref, ok := s.(*avro.RefSchema); ok {
		s = ref.Schema()
	}
	if n, ok := s.(avro.NamedSchema); ok {
		return n.FullName()
	}
	if p, ok := s.(*avro.PrimitiveSchema); ok && p.Logical() != nil {
		return string(p.Type()) + "." + string(p.Logical().Type())
	}
	return string(s.Type())
}

func primitiveFromJSON(s *avro.PrimitiveSchema, v any, bad func() error) (any, error) {
	num, isNum := v.(json.Number)
	switch s.Type() {
	case avro.String:
		if str, ok := v.(string); ok {
			return str, nil
		}
	case avro.Boolean:
		if b, ok := v.(bool); ok {
			return b, nil
		}
	case avro.Bytes:
		if str, ok := v.(string); ok {
			return []byte(str), nil
		}
	case avro.Int:
		if isNum {
			n, err := num.Int64()
			if err != nil {
				return nil, bad()
			}
			if s.Logical() != nil && s.Logical().Type() == avro.Date {
				return time.Unix(n*86400, 0).UTC(), nil
			}
			return int(n), nil
		}
	case avro.Long:
		if isNum {
			n, err := num.Int64()
			if err != nil {
				return nil, bad()
			}
			if s.Logical() != nil {
				switch s.Logical().Type() {
				case avro.TimestampMillis:
					return time.UnixMilli(n).UTC(), nil
				case avro.TimestampMicros:
					return time.UnixMicro(n).UTC(), nil
				}
			}
			return n, nil
		}
		if str, ok := v.(string); ok && s.Logical() != nil {
			t, err := time.Parse(time.RFC3339Nano, str)
			if err != nil {
				return nil, bad()
			}
			return t, nil
		}
	case avro.Float:
		if isNum {
			f, err := num.Float64()
			if err != nil {
				return nil, bad()
			}
			return float32(f), nil
		}
	case avro.Double:
		if isNum {
			f, err := num.Float64()
			if err != nil {
				return nil, bad()
			}
			return f, nil
		}
	}
	return nil, bad()
}

func pathLabel(path string) string {
	if path == "" {
		return "value"
	}
	return "field " + path[1:]
}

// toJSON makes decoded Avro values JSON friendly: bytes become strings and
// decimals become numbers.
func toJSON(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, item := range x {
			out[k] = toJSON(item)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, item := range x {
			out[i] = toJSON(item)
		}
		return out
	case []byte:
		return string(x)
	default:
		if rv := reflect.ValueOf(v); rv.Kind() == reflect.Array && rv.Type().Elem().Kind() == reflect.Uint8 {
			b := make([]byte, rv.Len())
			reflect.Copy(reflect.ValueOf(b), rv)
			return string(b)
		}
	case *big.Rat:
		f, _ := x.Float64()
		return f
	}
	return v
}
