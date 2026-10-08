package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// rootProtoFile is the name the schema itself is compiled under.
const rootProtoFile = "mqx-schema.proto"

// protoCodec encodes JSON (protojson) to Protobuf and back with a schema
// compiled from the registry text and its references. Encoding uses the first
// message of the file, index [0], like Confluent serializers do by default.
type protoCodec struct{ file protoreflect.FileDescriptor }

func newProtoCodec(ctx context.Context, l schemaLookup, rs regSchema) (codec, error) {
	sources := map[string]string{rootProtoFile: rs.Text}
	err := referenced(ctx, l, rs.Refs, map[string]bool{}, func(name string, ref regSchema) error {
		sources[name] = ref.Text
		return nil
	})
	if err != nil {
		return nil, err
	}
	c := protocompile.Compiler{Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
		Accessor: protocompile.SourceAccessorFromMap(sources),
	})}
	files, err := c.Compile(ctx, rootProtoFile)
	if err != nil {
		return nil, err
	}
	if files[0].Messages().Len() == 0 {
		return nil, errors.New("the schema defines no message")
	}
	return protoCodec{file: files[0]}, nil
}

// message resolves a Confluent message index path to a descriptor.
func (c protoCodec) message(index []int) (protoreflect.MessageDescriptor, error) {
	if len(index) == 0 {
		index = []int{0}
	}
	msgs := c.file.Messages()
	var md protoreflect.MessageDescriptor
	for _, i := range index {
		if i < 0 || i >= msgs.Len() {
			return nil, fmt.Errorf("message index %v does not exist in the schema", index)
		}
		md = msgs.Get(i)
		msgs = md.Messages()
	}
	return md, nil
}

func (c protoCodec) encode(data []byte) ([]byte, []int, error) {
	md, err := c.message(nil)
	if err != nil {
		return nil, nil, err
	}
	m := dynamicpb.NewMessage(md)
	if err := protojson.Unmarshal(data, m); err != nil {
		return nil, nil, fmt.Errorf("value does not match message %s: %w", md.FullName(), err)
	}
	out, err := proto.Marshal(m)
	return out, []int{0}, err
}

func (c protoCodec) decode(data []byte, index []int) ([]byte, error) {
	md, err := c.message(index)
	if err != nil {
		return nil, err
	}
	m := dynamicpb.NewMessage(md)
	if err := proto.Unmarshal(data, m); err != nil {
		return nil, err
	}
	return protojson.MarshalOptions{EmitUnpopulated: false}.Marshal(m)
}
