package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Max2535/mqx/internal/broker"
)

type publishFlags struct {
	key          string
	value        string
	file         string
	lines        bool
	keySep       string
	headers      []string
	partition    int32
	exchange     string
	routingKey   string
	properties   []string
	schema       string
	schemaVer    int
	schemaID     int
	schemaForKey bool
}

func newPublishCmd(o *options) *cobra.Command {
	pf := publishFlags{partition: broker.AnyPartition}
	cmd := &cobra.Command{
		Use:     "publish <topic|queue>",
		Aliases: []string{"produce", "pub"},
		Short:   "Publish messages to a topic, queue or exchange",
		Long: `Publish messages to a topic, queue or exchange.

The payload comes from --value, --file, or stdin. With --lines every stdin
line is one message; --key-separator splits a key off each line.
Publishing is a mutating command: it is refused on read_only contexts and
needs confirmation (--yes when not on a terminal).`,
		Example: `  mqx publish orders --key ord-1 --header source=mqx --value '{"total":990}' --yes
  mqx publish orders --lines --key-separator : --yes < orders.txt
  mqx publish orders --schema-subject orders-value --value '{"id":1}' --yes   # Avro/Protobuf via Schema Registry
  mqx publish orders.q --property content_type=application/json --value '{}' --yes
  mqx publish ignored --exchange orders.x --routing-key order.created --value '{}' --yes`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			msgs, err := pf.messages(stdin(cmd))
			if err != nil {
				return err
			}
			return o.withSession(cmd, func(ctx context.Context, s *session) error {
				target := args[0]
				if pf.exchange != "" {
					target = fmt.Sprintf("exchange %s (routing key %q)", pf.exchange, msgs[0].RoutingKey)
				}
				if err := s.guard(cmd, fmt.Sprintf("publish %d message(s) to %s", len(msgs), target)); err != nil {
					return err
				}
				for i, m := range msgs {
					if err := s.b.Publish(ctx, args[0], m); err != nil {
						return fmt.Errorf("message %d of %d: %w", i+1, len(msgs), err)
					}
				}
				return o.done(cmd, "Published %d message(s) to %s.", len(msgs), target)
			})
		},
	}
	f := cmd.Flags()
	f.StringVarP(&pf.key, "key", "k", "", "message key (RabbitMQ: message_id)")
	f.StringVar(&pf.value, "value", "", "payload; default reads --file or stdin")
	f.StringVar(&pf.file, "file", "", "read the payload from this file")
	f.BoolVar(&pf.lines, "lines", false, "publish every input line as its own message")
	f.StringVar(&pf.keySep, "key-separator", "", "with --lines: split each line into key<sep>value")
	f.StringArrayVarP(&pf.headers, "header", "H", nil, "header key=value (repeatable)")
	f.Int32Var(&pf.partition, "partition", broker.AnyPartition, "Kafka: target partition (default: partitioner)")
	f.StringVar(&pf.exchange, "exchange", "", "RabbitMQ: publish through this exchange instead of to the queue")
	f.StringVar(&pf.routingKey, "routing-key", "", "RabbitMQ: routing key (default: the queue name)")
	f.StringArrayVar(&pf.properties, "property", nil,
		"RabbitMQ: property key=value, e.g. content_type, correlation_id, reply_to, expiration, priority, delivery_mode (repeatable)")
	f.StringVar(&pf.schema, "schema-subject", "", "Kafka: encode the JSON payload with this Schema Registry subject")
	f.IntVar(&pf.schemaVer, "schema-version", 0, "Kafka: subject version (default latest)")
	f.IntVar(&pf.schemaID, "schema-id", 0, "Kafka: encode with this schema id instead of a subject")
	f.BoolVar(&pf.schemaForKey, "schema-key", false, "Kafka: apply the schema to the key instead of the value")
	return cmd
}

func (pf publishFlags) messages(in io.Reader) ([]broker.Message, error) {
	headers, err := parseHeaders(pf.headers)
	if err != nil {
		return nil, err
	}
	props, err := parseKV("property", pf.properties)
	if err != nil {
		return nil, err
	}
	base := broker.Message{Partition: pf.partition, Headers: headers, Exchange: pf.exchange, RoutingKey: pf.routingKey}
	if len(props) > 0 {
		base.Properties = props
	}
	if pf.schema != "" || pf.schemaID != 0 {
		base.Schema = &broker.SchemaRef{Subject: pf.schema, Version: pf.schemaVer, ID: pf.schemaID, KeySide: pf.schemaForKey}
	}
	if pf.key != "" {
		base.Key = []byte(pf.key)
	}
	if pf.keySep != "" && !pf.lines {
		return nil, errors.New("--key-separator needs --lines")
	}
	if pf.value != "" && pf.file != "" {
		return nil, errors.New("pass only one of --value and --file")
	}

	var r io.Reader
	switch {
	case pf.value != "":
		r = strings.NewReader(pf.value)
	case pf.file != "":
		fh, err := os.Open(pf.file)
		if err != nil {
			return nil, fmt.Errorf("--file: %w", err)
		}
		defer fh.Close()
		r = fh
	default:
		r = in
	}

	if !pf.lines {
		data, err := io.ReadAll(r)
		if err != nil {
			return nil, fmt.Errorf("read payload: %w", err)
		}
		m := base
		m.Value = data
		return []broker.Message{m}, nil
	}

	var msgs []broker.Message
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		m := base
		m.Value = []byte(line)
		if pf.keySep != "" {
			k, v, ok := strings.Cut(line, pf.keySep)
			if !ok {
				return nil, fmt.Errorf("line %d has no key separator %q", len(msgs)+1, pf.keySep)
			}
			m.Key, m.Value = []byte(k), []byte(v)
		}
		msgs = append(msgs, m)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read lines: %w", err)
	}
	if len(msgs) == 0 {
		return nil, errors.New("no messages: input was empty")
	}
	return msgs, nil
}

func parseHeaders(pairs []string) ([]broker.Header, error) {
	var out []broker.Header
	for _, p := range pairs {
		k, v, ok := strings.Cut(p, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("--header %q: want key=value", p)
		}
		out = append(out, broker.Header{Key: k, Value: []byte(v)})
	}
	return out, nil
}
