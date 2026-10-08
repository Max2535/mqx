package cli

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"time"
	"unicode/utf8"

	"github.com/spf13/cobra"

	"github.com/Max2535/mqx/internal/broker"
)

type peekFlags struct {
	limit      int
	partitions []string
	from, to   string
	follow     bool
	key, value string
	headers    []string
	since      string
	until      string
	raw        bool
	noDecode   bool
}

func newPeekCmd(o *options) *cobra.Command {
	var pf peekFlags
	cmd := &cobra.Command{
		Use:   "peek <topic|queue>",
		Short: "Read messages without consuming them",
		Long: `Read messages without consuming them.

Kafka: reads with a standalone consumer that never joins a group or commits.
RabbitMQ: gets messages with manual ack and requeues them all afterwards
(their redelivered flag is set).`,
		Example: `  mqx peek orders                       # first 20 messages
  mqx peek orders --from -5             # last 5 per partition
  mqx peek orders --from 1h -n 0        # everything from the last hour
  mqx peek orders --key '^ord-88' --header source=mqx --value '"total":\s*990'
  mqx peek orders -f                    # follow new messages
  mqx peek orders.q -o json | jq .value`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts, err := pf.options(time.Now())
			if err != nil {
				return err
			}
			s, err := o.open(cmd)
			if err != nil {
				return err
			}
			defer s.Close()
			// No --timeout here: a peek streams for as long as it has data or --follow is set.
			ch, err := s.b.Peek(cmd.Context(), args[0], opts)
			if err != nil {
				return err
			}
			w := cmd.OutOrStdout()
			n := 0
			for m := range ch {
				if m.Err != nil {
					return fmt.Errorf("peek %s stopped after %d message(s): %w", args[0], n, m.Err)
				}
				n++
				if err := o.writeMessage(w, m, pf.raw); err != nil {
					return err
				}
			}
			if n == 0 && o.output != "json" && !pf.raw {
				cmd.PrintErrln("No messages matched.")
			}
			return nil
		},
	}
	f := cmd.Flags()
	f.IntVarP(&pf.limit, "limit", "n", 20, "max messages to show after filtering (0 = no limit)")
	f.StringSliceVarP(&pf.partitions, "partitions", "p", nil, "Kafka: only these partitions, e.g. 0,2")
	f.StringVar(&pf.from, "from", "earliest", "start position: "+positionHelp)
	f.StringVar(&pf.to, "to", "", "Kafka: stop position (exclusive offset or time); default = end at start of peek")
	f.BoolVarP(&pf.follow, "follow", "f", false, "Kafka: keep streaming new messages")
	f.StringVar(&pf.key, "key", "", "only keys matching this regex (RabbitMQ: message_id)")
	f.StringVar(&pf.value, "value", "", "only values matching this regex")
	f.StringArrayVar(&pf.headers, "header", nil, "only messages with this header: key or key=regex (repeatable)")
	f.StringVar(&pf.since, "since", "", "only messages at or after this time (RFC3339 or duration ago)")
	f.StringVar(&pf.until, "until", "", "only messages before this time (RFC3339 or duration ago)")
	f.BoolVar(&pf.raw, "raw", false, "print only message values, one per line")
	f.BoolVar(&pf.noDecode, "no-decode", false, "Kafka: show Schema Registry payloads as raw bytes")
	return cmd
}

func (pf peekFlags) options(now time.Time) (broker.PeekOptions, error) {
	opts := broker.PeekOptions{Limit: pf.limit, Follow: pf.follow, Decode: !pf.noDecode}
	var err error
	if pf.limit < 0 {
		return opts, fmt.Errorf("--limit must be >= 0")
	}
	if opts.Partitions, err = parsePartitions(pf.partitions); err != nil {
		return opts, err
	}
	if opts.From, err = parsePosition(pf.from, now); err != nil {
		return opts, fmt.Errorf("--from: %w", err)
	}
	if pf.to != "" {
		to, err := parsePosition(pf.to, now)
		if err != nil {
			return opts, fmt.Errorf("--to: %w", err)
		}
		if to.Kind != broker.AtOffset && to.Kind != broker.AtTime {
			return opts, fmt.Errorf("--to must be an offset or a time")
		}
		opts.To = &to
	}
	if opts.Filter, err = pf.filter(now); err != nil {
		return opts, err
	}
	return opts, nil
}

func (pf peekFlags) filter(now time.Time) (broker.Filter, error) {
	var f broker.Filter
	var err error
	if pf.key != "" {
		if f.Key, err = regexp.Compile(pf.key); err != nil {
			return f, fmt.Errorf("--key: %w", err)
		}
	}
	if pf.value != "" {
		if f.Value, err = regexp.Compile(pf.value); err != nil {
			return f, fmt.Errorf("--value: %w", err)
		}
	}
	for _, h := range pf.headers {
		hm, err := broker.ParseHeaderMatch(h)
		if err != nil {
			return f, err
		}
		f.Headers = append(f.Headers, hm)
	}
	for _, tf := range []struct {
		flag, value string
		dst         *time.Time
	}{{"since", pf.since, &f.Since}, {"until", pf.until, &f.Until}} {
		if tf.value == "" {
			continue
		}
		p, err := parsePosition(tf.value, now)
		if err != nil || p.Kind != broker.AtTime {
			return f, fmt.Errorf("--%s %q: want an RFC3339 time or a duration ago such as 15m", tf.flag, tf.value)
		}
		*tf.dst = p.Time
	}
	return f, nil
}

// jsonMessage is the -o json shape of a message: one object per line. Values
// that are JSON are embedded as JSON, other UTF-8 as a string, binary as base64.
type jsonMessage struct {
	Topic         string            `json:"topic"`
	Partition     *int32            `json:"partition,omitempty"`
	Offset        int64             `json:"offset"`
	Timestamp     time.Time         `json:"timestamp,omitzero"`
	Key           any               `json:"key,omitempty"`
	KeyEncoding   string            `json:"key_encoding,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
	Exchange      string            `json:"exchange,omitempty"`
	RoutingKey    string            `json:"routing_key,omitempty"`
	Properties    map[string]string `json:"properties,omitempty"`
	Redelivered   bool              `json:"redelivered,omitempty"`
	Schema        *broker.SchemaRef `json:"schema,omitempty"`
	Value         any               `json:"value"`
	ValueEncoding string            `json:"value_encoding,omitempty"`
}

func toJSONMessage(m broker.Message, kafkaLike bool) jsonMessage {
	jm := jsonMessage{Topic: m.Topic, Offset: m.Offset, Timestamp: m.Timestamp, Exchange: m.Exchange,
		RoutingKey: m.RoutingKey, Properties: m.Properties, Redelivered: m.Redelivered, Schema: m.Schema}
	if kafkaLike {
		p := m.Partition
		jm.Partition = &p
	}
	if len(m.Key) > 0 {
		jm.Key, jm.KeyEncoding = encodeBytes(m.Key, false)
	}
	jm.Value, jm.ValueEncoding = encodeBytes(m.Value, true)
	if len(m.Headers) > 0 {
		jm.Headers = make(map[string]string, len(m.Headers))
		for _, h := range m.Headers {
			jm.Headers[h.Key] = printable(h.Value)
		}
	}
	return jm
}

func encodeBytes(b []byte, allowJSON bool) (any, string) {
	switch {
	case allowJSON && len(b) > 0 && json.Valid(b):
		return json.RawMessage(b), ""
	case utf8.Valid(b):
		return string(b), ""
	default:
		return base64.StdEncoding.EncodeToString(b), "base64"
	}
}

// printable shows UTF-8 as is and anything else as base64.
func printable(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}
	return "base64:" + base64.StdEncoding.EncodeToString(b)
}

func (o *options) writeMessage(w io.Writer, m broker.Message, raw bool) error {
	kafkaLike := m.Exchange == "" && m.RoutingKey == "" && m.Properties == nil
	switch {
	case raw:
		_, err := fmt.Fprintf(w, "%s\n", m.Value)
		return err
	case o.output == "json":
		return json.NewEncoder(w).Encode(toJSONMessage(m, kafkaLike))
	}
	var hdr bytes.Buffer
	if kafkaLike {
		fmt.Fprintf(&hdr, "--- %s[%d] @%d", m.Topic, m.Partition, m.Offset)
	} else {
		fmt.Fprintf(&hdr, "--- %s #%d", m.Topic, m.Offset)
		if m.Exchange != "" || m.RoutingKey != "" {
			fmt.Fprintf(&hdr, "  exchange=%q routing_key=%q", m.Exchange, m.RoutingKey)
		}
		if m.Redelivered {
			hdr.WriteString("  redelivered")
		}
	}
	if !m.Timestamp.IsZero() {
		fmt.Fprintf(&hdr, "  %s", m.Timestamp.Local().Format(time.RFC3339Nano))
	}
	if len(m.Key) > 0 {
		fmt.Fprintf(&hdr, "  key=%s", printable(m.Key))
	}
	if m.Schema != nil {
		fmt.Fprintf(&hdr, "  schema=%s#%d", m.Schema.Format, m.Schema.ID)
	}
	fmt.Fprintln(w, hdr.String())
	for _, h := range m.Headers {
		fmt.Fprintf(w, "  %s: %s\n", h.Key, printable(h.Value))
	}
	for _, k := range sortedKeys(m.Properties) {
		fmt.Fprintf(w, "  [%s] %s\n", k, m.Properties[k])
	}
	_, err := fmt.Fprintf(w, "%s\n", prettyValue(m.Value))
	return err
}

// prettyValue indents JSON values and leaves anything else readable.
func prettyValue(b []byte) string {
	if json.Valid(b) {
		var buf bytes.Buffer
		if json.Indent(&buf, b, "", "  ") == nil {
			return buf.String()
		}
	}
	return printable(b)
}
