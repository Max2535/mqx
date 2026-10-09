# mqx architecture

mqx is one Go binary with two front ends, a CLI (cobra) and a TUI (Bubble Tea).
Both talk to brokers only through the interfaces in `internal/broker`.

```
cmd/mqx/main.go ──► internal/cli ──┬──► internal/broker (core + capabilities + registry)
                    internal/tui ──┘          ▲                 ▲
                         │                    │ registers       │ registers
                         ▼                    │                 │
                  internal/config     internal/broker/kafka   internal/broker/rabbitmq
                  internal/groupdiag
```

## Core and capabilities

Every adapter implements the small core:

```go
type Broker interface {
    Name() string
    Ping(ctx context.Context) error
    ListTopics(ctx context.Context) ([]Topic, error)
    Publish(ctx context.Context, topic string, msg Message) error
    Peek(ctx context.Context, topic string, opts PeekOptions) (<-chan Message, error)
    Close() error
}
```

Everything else is an optional capability interface in
`internal/broker/capabilities.go`: `TopicDescriber`, `ClusterInspector`,
`ConsumerInspector`, `GroupInspector`, `LagReporter`, `ConnectionInspector`,
`OffsetManager`, `ConsumerTerminator`, `TopicAdmin`, `PartitionAdder`, `Purger`,
`TopologyInspector`, `TopologyEditor`, `RouteSimulator`, `SchemaRegistry`,
`ConnectManager`, `KSQLRunner`, `ACLAdmin`, `UserAdmin`, `PolicyAdmin` and
`MetricsReporter`.

Brokers are not forced into one shape. The CLI and TUI type-assert:

```go
lr, err := broker.As[broker.LagReporter](b, broker.CapLagReporter)
```

`broker.Capabilities(b)` lists what a broker supports. `mqx ctx describe` prints
it, and the TUI builds its panel list from it, so a panel exists only when the
broker can back it. An adapter whose Go type implements a capability but whose
backing service is not configured (Kafka without `schema_registry`) hides it by
implementing `broker.CapabilityChecker`.

Errors: adapters wrap `broker.ErrNotFound` for missing objects and
`broker.ErrUnsupported` for operations that do not apply (for example altering
the arguments of a RabbitMQ queue), always with a hint saying what to do instead.

## Registry

Each adapter registers a `broker.Driver` in `init()`:

```go
func init() {
    broker.Register("kafka", broker.Driver{Open: open, Validate: validate, Fields: []string{"brokers", ...}})
}
```

`Open` connects from a `config.Context` plus resolved `config.Credentials`;
`Validate` checks broker-specific fields offline; `Fields` names the
`config.Fields()` keys the broker uses, which is all the context editors (the
TUI form and `mqx ctx add/set` flags) need to show the right inputs. `internal/config` knows nothing
about individual brokers: `Config.Validate` takes a `config.BrokerValidator`,
which the registry implements. `cmd/mqx/main.go` blank-imports each adapter.
The registry is the only package-level mutable state in mqx.

## Config and credentials

Editors change contexts through `Config.Update`, which applies the edit,
validates the whole file and saves it, or leaves everything unchanged on any
error. Saving writes back into the YAML tree that was loaded, so comments and
key order survive.

`~/.config/mqx/config.yaml` (or `--config` / `$MQX_CONFIG`) holds named contexts,
kubectl style. Credentials are never stored in the file: `username_env` and
`password_env` name environment variables, and literal `password:` keys or
passwords inside URLs are rejected. Kafka ecosystem services (`schema_registry`,
`connect`, `ksqldb`) are endpoints with their own env-var credentials.
Broker-specific extras go in `options`, so a new broker needs no config changes.

## Safety: the mutation guard

Adapters never check `read_only`. Every mutating action passes one guard:

- CLI: `session.guard` in `internal/cli/session.go` refuses `read_only`
  contexts, then requires `--yes` or an interactive `[y/N]` on a terminal. A
  non-terminal without `--yes` is refused, so scripts never block.
- TUI: mutating actions are hidden on `read_only` contexts; elsewhere each opens
  a confirm dialog whose default is Cancel. An action can declare a `danger`
  (publishing to an internal topic) that makes the dialog ask for the name to be
  typed. The command palette runs actions by sending their keys, so it cannot
  bypass the guard. An action's `advise` returns non-blocking hints
  (`internal/tui/hints.go`) that the form shows live and the confirm dialog
  repeats; what would certainly fail goes in its `check` instead.

Publishing is treated as mutating.

## Messages

`Peek` returns a channel. It is non-destructive on both brokers: Kafka reads
with a standalone consumer that never joins a group or commits; RabbitMQ
`basic.get`s with manual ack and requeues everything afterwards. A `broker.Filter`
(key, value and header regexes, time range) is applied before `Limit`. A failure
after streaming started arrives as a final `Message` with `Err` set.

`--from` accepts `earliest`, `latest`, an offset, `-N` (last N per partition),
an RFC3339 time or a duration ago.

## Consumer group debugging

`internal/groupdiag` is pure logic over `GroupDescription` and `PartitionLag`
snapshots. `Watcher` turns successive descriptions into a timeline (state
changes, joins, leaves, epoch bumps, rebalance durations). `Diagnose` applies
rules such as stale members, more members than partitions and one group id
shared by apps that subscribe to different topics. The CLI (`mqx group watch`,
`mqx group diagnose`) and the TUI both use it.

## Routing simulation

`mqx route <exchange> --key <key>` resolves a routing key through the RabbitMQ
binding graph read from the Management API, without publishing: default,
direct, fanout and topic exchanges, exchange-to-exchange bindings (cycle safe)
and alternate exchanges. Headers and plugin exchange types are reported as "not
simulated" rather than guessed. Integration tests compare its answers with
where a real broker actually routes messages.

## Metrics

`MetricsReporter.Sample` returns monotonic counters and gauges at one instant.
Callers diff two samples (`broker.Rates`) to get per-second rates, so adapters
need no history: `mqx metrics` prints a line per interval and the TUI keeps a
ring buffer for sparklines.

## Testing

- Unit tests: table-driven, stdlib `testing`.
- `internal/testutil/fakebroker`: an in-memory broker implementing every
  capability. CLI tests run commands in-process against it; TUI tests drive
  Bubble Tea models as pure updates against it.
- Integration tests (`//go:build integration`) run adapters against real
  brokers started by testcontainers: `go test -tags integration ./...`.
- `docker-compose.yml` starts the full stack for manual testing with
  `deploy/mqx-config.yaml`.
