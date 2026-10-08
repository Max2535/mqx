# mqx — Multi-broker Message Queue TUI/CLI (Go)

> Project brief for Claude Code. Repo was created empty (README + MIT LICENSE only).
> Items marked **[ASSUMED]** were not explicitly confirmed by the owner. Ask the owner to confirm them before building on them.

## Goal

A single Go binary that covers what Kafka UI and the RabbitMQ Management UI do, across message
brokers, through one interface: inspect, peek, publish, manage consumers and administer the cluster.
Interactive TUI plus non-interactive CLI. Target audience: backend developers and platform engineers
working with event-driven systems. Aim: genuinely useful, easy to install, easy to extend with new brokers.

## Decisions

- Language: Go (latest stable, 1.22+). Single static binary.
- Repo: new repo, separate from `team-stack`.
- Project name: `mqx` (repo: Max2535/mqx)
- License: MIT (LICENSE file already in repo)
- v0.1 brokers: Kafka + RabbitMQ. Design for more from day one.
- v0.1 scope: feature parity with Kafka UI and the RabbitMQ Management UI (confirmed 2026-10-08), see
  "v0.1 features" below. Everything ships in v0.1; milestones order the work, not the releases.
- Interfaces: TUI and non-interactive CLI, kubectl-style named contexts.
- TUI entry: bare `mqx` opens the TUI when stdin and stdout are a TTY, otherwise prints help (never blocks
  a script). `mqx tui` opens it explicitly and takes deep-link flags: `--context`, `--topic`, `--group`.
  The TUI uses the same config, `--config` / `$MQX_CONFIG` and `current-context` as the CLI, and hides
  mutating actions on `read_only` contexts.
- Safety: a context can be marked `read_only: true`; every mutating command refuses to run against it.
  Mutating commands also require confirmation (`--yes` in the CLI, a confirm dialog in the TUI).

## v0.1 features

| Area | Kafka | RabbitMQ |
|---|---|---|
| Browse | topics, partitions, configs, brokers | queues, exchanges, bindings, vhosts, connections, channels |
| Messages | peek with filters (key, header, value, time/offset range), publish with key/headers | peek (get + requeue), publish with routing key/headers/properties |
| Consumers | groups, members, assignments, lag; reset offsets; delete group; remove static member | consumers per queue; close connection |
| Rebalance debugging | `group describe` (state, coordinator, assignor, members, subscriptions), `group watch` (state-change timeline with rebalance duration), `group diagnose` (rule-based findings with fixes) | |
| Admin | create/delete topic, alter config, add partitions, delete records | declare/delete queue/exchange, bind/unbind (exchange→queue and exchange→exchange), purge queue |
| Routing | | dry-run route: which queues a routing key reaches through the binding graph, without publishing |
| Ecosystem | Schema Registry (browse, Avro/Protobuf/JSON Schema serde), Kafka Connect, KSQL, ACLs | users, vhosts, permissions, policies, shovel, federation |
| Metrics | throughput and lag over time | message and connection rates over time |

## Libraries

| Concern | Choice |
|---|---|
| TUI | Bubble Tea + Bubbles + Lip Gloss |
| CLI | cobra |
| Kafka | franz-go (`kgo`, `kadm`, `sr` for Schema Registry) |
| Kafka Connect, KSQL | REST via `net/http` |
| RabbitMQ | amqp091-go (data) + Management HTTP API (topology, stats, admin) |
| Avro / Protobuf serde | chosen in M4; ask the owner before adding |
| Config | YAML (`gopkg.in/yaml.v3`), contexts file at `~/.config/mqx/config.yaml` |
| Tests | stdlib `testing`, testcontainers-go for integration |
| Lint/CI | golangci-lint, GitHub Actions, goreleaser for releases |

## Architecture

Small core interface plus optional capability interfaces. Do NOT force every broker into one shape.
The TUI/CLI type-asserts capabilities and shows or hides features accordingly.

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

Capabilities (exact signatures are fixed in each milestone's spec):

| Capability | Responsibility | Kafka | RabbitMQ |
|---|---|---|---|
| `ConsumerInspector` | consumers / groups attached to a topic or queue | ✅ | ✅ |
| `LagReporter` | per-partition lag for a group | ✅ | |
| `OffsetManager` | reset offsets, delete group | ✅ | |
| `ConsumerTerminator` | disconnect a consumer | ✅ static members | ✅ close connection |
| `TopicAdmin` | create/delete topic or queue, alter config | ✅ | ✅ |
| `Purger` | remove messages (delete records / purge) | ✅ | ✅ |
| `TopologyInspector` / `TopologyEditor` | exchanges and bindings (incl. exchange→exchange) | | ✅ |
| `RouteSimulator` | resolve a routing key to destination queues from bindings | | ✅ |
| `SchemaRegistry` | subjects, versions, serde | ✅ | |
| `ConnectManager`, `KSQLRunner` | Kafka Connect, KSQL | ✅ | |
| `ACLAdmin` | Kafka ACLs | ✅ | |
| `UserAdmin`, `PolicyAdmin` | users, vhosts, permissions, policies, shovel, federation | | ✅ |
| `MetricsReporter` | rates over time for graphs | ✅ | ✅ |
| `Replayer` | replay a range (post v0.1) | | |

Registry pattern: each adapter registers a factory in `init()` keyed by broker type, so adding a broker
means adding one package and one blank import.

Every method of a mutating capability goes through one guard in `internal/cli` / `internal/tui` that
checks `read_only` and confirmation. Adapters never check it themselves.

## Layout

```
cmd/mqx/main.go
internal/broker/          # interfaces, models, registry
internal/broker/kafka/
internal/broker/rabbitmq/
internal/config/          # contexts, loading, validation
internal/cli/             # cobra commands
internal/tui/             # Bubble Tea models, views, keymap
internal/testutil/
docs/ARCHITECTURE.md  docs/ADDING_A_BROKER.md
.github/workflows/ci.yml  .goreleaser.yaml
```

## Milestones

1. **M0 Skeleton**: go.mod, layout, lint, CI, config contexts (incl. `read_only`), `mqx version`, `mqx ctx list/use`.
2. **M1 Kafka core**: Ping, topics, Publish, Peek with filters, consumer groups, consumers, lag,
   `mqx group describe|watch|diagnose` (classic protocol + KIP-848 `ConsumerGroupDescribe` when available)
   + CLI + integration tests.
3. **M2 RabbitMQ core**: same core plus topology, consumers, connections/channels via Management API + tests.
4. **M3 Admin and consumer management**: mutation guard; topic/queue CRUD and config; purge / delete records;
   offset reset; delete group; terminate consumers; exchange/binding editing (incl. exchange→exchange);
   `mqx route` dry-run routing. Both brokers.
5. **M4 Kafka ecosystem**: Schema Registry (browse + serde in peek/publish), Kafka Connect, KSQL, ACLs.
6. **M5 RabbitMQ administration**: users, vhosts, permissions, policies, shovel, federation.
7. **M6 TUI**: `mqx` / `mqx tui` entry with deep links, context switcher, every capability as a panel, message viewer (JSON pretty-print), publish form,
   confirm dialogs, rate graphs, capability-driven panels.
8. **M7 Release**: goreleaser, Homebrew tap, `go install`, README with GIF, ADDING_A_BROKER.md.
9. **Later**: NATS JetStream, Redis Streams, AWS SQS/SNS, Pulsar, MQTT; Replayer capability.

## Acceptance criteria for v0.1

- Every feature in "v0.1 features" works from the CLI and the TUI against both brokers via the
  docker-compose test stack (Kafka + Schema Registry + Connect + ksqlDB, RabbitMQ with management plugin).
- `mqx consumers <topic|queue>` shows who is attached, on both brokers.
- `mqx route <exchange> --key <key>` matches RabbitMQ's actual routing for direct, topic and fanout
  exchanges (verified against a real broker); headers and plugin exchanges are reported as "not simulated".
- `mqx group diagnose` detects, in integration tests: a stale member left by an unclean shutdown,
  more members than partitions, and one group id shared by apps subscribing to different topics.
- Bare `mqx` opens the TUI on a TTY and prints help when piped; `mqx tui --context/--topic/--group` deep-links.
- Mutating commands are refused on `read_only` contexts and require confirmation elsewhere.
- TUI shows only panels the active broker supports.
- Never commits secrets; credentials come from config file referencing env vars, not literals.
- `go vet`, `golangci-lint`, `go test ./...` pass in CI; integration tests run via testcontainers.
- Adding a broker needs no changes outside its own package plus one import.

## Conventions

- Clean Code, SOLID, idiomatic Go, small packages, accept interfaces and return structs.
- Context on every I/O call; no global mutable state except the broker registry.
- Errors wrapped with `%w`; user-facing errors are actionable.
- Table-driven tests; adapters tested against real brokers, UI logic tested as pure model updates.
- Conventional Commits.

## Kickoff prompt (paste into Claude Code)

```
Read CLAUDE.md. Implement milestone M0 only: repo skeleton, go.mod, folder layout, golangci-lint config,
GitHub Actions CI, config contexts (load/validate/use, read_only flag), and `mqx version` plus `mqx ctx list/use`.
Include unit tests. Before coding, show me a short plan and list any assumptions marked [ASSUMED]
that you want me to confirm. Stop after M0 and summarize.
```
