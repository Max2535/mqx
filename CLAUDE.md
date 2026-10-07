# mqx — Multi-broker Message Queue TUI/CLI (Go)

> Project brief for Claude Code. Repo was created empty (README + MIT LICENSE only).
> Items marked **[ASSUMED]** were not explicitly confirmed by the owner. Ask the owner to confirm them before building on them.

## Goal

A single Go binary to inspect, publish, peek and (later) replay messages across message brokers
through one interface. Interactive TUI plus non-interactive CLI. Target audience: backend developers
debugging event-driven systems. Aim: genuinely useful, easy to install, easy to extend with new brokers.

## Decisions

- Language: Go (latest stable, 1.22+). Single static binary.
- Repo: new repo, separate from `team-stack`.
- Project name: `mqx` (repo: Max2535/mqx)
- License: MIT (LICENSE file already in repo)
- v0.1 brokers: Kafka + RabbitMQ. Design for more from day one.
- v0.1 features **[ASSUMED]**: list topics/queues, peek messages, publish, Kafka consumer lag,
  RabbitMQ topology (exchanges/bindings/queues).
- Interfaces: TUI and non-interactive CLI, kubectl-style named contexts **[ASSUMED]**.

## Libraries

| Concern | Choice |
|---|---|
| TUI | Bubble Tea + Bubbles + Lip Gloss |
| CLI | cobra |
| Kafka | franz-go (`kgo`, `kadm`) |
| RabbitMQ | amqp091-go (data) + Management HTTP API (topology/stats) |
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

type LagReporter interface {
    ConsumerLag(ctx context.Context, group string) ([]Lag, error)
}

type TopologyInspector interface {
    Topology(ctx context.Context) (Topology, error)
}

type Replayer interface { // post v0.1
    Replay(ctx context.Context, topic string, from Position, to string) error
}
```

Registry pattern: each adapter registers a factory in `init()` keyed by broker type, so adding a broker
means adding one package and one blank import.

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

1. **M0 Skeleton**: go.mod, layout, lint, CI, config contexts, `mqx version`, `mqx ctx list/use`.
2. **M1 Kafka adapter**: Ping, ListTopics, Publish, Peek, ConsumerLag + CLI commands + integration tests.
3. **M2 RabbitMQ adapter**: same core plus Topology via Management API + tests.
4. **M3 TUI**: context switcher, topic list, message viewer (JSON pretty-print), publish form, capability-driven panels.
5. **M4 Release**: goreleaser, Homebrew tap, `go install`, README with GIF, ADDING_A_BROKER.md.
6. **Later**: NATS JetStream, Redis Streams, AWS SQS/SNS, Pulsar, MQTT; Replayer capability.

## Acceptance criteria for v0.1

- `mqx topics`, `mqx peek <topic>`, `mqx publish <topic>`, `mqx lag <group>` work against both brokers
  via docker-compose test stack.
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
GitHub Actions CI, config contexts (load/validate/use), and `mqx version` plus `mqx ctx list/use`.
Include unit tests. Before coding, show me a short plan and list any assumptions marked [ASSUMED]
that you want me to confirm. Stop after M0 and summarize.
```
