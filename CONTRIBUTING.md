# Contributing to mqx

Thanks for helping. This page covers how to build, test and send a change.

## Set up

You need Go 1.26 or newer. Docker is only needed for the integration tests and the
local broker stack.

```sh
git clone https://github.com/Max2535/mqx.git && cd mqx
go build -o mqx ./cmd/mqx
```

## Run the checks

These are the same checks CI runs.

```sh
go vet ./...
golangci-lint run ./...
go test -race ./...                  # unit tests
go test -tags integration ./...      # real brokers via testcontainers (needs Docker)
```

To try your build against real brokers:

```sh
docker compose up -d                 # Kafka, Schema Registry, Connect, ksqlDB, RabbitMQ
export MQX_CONFIG=$PWD/deploy/mqx-config.yaml MQX_RABBIT_USER=guest MQX_RABBIT_PASS=guest
./mqx ctx describe --context local-kafka
```

## Where things live

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): the core `Broker` interface, optional
  capabilities, the registry and the mutation guard.
- [docs/ADDING_A_BROKER.md](docs/ADDING_A_BROKER.md): how to add a broker. It is one
  package plus one import, with no changes to the core.
- `internal/broker/<name>/`: one adapter per broker.
- `internal/cli/` and `internal/tui/`: commands and the Bubble Tea interface.

## Guidelines

- **Capabilities, not a lowest common denominator.** A feature that only one broker
  has (Kafka lag, RabbitMQ topology) is an optional capability. The CLI and TUI show
  it only when the active broker implements it.
- **Mutations go through the guard.** Anything that changes a cluster must respect
  `read_only` contexts and ask for confirmation. Adapters never check this themselves.
- **No secrets in config.** Credentials are read from environment variables named in
  the config. Never add a literal password, token or URL with credentials.
- **Test against real brokers where it matters.** Adapter behavior is covered by
  integration tests. UI logic is tested as plain model updates.
- **Idiomatic Go.** Small packages, `context.Context` on every I/O call, errors wrapped
  with `%w` and written so the user can act on them.

## Commits and pull requests

Use [Conventional Commits](https://www.conventionalcommits.org). The release notes
are generated from them, grouped by `feat:` and `fix:`.

```text
feat(kafka): add partition reassignment
fix(tui): keep the cursor row after a refresh
docs: explain read_only contexts
```

Before you open a pull request:

1. Run the checks above.
2. Add or update tests for the change.
3. Describe what changed and why, and how you tested it. Include the broker and its
   version if the change touches an adapter.

For a larger change, such as a new broker or a new capability, please open an issue
first so the approach can be agreed before you write the code.

## Reporting bugs and asking for features

Use the issue templates. For bugs, include the output of `mqx version`, the broker
type and version, and the command you ran. Remove credentials and internal hostnames
from anything you paste.

## License

By contributing you agree that your contribution is licensed under the MIT License.
