# mqx

**Find out why your Kafka consumer group keeps rebalancing, and where your RabbitMQ
message actually goes, without leaving the terminal.**

[![CI](https://github.com/Max2535/mqx/actions/workflows/ci.yml/badge.svg)](https://github.com/Max2535/mqx/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/Max2535/mqx)](https://github.com/Max2535/mqx/releases)
[![License: MIT](https://img.shields.io/github/license/Max2535/mqx)](LICENSE)

```sh
brew install --cask max2535/tap/mqx
```

![mqx demo: group diagnose finds an idle consumer, route dry-run follows a routing key through the binding graph, a read-only context refuses a delete, and the TUI opens the group](docs/hero.gif)

mqx is one CLI and TUI for **Kafka and RabbitMQ**. It is a single binary with no
server, and it does what you usually open Kafka UI or the RabbitMQ management
page for: browse, peek, publish, inspect consumers, administer. It also does a few
things those tools don't.

## Why mqx

### 🩺 It tells you what is wrong with a consumer group

```console
$ mqx group diagnose fraud
[warning] more-members-than-partitions: 3 members share 2 partitions
  The subscribed topics have 2 partitions in total, so at most 2 of the 3 members get work; 1 are idle and only take over on failure.
  Fix: Scale the consumers down to 2, or add partitions with `mqx topic add-partitions <topic> --total N` (this changes key-to-partition mapping).
  Members: fraud-fc7d413e-1bdf-4bec-9c73-35f82f4a9966
```

`group diagnose` samples the group and reports findings with a fix for each:

- stale members left by an unclean shutdown
- more members than partitions
- one group id shared by apps that subscribe to different topics
- groups stuck rebalancing
- empty groups that still have lag
- partitions with no committed offset
- uneven assignments

`group watch` prints a timeline of state changes with rebalance durations. Both
work with the classic protocol and with KIP-848.

### 🧭 It shows where a routing key goes, without publishing

```console
$ mqx route orders.x --key order.created.eu
Queues: audit.q, orders.all, orders.eu

Route of key "order.created.eu":
  orders.x [topic]
    "#" -> exchange
      audit.x [fanout]
        "" -> queue audit.q
    "order.#" -> queue orders.all
    "order.*.eu" -> queue orders.eu
```

It follows direct, topic, fanout and default exchanges, exchange-to-exchange
bindings and alternate exchanges. Headers and plugin exchanges are reported as
"not simulated" rather than guessed.

### 🛡️ It won't let you break production by accident

```console
$ mqx topic delete orders --context prod-kafka --yes
Error: refusing to delete orders and all its messages: context "prod-kafka" is read_only; use a writable context with --context: context is read_only
```

Mark a context `read_only: true` and every mutating command is refused, in the
CLI and the TUI. Elsewhere, every mutation asks for confirmation (`--yes` in
scripts, a dialog whose default answer is Cancel in the TUI). Credentials come
from environment variables. A password in the config file is rejected.

### 💬 You can ask it questions in plain language

The TUI's **Assistant** panel answers questions like "why is billing lagging?" or
"where does order.created.eu go?". Claude answers by running **read-only** queries
against the broker, such as lag, `group diagnose` and route dry-runs, and you can
watch each query as it runs. It cannot change anything. When a change would help,
it gives you the `mqx` command to run. By default it sends only metadata, never
message payloads.

### 🔌 Your coding agent can use it too, over MCP

`mqx mcp` serves the same read-only tools to Claude Code, Claude Desktop, Cursor
or any other MCP client. Each server is bound to one context and never changes
the broker.

```sh
claude mcp add mqx-local -- mqx mcp --context local-kafka
```

Then ask your agent "is the billing group healthy?" and it runs `group_lag`
and `diagnose_group` itself. See [MCP server](docs/USAGE.md#mcp-server) for the
client config and the tool list.

## Try it in 60 seconds

No cluster needed: the repo ships a Docker Compose stack. Install mqx first (see
[Install](#install)).

```sh
git clone https://github.com/Max2535/mqx.git && cd mqx
docker compose up -d kafka rabbitmq
export MQX_CONFIG=$PWD/deploy/mqx-config.yaml MQX_RABBIT_USER=guest MQX_RABBIT_PASS=guest

mqx topic create orders --partitions 3 --yes
mqx publish orders --key ord-1 --value '{"total":990}' --yes
mqx peek orders
mqx                                   # open the TUI
```

`docker compose up -d` with no service names also starts Schema Registry, Kafka
Connect and ksqlDB.

## A TUI for everything the CLI does

![mqx TUI tour: command palette, publish form with recall, admin form hints, message viewer](docs/demo.gif)

Run `mqx` in a terminal and it opens a TUI with the same contexts as the CLI.

- **Panels match the broker.** The TUI shows only the panels the active broker
  supports: topics or queues, messages with pretty-printed JSON, consumers, groups
  and lag, topology with route dry-run, cluster, Schema Registry, Connect, KSQL,
  ACLs, users and policies, rate graphs, and the Assistant.
- **`:` opens a command palette.** It fuzzy-finds any action, row, panel or
  context.
- **Forms help you.** `ctrl+r` brings back earlier input, and admin forms warn
  while you type. For example, a replication factor above the broker count is
  flagged.
- **Deep links.** For example, `mqx tui --context prod --topic orders --group billing`.

## Everything else

| Area | Kafka | RabbitMQ |
|---|---|---|
| Browse | topics, partitions, configs, brokers | queues, exchanges, bindings, vhosts, connections, channels |
| Messages | non-destructive peek with key, header, value and time filters, follow mode, publish with key and headers, Avro, Protobuf and JSON Schema serde via Schema Registry | peek with get and requeue, stream queues by offset, publish with routing key, headers and properties |
| Consumers | groups, members, assignments, lag, reset offsets (with `--dry-run`), delete group, remove static member | consumers per queue, close connection |
| Admin | topic CRUD, configs, add partitions, delete records, Kafka Connect, ksqlDB, ACLs | queue and exchange CRUD, bindings (including exchange to exchange), purge, users, vhosts, permissions, policies, shovels, federation |
| Metrics | throughput and lag over time | message rates and depth over time |
| Scripting | `-o json` on every command | `-o json` on every command |

A few more commands:

```sh
mqx peek orders --from 1h --value '"total":\s*990' -o json | jq .value
mqx group reset-offsets billing --topic orders --to 2026-10-08T09:00:00Z --dry-run
mqx bind --source orders.x --destination audit.x --destination-type exchange --key '#' --yes
mqx metrics orders --interval 5s
```

**[→ Full usage guide](docs/USAGE.md)**: configuration, TUI keys, the Assistant and
every command.

## Install

| How | Command |
|---|---|
| Homebrew (macOS, Linux) | `brew install --cask max2535/tap/mqx` |
| Go 1.26+ | `go install github.com/Max2535/mqx/cmd/mqx@latest` |
| Binary | download from [Releases](https://github.com/Max2535/mqx/releases) (Linux, macOS, Windows; amd64 and arm64) |
| Source | `git clone https://github.com/Max2535/mqx.git && cd mqx && go build -o mqx ./cmd/mqx` |

## Configure

Contexts work like kubectl contexts. They live in `~/.config/mqx/config.yaml`
(override with `--config` or `$MQX_CONFIG`), and you can create them from the CLI
or the TUI.

```sh
mqx ctx add local --broker kafka --brokers localhost:9092
mqx ctx add prod --broker kafka --brokers k1:9093,k2:9093 --sasl-mechanism SCRAM-SHA-512 \
  --username-env PROD_KAFKA_USER --password-env PROD_KAFKA_PASS --tls-enabled --read-only
mqx ctx add rabbit --broker rabbitmq --url amqp://localhost:5672/ --management-url http://localhost:15672 \
  --username-env MQX_RABBIT_USER --password-env MQX_RABBIT_PASS
mqx ctx use prod
mqx ctx describe            # connectivity and what this broker supports
```

See [Configure](docs/USAGE.md#configure) for the file format, TLS, SASL and Schema
Registry, Connect and ksqlDB endpoints.

## How it fits with other tools

mqx doesn't replace the mature tools. It's for people who live in a terminal, use
both brokers, or want to debug consumers faster.

| Tool | Interface | Brokers | Server to run |
|---|---|---|---|
| **mqx** | CLI and TUI | Kafka and RabbitMQ | none |
| [kcat](https://github.com/edenhill/kcat) | CLI | Kafka | none |
| [AKHQ](https://github.com/tchiotludo/akhq) | web UI | Kafka | yes |
| [Kafka UI](https://github.com/kafbat/kafka-ui) | web UI | Kafka | yes |
| [RabbitMQ management](https://www.rabbitmq.com/docs/management) | web UI, HTTP API, `rabbitmqadmin` | RabbitMQ | plugin |

If your team needs a shared web UI with role-based access control, use one of the
web UIs. mqx runs on your machine or a jump host, with your own credentials.

## Contributing

Issues and PRs are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md).

- Adding a broker takes one package and one import. See
  [docs/ADDING_A_BROKER.md](docs/ADDING_A_BROKER.md).
- NATS JetStream, Redis Streams, SQS/SNS, Pulsar and MQTT are on the list.
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) explains the core interface,
  capabilities, registry and mutation guard.

```sh
go test ./...                        # unit tests
go test -tags integration ./...      # real brokers via testcontainers (needs Docker)
golangci-lint run ./...
vhs docs/hero.tape                   # re-record the README demo (see the seed steps at the top of the file)
```

If mqx saves you time, a ⭐ helps other people find it.

## License

MIT
