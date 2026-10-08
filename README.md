# mqx

One CLI and TUI for Kafka and RabbitMQ. Inspect, peek, publish, debug consumers and
administer clusters through one interface, without a web UI.

![mqx demo: CLI peek, route dry-run, read-only refusal and the TUI](docs/demo.gif)

- **Browse**: topics, partitions, configs and brokers; queues, exchanges, bindings,
  vhosts, connections and channels.
- **Messages**: non-destructive peek with key, value, header and time filters, and
  publishing with keys, headers and properties. Schema Registry payloads
  (Avro, Protobuf, JSON Schema) are decoded and encoded.
- **Consumers**: groups, members, assignments and lag. You can reset offsets, delete
  groups, remove static members and close connections.
- **Rebalance debugging**: `mqx group describe | watch | diagnose`.
- **Routing dry-run**: `mqx route` shows which queues a routing key reaches,
  without publishing anything.
- **Admin**: topic and queue CRUD, configs, partitions, purge and delete-records,
  exchanges and bindings (including exchange to exchange), Kafka Connect, ksqlDB,
  ACLs, RabbitMQ users, vhosts, permissions, policies, shovels and federation.
- **Metrics**: message rates, lag and depth over time.
- **Safety**: contexts can be `read_only`. Every mutating action needs confirmation.

## Install

With [Homebrew](https://brew.sh) on macOS or Linux:

```sh
brew install --cask max2535/tap/mqx
```

Or download a binary for Linux, macOS or Windows (amd64 or arm64) from
[Releases](https://github.com/Max2535/mqx/releases), unpack it and put `mqx` on your `PATH`.

Or install with Go 1.26 or newer:

```sh
go install github.com/Max2535/mqx/cmd/mqx@latest
```

Or build from source:

```sh
git clone https://github.com/Max2535/mqx.git && cd mqx
go build -o mqx ./cmd/mqx
```

## Configure

Contexts live in `~/.config/mqx/config.yaml` on every OS. Override the location with
`--config <path>` or `$MQX_CONFIG`.

```yaml
current-context: local-kafka
contexts:
  - name: local-kafka
    broker: kafka
    brokers: ["localhost:9092"]
    schema_registry: { url: "http://localhost:8081" }
    connect: { url: "http://localhost:8083" }
    ksqldb: { url: "http://localhost:8088" }

  - name: prod-kafka
    broker: kafka
    brokers: ["kafka-1.prod:9093", "kafka-2.prod:9093"]
    sasl_mechanism: SCRAM-SHA-512        # PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
    username_env: PROD_KAFKA_USER
    password_env: PROD_KAFKA_PASS
    tls: { enabled: true, ca_file: /etc/ssl/prod-ca.pem }
    read_only: true                      # every mutating command is refused

  - name: dev-rabbit
    broker: rabbitmq
    url: amqp://dev-host:5672/orders     # the path is the vhost
    management_url: http://dev-host:15672
    username_env: MQX_RABBIT_USER
    password_env: MQX_RABBIT_PASS
```

Credentials are never stored in the file. `username_env` and `password_env` name
the environment variables that hold them. A literal `password:` key, or a password
inside a URL, is rejected.

Create and change contexts from the CLI or the TUI instead of editing the file.
Either way the file keeps its comments, and the result is validated before it
is saved.

```sh
mqx ctx add local --broker kafka --brokers localhost:9092 --schema-registry-url http://localhost:8081
mqx ctx add prod --broker kafka --brokers k1:9093,k2:9093 --sasl-mechanism SCRAM-SHA-512 \
  --username-env PROD_KAFKA_USER --password-env PROD_KAFKA_PASS --tls-enabled --read-only
mqx ctx set prod --tls-ca-file /etc/ssl/prod-ca.pem   # only the flags given change
mqx ctx delete old-kafka --yes

mqx ctx list                 # * marks the current context
mqx ctx use dev-rabbit
mqx ctx describe             # connectivity and what this broker supports
mqx topics --context prod-kafka
```

## Use it

### TUI

Run `mqx` with no arguments in a terminal to open the TUI. When stdout is piped it
prints help instead, so it never blocks a script.

```sh
mqx                                        # current context
mqx tui --context prod-kafka --topic orders --group billing   # deep link
```

The TUI shows only the panels the active broker supports: topics or queues, a
message browser with pretty-printed JSON, a publish form, consumers, groups with
lag, topology with a route dry-run, connections, cluster, Schema Registry, Connect,
KSQL, ACLs, users and policies, and rate graphs.

| Key | Action |
|---|---|
| `tab` / `shift+tab` | next / previous panel |
| `↑` `↓` / `j` `k`, `enter`, `esc` | move, open, back |
| `/` | filter |
| `r` | refresh |
| `p` | publish |
| `c` | contexts: `enter` open, `t` test connection, `n` new, `e` edit, `d` delete, `u` set default |
| `?` | help, including the keys of the current panel |
| `q` | quit |

Contexts edited in the TUI are saved to the config file. With no config file
yet, the TUI opens empty: press `c` then `n` to create the first context.

On `read_only` contexts mutating keys are hidden. Elsewhere, every mutation opens a
confirm dialog whose default answer is Cancel.

### Messages

```sh
mqx peek orders                                  # first 20 messages
mqx peek orders --from -5                        # last 5 per partition
mqx peek orders --from 1h -n 0                   # everything from the last hour
mqx peek orders --key '^ord-88' --header source=mqx --value '"total":\s*990'
mqx peek orders -f                               # follow new messages
mqx peek orders -o json | jq .value              # one JSON object per line

mqx publish orders --key ord-1 -H source=mqx --value '{"total":990}' --yes
mqx publish orders --lines --key-separator : --yes < orders.txt
mqx publish orders --schema-subject orders-value --value '{"id":1}' --yes
mqx publish orders.q --property content_type=application/json --value '{}' --yes
mqx publish x --exchange orders.x --routing-key order.created --value '{}' --yes
```

Peeking never consumes messages. On Kafka, mqx reads with a standalone consumer
that never joins a group or commits offsets. On RabbitMQ, it gets messages with
manual ack and requeues them all, which leaves their order intact and sets their
redelivered flag. Stream queues are read by offset.

### Consumers and rebalances (Kafka)

```sh
mqx consumers orders                 # who is attached (also works for RabbitMQ queues)
mqx groups
mqx group describe billing           # state, coordinator, assignor, members, assignments
mqx group lag billing
mqx group watch billing              # timeline of state changes with rebalance durations
mqx group diagnose billing           # findings with fixes
mqx group reset-offsets billing --topic orders --to earliest --dry-run
mqx group reset-offsets billing --topic orders --to 2026-10-08T09:00:00Z --yes
mqx group remove-member billing --instance-id billing-0 --yes
```

`group diagnose` detects:

- stale members left by an unclean shutdown
- more members than partitions
- one group id shared by apps that subscribe to different topics
- rebalancing groups
- empty groups that still have lag
- uneven assignments

It works with both the classic protocol and KIP-848 (`group.protocol=consumer`).

### Routing (RabbitMQ)

```sh
mqx route orders.x --key order.created.eu
mqx exchanges
mqx bindings --source orders.x
mqx bind --source orders.x --destination audit.x --destination-type exchange --key '#' --yes
```

`route` resolves direct, topic, fanout and default exchanges,
exchange-to-exchange bindings and alternate exchanges. Headers and plugin
exchanges are reported as not simulated rather than guessed.

### Administration

```sh
mqx topic create orders --partitions 6 --replication-factor 3 --set retention.ms=86400000 --yes
mqx queue create orders.q --type quorum --arg x-max-length=10000 --yes
mqx topic alter-config orders --set retention.ms=3600000 --yes
mqx topic add-partitions orders --total 12 --yes
mqx topic delete-records orders --before 2026-10-01T00:00:00Z --yes
mqx purge orders.q --yes

mqx schema subjects                  # Schema Registry
mqx connect list                     # Kafka Connect
mqx ksql "SHOW STREAMS;"             # ksqlDB
mqx acl list --principal User:billing

mqx user create billing-app --password-env APP_PW --yes
mqx permission set billing-app --vhost / --configure '' --write '^orders' --read '^orders' --yes
mqx policy set ttl --pattern '^tmp\.' --apply-to queues --definition message-ttl=60000 --yes
mqx shovel create mirror --src-uri-env SRC_URI --src-queue a --dest-uri-env DEST_URI --dest-queue b --yes
mqx federation upstreams
```

### Metrics

```sh
mqx metrics orders --interval 5s     # messages in/out per second, lag or depth
mqx metrics --count 10 -o json
```

### Scripting

Every command accepts `-o json`, `--context` and `--timeout`.

Mutating commands work like this:

- On a terminal they ask `[y/N]`.
- In scripts they need `--yes`; without it they fail rather than block.
- On a `read_only` context they always fail, `--yes` or not.

## Try it locally

```sh
docker compose up -d                 # Kafka, Schema Registry, Connect, ksqlDB, RabbitMQ
export MQX_CONFIG=$PWD/deploy/mqx-config.yaml MQX_RABBIT_USER=guest MQX_RABBIT_PASS=guest
mqx ctx describe --context local-kafka
mqx --context local-rabbit
```

## Develop

```sh
go test ./...                        # unit tests
go test -tags integration ./...      # against real brokers via testcontainers (needs Docker)
golangci-lint run ./...
```

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) explains the core interface,
  capabilities, registry and guard.
- [docs/ADDING_A_BROKER.md](docs/ADDING_A_BROKER.md) explains how to add a broker:
  one package plus one import.

## License

MIT
