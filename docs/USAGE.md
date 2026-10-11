# Using mqx

The full guide: configuration, the TUI, and every CLI command group. For a quick
overview see the [README](../README.md).

- [Configure](#configure)
- [TUI](#tui)
- [Messages](#messages)
- [Consumers and rebalances (Kafka)](#consumers-and-rebalances-kafka)
- [Routing (RabbitMQ)](#routing-rabbitmq)
- [Administration](#administration)
- [Metrics](#metrics)
- [Scripting](#scripting)
- [MCP server](#mcp-server)

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


## TUI

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
| `:` or `ctrl+p` | command palette: fuzzy-find any action of the current view, a row to jump to, a panel or a context |
| `tab` / `shift+tab` | next / previous panel |
| `↑` `↓` / `j` `k`, `enter`, `esc` | move, open, back |
| `/` | filter |
| `r` | refresh |
| `p` | publish |
| `c` | contexts: `enter` open, `t` test connection, `n` new, `e` edit, `d` delete, `u` set default |
| `?` | help, including the keys of the current panel |
| `q` | quit |

In forms, `ctrl+r` brings back what you submitted earlier in the same form (newest
first, secrets excluded), and key/value fields such as Headers accept either
`k=v` lines or a JSON object. Publishing to a topic the broker marks internal,
such as `__consumer_offsets`, asks you to type its name; the CLI needs
`--allow-internal`.

Admin forms hint as you type, and the confirm dialog repeats the hints. Examples:
a replication factor above the broker count (refused before it reaches Kafka),
replication factor 1 on a multi-broker cluster, `min.insync.replicas` above the
replication factor, a name that already exists or mixes `.` and `_`, adding
partitions to a keyed topic, and quorum or stream queues declared non-durable or
auto-delete.

Contexts edited in the TUI are saved to the config file. With no config file
yet, the TUI opens empty: press `c` then `n` to create the first context.

On `read_only` contexts mutating keys are hidden. Elsewhere, every mutation opens a
confirm dialog whose default answer is Cancel.

### Assistant

The last panel, **Assistant**, answers questions about the open context in plain
language: "why is billing lagging?", "which topics have a single replica?",
"where does order.created.eu go?". Claude answers by running read-only queries
against the broker (topics, partitions, nodes, groups, lag, `group diagnose`,
exchanges, bindings, route dry-runs), and you can watch each query as it runs.
It cannot change anything. When a change would help, it gives you the `mqx`
command to run, which then goes through the usual confirmation.

It needs `ANTHROPIC_API_KEY` (or `ant auth login`). Only metadata is sent to the
API by default; message keys, headers and values are sent only if you allow it:

```yaml
assistant:
  model: claude-opus-5-5   # default
  effort: medium           # low, medium (default), high, xhigh, max
  send_payloads: false     # true lets it peek at up to 20 messages
  disabled: false          # true removes the panel
```

## Messages

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

## Consumers and rebalances (Kafka)

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
- partitions with no committed offset
- uneven assignments

It works with both the classic protocol and KIP-848 (`group.protocol=consumer`).

## Routing (RabbitMQ)

```sh
mqx route orders.x --key order.created.eu
mqx exchanges
mqx bindings --source orders.x
mqx bind --source orders.x --destination audit.x --destination-type exchange --key '#' --yes
```

`route` resolves direct, topic, fanout and default exchanges,
exchange-to-exchange bindings and alternate exchanges. Headers and plugin
exchanges are reported as not simulated rather than guessed.

## Administration

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

## Metrics

```sh
mqx metrics orders --interval 5s     # messages in/out per second, lag or depth
mqx metrics --count 10 -o json
```

## Scripting

Every command accepts `-o json`, `--context` and `--timeout`.

Mutating commands work like this:

- On a terminal they ask `[y/N]`.
- In scripts they need `--yes`; without it they fail rather than block.
- On a `read_only` context they always fail, `--yes` or not.

## MCP server

`mqx mcp` serves one context's read-only broker tools over the
[Model Context Protocol](https://modelcontextprotocol.io) on stdio. MCP clients
such as Claude Code, Claude Desktop and Cursor can then query the broker for
you. These are the same tools the TUI Assistant uses.

```sh
mqx mcp --context prod-kafka                    # bound to prod-kafka
mqx mcp --context dev-rabbit --allow-payloads   # also offer peek_messages
```

The server:

- is bound to the context from `--context` or `current-context`, so an agent
  cannot switch to another cluster. To give it several clusters, run one
  server per context.
- never changes the broker. Every tool is annotated read-only. When a change
  would help, the agent is told to give you the `mqx` command, which still
  goes through `read_only` and confirmation.
- keeps message keys, headers and values private unless `--allow-payloads`
  adds `peek_messages` (at most 20 messages, values cut at 2,000 bytes).
- bounds each tool call with `--timeout` (default 30s).
- reads credentials from the environment variables your context names, so
  pass them in the client config.

| Tool | Kafka | RabbitMQ |
|---|---|---|
| `list_topics`, `describe_topic`, `topic_consumers`, `list_nodes` | ✅ | ✅ |
| `list_groups`, `describe_group`, `group_lag`, `diagnose_group` | ✅ | |
| `list_exchanges`, `list_bindings`, `route` | | ✅ |
| `peek_messages` (with `--allow-payloads`) | ✅ | ✅ |

The tools offered depend on what the broker supports.

### Claude Code

```sh
claude mcp add mqx-prod --env PROD_KAFKA_USER=... --env PROD_KAFKA_PASS=... \
  -- mqx mcp --context prod-kafka
```

Or check it into a project as `.mcp.json`:

```json
{
  "mcpServers": {
    "mqx-local": {
      "command": "mqx",
      "args": ["mcp", "--context", "local-kafka"]
    }
  }
}
```

### Claude Desktop, Cursor and other clients

Add the same `command` and `args` to the client's MCP config. For example, in
Claude Desktop's `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "mqx-prod": {
      "command": "/opt/homebrew/bin/mqx",
      "args": ["mcp", "--context", "prod-kafka", "--config", "/Users/me/.config/mqx/config.yaml"],
      "env": { "PROD_KAFKA_USER": "...", "PROD_KAFKA_PASS": "..." }
    }
  }
}
```

Desktop apps often start with a minimal `PATH`, so give the full path to `mqx`.
