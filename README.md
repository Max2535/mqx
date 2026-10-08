# mqx

Multi-broker message queue TUI/CLI. Inspect, peek and publish across Kafka and RabbitMQ through one interface.

> Work in progress: M0 provides config contexts only.

## Usage

```sh
go run ./cmd/mqx version
go run ./cmd/mqx ctx list
go run ./cmd/mqx ctx use <name>
```

Config lives at `~/.config/mqx/config.yaml` on every OS. Override it with `--config <path>` or `$MQX_CONFIG`.

```yaml
current-context: local-kafka
contexts:
  - name: local-kafka
    broker: kafka
    brokers: ["localhost:9092"]
  - name: dev-rabbit
    broker: rabbitmq
    url: amqp://dev-host:5672/
    management_url: http://dev-host:15672
    username_env: MQX_RABBIT_USER
    password_env: MQX_RABBIT_PASS
  - name: prod-kafka
    broker: kafka
    brokers: ["prod:9092"]
    read_only: true   # mutating commands will be refused
```

Credentials are never stored in the file. `username_env` and `password_env` name the environment
variables that hold them; a literal `password:` key or a password inside a URL is rejected.

## TUI preview (planned, M3)

> Mockup only. The TUI is not built yet; layout may change.

Panels follow broker capabilities. Kafka supports consumer lag, so the lag panel shows:

```text
┌ mqx ─────────────────────────────────────────────────────────────────────────┐
│ ctx: local-kafka ▾   broker: kafka   localhost:9092   ● connected            │
├ Topics ─────────────────┬ Messages: orders.created ──────────────────────────┤
│ > orders.created    3p  │  #  part  offset  key        time                  │
│   orders.paid       3p  │  1  0     10421   ord-8812   2026-10-08 09:14:02   │
│   payments.failed   1p  │> 2  1     10388   ord-8813   2026-10-08 09:14:05   │
│   users.signup      6p  │  3  2     9977    ord-8814   2026-10-08 09:14:09   │
│                         ├ Payload ───────────────────────────────────────────┤
│                         │ {                                                  │
│                         │   "order_id": "ord-8813",                          │
│                         │   "customer": "c-1029",                            │
│                         │   "total": 1250.00,                                │
│                         │   "currency": "THB"                                │
│                         │ }                                                  │
├ Consumer lag ───────────┴────────────────────────────────────────────────────┤
│ group billing-svc   part 0: 0   part 1: 12   part 2: 3     total: 15         │
└──────────────────────────────────────────────────────────────────────────────┘
 ↑/↓ move  enter open  p publish  c context  / filter  ? help  q quit
```

RabbitMQ has no lag reporter, so the topology panel shows instead:

```text
┌ mqx ─────────────────────────────────────────────────────────────────────────┐
│ ctx: dev-rabbit ▾   broker: rabbitmq   amqp://dev-host:5672/   ● connected   │
├ Queues ─────────────────┬ Topology ──────────────────────────────────────────┤
│ > orders.q        42    │  exchange orders.x (topic)                         │
│   payments.q       0    │    ├─ order.*    → orders.q                        │
│   dead-letter.q    7    │    └─ payment.#  → payments.q                      │
│                         │  exchange dlx (fanout)                             │
│                         │    └─ (all)      → dead-letter.q                   │
└─────────────────────────┴────────────────────────────────────────────────────┘
 ↑/↓ move  enter peek  p publish  c context  t topology  ? help  q quit
```

Publish form (`p`):

```text
┌ Publish → orders.created ──────────────────────────────┐
│ Key      [ ord-8815                                  ] │
│ Headers  [ source=mqx                                ] │
│ Payload                                                │
│ ┌────────────────────────────────────────────────────┐ │
│ │ {"order_id":"ord-8815","total":990}                │ │
│ └────────────────────────────────────────────────────┘ │
│                     [ Send ]  [ Cancel ]               │
└────────────────────────────────────────────────────────┘
```
