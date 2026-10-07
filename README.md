# mqx

Multi-broker message queue TUI/CLI. Inspect, peek and publish across Kafka and RabbitMQ through one interface.

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
