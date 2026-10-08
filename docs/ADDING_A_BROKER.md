# Adding a broker

Adding a broker touches one new package plus one import. Nothing else changes:
not the config, the CLI or the TUI.

## 1. Create the package

```
internal/broker/nats/
  nats.go            // Type, init(), validate, open
  client.go          // the Broker implementation
  ...                // one file per capability
  nats_test.go
  integration_test.go  // //go:build integration
```

## 2. Register a driver

```go
package nats

import (
    "context"
    "errors"

    "github.com/Max2535/mqx/internal/broker"
    "github.com/Max2535/mqx/internal/config"
)

// Type is the broker type name used in config files.
const Type = "nats"

func init() {
    broker.Register(Type, broker.Driver{Open: open, Validate: validate})
}

// validate checks broker-specific fields without network access. The caller
// prefixes errors with the context name. Never echo a value that may hold a secret.
func validate(c config.Context) []error {
    if c.URL == "" {
        return []error{errors.New("nats needs url")}
    }
    return nil
}

func open(ctx context.Context, c config.Context, creds config.Credentials) (broker.Broker, error) {
    // Connect with c.URL / c.Brokers, c.TLS, creds.Username / creds.Password.
    // Read anything broker-specific from c.Options.
    ...
}
```

Use the generic context fields (`url`, `brokers`, `username_env`,
`password_env`, `tls`) where they fit, and `options:` for the rest:

```yaml
- name: local-nats
  broker: nats
  url: nats://localhost:4222
  options:
    jetstream_domain: hub
```

## 3. Implement the core, then capabilities

Implement `broker.Broker` (`Name`, `Ping`, `ListTopics`, `Publish`, `Peek`,
`Close`). Then implement whichever capability interfaces from
`internal/broker/capabilities.go` the broker can genuinely back. Do not fake a
capability: the CLI and TUI show features only for capabilities that exist.

Assert what you implement at compile time:

```go
var (
    _ broker.Broker            = (*NATS)(nil)
    _ broker.ConsumerInspector = (*NATS)(nil)
    _ broker.Purger            = (*NATS)(nil)
)
```

If your type implements a capability whose backing service may be absent,
implement `broker.CapabilityChecker` to hide it at runtime.

Rules:

- `Peek` must not consume or acknowledge messages for real, and must apply
  `opts.Filter` before counting `opts.Limit`. Options that do not apply return
  an error wrapping `broker.ErrUnsupported` with a hint.
- Wrap `broker.ErrNotFound` for missing topics, queues and groups.
- Errors are wrapped with `%w` and say what to do next.
- Never check `read_only` or ask for confirmation: the CLI and TUI guard does that.
- Never log or return credentials.
- Take a `context.Context` on every I/O call.

## 4. Wire it in

Add one blank import to `cmd/mqx/main.go`:

```go
_ "github.com/Max2535/mqx/internal/broker/nats"
```

`mqx ctx describe --context local-nats` now lists the capabilities the TUI and
CLI will offer.

## 5. Test it

- Unit tests for pure logic (parsers, matchers, mapping).
- Integration tests under `//go:build integration` against a real broker from
  testcontainers-go, covering every capability you implemented. Put reusable
  container setup in `internal/testutil/<broker>test`.
- Add the broker to `docker-compose.yml` and a context to
  `deploy/mqx-config.yaml`.
