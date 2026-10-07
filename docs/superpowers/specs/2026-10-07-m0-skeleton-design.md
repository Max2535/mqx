# M0 Skeleton — Design

Date: 2026-10-07 (revised 2026-10-08: v0.1 scope widened, `read_only` added) · Status: approved in brainstorming, pending spec review · Source of truth: `CLAUDE.md`

## Goal

Lay the foundation every later milestone builds on: module, layout, config contexts with
env-var-only credentials and a per-context `read_only` flag, `mqx version`, `mqx ctx list`,
`mqx ctx use`, lint and CI.
Out of scope: broker adapters, registry, mutation guard, TUI, goreleaser (M1+).

## Confirmed decisions

| # | Decision |
|---|---|
| 1 | v0.1 = feature parity with Kafka UI and the RabbitMQ Management UI, incl. consumer management (revised 2026-10-08; see `CLAUDE.md`). M0 itself is unaffected except decision 6. |
| 2 | Credentials via dedicated `username_env` / `password_env` fields. Literal secrets are rejected. |
| 3 | M0 creates only `cmd/mqx`, `internal/config`, `internal/cli` (plus `docs/` for this spec). Other layout dirs arrive with their milestone. |
| 4 | Config path `~/.config/mqx/config.yaml` on every OS. Override order: `--config` > `$MQX_CONFIG` > default. No XDG handling. |
| 5 | golangci-lint v2 installed locally with `go install …@<pinned>`; CI pins the same version. |
| 6 | Each context has an optional `read_only` bool (default false). M0 stores, validates the type of, and displays it; the guard that enforces it arrives with the first mutating command (M3). |

## Layout

```
cmd/mqx/main.go           signal.NotifyContext → cli.Execute(ctx) → os.Exit(code)
internal/config/
  config.go               types, DefaultPath, Load, Save, Use
  validate.go             Validate, literal-secret detection
  resolve.go              Context.Resolve → Credentials via os.LookupEnv
internal/cli/
  root.go                 NewRootCmd(opts), --config flag, Execute
  version.go              version/commit/date vars (ldflags) + debug.ReadBuildInfo fallback
  ctx.go                  ctx list, ctx use
.golangci.yml
.github/workflows/ci.yml
```

## Config file

```yaml
current-context: local-kafka
contexts:
  - name: local-kafka
    broker: kafka                 # kafka | rabbitmq
    brokers: ["localhost:9092"]   # kafka: required, >= 1
  - name: dev-rabbit
    broker: rabbitmq
    url: amqp://dev-host:5672/    # rabbitmq: required; must not embed a password
    management_url: http://dev-host:15672
    username_env: MQX_RABBIT_USER
    password_env: MQX_RABBIT_PASS
  - name: prod-kafka
    broker: kafka
    brokers: ["prod-1:9092", "prod-2:9092"]
    read_only: true               # mutating commands refused (enforced from M3)
```

## `internal/config` API

```go
type Config struct {
    CurrentContext string    `yaml:"current-context"`
    Contexts       []Context `yaml:"contexts"`
}

type Context struct {
    Name          string   `yaml:"name"`
    Broker        string   `yaml:"broker"`
    Brokers       []string `yaml:"brokers,omitempty"`
    URL           string   `yaml:"url,omitempty"`
    ManagementURL string   `yaml:"management_url,omitempty"`
    UsernameEnv   string   `yaml:"username_env,omitempty"`
    PasswordEnv   string   `yaml:"password_env,omitempty"`
    ReadOnly      bool     `yaml:"read_only,omitempty"`
}

type Credentials struct{ Username, Password string }

func DefaultPath() (string, error)               // $MQX_CONFIG, else ~/.config/mqx/config.yaml
func Load(path string) (*Config, error)          // missing file: error wraps fs.ErrNotExist
func (c *Config) Save(path string) error         // mkdir 0700, temp file + rename, mode 0600
func (c *Config) Validate() error                // errors.Join of every problem
func (c *Config) Use(name string) error          // sets CurrentContext; unknown name is an error
func (c Context) Resolve() (Credentials, error)  // reads referenced env vars
```

Notes:

- `Load` decodes with `KnownFields(true)`, so unknown keys (including a literal `password:` or
  `username:`) fail with an error that names the `_env` alternative.
- Known broker types are a package-level set `{kafka, rabbitmq}` for now, marked with a
  `ponytail:` comment; M1's registry replaces it.
- `Load`/`Save` take no `context.Context`: local file I/O is not cancellable and stdlib has no
  context-aware variant. Context starts at broker I/O in M1.
- No global mutable state; `--config` reaches commands through an options struct.

## Validation rules

All violations are collected and returned together via `errors.Join`.

1. Context `name` non-empty and unique.
2. `broker` non-empty and in the known set.
3. kafka: `brokers` has at least one entry. rabbitmq: `url` non-empty.
4. `url` and `management_url` must not contain a userinfo password (`u.User.Password()`).
5. `current-context`, if set, must name an existing context. Empty is valid (none selected).

Validation never reads environment variables, so `ctx list` works without credentials set.

## Commands

| Command | Behaviour |
|---|---|
| `mqx version` | Prints `mqx <version> (commit <sha>, built <date>, <goos>/<goarch>)`. Never reads config. |
| `mqx ctx list` | Missing file: hint on stderr, exit 0. Prints `CURRENT NAME BROKER MODE ENDPOINT` table, `*` on current, MODE `ro` or `rw`. Invalid config: prints table, then problems, exit 1. |
| `mqx ctx use <name>` | Missing file: error, exit 1. Invalid config or unknown name: error, file unchanged. Success: atomic save, prints `Switched to context "<name>".` |

## Error handling

- Wrap with `%w` at each layer (`load config %q: %w`), so callers can use `errors.Is`.
- Every user-facing message states the fix, e.g.
  `context "x" not found; available: a, b`,
  `context "prod": password must not be a literal; use password_env`,
  `context "prod": env var PROD_KAFKA_PASS (password_env) is not set`.
- Secret values are never printed. `Resolve` is implemented and tested in M0 but first called in M1.
- Exit codes: 0 success, 1 any error. Errors go to stderr; usage is printed only for argument errors.

## Testing

Stdlib `testing`, table-driven, no testify.

- config: Load (valid, missing, malformed, literal `password` key, `read_only` round-trip); every Validate rule plus a
  multi-error case; Save→Load round-trip and 0600 mode (perm check skipped on Windows); Use;
  Resolve with `t.Setenv`; DefaultPath with and without `$MQX_CONFIG`.
- cli: in-process `NewRootCmd` with `SetArgs`/`SetOut`/`SetErr` against a `t.TempDir()` config,
  covering version, ctx list (current marker, `ro`/`rw` mode, missing file), and ctx use (success changes file;
  unknown name errors and leaves file unchanged).
- Smoke: `go run ./cmd/mqx version|ctx list|ctx use` against a temp config.

## Lint and CI

`.golangci.yml` (v2): `linters.default: standard` plus errorlint, gocritic, misspell, revive,
unconvert, bodyclose; formatters gofmt and goimports.

`.github/workflows/ci.yml`: triggers on `pull_request` and push to `main`; `permissions: contents: read`.

- `lint` (ubuntu-latest): setup-go from `go.mod`, `go vet ./...`, golangci-lint-action pinned to the local version.
- `test` (matrix ubuntu-latest, windows-latest): `go test -race ./...`.

## Toolchain and dependencies

- `go.mod`: module `github.com/Max2535/mqx`, `go 1.24`.
- Dependencies: `github.com/spf13/cobra`, `gopkg.in/yaml.v3` only.

## Delivery

Branch `m0-skeleton`, small Conventional Commits, PR to `main` (never push to `main`).
README gets a short usage section only. Done when vet, golangci-lint and tests pass locally,
the smoke test works, and CI is green on the PR.
