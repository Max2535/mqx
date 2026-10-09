## What and why

<!-- What does this change, and why? Link the issue if there is one. -->

## How I tested it

<!-- Commands run, brokers and versions used. -->

## Checklist

- [ ] `go vet ./...`, `golangci-lint run ./...` and `go test -race ./...` pass
- [ ] Tests added or updated (integration tests for adapter changes)
- [ ] Mutating actions respect `read_only` and ask for confirmation
- [ ] No credentials or internal hostnames in code, config or docs
- [ ] Commit messages follow Conventional Commits
