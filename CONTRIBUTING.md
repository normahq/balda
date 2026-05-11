# Contributing

Thanks for contributing to `relay`.

## Development Setup

1. Use Go `1.26.3` (see `go.mod`).
2. Clone the repository and fetch dependencies:

```bash
go mod tidy
```

3. Run relay locally when needed:

```bash
go run ./cmd/relay --help
```

## Required Quality Gates

Run before opening or updating a PR:

```bash
go test -race ./...
go tool golangci-lint run
```

## Code Standards

- Follow idiomatic Go and Google Go best practices.
- Prefer project-local tooling via `go tool ...` when available.
- Use Conventional Commits for commit messages.
- Sync shared branches with merge (`git pull --no-rebase`), not rebase.

## Logging Policy

- Allowed: `github.com/rs/zerolog`, `log/slog`.
- Disallowed: `logrus`, `zap`, direct standard `log` usage.
- Initialize logging through `internal/logging.Init()`.
- Prefer structured fields over formatted strings.

## Documentation Policy

- Keep `README.md` focused on installation and usage.
- Keep `docs/relay.md` focused on technical architecture/spec details.
- Keep `AGENTS.md` focused on agent workflow guardrails.
- If bot commands or config contracts change, update both `README.md` and `docs/relay.md`.
