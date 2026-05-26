# Runtime Contract

Owner: Balda maintainers  
Status: active

## Invariants

- Startup order stays strict: config -> bundled MCP lifecycle -> balda provider -> channel runtime.
- JetStream must be available before ingress accepts work.
- No runtime path executes user work without a JetStream command publish ack.
- SessionActor is the only adapter that can enqueue TurnDispatcher work.
- SQLite does not own command selection, claim, retry, or wakeup semantics.

## Related tests

- `internal/apps/balda/architecture_contract_test.go`
- `internal/apps/balda/swarm/config_test.go`
- `internal/apps/balda/eventbus/config_test.go`

## Related packages

- `internal/apps/balda/app.go`
- `internal/apps/balda/handlers`
- `internal/apps/balda/swarm`
- `internal/apps/balda/eventbus`

## Update triggers

- Runtime startup wiring changes.
- Any command execution path change.
- New config keys that affect transport or execution mode.
