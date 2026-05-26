# Balda Architecture Map

Owner: Balda maintainers  
Status: active

Use this map to find the authoritative runtime contracts.

## Documents

- [Runtime contract](runtime-contract.md)
- [JetStream command bus](jetstream-command-bus.md)
- [Actor runtime](actor-runtime.md)
- [Projections and read models](projections-and-read-models.md)
- [Reliability](reliability.md)
- [Testing and evals](testing-and-evals.md)

## Invariants

- JetStream is the only command/event transport.
- SQLite is product/read-model state, not a command queue.
- Ingress handlers publish commands; actors execute commands.

## Related tests

- `internal/apps/balda/architecture_contract_test.go`
- `internal/apps/balda/eventbus/nats/connection_test.go`
- `internal/apps/balda/swarm/runtime_test.go`

## Related packages

- `internal/apps/balda/eventbus/nats`
- `internal/apps/balda/swarm`
- `internal/apps/balda/handlers`
- `internal/apps/balda/state`

## Update triggers

- New command/event subjects.
- Startup or transport lifecycle changes.
- Changes to retries, dedupe, or DLQ semantics.
- New ingress or actor execution paths.
