# Projections and Read Models

Owner: Balda maintainers  
Status: active

## Invariants

- SQLite task/command views are read models projected from durable events.
- Projection failures do not stop command execution.
- Projection handlers are idempotent.
- Internal task/read-model views read product state + projections; they are not chat commands.

## Related tests

- `internal/apps/balda/swarm/tasks_test.go`
- `internal/apps/balda/handlers/command_test.go`
- `internal/apps/balda/memory/store_test.go`

## Related packages

- `internal/apps/balda/swarm`
- `internal/apps/balda/state`
- `internal/apps/balda/handlers`

## Update triggers

- Event schema/version changes.
- Read-model schema changes.
- New internal projection consumers or operator inspection tooling.
