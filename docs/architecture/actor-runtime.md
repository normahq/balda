# Actor Runtime

Owner: Balda maintainers  
Status: active

## Invariants

- ActorRuntime consumes commands from JetStream only.
- Keyed actor lanes serialize mutable state by actor key.
- Command settlement happens after actor side effects complete.
- Retry/permanent failure handling is explicit and classified.
- Task actors attach role-based shell execution policy metadata (`none`, `read_only`, `workspace_write`) to runtime status surfaces.

## Related tests

- `internal/apps/balda/swarm/runtime_test.go`
- `internal/apps/balda/swarm/agents_test.go`
- `internal/apps/balda/handlers/swarm_session_actor_test.go`
- `internal/apps/balda/handlers/swarm_control_actor_test.go`
- `internal/apps/balda/handlers/swarm_delivery_actor_test.go`
- `internal/apps/balda/handlers/task_visibility_test.go`

## Related packages

- `internal/apps/balda/swarm`
- `internal/apps/balda/handlers/swarm_*.go`

## Update triggers

- Actor key mapping changes.
- Command heartbeat or settlement behavior changes.
- Task/agent/delivery actor lifecycle changes.
- Role tool/shell policy changes.
