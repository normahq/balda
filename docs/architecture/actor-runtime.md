# Actor Runtime

Owner: Balda maintainers  
Status: active

## Invariants

- ActorRuntime consumes commands from JetStream only.
- Actorlayer engine lanes serialize mutable state by actor key.
- Command settlement happens after actor side effects complete.
- Retry/permanent failure handling is explicit and classified.
- Task actors attach role-based shell execution policy metadata (`none`, `read_only`, `workspace_write`) to runtime status surfaces.
- Role-level allowed tool contracts are stable and inspectable (`planner=none`, `executor=workspace,shell,mcp`, `reviewer=workspace,shell`, `memory=memory`).
- Workspace access boundaries are role-based and inspectable (`none`, `read_only`, `read_write`).
- Agent commands that request tools outside role policy are rejected before runtime execution.
- Task progress/results and task visibility payload summaries redact common secret/token patterns before persistence and delivery.

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
