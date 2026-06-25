# Actor Runtime

Owner: Balda maintainers  
Status: active

## Invariants

- ActorRuntime consumes durable command deliveries.
- Actorlayer engine lanes serialize mutable state by actor key.
- Runtime execution uses Balda's local `pkg/actorlayer/engine.DispatchRuntime`; Balda adapts durable command transport into actorlayer deliveries and supplies only Balda-specific delivery wrapping.
- Command settlement happens after actor side effects complete.
- Retry/permanent failure handling is explicit and classified.
- Product actors own Balda behavior: session turns, webhook/scheduled work routing, `/goal` execution, outbound delivery, and cancellation.
- `/goal` uses Balda's goal workflow wrapper built on Norma's reusable goal loop runtime.
- Task progress/results and projected task-event payload summaries redact common secret/token patterns before persistence and delivery.
- The execution core does not depend on Balda, Telegram, MCP, transport, or provider SDK APIs.

## Related tests

- `internal/apps/balda/swarm/runtime_test.go`
- `internal/apps/balda/actors/swarm_session_actor_test.go`
- `internal/apps/balda/actors/swarm_goal_actor_test.go`
- `internal/apps/balda/actors/swarm_control_actor_test.go`
- `internal/apps/balda/actors/swarm_delivery_actor_test.go`
- `internal/apps/balda/handlers/command_test.go`

## Related packages

- `internal/apps/balda/swarm`
- `internal/apps/balda/actors`
- `internal/apps/balda/handlers`

## Update triggers

- Actor key mapping changes.
- Command heartbeat or settlement behavior changes.
- Task/goal/delivery lifecycle changes.
- Goal workflow, session, or task-result behavior changes.

## Local actorlayer contract boundaries

### Contract shape

- Engine contract: Balda's local `pkg/actorlayer` is the fixed typed dispatch+state model Balda uses for actors through `actorengine.NewDispatchRuntime`. It exposes:
  - actor keying and deterministic lane routing,
  - typed envelope handling,
  - dispatch result states (`acked`, `running`, `in_progress`, `retry`, `deadletter`, `noop`),
  - lifecycle events suitable for external telemetry,
  - and generic transport-facing contracts for dispatch and event flow.
- Provider runtime: `balda.provider` selects the single app-scoped provider runtime used by all Balda sessions and `/goal` worker-validator runs; goal runs get isolated worker/validator ADK sessions and workspace state but reuse that same provider runtime/client ownership.
- Delivery boundary: Balda maps transport messages inside `eventbus/nats` into actorlayer `Source`/`Delivery` contracts; runtime and product actors never consume transport APIs directly.

### Ownership split

- Actorlayer owns:
  - generic actor mechanics: registration, addressing, deterministic lane execution, lifecycle state transitions, and delivery hooks.
- Balda product actor code owns:
  - product actor implementations in `internal/apps/balda/actors` for session, task, goal, delivery, and control behavior,
  - product command payloads/envelope builders consumed by ingress,
  - provider runtime invocation details (session execution, tools, model/runtime context),
  - task/session/delivery state transitions and user-visible outcomes.
- Balda transport/runtime code owns:
  - transport protocol and transport-level acknowledgements,
  - queue policy integration (retry/dead-letter thresholds, heartbeats, and backoff tuning),
  - and projection/read-model integration.

### Why this split exists

- All actor sessions in one Balda process use the configured `balda.provider`; actor contracts do not choose providers.
- Balda can own product semantics (queue policy, telemetry, task projection, and workspace/task metadata) while still reusing the same execution kernel.
- Future transport/provider integration code must preserve the local actorlayer engine contract and keep product policy in Balda.

### Balda implementation map

- Actor dispatch and lane execution live in `internal/apps/balda/swarm/runtime.go`, backed by `github.com/normahq/balda/pkg/actorlayer/engine.DispatchRuntime`.
- Balda product actor definitions live in `internal/apps/balda/actors` and are registered through `actors.Module`.
- Telegram/Zulip/Slack/webhook/scheduler ingress lives in `internal/apps/balda/handlers`; handlers publish actor commands through actorlayer transport contracts and do not own actor behavior or actor registration.
- Session/provider runtime ownership lives in `internal/apps/balda/agent` and `internal/apps/balda/session`; all sessions use the configured `balda.provider`.
- Command delivery and settlement live in `internal/apps/balda/eventbus/nats` behind actorlayer `Source`/`Delivery` and actorlayer transport contracts.
- The NATS adapter is the only concrete transport owner. It exposes small interfaces from one bus instance: actorlayer transport `Dispatcher`, `EventPublisher`, `EventConsumer`, `Drainer`, plus actorlayer `Source`.
- Task projection, retry classification, DLQ reporting, and task/read-model persistence live in Balda packages (`swarm`, `handlers`, and `state`), not in `pkg/actorlayer`.
