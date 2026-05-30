# Runtime Contract

Owner: Balda maintainers  
Status: active

## Invariants

- Startup order stays strict: config -> bundled MCP lifecycle -> balda provider -> channel runtime.
- JetStream must be available before ingress accepts work.
- No runtime path executes user work without durable actor dispatch acceptance.
- SessionActor is the only actor that can enqueue TurnDispatcher work.
- SQLite does not own command selection, claim, retry, or wakeup semantics.
- Runtime boundaries are strict and explicit: ingress publishes through `ActorDispatcher`, actor execution and delivery settlement flow through Norma actorlayer contracts, and concrete JetStream policy stays in Balda's NATS adapter.
- Balda owns queue, retry exhaustion, dead-letter side effects, projection writes, and command visibility telemetry.
- Norma actorlayer is a typed engine only: it can receive commands/deliveries and emit events, but does not make Balda-specific product policy decisions.

## Boundary contract

- Norma actorlayer core:
  - Typed command envelopes and actor keys.
  - Per-key deterministic lanes.
  - Delivery lifecycle hooks (accept/running/in_progress/acked/retry/deadletter/noop).
  - Actor dispatch and state transition primitives, including the dispatch runtime that owns address resolution and lane execution.
  - No Balda provider selection, queue runtime, Telegram, MCP, or task projection policy.

- Balda integration layer (policy owner):
  - Product actor implementations and command contracts in `internal/apps/balda/actors`.
  - Telegram, webhook, and scheduler ingress in `internal/apps/balda/handlers`; ingress publishes commands and does not register product actors.
  - Concrete JetStream adapter semantics: command stream, ack/nak/term behavior, heartbeats, in-progress redelivery, exposed upward only as actorlayer source/delivery and small Balda-facing dispatch/event interfaces.
  - Retry strategy and classification, dead-letter promotion logic, and DLQ reporting.
  - Task/projector side effects in SQLite (`swarm_tasks`, `swarm_task_events`, command/task status state).
  - Internal command visibility backed by logs and tooling.
  - Mapping between policy metadata (`chat_id`, `topic_id`, `goal_id`, `attempt`) and actor-level envelopes.
  - The single app-scoped ADK provider runtime selected by `balda.provider`.

- Boundary obligations:
  - Actor definitions and actor state must not select or branch on provider IDs.
  - Provider-specific types stay outside actorlayer-facing contracts.
  - JetStream settlement is hidden behind actorlayer delivery methods and exposes the same lifecycle outcomes regardless of command kind.

## Related tests

- `internal/apps/balda/swarm/config_test.go`
- `internal/apps/balda/eventbus/config_test.go`
- `internal/apps/balda/architecture_contract_test.go`

## Related packages

- `internal/apps/balda/app.go`
- `internal/apps/balda/actors`
- `internal/apps/balda/handlers`
- `internal/apps/balda/swarm`
- `internal/apps/balda/eventbus`

## Update triggers

- Runtime startup wiring changes.
- Any command execution path change.
- New config keys that affect transport or execution mode.
