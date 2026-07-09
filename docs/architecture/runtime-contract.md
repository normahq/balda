# Runtime Contract

Owner: Balda maintainers  
Status: active

## Invariants

- Startup order stays strict: config -> bundled MCP lifecycle -> balda provider -> channel runtime.
- The durable command runtime must be available before ingress accepts work.
- No runtime path executes user work without durable actor dispatch acceptance.
- The session-turn execution path is the only code path that can enqueue `TurnDispatcher` work.
- SQLite does not own command selection, claim, retry, or wakeup semantics.
- Runtime boundaries are strict and explicit: ingress publishes through actorlayer transport dispatcher contracts, actor execution and delivery settlement flow through Balda's local actorlayer contracts, and concrete transport policy stays in Balda's NATS adapter.
- Balda owns queue, retry exhaustion, dead-letter side effects, projection writes, and command visibility telemetry.
- Balda keeps that ownership inside `internal/apps/balda/execution` and `internal/apps/balda/jobs` through internal runtime decomposition: host loop, lane policy, heartbeat policy, dead-letter policy, and delivery wrapping remain Balda runtime code even when they use generic actorlayer contracts.
- The local `pkg/actorlayer` owns generic envelopes, retry/error helpers, runtime primitives, and transport-facing contracts, but does not make Balda-specific product policy decisions.

## Boundary contract

- Local actorlayer core:
  - Typed command envelopes and actor keys.
  - Per-key deterministic lanes.
  - Delivery lifecycle hooks (accept/running/in_progress/acked/retry/deadletter/noop).
  - Actor dispatch and state transition primitives, including the dispatch runtime that owns address resolution and lane execution.
  - Transport-facing interfaces for dispatch, event publication/consumption, and draining.
  - No Balda provider selection, queue runtime, Telegram, MCP, or job projection policy.

- Balda integration layer (policy owner):
  - Product actor implementations and command contracts in `internal/apps/balda/actors`.
  - Telegram, Slack, Zulip, webhook, and scheduler ingress in `internal/apps/balda/handlers`; ingress publishes commands and does not register product actors.
  - Concrete transport adapter semantics: command stream, ack/nak/term behavior, heartbeats, in-progress redelivery, exposed upward only as actorlayer source/delivery and small Balda-facing dispatch/event interfaces.
  - Retry strategy and classification, dead-letter promotion logic, and DLQ reporting.
  - Job/projector side effects in SQLite (legacy `runtime_*` tables plus command/job status state) for job-style orchestration and read models.
  - Internal command visibility backed by logs and tooling.
  - Mapping between policy metadata (`chat_id`, `topic_id`, `goal_id`, `attempt`) and actor-level envelopes.
  - The single app-scoped provider runtime selected by `balda.provider`.

- Internal Balda runtime decomposition:
  - `runtime.go`: host loop and dispatch-runtime wiring.
  - `runtime_lane_policy.go`: Balda actor addressing and lane-key policy.
  - `runtime_heartbeat.go`: Balda heartbeat cadence and in-progress visibility publication.
  - `runtime_deadletter.go`: Balda retry-exhaustion and job dead-letter side effects.
  - `runtime_delivery.go`: Balda delivery wrapping and envelope-context attachment.

- Boundary obligations:
  - Actor definitions and actor state must not select or branch on provider IDs.
  - Provider-specific types stay outside actorlayer-facing contracts.
  - Transport settlement is hidden behind actorlayer delivery methods and exposes the same lifecycle outcomes regardless of command kind.

## Related tests

- `internal/apps/balda/execution/config_test.go`
- `internal/apps/balda/eventbus/config_test.go`
- `internal/apps/balda/architecture_contract_test.go`

## Related packages

- `internal/apps/balda`
- `internal/apps/balda/actors`
- `internal/apps/balda/handlers`
- `internal/apps/balda/execution`
- `internal/apps/balda/jobs`
- `internal/apps/balda/eventbus`

## Update triggers

- Runtime startup wiring changes.
- Any command execution path change.
- New config keys that affect transport or execution mode.
