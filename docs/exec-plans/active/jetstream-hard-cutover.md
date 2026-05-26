# JetStream Hard Cutover Execution Plan

Status: active  
Owner: Balda maintainers

## Scope

- Keep JetStream as the only command/event transport.
- Keep SQLite limited to product/read-model state.
- Keep ingress publish-only and actor execute-only boundaries.

## Completed milestones

- Hard cut to JetStream command/event streams and durable consumers.
- Runtime contract enforcement tests added (`architecture_contract_test.go`).
- Telegram/webhook/scheduler/goal command-ingress paths migrated.
- Task visibility projected from events/product state.

## Open milestones

- Expand architecture docs and onboarding map.
- Maintain reliability/invariant regression checks as features evolve.
- Keep CI structural checks aligned with runtime contracts.

## Key decisions

- JetStream lifecycle events are visibility-first; command settlement remains transport-authoritative.
- SessionActor is the only TurnDispatcher adapter.
- Delivery outbox semantics are required for user-visible idempotency.

## Risks

- Documentation drift from runtime behavior.
- New handlers bypassing publish-only ingress contract.
- Regression in retry/dedupe semantics when adding actor capabilities.

## Evidence links

- Code: `internal/apps/balda/swarm`, `internal/apps/balda/eventbus/nats`, `internal/apps/balda/handlers`
- Tests: `internal/apps/balda/architecture_contract_test.go`
- Product docs: `README.md`, `docs/balda.md`
