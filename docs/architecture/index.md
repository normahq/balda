# Balda Architecture Map

Owner: Balda maintainers  
Status: active

Use this map to find the authoritative runtime contracts.

## Documents

- [Runtime contract](runtime-contract.md)
- [Command runtime adapter](command-runtime-adapter.md)
- [Actor runtime](actor-runtime.md)
- [Local actorlayer contract boundary](actor-runtime.md#local-actorlayer-contract-boundaries)
- [Projections and read models](projections-and-read-models.md)
- [Reliability](reliability.md)
- [Testing and evals](testing-and-evals.md)

## Invariants

- Durable command/event transport is exposed to Balda runtime code through actorlayer abstractions.
- SQLite is product/read-model state, not a command queue.
- Ingress handlers dispatch actor envelopes through actorlayer transport dispatcher contracts; actors execute commands.
- Actor execution contract is split: core actor mechanics in Norma; Balda owns product actors, the configured provider runtime, command runtime adapter policy, and all queue/retry/dead-letter policy.

## Runtime Flow

Telegram/Zulip/Slack/webhook/scheduler ingress -> actorlayer transport dispatcher -> NATS adapter -> actorlayer `Source`/`Delivery` -> local dispatch runtime -> Balda product actor -> event projection/read-model updates.

## Related tests

- `internal/apps/balda/eventbus/nats/connection_test.go`
- `internal/apps/balda/swarm/runtime_test.go`
- `internal/apps/balda/architecture_contract_test.go`

## Related packages

- `internal/apps/balda/eventbus/nats`
- `internal/apps/balda/swarm`
- `internal/apps/balda/actors`
- `internal/apps/balda/handlers`
- `internal/apps/balda/agent`
- `internal/apps/balda/session`
- `internal/apps/balda/state`

## Update triggers

- New command/event subjects.
- Startup or transport lifecycle changes.
- Changes to retries, dedupe, or DLQ semantics.
- New ingress or actor execution paths.
