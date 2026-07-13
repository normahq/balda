# Balda Architecture Map

Owner: Balda maintainers  
Status: active

Use this map to find the authoritative runtime contracts.

## Documents

- [Runtime contract](runtime-contract.md)
- [Application sub-zones](application-zones.md)
- [Command runtime adapter](command-runtime-adapter.md)
- [Conversational turns and jobs](conversational-turns-and-jobs.md)
- [Background monitors](background-monitors.md)
- [Actor runtime](actor-runtime.md)
- [Local actorlayer contract boundary](actor-runtime.md#local-actorlayer-contract-boundaries)
- [Projections and read models](projections-and-read-models.md)
- [Reliability](reliability.md)
- [Testing and evals](testing-and-evals.md)

## Invariants

- Durable command/event transport is exposed to Balda runtime code through actorlayer abstractions.
- SQLite is product/read-model state, not a command queue.
- Ingress handlers dispatch actor envelopes through actorlayer transport dispatcher contracts; actors execute commands.
- `actorcmd` is the canonical leaf package for Balda actor targets, namespaces, subjects, headers, and job scope metadata; `execution` re-exports that taxonomy as the runtime-facing compatibility facade.
- `deliverycmd` is the leaf package for transport-neutral delivery contracts: locator, formatting profile, progress policy, delivery payloads, and adapter-facing delivery operations.
- `session` owns session lifecycle and restore semantics, but does not own transport delivery contracts.
- `channel/*` packages are concrete transport adapters only. They must not define shared cross-transport contracts and must not import Balda application/session internals for convenience.
- `locatorref` owns the public `<channel_type>:<address_key>` reference form and must stay independent from concrete transport adapter packages.
- Use-case packages such as `sessionturn` and MCP surfaces own local ports and depend on small interfaces; composition/wiring code provides concrete adapters.
- `sessionturn` owns queued-turn restoration and delegates provider iteration through a narrow executor port.
- One composition-root lifecycle coordinator starts durable infrastructure before every ingress and stops it in reverse order.
- Actor execution contract is split: core actor mechanics in Norma; Balda owns product actors, the configured provider runtime, command runtime adapter policy, and all queue/retry/dead-letter policy.

## Runtime Flow

Telegram/Zulip/Slack/webhook/scheduler ingress -> actorlayer transport dispatcher -> NATS adapter -> actorlayer `Source`/`Delivery` -> local dispatch runtime -> Balda product actor -> event projection/read-model updates.

## Related tests

- `internal/apps/balda/eventbus/nats/connection_test.go`
- `internal/apps/balda/execution/host_test.go`
- `internal/apps/balda/application_lifecycle_test.go`
- `internal/apps/balda/architecture_dependencies_test.go`
- `internal/apps/balda/actors/turn_dispatcher_test.go`
- `internal/apps/balda/jobs/service_test.go`

## Related packages

- `internal/apps/balda/eventbus/nats`
- `internal/apps/balda/actorcmd`
- `internal/apps/balda/execution`
- `internal/apps/balda/jobs`
- `internal/apps/balda/actors`
- `internal/apps/balda/sessionturn`
- `internal/apps/balda/internalmcp`
- `internal/apps/balda/handlers`
- `internal/apps/balda/agent`
- `internal/apps/balda/session`
- `internal/apps/balda/state`

## Update triggers

- New command/event subjects.
- Startup or transport lifecycle changes.
- Changes to retries, dedupe, or DLQ semantics.
- New ingress or actor execution paths.
