# Reliability

Owner: Balda maintainers  
Status: active

## Invariants

- Delivery is at-least-once at transport level and idempotent at side-effect level.
- Retry policy and max deliver behavior are explicit and observable.
- DLQ entries include enough context for diagnosis and replay planning.
- User-visible delivery paths remain transport-durable; provider-side dedupe/outbox policy depends on the ingress/runtime path.

## Related tests

- `internal/apps/balda/execution/host_test.go`
- `internal/apps/balda/actors/delivery_actor_test.go`
- `internal/apps/balda/eventbus/nats/connection_test.go`
- `internal/apps/balda/handlers/command_test.go`

## Related packages

- `internal/apps/balda/execution`
- `internal/apps/balda/jobs`
- `internal/apps/balda/actors`
- `internal/apps/balda/eventbus/nats`
- `internal/apps/balda/handlers`
- `internal/apps/balda/state`

## Update triggers

- Error taxonomy or retry classification changes.
- Outbox/dedupe storage changes.
- DLQ schema or inspection command changes.
