# Reliability

Owner: Balda maintainers  
Status: active

## Invariants

- Delivery is at-least-once at transport level and idempotent at side-effect level.
- Retry policy and max deliver behavior are explicit and observable.
- DLQ entries include enough context for diagnosis and replay planning.
- User-visible delivery paths use dedupe keys/outbox guards.

## Related tests

- `internal/apps/balda/swarm/runtime_test.go`
- `internal/apps/balda/handlers/swarm_delivery_actor_test.go`
- `internal/apps/balda/eventbus/nats/connection_test.go`
- `internal/apps/balda/handlers/command_test.go`

## Related packages

- `internal/apps/balda/swarm`
- `internal/apps/balda/eventbus/nats`
- `internal/apps/balda/handlers`
- `internal/apps/balda/state`

## Update triggers

- Error taxonomy or retry classification changes.
- Outbox/dedupe storage changes.
- DLQ schema or inspection command changes.
