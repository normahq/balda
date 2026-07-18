# Reliability

Owner: Balda maintainers  
Status: active

## Invariants

- Delivery is at-least-once at transport level and idempotent at side-effect level.
- Retry policy and max deliver behavior are explicit and observable.
- DLQ entries include enough context for diagnosis and replay planning.
- User-visible delivery paths remain transport-durable; provider-side dedupe/outbox policy depends on the ingress/runtime path.
- Concrete channel adapters classify provider failures as retryable or permanent at the transport-neutral `deliverycmd` boundary. Unknown legacy errors retain the existing bounded external-delivery retry behavior.
- Provider runtime log adapters redact credentials before forwarding generated request errors to application logging.
- Delivery adapters may retry locally only when the provider explicitly rejected presentation semantics before accepting the side effect, such as a Telegram entity-parse error. Transport timeouts must not trigger a second immediate send through a different presentation path.
- Retried interactive-question delivery rechecks durable question state before every provider side effect. A question that is already answered, timed out, or failed is never presented again by a late command retry.
- Job state transitions atomically enqueue semantic events in `execution_job_event_outbox`; publication is at-least-once with stable envelope IDs and background retry.

## Related tests

- `internal/apps/balda/execution/host_test.go`
- `internal/apps/balda/actors/delivery_actor_test.go`
- `internal/apps/balda/deliveryworkflow/service_test.go`
- `internal/apps/balda/messenger/messenger_test.go`
- `internal/apps/balda/eventbus/nats/connection_test.go`
- `internal/apps/balda/handlers/command_test.go`
- `internal/apps/balda/jobs/service_test.go`
- `internal/apps/balda/state/sqlite_jobs_test.go`

## Related packages

- `internal/apps/balda/execution`
- `internal/apps/balda/jobs`
- `internal/apps/balda/actors`
- `internal/apps/balda/deliverycmd`
- `internal/apps/balda/deliveryworkflow`
- `internal/apps/balda/messenger`
- `internal/apps/balda/eventbus/nats`
- `internal/apps/balda/handlers`
- `internal/apps/balda/state`

## Update triggers

- Error taxonomy or retry classification changes.
- Outbox/dedupe storage changes.
- DLQ schema or inspection command changes.
