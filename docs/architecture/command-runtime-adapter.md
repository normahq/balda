# Command Runtime Adapter

Owner: Balda maintainers  
Status: active

## Invariants

- Command delivery uses one durable command transport.
- Event projection and lifecycle history use dedicated durable event streams.
- Terminal command failures are retained for inspection and replay decisions.
- Command and event processing use explicit settlement.
- Command subjects stay under `balda.v1.cmd.*`; events under `balda.v1.evt.*`.
- Product/runtime packages consume actorlayer `Source`/`Delivery` and
  actorlayer transport dispatcher abstractions, not transport APIs directly.

## Related tests

- `internal/apps/balda/eventbus/nats/connection_test.go`
- `internal/apps/balda/execution/subjects_test.go`
- `internal/apps/balda/execution/config_test.go`
- `internal/apps/balda/handlers/inbound_webhook_test.go`

## Related packages

- `internal/apps/balda/eventbus/nats`
- `internal/apps/balda/execution`
- `internal/apps/balda/handlers`

## Update triggers

- Transport config changes.
- Subject taxonomy or envelope/header changes.
- Publish/consume settlement behavior changes.
