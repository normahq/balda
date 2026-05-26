# JetStream Command Bus

Owner: Balda maintainers  
Status: active

## Invariants

- `BALDA_COMMANDS` is the only durable command queue.
- `BALDA_EVENTS` is the durable lifecycle and domain event stream.
- `BALDA_DLQ` stores terminal command failures.
- Worker and projector use durable pull consumers with explicit settlement.
- Command subjects stay under `balda.v1.cmd.*`; events under `balda.v1.evt.*`.

## Related tests

- `internal/apps/balda/eventbus/nats/connection_test.go`
- `internal/apps/balda/swarm/bus_test.go`
- `internal/apps/balda/swarm/config_test.go`
- `internal/apps/balda/handlers/inbound_webhook_test.go`

## Related packages

- `internal/apps/balda/eventbus/nats`
- `internal/apps/balda/swarm`
- `internal/apps/balda/handlers`

## Update triggers

- Stream or consumer config changes.
- Subject taxonomy or envelope/header changes.
- Publish/consume settlement behavior changes.
