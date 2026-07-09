# Testing and Evals

Owner: Balda maintainers  
Status: active

## Invariants

- Architecture contracts are enforced by repository tests.
- Built-in runtime integration coverage remains first-class.
- Reliability scenarios cover redelivery, retry, and terminal failure flows.
- Projection and runtime behavior are testable without external infra.

## Related tests

- `internal/apps/balda/eventbus/nats/connection_test.go`
- `internal/apps/balda/execution/host_test.go`
- `internal/apps/balda/architecture_contract_test.go`
- `internal/apps/balda/handlers/inbound_webhook_test.go`
- `internal/apps/balda/handlers/command_test.go`

## Related packages

- `internal/apps/balda/eventbus/nats`
- `internal/apps/balda/execution`
- `internal/apps/balda/jobs`
- `internal/apps/balda/handlers`
- `internal/apps/balda`

## Update triggers

- New ingress channels.
- New actor types or lifecycle stages.
- Changes to command/event subjects, settlement, or projection rules.
