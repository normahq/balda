# JetStream Migration Debt

Status: active  
Owner: Balda maintainers

## Debt items

1. Keep architecture docs synchronized with runtime/test contract updates.
2. Expand deterministic scenario fixtures and replay diagnostics for failure triage.
3. Continue tightening status/projection UX (`/swarm status`, `/task`, `/dlq`) as schema evolves.
4. Track compatibility alias cleanup timeline (`/mailbox status` -> `/queue status`).

## Exit criteria

- Every runtime contract change updates architecture docs in the same PR.
- Regression tests cover each new command/event subject.
- Operator-facing status includes enough metadata to debug replay/retry/DLQ without code reads.
