# Actor Runtime

Owner: Balda maintainers  
Status: active

## Invariants

- ActorRuntime consumes commands from JetStream.
- Actorlayer engine lanes serialize mutable state by actor key.
- Runtime execution uses `actorlayer/engine.Runtime`; Balda adapts JetStream commands into actorlayer deliveries.
- Command settlement happens after actor side effects complete.
- Retry/permanent failure handling is explicit and classified.
- Task actor status includes role configuration and associated tools; runtime capability is derived from those tool sets.
- Role-level tool contracts are advisory and visible in runtime status payloads.
- Per-role shell/workspace behavior is derived from each agent's configured toolset at dispatch time.
- Task progress/results and task visibility payload summaries redact common secret/token patterns before persistence and delivery.
- The execution core does not depend on ADK, Balda, JetStream, Telegram, MCP, or provider SDK APIs.

## Related tests

- `internal/apps/balda/swarm/runtime_test.go`
- `internal/apps/balda/swarm/agents_test.go`
- `internal/apps/balda/handlers/swarm_session_actor_test.go`
- `internal/apps/balda/handlers/swarm_control_actor_test.go`
- `internal/apps/balda/handlers/swarm_delivery_actor_test.go`
- `internal/apps/balda/handlers/task_visibility_test.go`

## Related packages

- `internal/apps/balda/swarm`
- `internal/apps/balda/handlers/swarm_*.go`

## Update triggers

- Actor key mapping changes.
- Command heartbeat or settlement behavior changes.
- Task/agent/delivery actor lifecycle changes.
- Agent toolset and allocator behavior changes.

## Norma actorlayer contract boundaries

### Contract shape

- Engine contract: Norma actorlayer is the fixed typed dispatch+state model Balda uses for actors. It exposes:
  - actor keying and deterministic lane routing,
  - typed envelope handling,
  - dispatch result states (`acked`, `running`, `in_progress`, `retry`, `deadletter`, `noop`),
  - and lifecycle events suitable for external telemetry.
- Provider runtime: `balda.provider` selects the single app-scoped ADK provider runtime used by all Balda sessions and task role agents.
- Delivery boundary: Balda maps JetStream command messages into actorlayer deliveries and owns command settlement.

### Ownership split

- Actorlayer owns:
  - typed actor contracts and interfaces,
  - execution correctness of lane transitions and actor state.
- Balda integration code owns:
  - transport protocol and transport-level acknowledgements,
  - provider runtime invocation details (ADK session execution, tools, model/runtime context),
  - queue policy integration (retry/dead-letter thresholds, heartbeats, and backoff tuning),
  - and persistence side effects in product state stores.

### Why this split exists

- All actor sessions in one Balda process use the configured `balda.provider`; actor contracts do not choose providers.
- Balda can own product semantics (queue policy, telemetry, task projection, and workspace/task metadata) while still reusing the same execution kernel.
- Future transport/provider integration code must preserve the actorlayer engine contract and keep product policy in Balda.

### Balda implementation map

- Actor dispatch and lane execution live in `internal/apps/balda/swarm/runtime.go`, backed by `github.com/normahq/norma/pkg/actorlayer/engine`.
- Actor definitions live in `internal/apps/balda/handlers/swarm_*.go` and are registered as actorlayer dispatch actors.
- ADK session/provider runtime ownership lives in `internal/apps/balda/agent` and `internal/apps/balda/session`; all sessions use the configured `balda.provider`.
- JetStream command delivery and settlement live in `internal/apps/balda/eventbus/nats` and the `swarm.CommandMessage` contract.
- Task projection, retry classification, DLQ reporting, and user-visible status live in Balda packages (`swarm`, `handlers`, and `state`), not in Norma actorlayer.
- Balda must not grow `internal/apps/balda/norma`, `internal/apps/balda/adapters`, or actor-runtime selector packages. Future generic actor adapters belong to Norma-owned public packages, with package-layout cleanup tracked separately by `balda-e10r`.
