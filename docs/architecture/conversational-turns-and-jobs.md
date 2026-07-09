# Conversational Turns And Jobs

Owner: Balda maintainers  
Status: active

## Problem

Balda currently mixes two different execution shapes under the same job-backed ingress path:

- ordinary conversational turns from interactive chat sessions;
- durable job-like work such as goals, scheduled jobs, and inbound webhooks.

That coupling makes the runtime harder to reason about. It also encourages transport concerns such as delivery outbox policy and job persistence to bleed into the common chat path, even though session actors already support direct durable dispatch through actorlayer.

## Decision

Balda keeps `actorlayer` as the single transport and execution boundary, but splits runtime intent into two distinct paths:

- Ordinary conversational turns dispatch directly from ingress to the session actor through `balda.v1.cmd.session` envelopes.
- Durable job-like work continues to use job-backed orchestration through the job actor and the legacy `execution_tasks` persistence layer.

The legacy `execution_tasks` table stays as the durable orchestration/read-model layer for true jobs, not as a mandatory wrapper for every user message.

## Target layering

1. Ingress adapters publish envelopes through actorlayer transport contracts.
2. Product actors may dispatch additional envelopes to one or many other product actors while handling work.
3. Delivery stays a product actor boundary for external side effects.
4. SQLite remains a projection/read-model store and orchestration state store for tasks, not a generic transport queue.

## Path split

### Conversational session turns

Use the direct path for interactive user conversation where the primary unit is the session lane:

`ingress -> session actor -> delivery actor(s)`

Properties:

- durable dispatch acceptance still happens before work executes;
- session serialization remains owned by the session actor lane;
- intermediate and final replies are separate delivery envelopes;
- semantic progress such as thinking or plan snapshots must be emitted as separate session-owned deliveries, not sent inline from ingress/handler code;
- transport-specific UX signals such as Telegram `typing` are derived delivery behavior, not first-class runtime progress semantics;
- no `execution_tasks` row is required for a normal interactive turn.

### Durable jobs

Keep the job-backed path for work that needs explicit lifecycle, result state, or operational controls beyond a single conversational turn:

`ingress -> job actor -> product actor -> delivery actor(s)`

Current examples:

- goal execution;
- scheduled jobs;
- inbound webhook work;
- any future long-running or resumable orchestration.

## Delivery policy

Delivery is no longer modeled as one universal persistence rule for every user-visible message.

- Transport durability stays in actorlayer and JetStream.
- Delivery actors still own provider-side idempotency and any outbox-backed settlement that is needed for a given source/path.
- Conversational session replies may bypass the SQLite delivery outbox when the source path already has sufficient transport durability and the product requirement is immediate fan-out rather than outbox replay.
- Conversational progress delivery is session-owned at the semantic level. Channel-specific affordances such as Telegram `typing` are transport-side embellishments derived from semantic progress deliveries like thinking, not independent domain events.

## Consequences

- The runtime model becomes easier to explain: sessions are sessions, jobs are jobs.
- Telegram, Slack, and Zulip conversational ingress can share the same direct session-dispatch model without affecting goals, schedules, or webhooks.
- Documentation must stop describing `execution_tasks` and the delivery outbox as mandatory for every conversational reply.
- Operational tooling should continue to focus the legacy `execution_tasks` persistence on real job lifecycle management.

## Migration plan

1. Keep ordinary Telegram, Slack, and Zulip conversational turns on direct `SessionTurnEnvelope` dispatch.
2. Keep webhook, schedule, and goal flows job-backed until their job semantics change.
3. Update runtime and reliability docs to describe the split model.
4. Remove obsolete task assumptions from chat-path tests and telemetry over time.

## Update triggers

- Any new ingress path deciding between direct session dispatch and job-backed orchestration.
- Changes to delivery durability policy.
- Changes to job lifecycle ownership or operator controls.
