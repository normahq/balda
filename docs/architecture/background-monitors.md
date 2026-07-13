# Wait Wake-Up via Scheduled Jobs

Owner: Balda maintainers  
Status: active

## Problem

Balda needs a minimal way to resume work after a delay without keeping an interactive turn alive.

Keeping a session turn or model run open across time would mix ingress, session execution, retry policy, and delayed wake-up behavior in one path. That breaks restart safety and layer boundaries.

## Decision

Balda implements `wait` by reusing existing scheduled job storage and scheduling flow.

A scheduled job may be either:

- recurring; or
- one-shot.

The `wait` prototype is modeled as a one-shot scheduled job. When its due time arrives, the scheduler publishes normal product work and starts a new unit of work. It does not resume an old LLM turn.

Version one is intentionally narrow:

- one-shot wake-up only;
- no goal evaluation;
- no “monitor completed” semantics;
- no transport-owned timers;
- no separate wait store.

## Layer boundaries

Ingress layers may:

- accept wait requests;
- authenticate and validate input;
- publish product commands.

Ingress layers must not:

- own wake-up timers;
- own persisted wait state;
- resume old turns directly.

Product runtime owns:

- creating one-shot scheduled jobs for wait requests;
- deciding when due scheduled jobs become wake-up work;
- dispatching new work when the wake-up fires.

State owns durable scheduled job records.

Delivery remains separate from wake-up scheduling.

## Model

Flow:

`chat ingress -> session/control actor -> wait.start -> persist one-shot scheduled job -> scheduler observes due job -> publish scheduled job command -> new turn/command runs`

The wake-up path reuses the existing scheduled job command flow rather than introducing a separate wait runtime.

## Metadata

The prototype should align timing metadata with existing scheduled job fields rather than introducing a parallel timing model.

The main fields are:

- `CreatedAt` — when the wait request was accepted;
- `NextRunAt` — when the wake-up is due;
- `LastRunAt` — when the wake-up actually fired;
- `UpdatedAt` — when the scheduled job record was last changed.

If resumed work needs timing context, that context should be derived from these scheduled job fields.

## Required properties

- Wake-up survives process restarts.
- Wake-up derives from persisted scheduled job state.
- Wake-up is product-owned, not transport-owned.
- A wake-up starts new work rather than reviving an old turn.
- One-shot and recurring jobs share the same durable scheduling path.

## Initial scope

Version one should include only:

- `wait.start`
- optional `wait.cancel`
- one-shot scheduled jobs
- scheduler-driven wake-up
- reuse of existing scheduled job command flow

Version one should not include:

- goal-achieved detection
- generic monitor rules
- event-driven waits
- a dedicated wait store
- transport-specific timer ownership

## Current answers

- One-shot versus recurring behavior is represented by the scheduled job's existing recurrence fields; a wait creates a job without a recurrence schedule.
- Wake-up dispatch continues through the existing scheduled-job/session actor path, so retries and session boundaries remain consistent.
- The persisted job payload carries the session and wait context needed to create the new work item; durable job metadata remains the source of timing truth.

## Update triggers

- Introduction of richer monitor semantics.
- Changes to scheduled job ownership or scheduling behavior.
- Changes to actor/job layering for delayed wake-up work.
