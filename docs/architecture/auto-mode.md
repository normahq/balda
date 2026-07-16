# Auto mode (`/auto`) v1

`/auto` is a session-scoped continuation mode for Balda sessions.

When enabled, Balda schedules an internal synthetic user turn after each
completed agent turn. That synthetic turn is not shown to the user and acts as
steering for whether the agent should continue work without waiting for a new
user message.

## Goals

- Allow a session to keep moving after one user request while the agent can
  continue autonomously.
- Keep the mode session-scoped and durable across process restarts.
- Make the feature observable from chat through `/auto` status.

## Non-goals

- Background execution outside the normal session turn runtime.
- Replacing explicit user approval for sensitive operations.
- Making internal auto turns visible in transport history.

## Commands

- `/auto` — show current mode status for the session.
- `/auto on` — enable auto continuation for future turns.
- `/auto off` — disable future auto turns.

`/auto off` prevents the next continuation from being scheduled. It does not
retroactively cancel a turn that already started.

## Runtime model

When `/auto` is enabled:

1. user turn executes
2. agent turn completes
3. Balda checks session auto state
4. if allowed, Balda schedules an internal synthetic user turn
5. that internal turn asks the model whether the task is done or should
   continue
6. if the model decides to continue, the next visible agent response is sent to
   the user
7. the loop repeats until done, waiting on user input, `/auto off`, or limit
   reached

## Synthetic internal turn

The synthetic turn is internal-only and does not appear in transport history.
It uses a fixed steering prompt that tells the model:

- if the task is done, respond with the exact sentinel
  `AUTO_DECISION:DONE`
- if the task needs explicit user input, clarification, or approval, respond
  with the exact sentinel `AUTO_DECISION:WAIT_FOR_USER`
- otherwise continue the task immediately and provide only the next
  user-visible response

Balda intercepts these sentinels and suppresses delivery for that internal turn.

## Session state

The session runtime state stores:

- `balda_auto_enabled`
- `balda_auto_state`
- `balda_auto_consecutive_turns`
- `balda_auto_max_turns`
- `balda_auto_last_turn_at`
- `balda_auto_last_stop_reason`

Recommended default:

- `balda_auto_max_turns = 5`

## States

- `idle` — auto mode enabled but no active continuation chain is currently
  running
- `running` — Balda is scheduling or executing synthetic continuation turns
- `waiting_for_user` — the model reported it is done or cannot safely continue
- `limit_reached` — Balda hit the configured consecutive auto-turn limit
- `no_progress` — Balda observed repeated auto output and stopped the loop

## Steering messages during auto mode

New user messages do not disable `/auto`. They are treated as steering input for
the next normal turn, and auto mode remains enabled for subsequent turns.

## Guardrails

v1.1 guardrails:

- consecutive auto-turn limit
- stop on `AUTO_DECISION:DONE`
- stop on `AUTO_DECISION:WAIT_FOR_USER`
- stop on repeated identical auto output
- stop when `/auto off` disables the mode before the next continuation is
  scheduled

When the model reports `AUTO_DECISION:DONE`, Balda ends the current
auto-continuation chain and resets session auto state back to `idle`. The
session-level auto mode flag stays enabled for future tasks.

## Observability

`/auto` reports:

- enabled on/off
- state
- consecutive auto turns
- max auto turns
- last auto turn timestamp
- last stop reason
