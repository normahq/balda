# Goalkeeper

Balda `/goal <objective>` runs a deterministic ADK Goalkeeper workflow in the current Balda session and workspace.

The workflow boots:

- one ADK `LoopAgent` workflow agent named `Goalkeeper`
- one worker child agent named `GoalkeeperWorker`
- one validator child agent named `GoalkeeperValidator`

Both child agents are built from the configured `balda.provider`. They use the same resolved workspace directory, Balda MCP server set, and ADK session as the current chat session.

## Workflow

The loop is fixed:

- the worker receives the goal and performs the requested work in the current workspace
- the worker final visible response is persisted in ADK session state as `app:goalkeeper_worker_output`
- the validator runs after the worker and validates the result against the same goal
- the validator prompt injects `{app:goalkeeper_worker_output?}` so validation sees the worker summary even when session transcript context is limited
- if the validator final visible response starts with `verdict: pass`, the loop exits
- otherwise the worker and validator retry until `balda.goal.max_iterations` is exhausted

Balda sends:

- a start message with the objective and max iteration count
- one validation update for each validator final response
- a final completion or max-iterations message

## Prompt Contract

Balda converts `/goal <objective>` into this workflow prompt:

```text
Goal:
<objective>
```

The worker returns a concise plain-text summary and evidence. The validator must start its final response with exactly one of:

```text
verdict: pass
```

```text
verdict: fail
```

`verdict: pass` means the objective is complete. `verdict: fail` means the objective is not complete yet and the loop should continue until the configured iteration cap.

Thought parts are ignored when checking the validator verdict. Only visible final response text is considered.

## Runtime Notes

The ADK workflow stream includes metadata-only `session.Event` records around each worker and validator step. These events have no `Content`, are persisted in ADK session history, and identify `step_started`, `step_completed`, or `step_failed` in `CustomMetadata["norma.goalkeeper.event"]`.

A passing validation is detected from the copied Goalkeeper workflow escalation marker, not from Balda parsing a separate `STATUS: done` response. Malformed verdicts, missing verdicts, and `verdict: fail` do not pass validation.

## Not Used

Goalkeeper does not use:

- Taskmaster queues
- scheduled tasks
- PDCA phase agents
- structured PDCA JSON contracts
- the removed single-root prompt loop that asked for `STATUS: done|continue`
