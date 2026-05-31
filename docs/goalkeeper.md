# Goalkeeper

Balda `/goal <objective>` starts goal work in the current session and workspace.

The workflow uses the configured `balda.provider` and the same workspace, Balda MCP server set, and session context as the current chat session.

## Workflow

The loop is fixed:

- Balda performs work toward the goal in the current workspace
- Balda then validates the result against the same goal using the latest visible work summary
- if the validation final visible response starts with `verdict: pass`, the loop exits
- otherwise work and validation repeat until `balda.goal.max_iterations` is exhausted

Balda sends:

- a start message with the objective and max iteration count
- step updates during work and validation
- a final completion or max-iterations message

## Prompt Contract

Balda converts `/goal <objective>` into this workflow prompt:

```text
Goal:
<objective>
```

The work phase returns a concise plain-text summary and evidence. The validation phase must start its final response with exactly one of:

```text
verdict: pass
```

```text
verdict: fail
```

`verdict: pass` means the objective is complete. `verdict: fail` means the objective is not complete yet and the loop should continue until the configured iteration cap.

Thought parts are ignored when checking the validation verdict. Only visible final response text is considered.

## Runtime Notes

Balda records enough session state to continue the workflow across the work and validation loop.

A passing validation is detected only when the validation phase's visible final response starts with `verdict: pass`. Malformed verdicts, missing verdicts, and `verdict: fail` do not pass validation.
