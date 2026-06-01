# Goal Workflow

Balda `/goal <objective>` starts goal work from the current session context in an isolated GoalKeeper runtime.

Use `/goal clear` to stop active goal work for the current session. `/cancel` does not stop GoalKeeper runs.

The workflow uses the configured `balda.provider` and the Balda MCP server set, but it does not reuse the current chat runtime session, workspace, or state. Each `/goal` run gets:

- a separate GoalKeeper ADK session/state
- a separate goal workspace under Balda state
- a goal branch created from `balda.workspace.base_branch`
- automatic export back to the base branch when validation passes

`/goal` requires `balda.workspace.mode` to resolve to an enabled git-worktree mode.

Only one `/goal` run can be active per session. New `/goal <objective>` requests in the same session are rejected until the active run completes, fails, or is cleared.

## Workflow

The loop is fixed:

- GoalKeeper performs work toward the goal in the isolated goal workspace
- Balda then validates the result against the same goal using the latest visible work summary
- if the validation final visible response starts with `verdict: pass`, the loop exits
- otherwise work and validation repeat until `balda.goal.max_iterations` is exhausted

Balda sends:

- a start message with the objective and max iteration count
- step updates during work and validation
- a final completion, export-failure, cancellation, or max-iterations message

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

Balda records enough isolated goal session state to continue the workflow across the work and validation loop.

When validation passes:

- Balda generates a Conventional Commit message for export
- Balda squash-merges the goal branch into `balda.workspace.base_branch`
- Balda deletes the goal workspace and goal session state after successful export

When validation passes but export fails:

- the task is marked failed with `export_failed` metadata
- the goal workspace and goal session state are preserved for recovery
- the terminal task output includes the preserved workspace/branch details

A passing validation is detected only when the validation phase's visible final response starts with `verdict: pass`. Malformed verdicts, missing verdicts, and `verdict: fail` do not pass validation.
