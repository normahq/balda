# balda — AGENTS.md

## Development Standards

- Follow idiomatic Go and Google Go best practices.
- Prefer project-local tooling via `go tool ...` when available.
- Use Conventional Commits for all commits.
- Sync shared branches with merge (`git pull --no-rebase`), not rebase.
- When you finish implementation work, commit the completed changes before handing off.

## Quality Gates (Required)

Run before submitting changes:

```bash
go test -race ./...
go tool golangci-lint run
```

## Logging Policy

- Allowed: `github.com/rs/zerolog`, `log/slog`.
- Disallowed: `logrus`, `zap`, direct standard `log` usage.
- Initialize logging through `internal/logging.Init()`.
- Prefer structured logging fields over formatted strings.

## Balda Guardrails

- Keep Balda startup order strict: config load -> bundled MCP lifecycle -> balda provider -> channel runtime.
- Keep channel/session boundaries stable (`chat_id`, `topic_id`) and preserve lazy restore semantics.
- Keep workspace mode behavior stable (`on|off|auto`) with safe defaults and explicit failures.
- Keep Balda MCP/server contracts stable (`balda.workspace.*`).
- Keep config loading via app-specific `.config/balda/config.yaml` with `BALDA_*` env overrides.
- Do not add or maintain tests whose primary purpose is checking deprecated, legacy, removed, or unsupported surfaces by name.
- Do not add blacklist-style tests or command/config tables that only assert old names stay gone.
- Prefer positive tests of the current contract and behavior.

## Bot Commands (Current Contract)

- `/start owner=<owner_token>`: direct message only; owner authentication/bootstrap entrypoint.
- `/start invite=<invite_token>`: direct message only; collaborator invite onboarding entrypoint.
- `/topic <name>`: owner/collaborator, direct message only; creates a topic session labeled `<name>` using the configured balda provider.
- `/goal <objective>`: owner/collaborator; starts goal work from the current session context in an isolated GoalKeeper workspace/state with started/validation/final updates and terminal Result/Artifacts/Confidence/Next action outcome.
- `/goal clear`: owner/collaborator; stops active goal work for the current session only.
- `/close`: owner/collaborator, direct message only; resets the current session history and closes the topic when used from a topic session.
- `/cancel`: owner/collaborator; cancels the current session turn and drops queued turns for that session. It does not stop active `/goal` runs.
- `/user add`: owner only; generate a collaborator invite link.
- `/user list`: owner only; list collaborators and active invites.
- `/user remove <user_id>`: owner only; remove a collaborator by user ID.
- Keep command behavior and access expectations backward compatible; when changing commands, update `README.md` and `docs/balda.md` as part of the same change.

## Scheduling

Recurring scheduling is config-managed via `balda.scheduler.tasks`. It is not a chat command surface.

## Documentation

- Architecture navigation map for agents is in `docs/architecture/index.md`.
- Runtime contract and invariants are in `docs/architecture/runtime-contract.md`.
- Track migration execution plans with `bd`.
- Track migration debt and follow-up work with `bd`.
- Product installation/usage docs are in `README.md`.
- Development/contribution workflow is in `CONTRIBUTING.md`.
- Balda technical spec and operational details are in `docs/balda.md`.

## Release

- Omnidist profile is authoritative (`.omnidist/omnidist.yaml`, profile `balda`).
- Version source is Git tags (`version.source: git-tag`).
- Publish flows are tag-driven via GitHub Actions (`release.yaml`, `omnidist-release.yaml`).

<!-- BEGIN BEADS INTEGRATION v:1 profile:full hash:f65d5d33 -->
## Issue Tracking with bd (beads)

**IMPORTANT**: This project uses **bd (beads)** for ALL issue tracking. Do NOT use markdown TODOs, task lists, or other tracking methods.

### Why bd?

- Dependency-aware: Track blockers and relationships between issues
- Git-friendly: Dolt-powered version control with native sync
- Agent-optimized: JSON output, ready work detection, discovered-from links
- Prevents duplicate tracking systems and confusion

### Quick Start

**Check for ready work:**

```bash
bd ready --json
```

**Create new issues:**

```bash
bd create "Issue title" --description="Detailed context" -t bug|feature|task -p 0-4 --json
bd create "Issue title" --description="What this issue is about" -p 1 --deps discovered-from:bd-123 --json
```

**Claim and update:**

```bash
bd update <id> --claim --json
bd update bd-42 --priority 1 --json
```

**Complete work:**

```bash
bd close bd-42 --reason "Completed" --json
```

### Issue Types

- `bug` - Something broken
- `feature` - New functionality
- `task` - Work item (tests, docs, refactoring)
- `epic` - Large feature with subtasks
- `chore` - Maintenance (dependencies, tooling)

### Priorities

- `0` - Critical (security, data loss, broken builds)
- `1` - High (major features, important bugs)
- `2` - Medium (default, nice-to-have)
- `3` - Low (polish, optimization)
- `4` - Backlog (future ideas)

### Workflow for AI Agents

1. **Check ready work**: `bd ready` shows unblocked issues
2. **Claim your task atomically**: `bd update <id> --claim`
3. **Work on it**: Implement, test, document
4. **Discover new work?** Create linked issue:
   - `bd create "Found bug" --description="Details about what was found" -p 1 --deps discovered-from:<parent-id>`
5. **Complete**: `bd close <id> --reason "Done"`

### Quality
- Use `--acceptance` and `--design` fields when creating issues
- Use `--validate` to check description completeness

### Lifecycle
- `bd defer <id>` / `bd supersede <id>` for issue management
- `bd stale` / `bd orphans` / `bd lint` for hygiene
- `bd human <id>` to flag for human decisions
- `bd formula list` / `bd mol pour <name>` for structured workflows

### Auto-Sync

bd automatically syncs via Dolt:

- Each write auto-commits to Dolt history
- Use `bd dolt push`/`bd dolt pull` for remote sync
- No manual export/import needed!

### Important Rules

- ✅ Use bd for ALL task tracking
- ✅ Always use `--json` flag for programmatic use
- ✅ Link discovered work with `discovered-from` dependencies
- ✅ Check `bd ready` before asking "what should I work on?"
- ❌ Do NOT create markdown TODO lists
- ❌ Do NOT use external issue trackers
- ❌ Do NOT duplicate tracking systems

For more details, see README.md and docs/balda.md.

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --no-rebase
   bd dolt push
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- Always commit completed work before handing off; do not leave finished implementation changes only in the working tree
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

<!-- END BEADS INTEGRATION -->
