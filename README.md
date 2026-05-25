# balda

[![test](https://github.com/normahq/balda/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/normahq/balda/actions/workflows/test.yml)
[![lint](https://github.com/normahq/balda/actions/workflows/lint.yml/badge.svg?branch=main)](https://github.com/normahq/balda/actions/workflows/lint.yml)

## Autonomous Worker Comrade for Teams

Balda exists to give teams a persistent AI worker they can assign real work to.

It takes work from the team's conversation, keeps project context, uses the
team's tools, and keeps moving until there is a concrete result to review. A
task can start as a message, a topic, a goal, a schedule, or an external event;
Balda turns that intent into an active worker session.

The name comes from Pushkin's работник Балда: practical, direct, and focused on
getting the job done. That is the project goal: an autonomous worker comrade for
teams.

```bash
npm install -g -y @normahq/balda
balda init
balda start
```

## Product Goals

| Feature | What it means |
|---------|---------------|
| Assignable work | People can give Balda a task or goal the same way they would assign work to a teammate. |
| Persistent worker context | Balda remembers team and project facts and carries task context across sessions, interruptions, and restarts. |
| Team conversation as work intake | Work can start from chat, topics, mentions, commands, schedules, or incoming external events. |
| Focused work sessions | Separate threads/topics can become separate work contexts, so different tasks do not collapse into one conversation. |
| Autonomous execution loops | Balda can keep working toward a goal, validate progress, and continue until there is a result. |
| Project tool access | Balda can use the same project tools the team relies on, including repo/workspace operations and configured integrations. |
| Event-driven work | Webhooks and scheduled triggers become inputs to agents, not just notifications. |
| Collaborative visibility | The team can see progress, cancel work, reset context, inspect memory, and manage who can assign work. |
| Reviewable outcomes | Balda should return something the team can evaluate: a summary, changed files, a commit, validation output, or a next action. |
| Operationally simple deployment | Teams can run Balda close to their project without building a platform first. |

## How Balda Works Today

1. Pick an ACP provider.
2. Connect a Telegram bot token.
3. Chat, create topics, and let Balda persist session state, memory, and workspaces.

Balda runs one provider runtime per process and maps Telegram chats/topics to
separate agent sessions. That keeps the bot simple to operate while preserving
session boundaries.

## Quickstart

You need:

- a Telegram bot token from BotFather
- at least one provider CLI for host installs: `codex`, `opencode`, `copilot`,
  `gemini`, or `claude`
- Node.js/npm, unless you use the Docker Compose flow

Install Balda:

```bash
npm install -g -y @normahq/balda
```

Initialize Balda in your project:

```bash
balda init
```

`balda init` detects provider CLIs, validates the Telegram token, writes
`.config/balda/config.yaml`, initializes `.config/balda/state.db`, and prints
the next commands. By default, the Telegram token is stored in `.env`.
To preserve older state, rename `.config/balda/balda.db` to
`.config/balda/state.db` or copy `.config/relay/relay.db` there.

Start Balda:

```bash
balda start
```

Authenticate in Telegram with the printed auth URL, or send the printed command
directly to your bot:

```text
/start owner=<owner_token>
```

After owner auth, send a normal direct message to use the owner session. Create
a named topic session when you want an isolated workspace and conversation:

```text
/topic <name>
```

## Docker Compose

Balda ships a root [`Dockerfile`](Dockerfile) and [`compose.yaml`](compose.yaml)
for local Docker Compose runtime.

This path is designed for real project work. The current directory is mounted as
`/workspace`, so Balda sees your host checkout, `.git`, `.env`,
`.config/balda/config.yaml`, and `.config/balda/state.db`.

```bash
docker compose build balda
docker compose run --rm balda init
docker compose up -d balda
```

Provider credentials are not baked into the image. Authenticate with provider
environment variables or provider login commands run through Compose.
`balda-home` persists provider CLI home config across container recreates.

Polling mode is the default and does not require publishing a port. Webhook
setup and image details are documented in [`docs/balda.md`](docs/balda.md).

## Configure Any ACP Agent

Balda has built-in provider types for common CLIs and a generic ACP adapter for
anything else that speaks ACP.

```yaml
runtime:
  providers:
    my-agent:
      type: generic_acp
      generic_acp:
        cmd: ["my-acp-agent", "--stdio"]
        model: "my-model"

balda:
  provider: my-agent
```

Built-in provider types:

- `codex_acp`
- `opencode_acp`
- `copilot_acp`
- `gemini_acp`
- `claude_code_acp`
- `generic_acp`
- `pool`

## Bot Commands

- `/topic <name>`: create a named topic session.
- `/goal <objective>`: create a durable task record and work toward the goal in the current session context/workspace. Legacy/shadow modes run the Goalkeeper worker -> validator loop directly; mailbox mode routes the task through task, planner, executor, reviewer, and delivery actors. Goal updates use `balda.telegram.formatting_mode`; terminal updates include Result, Artifacts, Confidence, and Next action sections. See [`docs/goalkeeper.md`](docs/goalkeeper.md).
- `/tasks`: list active task records for the current session.
- `/task <id>`: inspect task status, objective, latest events, and reviewable outcome when the task is terminal.
- `/task <id> events`: print the task event stream.
- `/task <id> cancel`: cancel queued mailbox work, the active task run when present, and mark the task canceled.
- `/swarm status`: show swarm rollout mode, event bus state, runtime state, shadow counters, configured logical agents, task status counts, and ready mailboxes.
- `/mailbox status`: show non-terminal mailbox message counts by mailbox and status.
- `/reset`: clear conversation history for the current session.
- `/close`: reset history, then close the current topic or restart the owner session on the next message.
- `/cancel`: cancel in-flight work, drop queued turns, cancel active task records, and abort active `/goal` work for the current session.
- `/memory`: print current `${balda.state_dir}/MEMORY.md` contents when memory is enabled.
- `/start owner=<owner_token>`: authenticate the owner in direct messages.
- `/start invite=<invite_token>`: onboard a collaborator in direct messages.
- `/user add|list|remove`: manage collaborators; owner only.

## Configuration

Balda loads `.config/balda/config.yaml`, then applies `BALDA_*` environment
overrides. If `.env` exists in the working directory, Balda loads it before
config resolution.

Minimal shape:

```yaml
runtime:
  providers:
    <provider_id>:
      # generic_acp | gemini_acp | codex_acp | opencode_acp | copilot_acp | claude_code_acp | pool
      type: <provider_type>
  mcp_servers: {}

balda:
  provider: <provider_id>
  telegram:
    token: ""
    formatting_mode: "markdownv2"
    plan_updates: true
    webhook:
      enabled: false
      listen_addr: "0.0.0.0:8080"
      path: "/telegram/webhook"
      url: ""
      auth_token: ""
  webhooks:
    enabled: false
    listen_addr: "127.0.0.1:8090"
    routes: {}
  logger:
    level: "info"
    pretty: true
  working_dir: ""
  state_dir: ".config/balda"
  sessions:
    persistence: "sqlite"
  memory:
    enabled: true
  goal:
    max_iterations: 25
  event_bus:
    mode: "nats_core"
    nats:
      embedded: true
      host: "127.0.0.1"
      port: -1
      jetstream: true
      store_dir: ".balda/nats"
      max_memory: "256mb"
      max_store: "2gb"
      sync_always: false
      expose_monitoring: false
  swarm:
    enabled: true
    mode: "shadow"
    webhook_mode: "shadow"
    scheduler_mode: "shadow"
    shadow:
      enabled: true
    queue:
      default_mode: "followup"
      debounce_ms: 500
      cap: 20
      drop: "summarize"
      by_namespace:
        task.control: "interrupt"
        webhook.inbound: "followup"
        schedule.inbound: "collect"
        memory.sync: "collect"
    agents:
      planner:
        role: "Plan work and split into subtasks"
        tools: []
      executor:
        role: "Use project tools and make changes"
        tools: ["workspace", "shell", "mcp"]
      reviewer:
        role: "Validate result and inspect risks"
        tools: ["workspace", "shell"]
      memory:
        role: "Extract durable facts and summaries"
        tools: ["memory"]
  scheduler:
    jobs: []
  workspace:
    mode: "auto"
    base_branch: ""
  mcp_servers: []
  global_instruction: ""
```

Common settings:

- `balda.provider`: provider ID selected during `balda init`.
- `balda.telegram.token`: Telegram bot token, usually supplied by `.env` as `BALDA_TELEGRAM_TOKEN`.
- `balda.telegram.webhook.auth_token`: required when Telegram webhook mode is enabled; Telegram sends it as `X-Telegram-Bot-Api-Secret-Token`.
- `balda.webhooks.*`: optional local inbound webhook receiver for external event-to-session prompt injection via route templates (`path`, `prompt_template`) into the owner DM session.
- `balda.webhooks.*` security: inbound webhook requests are not authenticated by Balda; keep `listen_addr` private (localhost/private network) or front it with trusted gateway auth.
- `balda.sessions.persistence`: `sqlite` by default; keeps ADK conversation history across restarts until `/reset` or explicit `/close`.
- `balda.memory.enabled`: `true` by default; controls `${balda.state_dir}/MEMORY.md`, `/memory`, and `balda.memory.*` MCP tools.
- `balda.goal.max_iterations`: maximum Goalkeeper worker/validator iterations for `/goal`; defaults to `25`.
- `balda.event_bus.mode`: `nats_core` by default. `sqlite` disables NATS and uses SQLite mailbox polling only; `nats_core` keeps SQLite as the durable mailbox and uses embedded NATS for wakeups/events; `nats_jetstream` also creates JetStream command/event/control/DLQ streams and shadow-publishes commands while SQLite remains product state.
- `balda.event_bus.nats.*`: embedded NATS defaults bind to `127.0.0.1` on a random local port, keep monitoring disabled, and store JetStream files under `.balda/nats`.
- `balda.swarm.enabled`: `true` by default; enables swarm rollout plumbing.
- `balda.swarm.mode`: `shadow` by default; `shadow` dual-writes envelopes to SQLite and keeps the existing direct dispatch path, while `mailbox` routes work through SQLite-backed actor mailboxes with EventBus wakeups. `/goal` creates `swarm_tasks` records in all modes; mailbox mode coordinates Goal tasks through TaskActor -> AgentActor planner/executor/reviewer -> DeliveryActor.
- `balda.swarm.webhook_mode`: `shadow` by default; controls only generic inbound webhook intake (`legacy|shadow|mailbox`).
- `balda.swarm.scheduler_mode`: `shadow` by default; controls only config-managed recurring jobs (`legacy|shadow|mailbox`).
- `balda.swarm.shadow.enabled`: `true` by default; stores Telegram, webhook, schedule, and `/goal` envelopes with `status=shadow` for rollout comparison when a resolved mode is `shadow`.
- `balda.swarm.queue.*`: mailbox-mode queue policy only; defaults to `followup`, `500ms` collect debounce, cap `20`, and `summarize` overflow handling. Namespace overrides make `task.control` interrupt active work, while webhook intake follows up and schedule/memory inputs can collect. Deterministic collect/summarize rewrites only session envelopes; typed task envelopes keep their original payload contracts.
- `balda.swarm.agents.*`: logical single-process agent roles used by the swarm allocator. Defaults are `planner`, `executor`, `reviewer`, and `memory`; `tools` are advisory routing hints (`workspace`, `shell`, `mcp`, `memory`), not separate runtimes. Optional `cost_penalty` lowers allocator preference for expensive roles.
- Task visibility: `/tasks`, `/task <id>`, `/task <id> events`, `/task <id> cancel`, `/swarm status`, and `/mailbox status` read from `swarm_tasks`, `swarm_task_events`, and mailbox state.
- `balda.scheduler.jobs`: startup-reconciled recurring jobs (`id`, `cron`, `prompt`) that target the owner DM session.
- `${balda.state_dir}/SOUL.md`: optional operator instructions read at session start/restore when the file exists.
- `balda.workspace.mode`: `auto` by default; uses git worktrees when Balda runs in a git repository.
- `balda.mcp_servers`: extra MCP server IDs added to every Balda-started session.

## MCP Servers

MCP servers can be attached to providers or injected into every Balda session.
Balda also includes a built-in `balda` MCP server for memory and workspace
tools.

### MCP Servers Example

```yaml
runtime:
  mcp_servers:
    local-tools:
      type: stdio
      cmd: ["npx", "-y", "@modelcontextprotocol/server-filesystem", "/workspace"]
    remote-tools:
      type: http
      url: https://mcp.example.com/mcp

  providers:
    codex:
      type: codex_acp
      mcp_servers:
        - local-tools

balda:
  provider: codex
  mcp_servers:
    - remote-tools
```

Effective MCP IDs are built-in balda + provider mcp_servers + balda.mcp_servers.
Do not define `runtime.mcp_servers.balda`; Balda owns that bundled server.

## Troubleshooting

- `telegram token is required`: run `balda init`, set `BALDA_TELEGRAM_TOKEN` in `.env`, or set `balda.telegram.token` in config.
- `no supported agent CLI detected`: install or expose one of `codex`, `opencode`, `copilot`, `gemini`, or `claude`.
- `balda.provider is required`: rerun `balda init` or set `balda.provider` to a configured provider ID.
- Session history should not survive restarts: set `balda.sessions.persistence=memory` or `BALDA_SESSIONS_PERSISTENCE=memory`.
- Memory facts are not visible in an active session: memory is snapshotted when a session starts or restores; use `/reset` or `/close` to recreate the provider session.
- Workspace import/export issues: check `balda.workspace.mode`, `balda.workspace.base_branch`, and that Balda is running in the expected git checkout.
- Progress updates are too noisy: set `balda.telegram.plan_updates=false`.

## Documentation

- Technical specification: [`docs/balda.md`](docs/balda.md)
- Release notes: [`docs/release-notes.md`](docs/release-notes.md)
- Telegram formatting guide: [`docs/telegram-formatting.md`](docs/telegram-formatting.md)
- Contributing guide: [`CONTRIBUTING.md`](CONTRIBUTING.md)
- Agent workflow/policies: [`AGENTS.md`](AGENTS.md)

## Release

- GitHub Releases: <https://github.com/normahq/balda/releases>
- npm package: <https://www.npmjs.com/package/@normahq/balda>
