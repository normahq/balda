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
| Collaborative visibility | The team can see progress, cancel work, and manage who can assign work. |
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

Local repo development helper:

```bash
make dev
```

Run fake ingress scenarios (Telegram/webhook/scheduler command publish paths):

```bash
make scenarios
```

Replay projection events through the deterministic projector replay suite:

```bash
make projection-replay
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

- `/start owner=<owner_token>`: authenticate the owner in direct messages.
- `/start invite=<invite_token>`: onboard a collaborator in direct messages.
- `/topic <name>`: create a named topic session.
- `/goal <objective>`: start goal work in the current session context/workspace. Goal updates use `balda.telegram.formatting_mode`; terminal updates include Result, Artifacts, Confidence, and Next action sections. See [`docs/goalkeeper.md`](docs/goalkeeper.md).
- `/close`: direct messages only; reset the current session history. In a topic, it also closes that topic.
- `/cancel`: request cancellation of in-flight work in the current session, including active `/goal` runs.
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
  swarm: {}
  scheduler:
    tasks: []
  workspace:
    mode: "auto"
    base_branch: ""
    sessions_dir: "sessions"
  mcp_servers: []
  global_instruction: ""
```

Common settings:

- `balda.provider`: provider ID selected during `balda init`.
- `balda.telegram.token`: Telegram bot token, usually supplied by `.env` as `BALDA_TELEGRAM_TOKEN`.
- `balda.telegram.webhook.auth_token`: required when Telegram webhook mode is enabled; Telegram sends it as `X-Telegram-Bot-Api-Secret-Token`.
- `balda.webhooks.*`: optional local inbound webhook receiver for external event-to-session ingress. Each route defines `path`, `prompt_template`, `envelope` (`target`, `key`, optional `mode=task|session`, optional `report_to`), `auth` (`type=none|header`, `header`, `value` or `secret_env`), and `dedupe` (`source=request_id|header|body_sha256`, optional `header` for header source).
- `balda.webhooks.*` security: set route `auth` (for example shared-token header) and keep `listen_addr` private (localhost/private network) or front it with trusted gateway auth.
- `balda.sessions.persistence`: `sqlite` by default; keeps ADK conversation history across restarts until the session is explicitly closed.
- `balda.memory.enabled`: `true` by default; controls `${balda.state_dir}/MEMORY.md` and `balda.memory.*` MCP tools.
- `balda.goal.max_iterations`: maximum Goalkeeper worker/validator iterations for `/goal`; defaults to `25`.
- `balda.nats.*`: embedded JetStream is required by default, binds to `127.0.0.1` on a random local port, keeps monitoring disabled, and stores JetStream files under `.balda/nats`.
- Removed runtime keys are rejected at startup (`balda.event_bus.*`, `balda.swarm.mode`, `balda.webhooks.mode`, `balda.scheduler.mode`).
- `balda.swarm` configures command processing for goals, scheduled work, retries, and webhook delivery. The runtime is always on.
- `balda.swarm.commands.*`: command-processing capacity and timing settings.
- `balda.swarm.events.*`: internal event-stream settings.
- `balda.swarm.dlq.*`: internal failure-retention settings.
- `balda.scheduler.tasks`: startup-reconciled recurring tasks. Each task has `id`, `cron`, and `envelope` with `target`, `key`, `content`, and optional `report_to`. Scheduled work publishes first-class task commands; replies are fire-and-forget unless `report_to` is set.
- `${balda.state_dir}/SOUL.md`: optional operator instructions read at session start/restore when the file exists.
- `balda.workspace.mode`: `auto` by default; uses git worktrees when Balda runs in a git repository.
- `balda.workspace.sessions_dir`: directory name under `balda.state_dir` used for per-session worktrees (defaults to `sessions`).
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
- Memory facts are not visible in an active session: memory is snapshotted when a session starts or restores; close and reopen the session to refresh it.
- Workspace import/export issues: check `balda.workspace.mode`, `balda.workspace.base_branch`, and that Balda is running in the expected git checkout.
- Progress updates are too noisy: set `balda.telegram.plan_updates=false`.
- Startup fails with `jetstream is required` or `create or update stream`: keep `balda.nats.jetstream=true`, ensure `balda.nats.store_dir` is writable, and verify disk space.
- Startup fails with command/event consumer creation errors: verify unique consumer names in `balda.swarm.commands.consumer` and that no external process is mutating the same embedded store concurrently.
- Runtime issues show up in structured logs; check recent command failures and retry pressure before increasing transport limits.

## Documentation

- Technical specification: [`docs/balda.md`](docs/balda.md)
- Architecture map: [`docs/architecture/index.md`](docs/architecture/index.md)
- Active execution plans: [`docs/exec-plans/active/`](docs/exec-plans/active/)
- Migration debt register: [`docs/tech-debt/jetstream-migration-debt.md`](docs/tech-debt/jetstream-migration-debt.md)
- Release notes: [`docs/release-notes.md`](docs/release-notes.md)
- Telegram formatting guide: [`docs/telegram-formatting.md`](docs/telegram-formatting.md)
- Contributing guide: [`CONTRIBUTING.md`](CONTRIBUTING.md)
- Agent workflow/policies: [`AGENTS.md`](AGENTS.md)

## Release

- GitHub Releases: <https://github.com/normahq/balda/releases>
- npm package: <https://www.npmjs.com/package/@normahq/balda>
