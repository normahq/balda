# balda

[![test](https://github.com/normahq/balda/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/normahq/balda/actions/workflows/test.yml)
[![lint](https://github.com/normahq/balda/actions/workflows/lint.yml/badge.svg?branch=main)](https://github.com/normahq/balda/actions/workflows/lint.yml)

## Telegram control plane for coding agents

Balda is a lightweight Telegram app for running long-lived ACP coding agents
from DMs, groups, and topic chats. Point it at a project, connect your bot,
and keep agent work moving without leaving Telegram.

It runs as one app with local SQLite state: no Redis, Postgres, object storage,
queues, public webhook, or published port required. Use Codex, OpenCode,
Copilot, Gemini, Claude Code, or any ACP-compatible command, with durable
history, memory, MCP tools, and optional git workspace isolation.

```bash
npm install -g -y @normahq/balda
balda init
balda start
```

## What You Get

| Feature | What it means |
|---------|---------------|
| No backing services | Balda stores local state in SQLite and does not require Redis, Postgres, queues, or object storage. |
| No webhook required | Polling mode is the default, so local quickstarts do not need a public URL or published port. |
| Any ACP agent | Use built-in providers for `codex`, `opencode`, `copilot`, `gemini`, and `claude`, or wire any ACP-compatible command with `generic_acp`. |
| Telegram control plane | One owner, optional collaborators, direct-message sessions, topic sessions with `/topic <name>`, and public-chat mention/reply routing. |
| Git workspaces | Each session can get its own git worktree, with `balda.workspace.import` and `balda.workspace.export` MCP tools for safe branch flow. |
| Durable sessions | SQLite persistence is on by default, so conversation history survives restarts until `/reset` or explicit `/close`. |
| Memory system | `MEMORY.md` stores facts, `/memory` shows them, and `balda.memory.*` MCP tools let agents remember user-approved facts. |
| MCP support | Add stdio, HTTP, or SSE MCP servers globally, per provider, or for every Balda session. |
| Docker Compose runtime | Run Balda in a container while using the current directory, `.env`, `.git`, and `.config/balda` from the host. |

## How It Works

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
- `/goal <objective>`: start a Goalkeeper worker -> validator loop in the current session context/workspace. Goal updates are sent with `balda.telegram.formatting_mode`. See [`docs/goalkeeper.md`](docs/goalkeeper.md).
- `/reset`: clear conversation history for the current session.
- `/close`: reset history, then close the current topic or restart the owner session on the next message.
- `/cancel`: cancel in-flight work, drop queued turns, and abort active `/goal` run for the current session.
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
  inbound_webhooks:
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
  locators: {}
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
- `balda.inbound_webhooks.*`: optional local inbound webhook receiver for external event-to-session prompt injection via configured route aliases and templates.
- `balda.sessions.persistence`: `sqlite` by default; keeps ADK conversation history across restarts until `/reset` or explicit `/close`.
- `balda.memory.enabled`: `true` by default; controls `${balda.state_dir}/MEMORY.md`, `/memory`, and `balda.memory.*` MCP tools.
- `balda.goal.max_iterations`: maximum Goalkeeper worker/validator iterations for `/goal`; defaults to `25`.
- `balda.locators`: canonical session locator aliases for config-managed scheduler jobs and inbound webhook routes.
- `balda.scheduler.jobs`: startup-reconciled recurring jobs (`id`, `alias`, `cron`, `prompt`).
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
