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

1. Pick a provider runtime.
2. Connect a Telegram bot token, a Zulip outgoing webhook bot, **or** an internal Slack app.
3. Chat, create topics, and let Balda persist session state, memory, and workspaces.

Balda runs one provider runtime per process and maps chat conversations to
separate agent sessions. Each Telegram topic, Zulip stream+topic pair, or
Slack thread becomes an isolated session. That keeps the bot simple to operate
while preserving session boundaries.

## Quickstart

You need:

- a Telegram bot token from BotFather, a Zulip outgoing webhook bot, **or** an internal Slack app
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

Inspect the runtime streams and consumers:

```bash
make runtime-state
```

Replay projection events through the deterministic projector replay suite:

```bash
make projection-replay
```

Authenticate in Telegram with the printed auth link, or send the printed command
directly to your bot:

```text
/start owner=<owner_token>
```

After owner auth, send a normal direct message to start the bot's main DM
session. Create a named topic session when you want an isolated workspace and
conversation:

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

## Published Container Image

Balda also publishes an official container image to
`ghcr.io/normahq/balda:latest`.

This image is built from the tagged source tree with a separate
[`Dockerfile.release`](Dockerfile.release). Unlike the local Compose image, the
published GHCR image is a minimal carrier for the `balda` binary only. It does
not bundle `codex`, `opencode`, `copilot`, `gemini`, or `claude`.

Use it as a source stage in your own multistage Dockerfile, then add only the
provider CLI runtime you want in the final image. For example, a Codex-based
runtime can copy `balda` from GHCR and install Codex separately:

```dockerfile
FROM node:24-bookworm-slim AS cli-builder
RUN npm install -g @openai/codex

FROM ghcr.io/normahq/balda:latest AS balda

FROM node:24-bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates \
      git \
      openssh-client \
      ripgrep \
 && rm -rf /var/lib/apt/lists/*

COPY --from=cli-builder /usr/local/lib/node_modules /usr/local/lib/node_modules
COPY --from=cli-builder /usr/local/bin/codex /usr/local/bin/codex
COPY --from=balda /usr/local/bin/balda /usr/local/bin/balda

WORKDIR /workspace
ENTRYPOINT ["balda"]
```

Local Docker Compose still uses the root [`Dockerfile`](Dockerfile) and
[`compose.yaml`](compose.yaml), which remain the bundled-CLI runtime for
workspace-oriented local project work.

## Configure Any Provider Runtime

Balda has built-in provider types for common CLIs and a generic provider
adapter for anything else that speaks the same runtime protocol.

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
- `/topic <name>`: owner/collaborator, direct messages only; create a named topic session.
- `/goal <objective>`: owner/collaborator; start goal work from the current session context in isolated GoalKeeper worker/validator ADK sessions. With workspace mode enabled, Balda creates a goal workspace from `balda.workspace.base_branch`, exports it back automatically on success, and preserves it for recovery if export fails. With workspace mode disabled, GoalKeeper works directly in `balda.working_dir` and records `not_exported` on success. Goal updates use `balda.telegram.formatting_mode`; terminal updates include concise result, export, work, validation, and actionable next-step sections when needed. Only one `/goal` run can be active per session. See the [goal workflow doc](docs/goal-workflow.md).
- `/goal clear`: owner/collaborator; stop active `/goal` work for the current session only.
- `/reset`, `/restart`: owner/collaborator; cancel current session work, clear the current session history, and immediately start a fresh runtime session without closing the chat or topic. Works in any current session context.
- `/locator`: owner/collaborator; show the current transport type and a pasteable locator ref for scheduler/webhook `target: locator` config.
- `/close`: owner/collaborator, direct messages only; reset the current session history. In a topic, it also closes that topic.
- `/cancel`: owner/collaborator; cancel the current session turn and drop queued turns for that session. It does not stop active `/goal` work.
- `/user add`: owner only; generate a collaborator invite link.
- `/user list`: owner only; list collaborators and active invites.
- `/user remove <user_id>`: owner only; remove a collaborator by user ID.

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
      codex_acp:
        # Optional Codex reasoning effort.
        reasoning_effort: <minimal|low|medium|high|xhigh>
  mcp_servers: {}

balda:
  provider: <provider_id>
  telegram:
    token: ""
    formatting_mode: "rich_markdown"
    plan_updates: true
    webhook:
      enabled: false
      listen_addr: "0.0.0.0:8080"
      path: "/telegram/webhook"
      url: ""
      auth_token: ""
  zulip:
    bot_email: ""
    api_key: ""
    server_url: ""
    webhook_token: ""
    webhook:
      enabled: false
      listen_addr: "0.0.0.0:8090"
      path: "/zulip/webhook"
  slack:
    enabled: false
    bot_token: ""
    signing_secret: ""
    listen_addr: "0.0.0.0:8091"
    events_path: "/slack/events"
    commands_path: "/slack/commands"
    allowed_owners: []
    include_private_channels: false
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
- `balda.zulip.bot_email`, `balda.zulip.api_key`, `balda.zulip.server_url`: Zulip outgoing webhook bot credentials. `server_url` must be an absolute `http://` or `https://` URL. See [`docs/zulip-webhook.md`](docs/zulip-webhook.md) for setup steps.
- `balda.zulip.webhook_token`: verification token from the Zulip outgoing webhook bot settings.
- `balda.zulip.webhook.enabled`: set `true` to start the Zulip webhook receiver on `listen_addr`. When this is `true`, a Telegram token is not required — Balda can run Zulip-only.
- `balda.zulip.allowed_owners`: list of Zulip user emails trusted to auto-claim any topic by @-mentioning Balda, without needing `/start owner=<token>` first.
- `balda.slack.enabled`: set `true` to start the Slack HTTP receiver. Balda serves plain HTTP only; HTTPS termination and public Request URL routing are external deployment concerns. See [`docs/slack.md`](docs/slack.md).
- `balda.slack.bot_token`: Slack bot token (`xoxb-...`), usually supplied as `BALDA_SLACK_BOT_TOKEN`.
- `balda.slack.signing_secret`: Slack signing secret used to verify Events API and slash command requests, usually supplied as `BALDA_SLACK_SIGNING_SECRET`.
- `balda.slack.allowed_owners`: Slack subjects trusted to auto-claim owner on first message, formatted as `slack:<team_id>:<user_id>`.
- `balda.telegram.webhook.auth_token`: required when Telegram webhook mode is enabled; Telegram sends it as `X-Telegram-Bot-Api-Secret-Token`.
- `balda.webhooks.*`: optional local inbound webhook receiver for external event-to-session ingress. Each route defines `path`, `prompt_template`, `envelope` (`target`, `key`, optional `mode=task|session`, optional `report_to`), `auth` (`type=none|header`, `header`, `value` or `secret_env`), and `dedupe` (`source=request_id|header|body_sha256`, optional `header` for header source). Use `target: locator` with a `/locator` value in `key` to route directly to a specific session context.
- `balda.webhooks.*` security: set route `auth` (for example shared-token header) and keep `listen_addr` private (localhost/private network) or front it with trusted gateway auth.
- `balda.sessions.persistence`: `sqlite` by default; keeps conversation history across restarts until the session is explicitly closed.
- `balda.memory.enabled`: `true` by default; controls `${balda.state_dir}/MEMORY.md` and `balda.memory.*` MCP tools.
- `balda.goal.max_iterations`: maximum `/goal` worker-validator loop iterations; defaults to `25`.
- `balda.nats.*`: built-in command/event runtime settings. Defaults bind to `127.0.0.1` on a random local port, keep monitoring disabled, and store runtime files under `${balda.state_dir}/nats`.
- `balda.swarm`: optional advanced runtime tuning for goals, scheduled work, retries, and webhook delivery. Most installs should leave it at defaults.
- `balda.scheduler.tasks`: startup-reconciled recurring tasks. Each task has `id`, `cron`, and `envelope` with `target`, `key`, `content`, and optional `report_to`. Scheduled work publishes first-class task commands; replies are fire-and-forget unless `report_to` is set. Use `target: locator` with a `/locator` value in `key` to target a specific session.
- `balda.workspace.mode`: `auto` by default; uses git worktrees when Balda runs in a git repository.
- `balda.workspace.sessions_dir`: directory name under `balda.state_dir` used for per-session worktrees (defaults to `sessions`).
- `balda.mcp_servers`: extra MCP server IDs added to every Balda-started session.
- `runtime.providers.<id>.codex_acp.reasoning_effort`: optional Codex reasoning effort. Balda passes this through to Norma, which maps it to Codex ACP session startup/resume config.

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
      codex_acp:
        reasoning_effort: high
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
- Startup fails while initializing the built-in runtime streams: keep the default `balda.nats` settings unless you have a specific local runtime need, ensure `${balda.state_dir}/nats` is writable, and verify disk space.
- Startup fails while initializing built-in runtime consumers: stop any other Balda process sharing the same embedded store and restart.
- Runtime issues show up in structured logs; check recent command failures and retry pressure before increasing transport limits.
- `zulip webhook disabled; skipping server start`: set `balda.zulip.webhook.enabled=true` or `BALDA_ZULIP_WEBHOOK_ENABLED=true`.
- Zulip webhook token mismatch: verify `balda.zulip.webhook_token` matches the token shown in the Zulip outgoing webhook bot settings.
- Zulip 401 Unauthorized: check `balda.zulip.bot_email` and `balda.zulip.api_key`.
- `slack disabled; skipping server start`: set `balda.slack.enabled=true` or `BALDA_SLACK_ENABLED=true`.
- Slack request signature failures: verify `balda.slack.signing_secret` matches the app's signing secret and that the forwarding layer preserves request body bytes.
- Slack delivery failures: check `balda.slack.bot_token`, bot scopes, and whether Balda can reach `https://slack.com/api`.

## Documentation

- Technical specification: [`docs/balda.md`](docs/balda.md)
- Architecture map: [`docs/architecture/index.md`](docs/architecture/index.md)
- Telegram formatting guide: [`docs/telegram-formatting.md`](docs/telegram-formatting.md)
- Zulip webhook integration: [`docs/zulip-webhook.md`](docs/zulip-webhook.md)
- Slack integration: [`docs/slack.md`](docs/slack.md)
- Contributing guide: [`CONTRIBUTING.md`](CONTRIBUTING.md)
- Agent workflow/policies: [`AGENTS.md`](AGENTS.md)

## Release

- GitHub Releases: <https://github.com/normahq/balda/releases>
- npm package: <https://www.npmjs.com/package/@normahq/balda>
