# balda

[![test](https://github.com/baldaworks/balda/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/baldaworks/balda/actions/workflows/test.yml)
[![lint](https://github.com/baldaworks/balda/actions/workflows/lint.yml/badge.svg?branch=main)](https://github.com/baldaworks/balda/actions/workflows/lint.yml)

## Self-hosted engineering agent for team chat

Balda is a self-hosted engineering agent that lives in your team chat and works
inside your project.

You can give it a task in chat, open a focused topic for a piece of work, run a
longer goal loop, or wire external events into the same workflow. Balda keeps
context, uses your configured tools, and returns something reviewable: a
summary, changed files, validation output, a commit, or a concrete next step.

## What Balda is good for

- chat-native engineering help in Telegram, Zulip, or Slack
- focused task threads instead of one shared bot conversation
- long-running goal execution with progress updates and final results
- `wedge` style operation: put the agent in the middle of your team workflow so
  chat, tools, schedules, and external events all feed the same execution path
- self-hosted deployment close to your repo, config, and credentials

## Quickstart

You need:

- one chat surface: Telegram, Zulip, or Slack
- one supported provider CLI installed on the host or in Docker:
  `codex`, `opencode`, `copilot`, `gemini`, or `claude`
- Node.js/npm, unless you run the Docker Compose path

Install:

```bash
npm install -g -y @normahq/balda
```

Initialize in your project:

```bash
balda init
```

Start:

```bash
balda start
```

`balda init` creates `.config/balda/config.yaml`, initializes
`.config/balda/state.db`, detects available provider CLIs, and prints the next
step for your selected chat provider.

## First run

For Telegram, authenticate the owner with the command printed by `balda init`:

```text
/start owner=<owner_token>
```

Then send a normal direct message to the bot, or open an isolated topic:

```text
/topic release
```

From there you can:

- ask for ordinary help in chat
- start a goal loop with `/goal <objective>`
- stop the current turn with `/cancel`
- reset the current session with `/reset`

## Main workflows

### 1. Ordinary chat work

Send a message in the session where you want work to happen. Balda keeps that
conversation as the execution context.

### 2. Focused topic work

Use `/topic <name>` to create a separate session for a task, incident, release,
or stream of work.

### 3. Goal-driven execution

Use `/goal <objective>` when you want Balda to keep working until there is a
result to review.

Balda will:

- work in repeated passes
- post progress updates
- ask follow-up questions when critical input is missing
- return a terminal result with outcome details

See [docs/goal-workflow.md](docs/goal-workflow.md) for the detailed goal
contract.

### 4. Wedge mode

Balda can act as a wedge between team chat and the rest of your engineering
system:

- chat messages start work
- scheduled jobs wake work up
- inbound webhooks turn external events into session work
- the same session can continue through follow-up questions and delayed work

This is useful when you want one operational path for human requests,
automation, and agent execution instead of separate bots and scripts.

## Supported chat providers

- Telegram
- Zulip
- Slack

Balda maps each conversation scope to its own session:

- Telegram direct chat or topic
- Zulip stream + topic
- Slack thread

## Docker Compose

Balda ships a root [Dockerfile](Dockerfile) and [compose.yaml](compose.yaml)
for local Docker Compose deployment.

The current directory is mounted as `/workspace`, so Balda sees your checkout,
config, git metadata, and local state.

```bash
docker compose build balda
docker compose run --rm balda init
docker compose up -d balda
```

Polling mode is the default, so Telegram does not require publishing a port.
Webhook deployment details live in [docs/balda.md](docs/balda.md).

## Published container image

Balda publishes a release image at `ghcr.io/baldaworks/balda:latest`.

That image contains the `balda` binary only. It does not bundle provider CLIs.
Use it as a source stage in your own image and add the provider runtime you
want.

Example with Codex:

```dockerfile
FROM node:24-bookworm-slim AS cli-builder
RUN npm install -g @openai/codex

FROM ghcr.io/baldaworks/balda:latest AS balda

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

## Core commands

- `/start owner=<owner_token>` — owner bootstrap in direct messages
- `/start invite=<invite_token>` — collaborator onboarding
- `/topic <name>` — open a focused session
- `/goal <objective>` — run a longer goal loop
- `/goal clear` — stop active goal work in the current session
- `/cancel` — stop the current turn
- `/reset` or `/restart` — clear the current session and start fresh
- `/locator` — show the current session locator for scheduler/webhook routing

Full command behavior is documented in [docs/balda.md](docs/balda.md).

## Configuration

Balda loads `.config/balda/config.yaml` and then applies `BALDA_*` environment
overrides. If a local `.env` exists, Balda loads it before resolving config.

Minimal shape:

```yaml
runtime:
  providers:
    codex:
      type: codex_acp
  mcp_servers: {}

balda:
  provider: codex
  telegram:
    token: ""
```

Common settings:

- `balda.provider` — which configured provider runtime to use
- `balda.telegram.token` — Telegram bot token
- `balda.zulip.*` — Zulip outgoing webhook bot credentials and receiver config
- `balda.slack.*` — Slack bot token, signing secret, and HTTP receiver config
- `balda.webhooks.*` — optional inbound webhook routes
- `balda.scheduler.jobs` — recurring scheduled jobs
- `balda.workspace.*` — workspace/worktree behavior for goal execution
- `balda.mcp_servers` — MCP servers injected into Balda-started sessions

For complete configuration, examples, and provider-specific details, see
[docs/balda.md](docs/balda.md).

## Troubleshooting

- `telegram token is required` — run `balda init` or set
  `BALDA_TELEGRAM_TOKEN`
- `no supported agent CLI detected` — install or expose one of `codex`,
  `opencode`, `copilot`, `gemini`, or `claude`
- `balda.provider is required` — rerun `balda init` or set a configured
  provider id manually
- webhook or Slack/Zulip startup issues — verify the matching `balda.*`
  integration settings in config
- workspace import/export issues — check `balda.workspace.mode`,
  `balda.workspace.base_branch`, and the git checkout Balda is running in

## Docs

- Product and operator docs: [docs/balda.md](docs/balda.md)
- Goal workflow: [docs/goal-workflow.md](docs/goal-workflow.md)
- Architecture map: [docs/architecture/index.md](docs/architecture/index.md)
- Telegram formatting: [docs/telegram-formatting.md](docs/telegram-formatting.md)
- Zulip webhook setup: [docs/zulip-webhook.md](docs/zulip-webhook.md)
- Slack setup: [docs/slack.md](docs/slack.md)
- Contributing: [CONTRIBUTING.md](CONTRIBUTING.md)

## Release

- GitHub Releases: <https://github.com/baldaworks/balda/releases>
- npm package: <https://www.npmjs.com/package/@normahq/balda>
