# Slack Integration

Balda supports Slack as an internal app channel. Slack uses HTTP Events API and
slash command requests. Balda serves plain HTTP only; HTTPS termination,
certificates, reverse proxies, ingress, tunnels, and public Request URL
management are out of scope for Balda and must be handled by deployment
infrastructure.

## Architecture

```
Slack Events API / slash command
           ↓  HTTPS handled outside Balda
       Balda HTTP endpoint (:8091/slack/events or /slack/commands)
           ↓  verify Slack signature and ACK quickly
       Balda durable actor runtime
           ↓
       Slack Web API chat.postMessage
```

Balda maps Slack DMs to personal sessions and Slack channel threads to isolated
topic sessions.

Slack replies use Balda's shared delivery format. The `markdown` and `auto`
formats are sent as Slack `mrkdwn`; `plain` disables `mrkdwn`; `html` is not
supported for Slack delivery.

## Slack App Setup

Create an internal Slack app and configure:

- Bot token scopes: `commands`, `chat:write`, `app_mentions:read`,
  `im:history`, `channels:history`
- Optional private channel support: add `groups:history` and set
  `balda.slack.include_private_channels=true`
- Event subscriptions:
  - `app_mention`
  - `message.im`
  - `message.channels`
  - optional `message.groups`
- Slash command:
  - Command: `/balda`
  - Request URL: your public HTTPS URL that forwards to Balda's
    `balda.slack.commands_path`
- Events Request URL: your public HTTPS URL that forwards to Balda's
  `balda.slack.events_path`

## Balda Configuration

Environment:

```env
BALDA_SLACK_ENABLED=true
BALDA_SLACK_BOT_TOKEN=xoxb-...
BALDA_SLACK_SIGNING_SECRET=...
```

YAML:

```yaml
balda:
  slack:
    enabled: true
    bot_token: "xoxb-..."
    signing_secret: "..."
    listen_addr: "0.0.0.0:8091"
    events_path: "/slack/events"
    commands_path: "/slack/commands"
    include_private_channels: false
```

Owner channel-bind tokens can be consumed in a direct message by sending the
exact `balda_...` token, or with `/balda start <balda_token>`.

## Commands

Slack uses `/balda` instead of Telegram/Zulip slash commands:

| Slack command | Description |
| --- | --- |
| `/balda start owner=<token>` | Register owner in a DM |
| `/balda start invite=<token>` | Register collaborator in a DM |
| `/balda start <balda_token>` | Connect this Slack account to the existing owner |
| `/balda topic <name>` | Create a Slack thread-backed Balda session |
| `/balda goal <objective>` | Start goal work in the current session |
| `/balda goal clear` | Stop active goal work for the current session |
| `/balda cancel` | Cancel the current session turn and drop queued turns |
| `/balda locator` | Show the current locator ref |
| `/balda close` | Reset DM session history |
| `/balda user add` | Generate a collaborator invite token |
| `/balda user list` | List collaborators |
| `/balda user remove <user_id>` | Remove a collaborator |

## Session Rules

- DM messages create or restore a DM session.
- `@Balda ...` in a channel creates or restores a thread session.
- If an `@Balda` mention is not already in a thread, Balda uses that message as
  the thread root.
- Messages in active Balda threads continue the session.
- Messages outside active Balda threads are ignored.

## Troubleshooting

- **Slack receiver does not start**: set `balda.slack.enabled=true`,
  `balda.slack.bot_token`, and `balda.slack.signing_secret`.
- **Slack URL verification fails**: confirm the public Events Request URL
  forwards to `balda.slack.events_path`.
- **Signature verification fails**: confirm the signing secret and ensure the
  forwarding layer preserves the raw request body.
- **Replies fail**: check the bot token and `chat:write` scope.
- **Private channel messages are ignored**: add `groups:history`, invite the
  app to the channel, and set `include_private_channels=true`.
