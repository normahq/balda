# Zulip Webhook Integration

Balda supports Zulip as a channel transport alongside Telegram. Zulip uses an
**outgoing webhook bot** approach: Zulip pushes messages to Balda's HTTP
endpoint, and Balda replies via the Zulip REST API.

## Architecture

```
User → Zulip stream/topic or DM
           ↓  outgoing webhook POST
       Balda HTTP endpoint (:8090/zulip/webhook)
           ↓  process message
       Zulip REST API (POST /api/v1/messages)
           ↓
       Zulip stream/topic or DM
```

Balda maps Zulip stream+topic pairs to separate agent sessions (equivalent to
Telegram forum topics), and Zulip DMs to a personal DM session.

## Setup

### 1. Create the bot in Zulip

Open **Settings → Bots** and click **Add a new bot**. Choose:

- **Bot type**: Outgoing webhook
- **Full name**: any name (e.g. `Balda`)
- **Bot email**: becomes the bot's email address
- **Endpoint URL**: `http://<your-host>:8090/zulip/webhook`

After creation, copy two values:
- **API key** — used by Balda to send messages back via the Zulip REST API
- **Token** (shown in the webhook settings) — used to verify that incoming
  webhook payloads are authentic; set this as `balda.zulip.webhook_token`

### 2. Configure Balda

In your `.env` (recommended) or `config.yaml`:

```env
BALDA_ZULIP_BOT_EMAIL=my-bot@zulip.example.com
BALDA_ZULIP_API_KEY=<api-key from step 1>
BALDA_ZULIP_SERVER_URL=https://zulip.example.com
BALDA_ZULIP_WEBHOOK_TOKEN=<token from step 1>
BALDA_ZULIP_WEBHOOK_ENABLED=true
```

`BALDA_ZULIP_SERVER_URL` must be an absolute `http://` or `https://` URL.

Or in `config.yaml`:

```yaml
balda:
  zulip:
    bot_email: "my-bot@zulip.example.com"
    api_key: "<api-key>"
    server_url: "https://zulip.example.com"
    webhook_token: "<token>"
    webhook:
      enabled: true
      listen_addr: "0.0.0.0:8090"
      path: "/zulip/webhook"
```

### 3. (Optional) Allow trusted owners to auto-claim topics

By default, a user must send `/start owner=<token>` in a DM to register as
owner before they can assign Balda to a topic. If you trust a set of Zulip
users unconditionally, you can list their emails in `allowed_owners`:

```yaml
balda:
  zulip:
    allowed_owners:
      - alice@example.com
      - bob@example.com
```

Any listed user who @-mentions Balda in a stream topic will be automatically
registered as the topic owner (or added as a collaborator if the topic already
has one). A bare @-mention with no message text sends a welcome reply.

### 4. Authenticate as owner

Send a direct message to the bot in Zulip:

```
/start owner=<owner_token>
```

The `owner_token` is printed by `balda init` or logged at startup.

This step is not required for users listed in `allowed_owners`.

## Streams and Topics

Balda maps each stream+topic pair to its own session:

- `/stream-name/topic-name` → isolated session, persistent history
- DM to bot → personal DM session

This matches the Telegram model where each forum topic is a separate session.

## Bot Commands

Balda supports these commands in Zulip:

| Command | Description |
|---------|-------------|
| `/start owner=<token>` | Register as bot owner (DM only) |
| `/start invite=<token>` | Onboard as collaborator (DM only) |
| `/topic <name>` | Create a session in the current stream's native Zulip topic |
| `/goal <objective>` | Start goal work from the current session context |
| `/goal clear` | Stop active goal work for the current session |
| `/reset`, `/restart` | Restart current session history |
| `/cancel` | Cancel current session turn; active goal runs continue |
| `/locator` | Show current locator ref |
| `/close` | Reset DM session history (DM only) |
| `/user add` | Generate collaborator invite token |
| `/user list` | List collaborators |
| `/user remove <id>` | Remove a collaborator |

## Network Access

Balda's Zulip webhook server listens on `:8090` by default. Zulip must be able
to reach this address. Options:

- **Direct**: expose port 8090 on the host running Balda
- **Reverse proxy**: front with nginx/caddy, terminate TLS, forward to `:8090`
- **Tunnel**: use a tunnel service for development

Set `balda.zulip.webhook.listen_addr` to change the bind address.
If you customize `balda.zulip.webhook.path`, set it to an absolute HTTP path
starting with `/`, for example `/zulip/webhook`.

## Differences from Telegram

| Feature | Telegram | Zulip |
|---------|----------|-------|
| Ingress | Polling or webhook | Outgoing webhook only |
| Topic creation | `/topic <name>` command | Native Zulip topics |
| Topic close | Removes forum topic | Resets session history |
| Message formatting | MarkdownV2 / HTML / plain | Standard Markdown |
| Plan update drafts | Edits-in-place (`SendDraftPlain`) | No-op (not supported) |
| Progress typing | Typing indicator | Typing indicator |

## Troubleshooting

- **`zulip webhook disabled; skipping server start`**: set
  `balda.zulip.webhook.enabled=true` or `BALDA_ZULIP_WEBHOOK_ENABLED=true`.
- **Webhook token mismatch**: verify `balda.zulip.webhook_token` matches the
  token shown in the Zulip bot's outgoing webhook settings.
- **401 Unauthorized from Zulip API**: check `balda.zulip.bot_email` and
  `balda.zulip.api_key`.
- **Bot not responding**: ensure Balda's `:8090` is reachable from the Zulip
  server; check firewall and NAT rules.
- **Bot responds to all messages, not just mentions**: outgoing webhook bots in
  Zulip fire on every message unless scoped by stream subscription; consider
  restricting the bot's stream access.
- **Bot posts trigger new webhook events**: Balda ignores webhook payloads where
  `message.sender_email` matches Zulip's `bot_email`, so API replies do not
  recurse into new turns.
- **Webhook payload text field differs**: Balda prefers Zulip's top-level
  `data` field, and falls back to `message.content` when `data` is empty.
- **Zulip rejects rendered content**: Balda retries agent/Markdown replies once
  as plain text when Zulip returns a client-side content rejection. Transient
  Zulip API failures are left to the durable delivery retry path.
- **Zulip API delivery fails**: queued turns return delivery errors to the actor
  runtime, so transient failures can be retried and persistent failures surface
  through the runtime's failure handling instead of being silently acknowledged.
- **Invalid locator in scheduler/webhook config**: Zulip stream and DM locators
  reject nonpositive `stream_id` or `user_id` values before calling Zulip's REST
  API.
- **Bot ignores first message in a new topic**: this was a bug where the HTTP
  request context was cancelled before the goroutine finished processing.
  Fixed in `zulip_handler.go` by using `context.WithoutCancel`.
