# Telegram Message Formatting

Balda sends final assistant responses to Telegram with the configured
`balda.telegram.formatting_mode`.

This page is Telegram-specific. Other transports use Balda's shared delivery
formats and map `markdown` to their own channel-native dialects, such as Slack
`mrkdwn` and Zulip Markdown.

Allowed values:

- `rich_markdown` (default): agent output is Markdown or plain text. Balda sends it with Telegram rich messages.
- `rich_html`: agent output is rich-message HTML. Balda sends it with Telegram rich messages.
- `markdownv2`: legacy mode where agent output is normal Markdown or plain text. Balda converts it to Telegram MarkdownV2 and sends with `parse_mode=MarkdownV2`.
- `html`: legacy mode where agent output is Telegram HTML. Balda escapes unsafe raw text while preserving supported Telegram HTML tags and sends with `parse_mode=HTML`.
- `none`: legacy mode where Balda sends raw text without `parse_mode`.

Balda follows Telegram Bot API formatting rules:
<https://core.telegram.org/bots/api#formatting-options>

## Rich Markdown Mode

Use `rich_markdown` when agents should write natural Markdown. This is the default and recommended mode.

Supported input:

- plain text and paragraphs
- headings
- bold, italic, strike, and inline code
- fenced code blocks
- links
- blockquotes
- unordered, nested, and ordered lists

Balda behavior:

- Sends Markdown/plain text with Telegram rich messages.
- Preserves fenced code block content.
- Preserves standalone `---` separator lines for Telegram rich-message handling.
- Retries as plain text with no `parse_mode` if rich-message delivery fails.

Not supported or not recommended:

- Do not ask agents to write raw Telegram MarkdownV2 syntax.
- Do not pre-escape Telegram MarkdownV2 reserved characters in agent instructions.
- Do not rely on exact rendered bullet glyphs; Balda may normalize list markers for Telegram.
- Do not rely on raw Telegram entity syntax in Markdown mode.

Example model output:

~~~markdown
**Build:** success

- Run `balda start`
- Check logs

```bash
go test ./...
```
~~~

Separator example:

~~~markdown
First section.

---

Second section.
~~~

## Rich HTML Mode

Use `rich_html` when agents should write rich-message HTML directly. Balda sanitizes supported Telegram HTML before sending it as a Telegram rich message.

Supported tags and attributes:

- `<b>`, `<strong>`
- `<i>`, `<em>`
- `<u>`, `<ins>`
- `<s>`, `<strike>`, `<del>`
- `<tg-spoiler>`
- `<span class="tg-spoiler">`
- `<a href="...">`
- `<code>`
- `<pre>`
- `<pre><code class="language-...">...</code></pre>`
- `<blockquote>` and `<blockquote expandable>`
- `<tg-emoji emoji-id="...">`
- `<tg-time unix="..." format="...">`; `format` is optional

Balda behavior:

- Preserves supported Telegram HTML tags.
- Preserves only supported attributes for supported tags.
- Drops unsupported attributes on supported tags.
- Escapes unsupported tags as visible text.
- Escapes raw `<`, `>`, and `&` in text.
- Preserves Telegram-supported entities: `&lt;`, `&gt;`, `&amp;`, `&quot;`, decimal numeric entities, and hex numeric entities.
- Retries with legacy HTML delivery if rich-message delivery fails.

Not supported:

- Arbitrary HTML tags such as `<div>`, `<script>`, tables, images, and styles.
- Event handlers, CSS classes other than supported Telegram classes, inline styles, and custom attributes.
- Standalone `<code class="language-...">`; language classes are preserved only inside `<pre>`.
- `<tg-time datetime="...">`; use `unix` and optional `format`.
- Custom named HTML entities such as `&copy;`; use numeric entities when needed.

Example model output:

```html
<b>Build:</b> success.
Run <code>balda start</code>.

<pre><code class="language-bash">go test ./...</code></pre>
```

## Legacy MarkdownV2 Mode

Use `markdownv2` only when you need the older Telegram `parse_mode=MarkdownV2` path.
Balda converts Markdown/plain text to MarkdownV2, escapes reserved characters, normalizes list indentation, and trims converter-added leading/trailing newlines.
Standalone `---` separator lines outside fenced code blocks split final agent replies into multiple Telegram messages in this legacy mode.

## Legacy HTML Mode

Use `html` only when you need the older Telegram `parse_mode=HTML` path. Balda applies the same HTML sanitizer as rich HTML mode before sending.

## None Mode

Use `none` when the response must be delivered exactly as raw text.

Balda behavior:

- Omits Telegram `parse_mode`.
- Does not escape Markdown or HTML.
- Does not preserve formatting semantics.

This mode is useful for debugging malformed payloads or sending literal markup.
