package messenger

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/normahq/balda/internal/apps/balda/telegramfmt"
	"github.com/rs/zerolog"
	"github.com/tgbotkit/client"
)

const (
	testParseModeHTML       = "HTML"
	testParseModeMarkdownV2 = "MarkdownV2"
	testSecondRichChunk     = "second"
)

type fakeChatActionClient struct {
	client.ClientWithResponsesInterface
	chatActions          []client.SendChatActionJSONRequestBody
	chatActionResults    []sendChatActionResult
	chatActionContexts   []context.Context
	messages             []client.SendMessageJSONRequestBody
	drafts               []client.SendMessageDraftJSONRequestBody
	richMessages         []client.SendRichMessageJSONRequestBody
	richDrafts           []client.SendRichMessageDraftJSONRequestBody
	sendMessageResults   []sendMessageResult
	sendRichResults      []sendRichMessageResult
	sendRichDraftResults []sendRichDraftResult
	messageContexts      []context.Context
}

type sendChatActionResult struct {
	resp *client.SendChatActionResponse
	err  error
}

type sendMessageResult struct {
	resp *client.SendMessageResponse
	err  error
}

type sendRichMessageResult struct {
	resp *client.SendRichMessageResponse
	err  error
}

type sendRichDraftResult struct {
	resp *client.SendRichMessageDraftResponse
	err  error
}

func successfulSendMessageResponse(messageID int) *client.SendMessageResponse {
	return &client.SendMessageResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
		JSON200: &struct {
			Ok     client.SendMessage200Ok `json:"ok"`
			Result client.Message          `json:"result"`
		}{
			Ok:     true,
			Result: client.Message{MessageId: messageID},
		},
	}
}

func successfulSendRichMessageResponse(messageID int) *client.SendRichMessageResponse {
	return &client.SendRichMessageResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
		JSON200: &struct {
			Ok     client.SendRichMessage200Ok `json:"ok"`
			Result client.Message              `json:"result"`
		}{
			Ok:     true,
			Result: client.Message{MessageId: messageID},
		},
	}
}

func (f *fakeChatActionClient) SendChatActionWithResponse(
	ctx context.Context,
	body client.SendChatActionJSONRequestBody,
	_ ...client.RequestEditorFn,
) (*client.SendChatActionResponse, error) {
	f.chatActionContexts = append(f.chatActionContexts, ctx)
	f.chatActions = append(f.chatActions, body)
	if len(f.chatActionResults) > 0 {
		result := f.chatActionResults[0]
		f.chatActionResults = f.chatActionResults[1:]
		return result.resp, result.err
	}
	return &client.SendChatActionResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
		JSON200: &struct {
			Ok     client.SendChatAction200Ok `json:"ok"`
			Result bool                       `json:"result"`
		}{
			Ok:     true,
			Result: true,
		},
	}, nil
}

func (f *fakeChatActionClient) SendMessageWithResponse(
	ctx context.Context,
	body client.SendMessageJSONRequestBody,
	_ ...client.RequestEditorFn,
) (*client.SendMessageResponse, error) {
	f.messageContexts = append(f.messageContexts, ctx)
	f.messages = append(f.messages, body)
	if len(f.sendMessageResults) > 0 {
		result := f.sendMessageResults[0]
		f.sendMessageResults = f.sendMessageResults[1:]
		return result.resp, result.err
	}
	return successfulSendMessageResponse(len(f.messages)), nil
}

func (f *fakeChatActionClient) SendRichMessageWithResponse(
	ctx context.Context,
	body client.SendRichMessageJSONRequestBody,
	_ ...client.RequestEditorFn,
) (*client.SendRichMessageResponse, error) {
	f.messageContexts = append(f.messageContexts, ctx)
	f.richMessages = append(f.richMessages, body)
	if len(f.sendRichResults) > 0 {
		result := f.sendRichResults[0]
		f.sendRichResults = f.sendRichResults[1:]
		return result.resp, result.err
	}
	return successfulSendRichMessageResponse(len(f.richMessages)), nil
}

func (f *fakeChatActionClient) SendMessageDraftWithResponse(
	ctx context.Context,
	body client.SendMessageDraftJSONRequestBody,
	_ ...client.RequestEditorFn,
) (*client.SendMessageDraftResponse, error) {
	f.messageContexts = append(f.messageContexts, ctx)
	f.drafts = append(f.drafts, body)
	return &client.SendMessageDraftResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
		JSON200: &struct {
			Ok     client.SendMessageDraft200Ok `json:"ok"`
			Result bool                         `json:"result"`
		}{
			Ok:     true,
			Result: true,
		},
	}, nil
}

func (f *fakeChatActionClient) SendRichMessageDraftWithResponse(
	ctx context.Context,
	body client.SendRichMessageDraftJSONRequestBody,
	_ ...client.RequestEditorFn,
) (*client.SendRichMessageDraftResponse, error) {
	f.messageContexts = append(f.messageContexts, ctx)
	f.richDrafts = append(f.richDrafts, body)
	if len(f.sendRichDraftResults) > 0 {
		result := f.sendRichDraftResults[0]
		f.sendRichDraftResults = f.sendRichDraftResults[1:]
		return result.resp, result.err
	}
	return &client.SendRichMessageDraftResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
		JSON200: &struct {
			Ok     client.SendRichMessageDraft200Ok `json:"ok"`
			Result bool                             `json:"result"`
		}{
			Ok:     true,
			Result: true,
		},
	}, nil
}

func TestMessengerDebugLogsDoNotIncludeMessageContent(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	tgClient := &fakeChatActionClient{}
	logger := zerolog.New(&logs).Level(zerolog.DebugLevel)
	m := NewMessenger(tgClient, logger)

	if err := m.SendDraftPlain(context.Background(), 9001, 1, "secret draft text", 0); err != nil {
		t.Fatalf("SendDraftPlain() error = %v", err)
	}
	m.SetAgentReplyFormattingMode(telegramfmt.ModeHTML)
	if err := m.SendAgentReply(context.Background(), 9001, "secret reply text", 0); err != nil {
		t.Fatalf("SendAgentReply() error = %v", err)
	}

	got := logs.String()
	for _, forbidden := range []string{"secret draft text", "secret reply text"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("debug logs contain message content %q: %s", forbidden, got)
		}
	}
	for _, want := range []string{"rich_payload_bytes", "telegram_payload_bytes"} {
		if !strings.Contains(got, want) {
			t.Fatalf("debug logs missing safe metadata %q: %s", want, got)
		}
	}
}

func TestSendPlainUsesBoundedSendContext(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{}
	m := NewMessenger(tgClient, zerolog.Nop())

	if err := m.SendPlain(context.Background(), 9001, "hello", 0); err != nil {
		t.Fatalf("SendPlain() error = %v", err)
	}

	if len(tgClient.messageContexts) != 1 {
		t.Fatalf("message contexts = %d, want 1", len(tgClient.messageContexts))
	}
	assertContextDeadlineWithin(t, tgClient.messageContexts[0], telegramSendTimeout)
}

func TestSendPlain_IncludesMessageThreadIDWhenTopicProvided(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{}
	m := NewMessenger(tgClient, zerolog.Nop())

	if err := m.SendPlain(context.Background(), 9001, "hello", 77); err != nil {
		t.Fatalf("SendPlain() error = %v", err)
	}

	if len(tgClient.richMessages) != 1 {
		t.Fatalf("rich message calls = %d, want 1", len(tgClient.richMessages))
	}
	got := tgClient.richMessages[0]
	if got.ChatId != 9001 {
		t.Fatalf("chat_id = %d, want 9001", got.ChatId)
	}
	if got.RichMessage.Html == nil || *got.RichMessage.Html != "hello" {
		t.Fatalf("rich html = %v, want hello", got.RichMessage.Html)
	}
	if got.MessageThreadId == nil || *got.MessageThreadId != 77 {
		t.Fatalf("message_thread_id = %v, want 77", got.MessageThreadId)
	}
}

func TestSendPlain_ReturnsResponderError(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{
		sendMessageResults: []sendMessageResult{
			{
				resp: &client.SendMessageResponse{
					HTTPResponse: &http.Response{StatusCode: http.StatusBadRequest, Status: "400 Bad Request"},
					JSON400:      &client.ErrorResponse{Description: "Bad Request: chat not found"},
				},
			},
		},
	}
	m := NewMessenger(tgClient, zerolog.Nop())
	m.SetTelegramFormattingMode(telegramfmt.ModeNone)

	err := m.SendPlain(context.Background(), 9001, "hello", 0)
	if err == nil {
		t.Fatal("SendPlain() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "send message") {
		t.Fatalf("SendPlain() error = %v, want responder send error", err)
	}
}

func TestSendAgentReplyUsesBoundedSendContextForRetry(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{
		sendMessageResults: []sendMessageResult{
			{err: errors.New("network timeout")},
		},
	}
	m := NewMessenger(tgClient, zerolog.Nop())
	m.SetAgentReplyFormattingMode(telegramfmt.ModeHTML)

	if err := m.SendAgentReply(context.Background(), 9001, "hello", 77); err != nil {
		t.Fatalf("SendAgentReply() error = %v", err)
	}

	if len(tgClient.messageContexts) != 2 {
		t.Fatalf("message contexts = %d, want initial send and retry", len(tgClient.messageContexts))
	}
	for _, ctx := range tgClient.messageContexts {
		assertContextDeadlineWithin(t, ctx, telegramSendTimeout)
	}
	if tgClient.messageContexts[0] != tgClient.messageContexts[1] {
		t.Fatal("retry should reuse the same bounded context as the initial send")
	}
}

func TestSendChatActionPreservesParentCancellation(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{}
	m := NewMessenger(tgClient, zerolog.Nop())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := m.SendChatAction(ctx, 9001, 0, "typing"); err != nil {
		t.Fatalf("SendChatAction() error = %v", err)
	}

	if len(tgClient.chatActionContexts) != 1 {
		t.Fatalf("chat action contexts = %d, want 1", len(tgClient.chatActionContexts))
	}
	if got := tgClient.chatActionContexts[0].Err(); !errors.Is(got, context.Canceled) {
		t.Fatalf("chat action context err = %v, want context.Canceled", got)
	}
}

func assertContextDeadlineWithin(t *testing.T, ctx context.Context, maxDuration time.Duration) {
	t.Helper()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("context has no deadline")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		t.Fatalf("context deadline already expired: remaining=%s", remaining)
	}
	if remaining > maxDuration {
		t.Fatalf("context deadline remaining = %s, want <= %s", remaining, maxDuration)
	}
}

func TestSendChatAction_IncludesMessageThreadIDWhenTopicProvided(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{}
	m := NewMessenger(tgClient, zerolog.Nop())

	if err := m.SendChatAction(context.Background(), 9001, 77, "typing"); err != nil {
		t.Fatalf("SendChatAction() error = %v", err)
	}

	if len(tgClient.chatActions) != 1 {
		t.Fatalf("chatActions calls = %d, want 1", len(tgClient.chatActions))
	}
	got := tgClient.chatActions[0]
	if got.ChatId != 9001 {
		t.Fatalf("chat_id = %d, want 9001", got.ChatId)
	}
	if got.Action != "typing" {
		t.Fatalf("action = %q, want typing", got.Action)
	}
	if got.MessageThreadId == nil || *got.MessageThreadId != 77 {
		t.Fatalf("message_thread_id = %v, want 77", got.MessageThreadId)
	}
}

func TestSendChatAction_OmitsMessageThreadIDForRootChat(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{}
	m := NewMessenger(tgClient, zerolog.Nop())

	if err := m.SendChatAction(context.Background(), 9001, 0, "typing"); err != nil {
		t.Fatalf("SendChatAction() error = %v", err)
	}

	if len(tgClient.chatActions) != 1 {
		t.Fatalf("chatActions calls = %d, want 1", len(tgClient.chatActions))
	}
	if tgClient.chatActions[0].MessageThreadId != nil {
		t.Fatalf("message_thread_id = %v, want nil", tgClient.chatActions[0].MessageThreadId)
	}
}

func TestSendChatAction_AllowsEmptySuccessResponseBody(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{
		chatActionResults: []sendChatActionResult{
			{
				resp: &client.SendChatActionResponse{
					HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
				},
			},
		},
	}
	m := NewMessenger(tgClient, zerolog.Nop())

	if err := m.SendChatAction(context.Background(), -5173524191, 0, "typing"); err != nil {
		t.Fatalf("SendChatAction() error = %v, want nil", err)
	}
}

func TestSendChatAction_ReturnsTelegramErrorResponse(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{
		chatActionResults: []sendChatActionResult{
			{
				resp: &client.SendChatActionResponse{
					HTTPResponse: &http.Response{StatusCode: http.StatusBadRequest, Status: "400 Bad Request"},
					JSON400:      &client.ErrorResponse{Description: "Bad Request: chat not found"},
				},
			},
		},
	}
	m := NewMessenger(tgClient, zerolog.Nop())

	err := m.SendChatAction(context.Background(), 9001, 0, "typing")
	if err == nil {
		t.Fatal("SendChatAction() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "chat not found") {
		t.Fatalf("SendChatAction() error = %v, want chat not found", err)
	}
}

func TestSendMarkdown_DoesNotSplitStandaloneSeparator(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{}
	m := NewMessenger(tgClient, zerolog.Nop())

	if err := m.SendMarkdown(context.Background(), 9001, "first\n---\nsecond", 77); err != nil {
		t.Fatalf("SendMarkdown() error = %v", err)
	}

	if len(tgClient.richMessages) != 1 {
		t.Fatalf("rich messages calls = %d, want 1", len(tgClient.richMessages))
	}
	if tgClient.richMessages[0].RichMessage.Markdown == nil {
		t.Fatal("rich markdown = nil, want markdown payload")
	}
	got := *tgClient.richMessages[0].RichMessage.Markdown
	if !strings.Contains(got, "first") || !strings.Contains(got, "second") {
		t.Fatalf("rich markdown = %q, want both sections in one message", got)
	}
}

func TestSendAgentReply_RichMarkdownPreservesStandaloneSeparator(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{}
	m := NewMessenger(tgClient, zerolog.Nop())

	const input = "**first**\n\n---\n\nsecond"
	result, err := m.SendAgentReplyWithResult(context.Background(), 9001, input, 77)
	if err != nil {
		t.Fatalf("SendAgentReplyWithResult() error = %v", err)
	}

	if len(tgClient.richMessages) != 1 {
		t.Fatalf("rich message calls = %d, want 1", len(tgClient.richMessages))
	}
	if result.FirstMessageID != 1 || result.LastMessageID != 1 || result.MessageCount != 1 {
		t.Fatalf("result = %+v, want first=1 last=1 count=1", result)
	}
	got := tgClient.richMessages[0]
	if got.MessageThreadId == nil || *got.MessageThreadId != 77 {
		t.Fatalf("rich message message_thread_id = %v, want 77", got.MessageThreadId)
	}
	if got.RichMessage.Markdown == nil {
		t.Fatal("rich markdown = nil, want payload")
	}
	if payload := *got.RichMessage.Markdown; payload != input {
		t.Fatalf("rich markdown = %q, want original input", payload)
	}
}

func TestSendAgentReply_RichMarkdownFallsBackToPlainText(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{
		sendRichResults: []sendRichMessageResult{
			{
				resp: &client.SendRichMessageResponse{
					HTTPResponse: &http.Response{StatusCode: http.StatusBadRequest, Status: "400 Bad Request"},
					JSON400:      &client.ErrorResponse{Description: "Bad Request: can't parse entities"},
				},
			},
		},
	}
	m := NewMessenger(tgClient, zerolog.Nop())

	const input = "**final**\n\n---\n\n![bad](https://example.invalid/missing.png)"
	result, err := m.SendAgentReplyWithResult(context.Background(), 9001, input, 77)
	if err != nil {
		t.Fatalf("SendAgentReplyWithResult() error = %v", err)
	}

	if len(tgClient.richMessages) != 1 {
		t.Fatalf("rich message calls = %d, want 1 failed attempt", len(tgClient.richMessages))
	}
	if len(tgClient.messages) != 1 {
		t.Fatalf("legacy message calls = %d, want 1 fallback", len(tgClient.messages))
	}
	if tgClient.messages[0].ParseMode != nil {
		t.Fatalf("fallback parse_mode = %v, want nil", *tgClient.messages[0].ParseMode)
	}
	if tgClient.messages[0].Text != input {
		t.Fatalf("fallback text = %q, want original input", tgClient.messages[0].Text)
	}
	if result.FirstMessageID != 1 || result.LastMessageID != 1 || result.MessageCount != 1 {
		t.Fatalf("result = %+v, want fallback message metadata", result)
	}
}

func TestSendAgentReply_RichMarkdownTransportErrorDoesNotFallbackToPlainText(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{
		sendRichResults: []sendRichMessageResult{
			{err: context.DeadlineExceeded},
		},
	}
	m := NewMessenger(tgClient, zerolog.Nop())

	_, err := m.SendAgentReplyWithResult(context.Background(), 9001, "**final**", 77)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SendAgentReplyWithResult() error = %v, want deadline exceeded", err)
	}

	if len(tgClient.richMessages) != 1 {
		t.Fatalf("rich message calls = %d, want 1 failed attempt", len(tgClient.richMessages))
	}
	if len(tgClient.messages) != 0 {
		t.Fatalf("legacy message calls = %d, want 0 on transport error", len(tgClient.messages))
	}
}

func TestSendAgentReply_UsesConfiguredFormattingMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mode     string
		input    string
		wantText string
		wantMode *string
	}{
		{
			name:     "markdownv2 converts normal markdown",
			mode:     telegramfmt.ModeMarkdownV2,
			input:    "**final answer** with `code`",
			wantText: "***final answer*** with `code`",
			wantMode: func() *string {
				v := testParseModeMarkdownV2
				return &v
			}(),
		},
		{
			name:     "html preserves supported tags and escapes raw text",
			mode:     telegramfmt.ModeHTML,
			input:    "<b>final answer</b> with <code>x < y</code> & <div>raw</div>",
			wantText: "<b>final answer</b> with <code>x &lt; y</code> &amp; &lt;div&gt;raw&lt;/div&gt;",
			wantMode: func() *string {
				v := testParseModeHTML
				return &v
			}(),
		},
		{
			name:     "none sends raw text without parse mode",
			mode:     telegramfmt.ModeNone,
			input:    "**final answer** with <code>code</code>",
			wantText: "**final answer** with <code>code</code>",
			wantMode: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tgClient := &fakeChatActionClient{}
			m := NewMessenger(tgClient, zerolog.Nop())
			m.SetAgentReplyFormattingMode(tt.mode)

			if err := m.SendAgentReply(context.Background(), 9001, tt.input, 77); err != nil {
				t.Fatalf("SendAgentReply() error = %v", err)
			}

			if len(tgClient.messages) != 1 {
				t.Fatalf("messages calls = %d, want 1", len(tgClient.messages))
			}
			got := tgClient.messages[0].ParseMode
			switch {
			case tt.wantMode == nil && got != nil:
				t.Fatalf("parse_mode = %v, want nil", *got)
			case tt.wantMode != nil && (got == nil || *got != *tt.wantMode):
				if got == nil {
					t.Fatalf("parse_mode = nil, want %q", *tt.wantMode)
				}
				t.Fatalf("parse_mode = %q, want %q", *got, *tt.wantMode)
			}
			if tgClient.messages[0].Text != tt.wantText {
				t.Fatalf("message text = %q, want %q", tgClient.messages[0].Text, tt.wantText)
			}
		})
	}
}

func TestSendAgentReplyWithResultAndMode_DoesNotMutateConfiguredMode(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{}
	m := NewMessenger(tgClient, zerolog.Nop())
	m.SetAgentReplyFormattingMode(telegramfmt.ModeNone)

	result, err := m.SendAgentReplyWithResultAndMode(context.Background(), 9001, "**final**", 77, telegramfmt.ModeMarkdownV2)
	if err != nil {
		t.Fatalf("SendAgentReplyWithResultAndMode() error = %v", err)
	}
	if result.MessageCount != 1 {
		t.Fatalf("MessageCount = %d, want 1", result.MessageCount)
	}
	if got := m.TelegramFormattingMode(); got != telegramfmt.ModeNone {
		t.Fatalf("TelegramFormattingMode() = %q, want %q", got, telegramfmt.ModeNone)
	}
	if len(tgClient.messages) != 1 {
		t.Fatalf("messages calls = %d, want 1", len(tgClient.messages))
	}
	got := tgClient.messages[0].ParseMode
	if got == nil || *got != testParseModeMarkdownV2 {
		if got == nil {
			t.Fatal("parse_mode = nil, want MarkdownV2")
		}
		t.Fatalf("parse_mode = %q, want MarkdownV2", *got)
	}
}

func TestSendAgentReply_MarkdownV2SplitsOnStandaloneSeparator(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{}
	m := NewMessenger(tgClient, zerolog.Nop())
	m.SetAgentReplyFormattingMode(telegramfmt.ModeMarkdownV2)

	input := "**first**\n\n---\n\nsecond with `code`"
	if err := m.SendAgentReply(context.Background(), 9001, input, 77); err != nil {
		t.Fatalf("SendAgentReply() error = %v", err)
	}

	if len(tgClient.messages) != 2 {
		t.Fatalf("messages calls = %d, want 2", len(tgClient.messages))
	}
	for i, msg := range tgClient.messages {
		if msg.ChatId != 9001 {
			t.Fatalf("message[%d].chat_id = %d, want 9001", i, msg.ChatId)
		}
		if msg.MessageThreadId == nil || *msg.MessageThreadId != 77 {
			t.Fatalf("message[%d].message_thread_id = %v, want 77", i, msg.MessageThreadId)
		}
		if msg.ParseMode == nil || *msg.ParseMode != testParseModeMarkdownV2 {
			t.Fatalf("message[%d].parse_mode = %v, want MarkdownV2", i, msg.ParseMode)
		}
	}
	if tgClient.messages[0].Text != "***first***" {
		t.Fatalf("first message text = %q, want converted first chunk", tgClient.messages[0].Text)
	}
	if tgClient.messages[1].Text != "second with `code`" {
		t.Fatalf("second message text = %q, want converted second chunk", tgClient.messages[1].Text)
	}
}

func TestSendAgentReply_MarkdownV2DoesNotSplitInsideFencedCode(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{}
	m := NewMessenger(tgClient, zerolog.Nop())
	m.SetAgentReplyFormattingMode(telegramfmt.ModeMarkdownV2)

	input := "before\n\n```txt\n---\n```\n\nafter"
	if err := m.SendAgentReply(context.Background(), 9001, input, 77); err != nil {
		t.Fatalf("SendAgentReply() error = %v", err)
	}

	if len(tgClient.messages) != 1 {
		t.Fatalf("messages calls = %d, want 1", len(tgClient.messages))
	}
	if !strings.Contains(tgClient.messages[0].Text, "```txt\n---\n```") {
		t.Fatalf("message text = %q, want fenced separator preserved", tgClient.messages[0].Text)
	}
}

func TestSendAgentReply_DoesNotSplitHTMLOrNoneModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mode string
	}{
		{name: "html", mode: telegramfmt.ModeHTML},
		{name: "none", mode: telegramfmt.ModeNone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tgClient := &fakeChatActionClient{}
			m := NewMessenger(tgClient, zerolog.Nop())
			m.SetAgentReplyFormattingMode(tt.mode)

			input := "first\n---\nsecond"
			if err := m.SendAgentReply(context.Background(), 9001, input, 77); err != nil {
				t.Fatalf("SendAgentReply() error = %v", err)
			}
			if len(tgClient.messages) != 1 {
				t.Fatalf("messages calls = %d, want 1", len(tgClient.messages))
			}
			if tgClient.messages[0].Text != input {
				t.Fatalf("message text = %q, want unsplit input", tgClient.messages[0].Text)
			}
		})
	}
}

func TestSendAgentReply_MarkdownV2ReturnsErrorFromLaterSplitChunk(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{
		sendMessageResults: []sendMessageResult{
			{resp: successfulSendMessageResponse(1)},
			{err: errors.New("network timeout")},
			{err: errors.New("network timeout")},
		},
	}
	m := NewMessenger(tgClient, zerolog.Nop())
	m.SetAgentReplyFormattingMode(telegramfmt.ModeMarkdownV2)

	err := m.SendAgentReply(context.Background(), 9001, "first\n---\nsecond", 77)
	if err == nil {
		t.Fatal("SendAgentReply() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "network timeout") {
		t.Fatalf("SendAgentReply() error = %v, want network timeout", err)
	}
	if len(tgClient.messages) != 3 {
		t.Fatalf("messages calls = %d, want second chunk retry after transport error", len(tgClient.messages))
	}
	if tgClient.messages[0].Text != "first" || tgClient.messages[1].Text != "second" || tgClient.messages[2].Text != "second" {
		t.Fatalf("message texts = %#v, want first then second retry", []string{
			tgClient.messages[0].Text,
			tgClient.messages[1].Text,
			tgClient.messages[2].Text,
		})
	}
	if tgClient.messages[2].ParseMode != nil {
		t.Fatalf("retry parse_mode = %v, want nil", *tgClient.messages[2].ParseMode)
	}
}

func TestSendAgentReply_RetriesWithoutParseModeOnTelegramParseError(t *testing.T) {
	t.Parallel()

	parseErrorResp := &client.SendMessageResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusBadRequest, Status: "400 Bad Request"},
		JSON400: &client.ErrorResponse{
			Description: "Bad Request: can't parse entities: Character '_' is reserved and must be escaped",
		},
	}
	tgClient := &fakeChatActionClient{
		sendMessageResults: []sendMessageResult{
			{resp: parseErrorResp},
		},
	}
	m := NewMessenger(tgClient, zerolog.Nop())
	m.SetAgentReplyFormattingMode(telegramfmt.ModeHTML)

	if err := m.SendAgentReply(context.Background(), 9001, "Hello _world_", 77); err != nil {
		t.Fatalf("SendAgentReply() error = %v", err)
	}
	if len(tgClient.messages) != 2 {
		t.Fatalf("messages calls = %d, want 2", len(tgClient.messages))
	}
	if tgClient.messages[0].ParseMode == nil || *tgClient.messages[0].ParseMode != testParseModeHTML {
		t.Fatalf("first parse_mode = %v, want %s", tgClient.messages[0].ParseMode, testParseModeHTML)
	}
	if tgClient.messages[1].ParseMode != nil {
		t.Fatalf("second parse_mode = %v, want nil", *tgClient.messages[1].ParseMode)
	}
}

func TestSendAgentReply_DoesNotRetryWithoutParseModeOnNonParseBadRequest(t *testing.T) {
	t.Parallel()

	badReqResp := &client.SendMessageResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusBadRequest, Status: "400 Bad Request"},
		JSON400: &client.ErrorResponse{
			Description: "Bad Request: chat not found",
		},
	}
	tgClient := &fakeChatActionClient{
		sendMessageResults: []sendMessageResult{
			{resp: badReqResp},
		},
	}
	m := NewMessenger(tgClient, zerolog.Nop())
	m.SetAgentReplyFormattingMode(telegramfmt.ModeHTML)

	err := m.SendAgentReply(context.Background(), 9001, "hello", 77)
	if err == nil {
		t.Fatal("SendAgentReply() error = nil, want non-nil")
	}
	if len(tgClient.messages) != 1 {
		t.Fatalf("messages calls = %d, want 1", len(tgClient.messages))
	}
}

func TestSendAgentReply_RetriesWithoutParseModeOnTransportError(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{
		sendMessageResults: []sendMessageResult{
			{err: errors.New("network timeout")},
		},
	}
	m := NewMessenger(tgClient, zerolog.Nop())
	m.SetAgentReplyFormattingMode(telegramfmt.ModeHTML)

	if err := m.SendAgentReply(context.Background(), 9001, "hello", 77); err != nil {
		t.Fatalf("SendAgentReply() error = %v", err)
	}
	if len(tgClient.messages) != 2 {
		t.Fatalf("messages calls = %d, want 2", len(tgClient.messages))
	}
	if tgClient.messages[0].ParseMode == nil || *tgClient.messages[0].ParseMode != testParseModeHTML {
		t.Fatalf("first parse_mode = %v, want %s", tgClient.messages[0].ParseMode, testParseModeHTML)
	}
	if tgClient.messages[1].ParseMode != nil {
		t.Fatalf("second parse_mode = %v, want nil", *tgClient.messages[1].ParseMode)
	}
}

func TestSendAgentReply_MarkdownV2PreservesMarkdownStructure(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{}
	m := NewMessenger(tgClient, zerolog.Nop())
	m.SetAgentReplyFormattingMode(telegramfmt.ModeMarkdownV2)

	input := "Конечно. Вот быстрые практичные примеры:\n\n" +
		"**Статус задачи**\n" +
		"- **Task:** `balda-runtime`\n" +
		"- **Status:** in progress\n" +
		"- **Next:** run tests\n\n" +
		"**Результат команды**\n" +
		"- **Build:** success\n" +
		"- **Lint:** failed on `internal/balda/session.go:42`\n" +
		"- **Action:** fix lint and rerun\n\n" +
		"```bash\n" +
		"go test -race ./...\n" +
		"go tool golangci-lint run\n" +
		"```"
	if err := m.SendAgentReply(context.Background(), 9001, input, 77); err != nil {
		t.Fatalf("SendAgentReply() error = %v", err)
	}

	if len(tgClient.messages) != 1 {
		t.Fatalf("messages calls = %d, want 1", len(tgClient.messages))
	}
	got := tgClient.messages[0].Text
	for _, unwanted := range []string{
		"in progress\\- ***Next",
		"run tests***Результат",
		"success\\- ***Lint",
		"rerun```bash",
	} {
		if strings.Contains(got, unwanted) {
			t.Fatalf("message text = %q, contains glued markdown fragment %q", got, unwanted)
		}
	}
	for _, want := range []string{
		"***Статус задачи***",
		"***Результат команды***",
		"`balda\\-runtime`",
		"```bash\ngo test -race ./...\ngo tool golangci-lint run\n```",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("message text = %q, want to contain %q", got, want)
		}
	}
}

func TestSendAgentReply_MarkdownV2CleansConverterArtifacts(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{}
	m := NewMessenger(tgClient, zerolog.Nop())
	m.SetAgentReplyFormattingMode(telegramfmt.ModeMarkdownV2)

	input := "I can help you run the full Balda workflow in this session:\n\n" +
		"- Explain/debug Balda behavior, config, commands, and session issues\n" +
		"- Make code changes in `/home/bigboss/Projects/balda`\n" +
		"- Run checks and quality gates:\n" +
		"```bash\n" +
		"go test -race ./...\n" +
		"go tool golangci-lint run\n" +
		"```\n" +
		"- Prepare and land workspace changes using Balda flow:\n" +
		"  1. `balda.workspace.import`\n" +
		"  2. implement + verify\n" +
		"  3. `balda.workspace.export` with a Conventional Commit message\n" +
		"- Help with bot command contracts (`/start`, `/topic`, `/goal <objective>`, `/goal clear`, `/reset`, `/restart`, `/locator`, `/close`, `/cancel`, `/user ...`)\n" +
		"- Update docs when behavior changes (`README.md`, `docs/balda.md`)"
	if err := m.SendAgentReply(context.Background(), 9001, input, 77); err != nil {
		t.Fatalf("SendAgentReply() error = %v", err)
	}

	if len(tgClient.messages) != 1 {
		t.Fatalf("messages calls = %d, want 1", len(tgClient.messages))
	}
	got := tgClient.messages[0].Text
	for _, unwanted := range []string{
		"\n  • ",
		"\n    ‣ ",
		"\n      ◦ ",
	} {
		if strings.Contains(got, unwanted) {
			t.Fatalf("message text = %q, contains converter indentation artifact %q", got, unwanted)
		}
	}
	for _, want := range []string{
		"I can help you run the full Balda workflow in this session:",
		"\n• Explain/debug Balda behavior, config, commands, and session issues",
		"\n• Make code changes in `/home/bigboss/Projects/balda`",
		"```bash\ngo test -race ./...\ngo tool golangci-lint run\n```",
		"\n  ‣ `balda\\.workspace\\.import`",
		"\n• Help with bot command contracts",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("message text = %q, want to contain %q", got, want)
		}
	}
	if strings.HasPrefix(got, "\n") || strings.HasSuffix(got, "\n") {
		t.Fatalf("message text = %q, want no leading or trailing newline", got)
	}
}

func TestSendAgentReply_MarkdownV2PreservesExistingHardLineBreaks(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{}
	m := NewMessenger(tgClient, zerolog.Nop())
	m.SetAgentReplyFormattingMode(telegramfmt.ModeMarkdownV2)

	if err := m.SendAgentReply(context.Background(), 9001, "Hey there  \nWhat do you want to work on?", 77); err != nil {
		t.Fatalf("SendAgentReply() error = %v", err)
	}

	if len(tgClient.messages) != 1 {
		t.Fatalf("messages calls = %d, want 1", len(tgClient.messages))
	}
	if !strings.Contains(tgClient.messages[0].Text, "Hey there\nWhat do you want to work on?") {
		t.Fatalf("message text = %q, want preserved line break", tgClient.messages[0].Text)
	}
}

func TestSendAgentReply_MarkdownV2PreservesBlankLines(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{}
	m := NewMessenger(tgClient, zerolog.Nop())
	m.SetAgentReplyFormattingMode(telegramfmt.ModeMarkdownV2)

	if err := m.SendAgentReply(context.Background(), 9001, "First paragraph\n\nSecond paragraph", 77); err != nil {
		t.Fatalf("SendAgentReply() error = %v", err)
	}

	if len(tgClient.messages) != 1 {
		t.Fatalf("messages calls = %d, want 1", len(tgClient.messages))
	}
	if !strings.Contains(tgClient.messages[0].Text, "First paragraph\n\nSecond paragraph") {
		t.Fatalf("message text = %q, want preserved blank line", tgClient.messages[0].Text)
	}
}

func TestSendAgentReply_MarkdownV2DoesNotRewriteFencedCodeLineBreaks(t *testing.T) {
	t.Parallel()

	tgClient := &fakeChatActionClient{}
	m := NewMessenger(tgClient, zerolog.Nop())
	m.SetAgentReplyFormattingMode(telegramfmt.ModeMarkdownV2)

	input := "```txt\none\ntwo\n```"
	if err := m.SendAgentReply(context.Background(), 9001, input, 77); err != nil {
		t.Fatalf("SendAgentReply() error = %v", err)
	}

	if len(tgClient.messages) != 1 {
		t.Fatalf("messages calls = %d, want 1", len(tgClient.messages))
	}
	if strings.Contains(tgClient.messages[0].Text, "one  \n") || strings.Contains(tgClient.messages[0].Text, "two  \n") {
		t.Fatalf("message text = %q, code block line breaks were rewritten", tgClient.messages[0].Text)
	}
	if !strings.Contains(tgClient.messages[0].Text, "one\ntwo\n") {
		t.Fatalf("message text = %q, want original code block line breaks", tgClient.messages[0].Text)
	}
}
