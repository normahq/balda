package messenger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/normahq/balda/internal/apps/balda/telegramfmt"
	"github.com/rs/zerolog"
	"github.com/tgbotkit/client"
	"github.com/tgbotkit/runtime/respond"
)

const telegramSendTimeout = 30 * time.Second

// Messenger handles all Telegram message sending for Balda.
type Messenger struct {
	client                 client.ClientWithResponsesInterface
	responder              *respond.Responder
	logger                 zerolog.Logger
	telegramFormattingMode string
}

// AgentReplyResult carries provider delivery metadata for a final agent reply.
type AgentReplyResult struct {
	FirstMessageID int
	LastMessageID  int
	MessageCount   int
}

// NewMessenger creates a new Messenger.
func NewMessenger(client client.ClientWithResponsesInterface, logger zerolog.Logger) *Messenger {
	return &Messenger{
		client:                 client,
		responder:              respond.New(client),
		logger:                 logger.With().Str("component", "balda.messenger").Logger(),
		telegramFormattingMode: telegramfmt.ModeRichMarkdown,
	}
}

// SetAgentReplyFormattingMode sets balda.telegram.formatting_mode.
func (m *Messenger) SetAgentReplyFormattingMode(mode string) {
	m.SetTelegramFormattingMode(mode)
}

// SetTelegramFormattingMode sets balda.telegram.formatting_mode for outbound Telegram text.
func (m *Messenger) SetTelegramFormattingMode(mode string) {
	m.telegramFormattingMode = telegramfmt.NormalizeMode(mode)
}

// TelegramFormattingMode returns the normalized configured Telegram formatting mode.
func (m *Messenger) TelegramFormattingMode() string {
	if m == nil {
		return telegramfmt.ModeRichMarkdown
	}
	return telegramfmt.NormalizeMode(m.telegramFormattingMode)
}

// SendDraftPlain sends a plain-text draft (no parse_mode).
func (m *Messenger) SendDraftPlain(ctx context.Context, chatID int64, draftID int, text string, topicID int) error {
	if m.richMessagesEnabled() {
		return m.sendRichDraftWithFallback(ctx, chatID, draftID, richPlain(text), topicID, text)
	}
	return m.sendDraftPlainLegacy(ctx, chatID, draftID, text, topicID)
}

func (m *Messenger) sendDraftPlainLegacy(ctx context.Context, chatID int64, draftID int, text string, topicID int) error {
	m.logger.Debug().
		Int64("chat_id", chatID).
		Int("draft_id", draftID).
		Int("draft_text_bytes", len(text)).
		Msg("sending plain draft")
	req := client.SendMessageDraftJSONRequestBody{
		ChatId:  chatID,
		DraftId: draftID,
		Text:    &text,
	}
	if topicID != 0 {
		req.MessageThreadId = &topicID
	}

	sendCtx, cancel := telegramSendContext(ctx)
	defer cancel()

	resp, err := m.client.SendMessageDraftWithResponse(sendCtx, req)
	if err != nil {
		return fmt.Errorf("sending plain draft to chat %d: %w", chatID, err)
	}
	if resp.JSON400 != nil {
		return fmt.Errorf("sending plain draft to chat %d: %s", chatID, resp.JSON400.Description)
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("sending plain draft to chat %d: no response body", chatID)
	}
	return nil
}

// SendDraftMarkdown sends a Markdown draft using the supplied Telegram formatting mode.
func (m *Messenger) SendDraftMarkdown(ctx context.Context, chatID int64, draftID int, text string, topicID int) error {
	return m.SendDraftMarkdownWithMode(ctx, chatID, draftID, text, topicID, m.TelegramFormattingMode())
}

// SendDraftMarkdownWithMode sends a Markdown draft using the supplied formatting mode without mutating messenger state.
func (m *Messenger) SendDraftMarkdownWithMode(ctx context.Context, chatID int64, draftID int, text string, topicID int, mode string) error {
	switch telegramfmt.NormalizeMode(mode) {
	case telegramfmt.ModeRichMarkdown:
		return m.sendRichDraftWithFallback(ctx, chatID, draftID, richMarkdown(text), topicID, text)
	case telegramfmt.ModeRichHTML:
		return m.sendRichDraftWithFallback(ctx, chatID, draftID, richHTML(telegramfmt.HTML(text)), topicID, text)
	default:
		return m.sendDraftPlainLegacy(ctx, chatID, draftID, text, topicID)
	}
}

// SendPlain sends a plain-text message.
func (m *Messenger) SendPlain(ctx context.Context, chatID int64, text string, topicID int) error {
	if m.richMessagesEnabled() {
		_, err := m.sendRichMessageWithFallback(ctx, chatID, richPlain(text), topicID, func(ctx context.Context) (int, error) {
			return m.sendPlainLegacy(ctx, chatID, text, topicID)
		})
		return err
	}
	_, err := m.sendPlainLegacy(ctx, chatID, text, topicID)
	return err
}

func (m *Messenger) sendPlainLegacy(ctx context.Context, chatID int64, text string, topicID int) (int, error) {
	target := respond.ChatTarget{ChatID: chatID}
	if topicID != 0 {
		target.MessageThreadID = &topicID
	}
	sendCtx, cancel := telegramSendContext(ctx)
	defer cancel()

	msg, err := m.responder.SendText(sendCtx, target, text)
	if err != nil {
		return 0, fmt.Errorf("sending message to chat %d: %w", chatID, err)
	}
	if msg == nil {
		return 0, nil
	}
	return msg.MessageId, nil
}

// SendMarkdown converts standard Markdown to Telegram MarkdownV2 and sends.
func (m *Messenger) SendMarkdown(ctx context.Context, chatID int64, text string, topicID int) error {
	return m.SendMarkdownWithMode(ctx, chatID, text, topicID, m.TelegramFormattingMode())
}

// SendMarkdownWithMode sends Markdown using the supplied formatting mode without mutating messenger state.
func (m *Messenger) SendMarkdownWithMode(ctx context.Context, chatID int64, text string, topicID int, mode string) error {
	if telegramRichMessagesEnabled(mode) {
		_, err := m.sendRichMessageWithFallback(ctx, chatID, richMarkdown(text), topicID, func(ctx context.Context) (int, error) {
			return m.sendMarkdownLegacy(ctx, chatID, text, topicID)
		})
		return err
	}
	_, err := m.sendMarkdown(ctx, chatID, text, topicID)
	return err
}

func (m *Messenger) sendMarkdown(ctx context.Context, chatID int64, text string, topicID int) (int, error) {
	return m.sendMarkdownLegacy(ctx, chatID, text, topicID)
}

func (m *Messenger) sendMarkdownLegacy(ctx context.Context, chatID int64, text string, topicID int) (int, error) {
	payload, err := telegramfmt.MarkdownV2(text)
	if err != nil {
		m.logger.Warn().Err(err).Msg("failed to convert markdown to telegram format, falling back to escaped literal")
		payload = telegramfmt.EscapeMarkdownV2(text)
	}
	return m.sendMessageWithMode(ctx, chatID, payload, topicID, "MarkdownV2", "send message with MarkdownV2")
}

// SendAgentReply sends final model output with balda.telegram.formatting_mode.
func (m *Messenger) SendAgentReply(ctx context.Context, chatID int64, text string, topicID int) error {
	_, err := m.SendAgentReplyWithResult(ctx, chatID, text, topicID)
	return err
}

// SendAgentReplyWithResult sends final model output and returns provider message metadata.
func (m *Messenger) SendAgentReplyWithResult(ctx context.Context, chatID int64, text string, topicID int) (AgentReplyResult, error) {
	return m.SendAgentReplyWithResultAndMode(ctx, chatID, text, topicID, m.TelegramFormattingMode())
}

// SendAgentReplyWithResultAndMode sends final model output using the supplied formatting mode without mutating messenger state.
func (m *Messenger) SendAgentReplyWithResultAndMode(ctx context.Context, chatID int64, text string, topicID int, mode string) (AgentReplyResult, error) {
	var result AgentReplyResult
	switch telegramfmt.NormalizeMode(mode) {
	case telegramfmt.ModeRichHTML:
		messageID, err := m.sendRichMessageWithFallback(ctx, chatID, richHTML(telegramfmt.HTML(text)), topicID, func(ctx context.Context) (int, error) {
			return m.sendMessageWithMode(ctx, chatID, telegramfmt.HTML(text), topicID, telegramfmt.TelegramParseMode(telegramfmt.ModeHTML), "send message with HTML")
		})
		if err != nil {
			return AgentReplyResult{}, err
		}
		return AgentReplyResult{FirstMessageID: messageID, LastMessageID: messageID, MessageCount: 1}, nil
	case telegramfmt.ModeRichMarkdown:
		messageID, err := m.sendRichMessageWithFallback(ctx, chatID, richMarkdown(text), topicID, func(ctx context.Context) (int, error) {
			return m.sendPlainLegacy(ctx, chatID, text, topicID)
		})
		if err != nil {
			return AgentReplyResult{}, err
		}
		return AgentReplyResult{FirstMessageID: messageID, LastMessageID: messageID, MessageCount: 1}, nil
	case telegramfmt.ModeHTML:
		messageID, err := m.sendMessageWithMode(ctx, chatID, telegramfmt.HTML(text), topicID, telegramfmt.TelegramParseMode(telegramfmt.ModeHTML), "send message with HTML")
		if err != nil {
			return AgentReplyResult{}, err
		}
		return AgentReplyResult{FirstMessageID: messageID, LastMessageID: messageID, MessageCount: 1}, nil
	case telegramfmt.ModeNone:
		messageID, err := m.sendMessageWithMode(ctx, chatID, text, topicID, telegramfmt.TelegramParseMode(telegramfmt.ModeNone), "send message without parse_mode")
		if err != nil {
			return AgentReplyResult{}, err
		}
		return AgentReplyResult{FirstMessageID: messageID, LastMessageID: messageID, MessageCount: 1}, nil
	default:
		for _, chunk := range telegramfmt.SplitMarkdownMessageChunks(text) {
			messageID, err := m.sendMarkdown(ctx, chatID, chunk, topicID)
			if err != nil {
				return AgentReplyResult{}, err
			}
			if result.MessageCount == 0 {
				result.FirstMessageID = messageID
			}
			result.LastMessageID = messageID
			result.MessageCount++
		}
		return result, nil
	}
}

func (m *Messenger) SendAgentReplyLastMessageIDAndMode(ctx context.Context, chatID int64, text string, topicID int, mode string) (int, error) {
	result, err := m.SendAgentReplyWithResultAndMode(ctx, chatID, text, topicID, mode)
	if err != nil {
		return 0, err
	}
	return result.LastMessageID, nil
}

// SendAgentReplyWithInlineKeyboardLastMessageIDAndMode sends a single final
// reply with Telegram-native controls. If Telegram rejects the controlled
// message, it retries with a self-contained text fallback.
func (m *Messenger) SendAgentReplyWithInlineKeyboardLastMessageIDAndMode(
	ctx context.Context,
	chatID int64,
	text string,
	topicID int,
	mode string,
	keyboard client.InlineKeyboardMarkup,
	fallbackText string,
) (int, error) {
	messageID, err := m.sendAgentReplyWithInlineKeyboard(ctx, chatID, text, topicID, mode, keyboard)
	if err == nil {
		return messageID, nil
	}
	if !shouldFallbackRichSend(ctx, err) {
		return 0, err
	}
	m.logger.Warn().Err(err).Int64("chat_id", chatID).Msg("send telegram question controls failed, retrying with text choices")
	result, fallbackErr := m.SendAgentReplyWithResultAndMode(ctx, chatID, fallbackText, topicID, mode)
	if fallbackErr != nil {
		return 0, fmt.Errorf("send telegram question controls: %v; text fallback: %w", err, fallbackErr)
	}
	return result.LastMessageID, nil
}

// SendEphemeralAgentReplyWithInlineKeyboardLastMessageIDAndMode sends a
// group/supergroup question visible only to receiverUserID. It deliberately
// has no public fallback.
func (m *Messenger) SendEphemeralAgentReplyWithInlineKeyboardLastMessageIDAndMode(
	ctx context.Context,
	chatID, receiverUserID int64,
	text string,
	topicID int,
	mode string,
	keyboard client.InlineKeyboardMarkup,
) (int, error) {
	if receiverUserID <= 0 {
		return 0, fmt.Errorf("ephemeral telegram receiver is required")
	}
	switch telegramfmt.NormalizeMode(mode) {
	case telegramfmt.ModeRichHTML, telegramfmt.ModeHTML:
		return m.sendEphemeralMessageWithInlineKeyboard(ctx, chatID, receiverUserID, telegramfmt.HTML(text), topicID, telegramfmt.TelegramParseMode(telegramfmt.ModeHTML), keyboard)
	case telegramfmt.ModeNone:
		return m.sendEphemeralMessageWithInlineKeyboard(ctx, chatID, receiverUserID, text, topicID, "", keyboard)
	case telegramfmt.ModeRichMarkdown:
		fallthrough
	default:
		payload, err := telegramfmt.MarkdownV2(text)
		if err != nil {
			payload = telegramfmt.EscapeMarkdownV2(text)
		}
		return m.sendEphemeralMessageWithInlineKeyboard(ctx, chatID, receiverUserID, payload, topicID, "MarkdownV2", keyboard)
	}
}

func (m *Messenger) sendAgentReplyWithInlineKeyboard(ctx context.Context, chatID int64, text string, topicID int, mode string, keyboard client.InlineKeyboardMarkup) (int, error) {
	switch telegramfmt.NormalizeMode(mode) {
	case telegramfmt.ModeRichHTML:
		return m.sendRichMessageWithInlineKeyboard(ctx, chatID, richHTML(telegramfmt.HTML(text)), topicID, keyboard)
	case telegramfmt.ModeRichMarkdown:
		return m.sendRichMessageWithInlineKeyboard(ctx, chatID, richMarkdown(text), topicID, keyboard)
	case telegramfmt.ModeHTML:
		return m.sendMessageWithInlineKeyboard(ctx, chatID, telegramfmt.HTML(text), topicID, telegramfmt.TelegramParseMode(telegramfmt.ModeHTML), keyboard)
	case telegramfmt.ModeNone:
		return m.sendMessageWithInlineKeyboard(ctx, chatID, text, topicID, "", keyboard)
	default:
		payload, err := telegramfmt.MarkdownV2(text)
		if err != nil {
			payload = telegramfmt.EscapeMarkdownV2(text)
		}
		return m.sendMessageWithInlineKeyboard(ctx, chatID, payload, topicID, "MarkdownV2", keyboard)
	}
}

type sendRichMessageWithInlineKeyboardRequest struct {
	ChatID          int64                       `json:"chat_id"`
	RichMessage     client.InputRichMessage     `json:"rich_message"`
	MessageThreadID *int                        `json:"message_thread_id,omitempty"`
	ReplyMarkup     client.InlineKeyboardMarkup `json:"reply_markup"`
}

func (m *Messenger) sendRichMessageWithInlineKeyboard(ctx context.Context, chatID int64, rich client.InputRichMessage, topicID int, keyboard client.InlineKeyboardMarkup) (int, error) {
	request := sendRichMessageWithInlineKeyboardRequest{ChatID: chatID, RichMessage: rich, ReplyMarkup: keyboard}
	if topicID != 0 {
		request.MessageThreadID = &topicID
	}
	body, err := json.Marshal(request)
	if err != nil {
		return 0, fmt.Errorf("encode rich telegram question: %w", err)
	}
	sendCtx, cancel := telegramSendContext(ctx)
	defer cancel()
	resp, err := m.client.SendRichMessageWithBodyWithResponse(sendCtx, "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("send rich telegram question to chat %d: %w", chatID, err)
	}
	if resp == nil {
		return 0, fmt.Errorf("send rich telegram question to chat %d: no response body", chatID)
	}
	if resp.JSON400 != nil {
		return 0, fmt.Errorf("send rich telegram question to chat %d: %s", chatID, resp.JSON400.Description)
	}
	if resp.JSON200 == nil {
		return 0, fmt.Errorf("send rich telegram question to chat %d: no response body", chatID)
	}
	return resp.JSON200.Result.MessageId, nil
}

type sendMessageWithInlineKeyboardRequest struct {
	ChatID          int64                       `json:"chat_id"`
	Text            string                      `json:"text"`
	ParseMode       *string                     `json:"parse_mode,omitempty"`
	MessageThreadID *int                        `json:"message_thread_id,omitempty"`
	ReplyMarkup     client.InlineKeyboardMarkup `json:"reply_markup"`
}

type sendEphemeralMessageWithInlineKeyboardRequest struct {
	ChatID          int64                       `json:"chat_id"`
	ReceiverUserID  int64                       `json:"receiver_user_id"`
	Text            string                      `json:"text"`
	ParseMode       *string                     `json:"parse_mode,omitempty"`
	MessageThreadID *int                        `json:"message_thread_id,omitempty"`
	ReplyMarkup     client.InlineKeyboardMarkup `json:"reply_markup"`
}

func (m *Messenger) sendEphemeralMessageWithInlineKeyboard(ctx context.Context, chatID, receiverUserID int64, text string, topicID int, mode string, keyboard client.InlineKeyboardMarkup) (int, error) {
	request := sendEphemeralMessageWithInlineKeyboardRequest{ChatID: chatID, ReceiverUserID: receiverUserID, Text: text, ReplyMarkup: keyboard}
	if mode != "" {
		request.ParseMode = &mode
	}
	if topicID != 0 {
		request.MessageThreadID = &topicID
	}
	body, err := json.Marshal(request)
	if err != nil {
		return 0, fmt.Errorf("encode ephemeral telegram question: %w", err)
	}
	sendCtx, cancel := telegramSendContext(ctx)
	defer cancel()
	resp, err := m.client.SendMessageWithBodyWithResponse(sendCtx, "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("send ephemeral telegram question to chat %d: %w", chatID, err)
	}
	if resp == nil || resp.JSON200 == nil {
		if resp != nil && resp.JSON400 != nil {
			return 0, fmt.Errorf("send ephemeral telegram question to chat %d: %s", chatID, resp.JSON400.Description)
		}
		return 0, fmt.Errorf("send ephemeral telegram question to chat %d: no response body", chatID)
	}
	if resp.JSON200.Result.EphemeralMessageId == nil || *resp.JSON200.Result.EphemeralMessageId <= 0 {
		return 0, fmt.Errorf("send ephemeral telegram question to chat %d: missing ephemeral message id", chatID)
	}
	return *resp.JSON200.Result.EphemeralMessageId, nil
}

func (m *Messenger) sendMessageWithInlineKeyboard(ctx context.Context, chatID int64, text string, topicID int, mode string, keyboard client.InlineKeyboardMarkup) (int, error) {
	request := sendMessageWithInlineKeyboardRequest{ChatID: chatID, Text: text, ReplyMarkup: keyboard}
	if mode != "" {
		request.ParseMode = &mode
	}
	if topicID != 0 {
		request.MessageThreadID = &topicID
	}
	body, err := json.Marshal(request)
	if err != nil {
		return 0, fmt.Errorf("encode telegram question: %w", err)
	}
	sendCtx, cancel := telegramSendContext(ctx)
	defer cancel()
	resp, err := m.client.SendMessageWithBodyWithResponse(sendCtx, "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("send telegram question to chat %d: %w", chatID, err)
	}
	if resp == nil {
		return 0, fmt.Errorf("send telegram question to chat %d: no response body", chatID)
	}
	if resp.JSON400 != nil {
		return 0, fmt.Errorf("send telegram question to chat %d: %s", chatID, resp.JSON400.Description)
	}
	if resp.JSON200 == nil {
		return 0, fmt.Errorf("send telegram question to chat %d: no response body", chatID)
	}
	return resp.JSON200.Result.MessageId, nil
}

// ClearInlineKeyboard removes all inline controls from a Telegram message.
func (m *Messenger) ClearInlineKeyboard(ctx context.Context, chatID int64, messageID int) error {
	request := client.EditMessageReplyMarkupJSONRequestBody{ChatId: &chatID, MessageId: &messageID}
	sendCtx, cancel := telegramSendContext(ctx)
	defer cancel()
	resp, err := m.client.EditMessageReplyMarkupWithResponse(sendCtx, request)
	if err != nil {
		return fmt.Errorf("clear inline keyboard from message %d: %w", messageID, err)
	}
	if resp == nil {
		return fmt.Errorf("clear inline keyboard from message %d: no response body", messageID)
	}
	if resp.JSON400 != nil {
		description := strings.TrimSpace(resp.JSON400.Description)
		if strings.Contains(strings.ToLower(description), "message is not modified") {
			return nil
		}
		return fmt.Errorf("clear inline keyboard from message %d: %s", messageID, description)
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("clear inline keyboard from message %d: no response body", messageID)
	}
	return nil
}

// DeleteMessage removes a regular Telegram message in full.
func (m *Messenger) DeleteMessage(ctx context.Context, chatID int64, messageID int) error {
	request := client.DeleteMessageJSONRequestBody{ChatId: chatID, MessageId: messageID}
	sendCtx, cancel := telegramSendContext(ctx)
	defer cancel()
	resp, err := m.client.DeleteMessageWithResponse(sendCtx, request)
	if err != nil {
		return fmt.Errorf("delete message %d: %w", messageID, err)
	}
	if resp == nil {
		return fmt.Errorf("delete message %d: no response body", messageID)
	}
	if resp.JSON400 != nil {
		description := strings.TrimSpace(resp.JSON400.Description)
		if strings.Contains(strings.ToLower(description), "message to delete not found") {
			return nil
		}
		return fmt.Errorf("delete message %d: %s", messageID, description)
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("delete message %d: no response body", messageID)
	}
	return nil
}

// DeleteEphemeralMessage removes a settled ephemeral question in full.
func (m *Messenger) DeleteEphemeralMessage(ctx context.Context, chatID, receiverUserID int64, ephemeralMessageID int) error {
	request := client.DeleteEphemeralMessageJSONRequestBody{
		ChatId:             chatID,
		ReceiverUserId:     int(receiverUserID),
		EphemeralMessageId: ephemeralMessageID,
	}
	sendCtx, cancel := telegramSendContext(ctx)
	defer cancel()
	resp, err := m.client.DeleteEphemeralMessageWithResponse(sendCtx, request)
	if err != nil {
		return fmt.Errorf("delete ephemeral message %d: %w", ephemeralMessageID, err)
	}
	if resp == nil {
		return fmt.Errorf("delete ephemeral message %d: no response body", ephemeralMessageID)
	}
	if resp.JSON400 != nil {
		description := strings.TrimSpace(resp.JSON400.Description)
		if strings.Contains(strings.ToLower(description), "message to delete not found") {
			return nil
		}
		return fmt.Errorf("delete ephemeral message %d: %s", ephemeralMessageID, description)
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("delete ephemeral message %d: no response body", ephemeralMessageID)
	}
	return nil
}

// AnswerCallbackQuery stops Telegram's loading state and optionally displays a
// notification or alert.
func (m *Messenger) AnswerCallbackQuery(ctx context.Context, callbackQueryID, text string, showAlert bool) error {
	request := client.AnswerCallbackQueryJSONRequestBody{CallbackQueryId: strings.TrimSpace(callbackQueryID)}
	if strings.TrimSpace(text) != "" {
		trimmed := strings.TrimSpace(text)
		request.Text = &trimmed
	}
	request.ShowAlert = &showAlert
	sendCtx, cancel := telegramSendContext(ctx)
	defer cancel()
	resp, err := m.client.AnswerCallbackQueryWithResponse(sendCtx, request)
	if err != nil {
		return fmt.Errorf("answer callback query: %w", err)
	}
	if resp == nil {
		return fmt.Errorf("answer callback query: no response body")
	}
	if resp.JSON400 != nil {
		return fmt.Errorf("answer callback query: %s", resp.JSON400.Description)
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("answer callback query: no response body")
	}
	return nil
}

func (m *Messenger) richMessagesEnabled() bool {
	return telegramRichMessagesEnabled(m.TelegramFormattingMode())
}

func telegramRichMessagesEnabled(mode string) bool {
	switch telegramfmt.NormalizeMode(mode) {
	case telegramfmt.ModeRichMarkdown, telegramfmt.ModeRichHTML:
		return true
	default:
		return false
	}
}

func richPlain(text string) client.InputRichMessage {
	skipEntityDetection := true
	return client.InputRichMessage{
		Html:                stringPtr(telegramfmt.HTML(text)),
		SkipEntityDetection: &skipEntityDetection,
	}
}

func richMarkdown(text string) client.InputRichMessage {
	return client.InputRichMessage{Markdown: stringPtr(text)}
}

func richHTML(text string) client.InputRichMessage {
	return client.InputRichMessage{Html: stringPtr(text)}
}

func stringPtr(v string) *string {
	return &v
}

func (m *Messenger) sendRichDraftWithFallback(
	ctx context.Context,
	chatID int64,
	draftID int,
	rich client.InputRichMessage,
	topicID int,
	legacyText string,
) error {
	err := m.sendRichDraft(ctx, chatID, draftID, rich, topicID)
	if err == nil {
		return nil
	}
	if !shouldFallbackRichSend(ctx, err) {
		return err
	}
	m.logger.Warn().Err(err).Int64("chat_id", chatID).Msg("send rich draft failed, retrying with legacy draft")
	return m.sendDraftPlainLegacy(ctx, chatID, draftID, legacyText, topicID)
}

func (m *Messenger) sendRichDraft(ctx context.Context, chatID int64, draftID int, rich client.InputRichMessage, topicID int) error {
	m.logger.Debug().
		Int64("chat_id", chatID).
		Int("draft_id", draftID).
		Int("rich_payload_bytes", richPayloadBytes(rich)).
		Msg("sending rich draft")
	req := client.SendRichMessageDraftJSONRequestBody{
		ChatId:      chatID,
		DraftId:     draftID,
		RichMessage: rich,
	}
	if topicID != 0 {
		req.MessageThreadId = &topicID
	}

	sendCtx, cancel := telegramSendContext(ctx)
	defer cancel()

	resp, err := m.client.SendRichMessageDraftWithResponse(sendCtx, req)
	if err != nil {
		return fmt.Errorf("sending rich draft to chat %d: %w", chatID, err)
	}
	if resp == nil {
		return fmt.Errorf("sending rich draft to chat %d: no response body", chatID)
	}
	if resp.JSON400 != nil {
		return fmt.Errorf("sending rich draft to chat %d: %s", chatID, resp.JSON400.Description)
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("sending rich draft to chat %d: no response body", chatID)
	}
	return nil
}

func (m *Messenger) sendRichMessageWithFallback(
	ctx context.Context,
	chatID int64,
	rich client.InputRichMessage,
	topicID int,
	fallback func(context.Context) (int, error),
) (int, error) {
	messageID, err := m.sendRichMessage(ctx, chatID, rich, topicID)
	if err == nil {
		return messageID, nil
	}
	if !shouldFallbackRichSend(ctx, err) {
		return 0, err
	}
	m.logger.Warn().Err(err).Int64("chat_id", chatID).Msg("send rich message failed, retrying with legacy message")
	return fallback(ctx)
}

func (m *Messenger) sendRichMessage(ctx context.Context, chatID int64, rich client.InputRichMessage, topicID int) (int, error) {
	m.logger.Debug().
		Int64("chat_id", chatID).
		Str("mode", telegramfmt.NormalizeMode(m.telegramFormattingMode)).
		Int("rich_payload_bytes", richPayloadBytes(rich)).
		Msg("sending rich telegram message")
	req := client.SendRichMessageJSONRequestBody{
		ChatId:      chatID,
		RichMessage: rich,
	}
	if topicID != 0 {
		req.MessageThreadId = &topicID
	}
	sendCtx, cancel := telegramSendContext(ctx)
	defer cancel()

	resp, err := m.client.SendRichMessageWithResponse(sendCtx, req)
	if err != nil {
		return 0, fmt.Errorf("send rich message to chat %d: %w", chatID, err)
	}
	if resp == nil {
		return 0, fmt.Errorf("send rich message to chat %d: no response body", chatID)
	}
	if resp.JSON400 != nil {
		return 0, fmt.Errorf("send rich message to chat %d: %s", chatID, resp.JSON400.Description)
	}
	if resp.JSON200 == nil {
		return 0, fmt.Errorf("send rich message to chat %d: no response body", chatID)
	}
	return resp.JSON200.Result.MessageId, nil
}

func richPayloadBytes(rich client.InputRichMessage) int {
	switch {
	case rich.Markdown != nil:
		return len(*rich.Markdown)
	case rich.Html != nil:
		return len(*rich.Html)
	default:
		return 0
	}
}

func shouldFallbackRichSend(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	return true
}

func (m *Messenger) sendMessageWithMode(ctx context.Context, chatID int64, text string, topicID int, mode, logMsg string) (int, error) {
	m.logger.Debug().
		Int64("chat_id", chatID).
		Str("mode", mode).
		Int("telegram_payload_bytes", len(text)).
		Msg("sending telegram message")
	req := client.SendMessageJSONRequestBody{
		ChatId: chatID,
		Text:   text,
	}
	if mode != "" {
		req.ParseMode = &mode
	}
	if topicID != 0 {
		req.MessageThreadId = &topicID
	}
	sendCtx, cancel := telegramSendContext(ctx)
	defer cancel()

	resp, err := m.client.SendMessageWithResponse(sendCtx, req)
	retryWithoutParseMode := false
	if strings.TrimSpace(mode) != "" {
		switch {
		case err != nil:
			retryWithoutParseMode = true
		case resp != nil && resp.JSON400 != nil:
			desc := strings.ToLower(strings.TrimSpace(resp.JSON400.Description))
			retryWithoutParseMode = desc != "" &&
				(strings.Contains(desc, "can't parse entities") ||
					strings.Contains(desc, "cant parse entities") ||
					(strings.Contains(desc, "parse entities") && strings.Contains(desc, "entity")))
		}
	}
	if retryWithoutParseMode {
		retryReason := "transport error"
		if err == nil && resp != nil && resp.JSON400 != nil {
			retryReason = "telegram parse error"
		}
		m.logger.Warn().Err(err).Int64("chat_id", chatID).Str("retry_reason", retryReason).Msg(logMsg + " failed, retrying without parse_mode")
		req.ParseMode = nil
		resp, err = m.client.SendMessageWithResponse(sendCtx, req)
		if err != nil {
			return 0, fmt.Errorf("%s to chat %d: %w", logMsg, chatID, err)
		}
	}
	if resp.JSON400 != nil {
		return 0, fmt.Errorf("%s to chat %d: %s", logMsg, chatID, resp.JSON400.Description)
	}
	if resp.JSON200 == nil {
		return 0, fmt.Errorf("%s to chat %d: no response body", logMsg, chatID)
	}
	return resp.JSON200.Result.MessageId, nil
}

// SendChatAction sends a chat action (e.g., "typing").
func (m *Messenger) SendChatAction(ctx context.Context, chatID int64, topicID int, action string) error {
	if chatID == 0 {
		return nil
	}
	req := client.SendChatActionJSONRequestBody{
		ChatId: chatID,
		Action: action,
	}
	if topicID != 0 {
		req.MessageThreadId = &topicID
	}

	sendCtx, cancel := telegramSendContext(ctx)
	defer cancel()

	resp, err := m.client.SendChatActionWithResponse(sendCtx, req)
	if err != nil {
		return fmt.Errorf("sending chat action %q to chat %d: %w", action, chatID, err)
	}
	if resp == nil {
		return fmt.Errorf("sending chat action %q to chat %d: no response body", action, chatID)
	}
	if resp.JSON400 != nil {
		return fmt.Errorf("sending chat action %q to chat %d: %s", action, chatID, resp.JSON400.Description)
	}
	if resp.JSON200 == nil {
		if resp.HTTPResponse != nil && resp.HTTPResponse.StatusCode >= http.StatusOK && resp.HTTPResponse.StatusCode < http.StatusMultipleChoices {
			m.logger.Debug().
				Int64("chat_id", chatID).
				Int("topic_id", topicID).
				Str("action", action).
				Msg("sending chat action succeeded")
			return nil
		}
		return fmt.Errorf("sending chat action %q to chat %d: no response body", action, chatID)
	}
	m.logger.Debug().
		Int64("chat_id", chatID).
		Int("topic_id", topicID).
		Str("action", action).
		Msg("sending chat action succeeded")
	return nil
}

func telegramSendContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, telegramSendTimeout)
}
