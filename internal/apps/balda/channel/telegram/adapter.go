package telegram

import (
	"context"
	"fmt"
	gohtml "html"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	"github.com/rs/zerolog"
	"github.com/tgbotkit/client"
	"github.com/tgbotkit/runtime/events"
	"github.com/tgbotkit/runtime/messagetype"
	"go.uber.org/fx"
)

var _ deliverycmd.Adapter = (*Adapter)(nil)

const (
	chatTypePrivate     = "private"
	defaultTypingAction = "typing"
	modeRichMarkdown    = "rich_markdown"
)

type TelegramMessenger interface {
	TelegramFormattingMode() string
	SendPlain(ctx context.Context, chatID int64, text string, topicID int) error
	SendMarkdownWithMode(ctx context.Context, chatID int64, text string, topicID int, mode string) error
	SendAgentReply(ctx context.Context, chatID int64, text string, topicID int) error
	SendAgentReplyLastMessageIDAndMode(ctx context.Context, chatID int64, text string, topicID int, mode string) (int, error)
	SendAgentReplyWithInlineKeyboardLastMessageIDAndMode(ctx context.Context, chatID int64, text string, topicID int, mode string, keyboard client.InlineKeyboardMarkup, fallbackText string) (int, error)
	ClearInlineKeyboard(ctx context.Context, chatID int64, messageID int) error
	AnswerCallbackQuery(ctx context.Context, callbackQueryID, text string, showAlert bool) error
	SendDraftPlain(ctx context.Context, chatID int64, draftID int, text string, topicID int) error
	SendDraftMarkdownWithMode(ctx context.Context, chatID int64, draftID int, text string, topicID int, mode string) error
	SendChatAction(ctx context.Context, chatID int64, topicID int, action string) error
}

// Adapter maps Telegram runtime events and operations to balda session locators.
type Adapter struct {
	messenger          TelegramMessenger
	tgClient           client.ClientWithResponsesInterface
	logger             zerolog.Logger
	planUpdatesEnabled bool

	typingMu               sync.Mutex
	typingThrottleInterval time.Duration
	typingLastSentAt       map[string]time.Time
	now                    func() time.Time

	progressMu          sync.Mutex
	progressDrafts      map[string]int
	nextProgressDraftID int
}

// MessageContext is the balda-facing Telegram message shape.
type MessageContext struct {
	Locator          deliverycmd.Locator
	ChatID           int64
	TopicID          int
	MessageID        int
	ReplyToMessageID int
	UserID           int64
	Entities         []client.MessageEntity
	IsReply          bool
	ReplyToUserID    int64
	ReplyToIsBot     bool
	ReplyContent     string
	Text             string
	HasCommand       bool
	DeliveryOptions  deliveryfmt.Options
	ProgressPolicy   deliveryfmt.ProgressPolicy
	IsDM             bool
}

// CommandContext is the balda-facing Telegram command shape.
type CommandContext struct {
	Locator         deliverycmd.Locator
	DeliveryOptions deliveryfmt.Options
	ChatID          int64
	TopicID         int
	UserID          int64
	Command         string
	Args            string
	IsDM            bool
}

// TopicLifecycleContext is the balda-facing Telegram topic lifecycle shape.
type TopicLifecycleContext struct {
	Locator   deliverycmd.Locator
	ChatID    int64
	TopicID   int
	MessageID int
	UserID    int64
	Type      messagetype.MessageType
}

type AdapterParams struct {
	fx.In

	Messenger          TelegramMessenger
	TGClient           client.ClientWithResponsesInterface
	PlanUpdatesEnabled bool `name:"balda_telegram_plan_updates"`
	Logger             zerolog.Logger
}

// NewAdapter creates the Telegram balda adapter.
func NewAdapter(params AdapterParams) *Adapter {
	return &Adapter{
		messenger:           params.Messenger,
		tgClient:            params.TGClient,
		logger:              params.Logger.With().Str("component", "balda.channel.telegram").Logger(),
		planUpdatesEnabled:  params.PlanUpdatesEnabled,
		typingLastSentAt:    make(map[string]time.Time),
		now:                 time.Now,
		progressDrafts:      make(map[string]int),
		nextProgressDraftID: 1,
	}
}

func (a *Adapter) SetTypingThrottleInterval(interval time.Duration) {
	if a == nil {
		return
	}
	a.typingMu.Lock()
	defer a.typingMu.Unlock()
	a.typingThrottleInterval = interval
}

// Deliver executes one semantic Telegram delivery operation.
func (a *Adapter) Deliver(ctx context.Context, locator deliverycmd.Locator, operation deliverycmd.Operation) (deliverycmd.Result, error) {
	var err error
	result := deliverycmd.Result{}
	switch operation.Kind {
	case deliverycmd.OperationPlain:
		err = a.SendPlain(ctx, locator, operation.Text)
	case deliverycmd.OperationMarkdown:
		err = a.SendMarkdownWithProfile(ctx, locator, operation.Profile, operation.Text)
	case deliverycmd.OperationAgentReply:
		result.ProviderMessageID, err = a.SendAgentReplyWithQuestion(ctx, locator, operation.Profile, operation.Text, operation.Question)
	case deliverycmd.OperationDraft:
		err = a.SendDraftPlain(ctx, locator, operation.DraftID, operation.Text)
	case deliverycmd.OperationTyping:
		err = a.SendTyping(ctx, locator)
	case deliverycmd.OperationProgress:
		err = a.SendProgress(ctx, locator, operation.Progress)
	case deliverycmd.OperationClearQuestionControls:
		err = a.ClearQuestionControls(ctx, locator, operation.MessageID)
	default:
		err = fmt.Errorf("unsupported telegram delivery operation %q", operation.Kind)
	}
	return result, err
}

func (a *Adapter) shouldSendTyping(locator deliverycmd.Locator) bool {
	if a == nil {
		return false
	}
	a.typingMu.Lock()
	defer a.typingMu.Unlock()
	if a.typingThrottleInterval <= 0 {
		return true
	}
	now := time.Now()
	if a.now != nil {
		now = a.now()
	}
	key := locator.SessionID
	if last, ok := a.typingLastSentAt[key]; ok && now.Sub(last) < a.typingThrottleInterval {
		return false
	}
	a.typingLastSentAt[key] = now
	return true
}

func (a *Adapter) progressDraftID(locator deliverycmd.Locator) int {
	if a == nil {
		return 0
	}
	a.progressMu.Lock()
	defer a.progressMu.Unlock()
	key := locator.SessionID
	if draftID := a.progressDrafts[key]; draftID > 0 {
		return draftID
	}
	if a.nextProgressDraftID <= 0 {
		a.nextProgressDraftID = 1
	}
	draftID := a.nextProgressDraftID
	a.nextProgressDraftID++
	a.progressDrafts[key] = draftID
	return draftID
}

// MessageContextFromEvent converts a Telegram message event into balda channel context.
func (a *Adapter) MessageContextFromEvent(event *events.MessageEvent) (MessageContext, bool) {
	if event == nil || event.Message == nil || event.Message.From == nil {
		return MessageContext{}, false
	}

	topicID := a.topicIDFromMessage(event.Message)

	text := ""
	if event.Message.Text != nil {
		text = *event.Message.Text
	}
	var entities []client.MessageEntity
	if event.Message.Entities != nil {
		entities = append(entities, (*event.Message.Entities)...)
	}
	isReply := event.Message.ReplyToMessage != nil || event.Message.Quote != nil || event.Message.ExternalReply != nil
	replyToUserID := int64(0)
	replyToIsBot := false
	replyToMessageID := 0
	if event.Message.ReplyToMessage != nil && event.Message.ReplyToMessage.From != nil {
		replyToUserID = event.Message.ReplyToMessage.From.Id
		replyToIsBot = event.Message.ReplyToMessage.From.IsBot
		replyToMessageID = event.Message.ReplyToMessage.MessageId
	}
	replyContent := replyContentFromMessage(event.Message)

	hasCommand := false
	if event.Message.Entities != nil {
		for _, entity := range *event.Message.Entities {
			if entity.Type == "bot_command" {
				hasCommand = true
				break
			}
		}
	}

	return MessageContext{
		Locator:          NewLocator(event.Message.Chat.Id, topicID),
		ChatID:           event.Message.Chat.Id,
		TopicID:          topicID,
		MessageID:        event.Message.MessageId,
		ReplyToMessageID: replyToMessageID,
		UserID:           event.Message.From.Id,
		Entities:         entities,
		IsReply:          isReply,
		ReplyToUserID:    replyToUserID,
		ReplyToIsBot:     replyToIsBot,
		ReplyContent:     replyContent,
		Text:             text,
		HasCommand:       hasCommand,
		DeliveryOptions: deliveryfmt.Options{
			Profile: deliveryfmt.Profile{
				Format:       deliveryfmt.FormatAuto,
				TelegramMode: a.telegramFormattingMode(),
			},
			ProgressPolicy: deliveryfmt.ProgressPolicy{
				Typing:      true,
				Thinking:    event.Message.Chat.Type == chatTypePrivate,
				PlanUpdates: a.planUpdatesEnabled,
			},
		},
		ProgressPolicy: deliveryfmt.ProgressPolicy{
			Typing:      true,
			Thinking:    event.Message.Chat.Type == chatTypePrivate,
			PlanUpdates: a.planUpdatesEnabled,
		},
		IsDM: event.Message.Chat.Type == chatTypePrivate,
	}, true
}

func replyContentFromMessage(message *client.Message) string {
	if message == nil {
		return ""
	}
	if message.Quote != nil && strings.TrimSpace(message.Quote.Text) != "" {
		return message.Quote.Text
	}
	if message.ReplyToMessage == nil {
		return ""
	}
	if message.ReplyToMessage.Text != nil && strings.TrimSpace(*message.ReplyToMessage.Text) != "" {
		return *message.ReplyToMessage.Text
	}
	if message.ReplyToMessage.Caption != nil && strings.TrimSpace(*message.ReplyToMessage.Caption) != "" {
		return *message.ReplyToMessage.Caption
	}
	return richMessagePlainText(message.ReplyToMessage.RichMessage)
}

func richMessagePlainText(rich *client.RichMessage) string {
	if rich == nil || len(rich.Blocks) == 0 {
		return ""
	}
	return strings.TrimSpace(strings.Join(nonEmptyRichParts(richBlocksPlainText(rich.Blocks)), "\n"))
}

func richBlocksPlainText(blocks []client.RichBlock) []string {
	parts := make([]string, 0, len(blocks))
	for _, block := range blocks {
		parts = append(parts, richBlockPlainText(block)...)
	}
	return nonEmptyRichParts(parts)
}

func richBlockPlainText(block client.RichBlock) []string {
	if len(block) == 0 {
		return nil
	}
	blockType, _ := block["type"].(string)
	switch blockType {
	case "paragraph", "heading", "footer", "pre", "pullquote", "thinking":
		return richTextAndCreditPlainText(block["text"], block["credit"])
	case "mathematical_expression":
		return richTextParts(block["expression"])
	case "blockquote":
		parts := richBlockArrayPlainText(block["blocks"])
		parts = append(parts, richTextParts(block["credit"])...)
		return nonEmptyRichParts(parts)
	case "collage", "slideshow":
		parts := richBlockArrayPlainText(block["blocks"])
		parts = append(parts, richCaptionPlainText(block["caption"])...)
		return nonEmptyRichParts(parts)
	case "details":
		parts := richTextParts(block["summary"])
		parts = append(parts, richBlockArrayPlainText(block["blocks"])...)
		return nonEmptyRichParts(parts)
	case "list":
		return richListPlainText(block["items"])
	case "table":
		parts := richTablePlainText(block["cells"])
		parts = append(parts, richTextParts(block["caption"])...)
		return nonEmptyRichParts(parts)
	case "animation", "audio", "map", "photo", "video", "voice_note":
		return richCaptionPlainText(block["caption"])
	default:
		return fallbackRichBlockPlainText(block)
	}
}

func richTextAndCreditPlainText(text, credit interface{}) []string {
	parts := richTextParts(text)
	parts = append(parts, richTextParts(credit)...)
	return nonEmptyRichParts(parts)
}

func richCaptionPlainText(value interface{}) []string {
	caption, ok := value.(map[string]interface{})
	if !ok {
		return richTextParts(value)
	}
	parts := richTextParts(caption["text"])
	parts = append(parts, richTextParts(caption["credit"])...)
	return nonEmptyRichParts(parts)
}

func richBlockArrayPlainText(value interface{}) []string {
	switch blocks := value.(type) {
	case []client.RichBlock:
		return richBlocksPlainText(blocks)
	case []interface{}:
		parts := make([]string, 0, len(blocks))
		for _, item := range blocks {
			parts = append(parts, richBlockValuePlainText(item)...)
		}
		return nonEmptyRichParts(parts)
	default:
		return richBlockValuePlainText(value)
	}
}

func richBlockValuePlainText(value interface{}) []string {
	switch v := value.(type) {
	case map[string]interface{}:
		return richBlockPlainText(v)
	default:
		return richTextParts(v)
	}
}

func richListPlainText(value interface{}) []string {
	items, ok := value.([]interface{})
	if !ok {
		return nil
	}
	parts := make([]string, 0, len(items))
	for _, itemValue := range items {
		item, ok := itemValue.(map[string]interface{})
		if !ok {
			continue
		}
		itemParts := richBlockArrayPlainText(item["blocks"])
		if label, ok := item["label"].(string); ok && strings.TrimSpace(label) != "" && len(itemParts) > 0 {
			itemParts[0] = strings.TrimSpace(label) + " " + itemParts[0]
		}
		parts = append(parts, itemParts...)
	}
	return nonEmptyRichParts(parts)
}

func richTablePlainText(value interface{}) []string {
	rows, ok := value.([]interface{})
	if !ok {
		return nil
	}
	parts := make([]string, 0, len(rows))
	for _, rowValue := range rows {
		cells, ok := rowValue.([]interface{})
		if !ok {
			continue
		}
		rowParts := make([]string, 0, len(cells))
		for _, cellValue := range cells {
			cell, ok := cellValue.(map[string]interface{})
			if !ok {
				continue
			}
			rowParts = append(rowParts, richTextParts(cell["text"])...)
		}
		if row := strings.Join(nonEmptyRichParts(rowParts), " | "); row != "" {
			parts = append(parts, row)
		}
	}
	return nonEmptyRichParts(parts)
}

func richTextParts(value interface{}) []string {
	switch v := value.(type) {
	case nil:
		return nil
	case string:
		if strings.TrimSpace(v) == "" {
			return nil
		}
		return []string{v}
	case []interface{}:
		var out strings.Builder
		for _, item := range v {
			out.WriteString(strings.Join(richTextParts(item), ""))
		}
		if text := strings.TrimSpace(out.String()); text != "" {
			return []string{text}
		}
		return nil
	case map[string]interface{}:
		if text := strings.Join(richTextParts(v["text"]), ""); text != "" {
			return []string{text}
		}
		if text := strings.Join(richTextParts(v["alternative_text"]), ""); text != "" {
			return []string{text}
		}
		if text := strings.Join(richTextParts(v["expression"]), ""); text != "" {
			return []string{text}
		}
		return nil
	default:
		return nil
	}
}

func fallbackRichBlockPlainText(block client.RichBlock) []string {
	keys := []string{"text", "summary", "blocks", "items", "cells", "caption", "credit", "alternative_text", "expression"}
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, richBlockValuePlainText(block[key])...)
	}
	return nonEmptyRichParts(parts)
}

func nonEmptyRichParts(parts []string) []string {
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// CommandContextFromEvent converts a Telegram command event into balda channel context.
func (a *Adapter) CommandContextFromEvent(event *events.CommandEvent) (CommandContext, bool) {
	if event == nil || event.Message == nil || event.Message.From == nil {
		return CommandContext{}, false
	}
	topicID := a.topicIDFromMessage(event.Message)

	return CommandContext{
		Locator: NewLocator(event.Message.Chat.Id, topicID),
		DeliveryOptions: deliveryfmt.Options{
			Profile: deliveryfmt.Profile{
				Format:       deliveryfmt.FormatAuto,
				TelegramMode: a.telegramFormattingMode(),
			},
			ProgressPolicy: deliveryfmt.ProgressPolicy{
				Typing:      true,
				Thinking:    event.Message.Chat.Type == chatTypePrivate,
				PlanUpdates: a.planUpdatesEnabled,
			},
		},
		ChatID:  event.Message.Chat.Id,
		TopicID: topicID,
		UserID:  event.Message.From.Id,
		Command: event.Command,
		Args:    event.Args,
		IsDM:    event.Message.Chat.Type == chatTypePrivate,
	}, true
}

// TopicLifecycleFromEvent converts a Telegram topic lifecycle event into balda channel context.
func (a *Adapter) TopicLifecycleFromEvent(event *events.MessageEvent) (TopicLifecycleContext, bool) {
	if event == nil || event.Message == nil || event.Message.MessageThreadId == nil {
		return TopicLifecycleContext{}, false
	}
	if !isTopicMessage(event.Message) {
		a.logger.Debug().
			Str("chat_type", event.Message.Chat.Type).
			Int("message_thread_id", *event.Message.MessageThreadId).
			Msg("ignoring topic lifecycle event for non-topic message")
		return TopicLifecycleContext{}, false
	}

	topicID := *event.Message.MessageThreadId
	userID := int64(0)
	if event.Message.From != nil {
		userID = event.Message.From.Id
	}

	return TopicLifecycleContext{
		Locator:   NewLocator(event.Message.Chat.Id, topicID),
		ChatID:    event.Message.Chat.Id,
		TopicID:   topicID,
		MessageID: event.Message.MessageId,
		UserID:    userID,
		Type:      event.Type,
	}, true
}

func telegramReasoningMarkdown(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	return gohtml.EscapeString(text)
}

func telegramRichMarkdownEnabled(mode string) bool {
	return normalizeTelegramMode(mode) == modeRichMarkdown
}

func telegramPlanUpdateMarkdown(progress deliverycmd.Progress) string {
	if progress.Plan == nil || len(progress.Plan.Entries) == 0 {
		return strings.TrimSpace(progress.Text)
	}
	lines := make([]string, 0, len(progress.Plan.Entries)+2)
	lines = append(lines, "# Plan update", "")
	for _, entry := range progress.Plan.Entries {
		lines = append(lines, telegramPlanChecklistItem(entry.Content, entry.Status))
	}
	return strings.Join(lines, "\n")
}

func telegramPlanChecklistItem(content string, status string) string {
	content = strings.TrimSpace(content)
	if content == "" {
		content = "(no description)"
	}
	switch strings.TrimSpace(status) {
	case "completed":
		return "- [x] " + content
	case "in progress":
		return "- [ ] _In progress:_ " + content
	case "pending":
		return "- [ ] " + content
	case "blocked":
		return "- [ ] _Blocked:_ " + content
	case "failed":
		return "- [ ] _Failed:_ " + content
	case "cancelled":
		return "- [ ] ~~" + content + "~~"
	case "unknown", "":
		return "- [ ] " + content
	default:
		return "- [ ] _" + status + ":_ " + content
	}
}

// SendPlain sends a plain text reply to the locator.
func (a *Adapter) SendPlain(ctx context.Context, locator deliverycmd.Locator, text string) error {
	chatID, topicID, err := telegramTuple(locator)
	if err != nil {
		return err
	}
	return a.messenger.SendPlain(ctx, chatID, text, topicID)
}

// SendMarkdown sends a Markdown reply to the locator.
func (a *Adapter) SendMarkdown(ctx context.Context, locator deliverycmd.Locator, text string) error {
	return a.SendMarkdownWithProfile(ctx, locator, deliverycmd.Profile{}, text)
}

// SendMarkdownWithProfile sends a Markdown reply using a request-scoped formatting profile.
func (a *Adapter) SendMarkdownWithProfile(ctx context.Context, locator deliverycmd.Locator, profile deliverycmd.Profile, text string) error {
	chatID, topicID, err := telegramTuple(locator)
	if err != nil {
		return err
	}
	mode := deliveryfmt.EffectiveTelegramMode(telegramDeliveryProfile(profile), a.telegramFormattingMode())
	return a.messenger.SendMarkdownWithMode(ctx, chatID, text, topicID, mode)
}

// SendAgentReply sends final agent output for the locator using configured formatting mode.
func (a *Adapter) SendAgentReply(ctx context.Context, locator deliverycmd.Locator, text string) error {
	chatID, topicID, err := telegramTuple(locator)
	if err != nil {
		return err
	}
	return a.messenger.SendAgentReply(ctx, chatID, text, topicID)
}

// SendAgentReplyWithProviderMessageID sends final agent output and returns the provider message ID when available.
func (a *Adapter) SendAgentReplyWithProviderMessageID(ctx context.Context, locator deliverycmd.Locator, text string) (string, error) {
	return a.SendAgentReplyWithProviderMessageIDAndProfile(ctx, locator, deliverycmd.Profile{}, text)
}

// SendAgentReplyWithProviderMessageIDAndProfile sends final agent output using a request-scoped formatting profile.
func (a *Adapter) SendAgentReplyWithProviderMessageIDAndProfile(ctx context.Context, locator deliverycmd.Locator, profile deliverycmd.Profile, text string) (string, error) {
	return a.SendAgentReplyWithQuestion(ctx, locator, profile, text, nil)
}

// SendAgentReplyWithQuestion projects generic question options into Telegram
// inline controls while preserving a text-only fallback.
func (a *Adapter) SendAgentReplyWithQuestion(ctx context.Context, locator deliverycmd.Locator, profile deliverycmd.Profile, text string, question *deliverycmd.Question) (string, error) {
	chatID, topicID, err := telegramTuple(locator)
	if err != nil {
		return "", err
	}
	mode := deliveryfmt.EffectiveTelegramMode(telegramDeliveryProfile(profile), a.telegramFormattingMode())
	var lastMessageID int
	if question == nil {
		lastMessageID, err = a.messenger.SendAgentReplyLastMessageIDAndMode(ctx, chatID, text, topicID, mode)
	} else {
		keyboard, keyboardErr := questionInlineKeyboard(*question)
		if keyboardErr != nil {
			a.logger.Warn().Err(keyboardErr).Str("question_id", question.ID).Msg("build telegram question controls failed, using text choices")
			lastMessageID, err = a.messenger.SendAgentReplyLastMessageIDAndMode(ctx, chatID, questionTextFallback(text, *question), topicID, mode)
		} else {
			lastMessageID, err = a.messenger.SendAgentReplyWithInlineKeyboardLastMessageIDAndMode(
				ctx,
				chatID,
				text,
				topicID,
				mode,
				keyboard,
				questionTextFallback(text, *question),
			)
		}
	}
	if err != nil {
		return "", err
	}
	if lastMessageID <= 0 {
		return "", nil
	}
	return strconv.Itoa(lastMessageID), nil
}

// ClearQuestionControls removes the inline keyboard from a previously sent
// Telegram question.
func (a *Adapter) ClearQuestionControls(ctx context.Context, locator deliverycmd.Locator, messageID string) error {
	chatID, _, err := telegramTuple(locator)
	if err != nil {
		return err
	}
	id, err := strconv.Atoi(strings.TrimSpace(messageID))
	if err != nil || id <= 0 {
		return fmt.Errorf("invalid telegram message id %q", messageID)
	}
	return a.messenger.ClearInlineKeyboard(ctx, chatID, id)
}

// SendDraftPlain updates a draft message for the locator.
func (a *Adapter) SendDraftPlain(ctx context.Context, locator deliverycmd.Locator, draftID int, text string) error {
	chatID, topicID, err := telegramTuple(locator)
	if err != nil {
		return err
	}
	return a.messenger.SendDraftPlain(ctx, chatID, draftID, text, topicID)
}

// SendTyping sends a typing chat action to the locator chat/topic.
func (a *Adapter) SendTyping(ctx context.Context, locator deliverycmd.Locator) error {
	chatID, topicID, err := telegramTuple(locator)
	if err != nil {
		return err
	}
	if !a.shouldSendTyping(locator) {
		return nil
	}
	return a.messenger.SendChatAction(ctx, chatID, topicID, defaultTypingAction)
}

// SendProgress renders a semantic conversational progress update for Telegram.
func (a *Adapter) SendProgress(ctx context.Context, locator deliverycmd.Locator, progress deliverycmd.Progress) error {
	if progress.Policy.Typing {
		if err := a.SendTyping(ctx, locator); err != nil {
			a.logger.Warn().Err(err).Str("session_id", locator.SessionID).Msg("telegram typing progress sugar failed")
		}
	}
	if !progress.Visible {
		return nil
	}
	switch progress.Kind {
	case deliverycmd.ProgressThinking:
		a.logger.Debug().
			Str("session_id", locator.SessionID).
			Bool("visible", progress.Visible).
			Bool("policy_thinking", progress.Policy.Thinking).
			Int("text_char_count", len(strings.TrimSpace(progress.Text))).
			Int("sequence", progress.Sequence).
			Msg("telegram thinking progress received")
		if !progress.Policy.Thinking {
			return nil
		}
		if strings.TrimSpace(progress.Text) == "" {
			return nil
		}
		chatID, topicID, err := telegramTuple(locator)
		if err != nil {
			return err
		}
		draftID := a.progressDraftID(locator)
		a.logger.Debug().
			Str("session_id", locator.SessionID).
			Int("draft_id", draftID).
			Bool("rich_markdown", telegramRichMarkdownEnabled(a.telegramFormattingMode())).
			Msg("telegram rendering thinking progress")
		if telegramRichMarkdownEnabled(a.telegramFormattingMode()) {
			return a.messenger.SendDraftMarkdownWithMode(ctx, chatID, draftID, telegramReasoningMarkdown(progress.Text), topicID, modeRichMarkdown)
		}
		return a.messenger.SendDraftPlain(ctx, chatID, draftID, progress.Text, topicID)
	case deliverycmd.ProgressPlanUpdate:
		chatID, topicID, err := telegramTuple(locator)
		if err != nil {
			return err
		}
		if telegramRichMarkdownEnabled(a.telegramFormattingMode()) {
			markdown := telegramPlanUpdateMarkdown(progress)
			if progress.Policy.Thinking {
				return a.messenger.SendDraftMarkdownWithMode(ctx, chatID, a.progressDraftID(locator), markdown, topicID, modeRichMarkdown)
			}
			return a.messenger.SendMarkdownWithMode(ctx, chatID, markdown, topicID, modeRichMarkdown)
		}
		if progress.Policy.Thinking {
			return a.messenger.SendDraftPlain(ctx, chatID, a.progressDraftID(locator), progress.Text, topicID)
		}
		return a.messenger.SendPlain(ctx, chatID, progress.Text, topicID)
	default:
		return fmt.Errorf("unsupported telegram progress kind %q", progress.Kind)
	}
}

// CreateTopicLocator creates a Telegram forum topic and returns the balda locator for it.
func (a *Adapter) CreateTopicLocator(ctx context.Context, chatID int64, topicName string) (deliverycmd.Locator, error) {
	createTopicResp, err := a.tgClient.CreateForumTopicWithResponse(ctx, client.CreateForumTopicJSONRequestBody{
		ChatId: chatID,
		Name:   topicName,
	})
	if err != nil {
		return deliverycmd.Locator{}, fmt.Errorf("creating forum topic: %w", err)
	}
	if createTopicResp.JSON200 == nil {
		return deliverycmd.Locator{}, fmt.Errorf("failed to create forum topic: %s", createTopicResp.Status())
	}

	return NewLocator(chatID, createTopicResp.JSON200.Result.MessageThreadId), nil
}

// Close removes a Telegram forum topic for the locator. Root locators are ignored.
func (a *Adapter) Close(ctx context.Context, locator deliverycmd.Locator) error {
	chatID, topicID, err := telegramTuple(locator)
	if err != nil {
		return err
	}
	if topicID == 0 {
		return nil
	}

	closeResp, err := a.tgClient.DeleteForumTopicWithResponse(ctx, client.DeleteForumTopicJSONRequestBody{
		ChatId:          chatID,
		MessageThreadId: topicID,
	})
	if err != nil {
		return fmt.Errorf("removing forum topic: %w", err)
	}
	if closeResp.JSON200 == nil {
		return fmt.Errorf("failed to remove forum topic: %s", closeResp.Status())
	}
	return nil
}

func telegramTuple(locator deliverycmd.Locator) (int64, int, error) {
	address, ok, err := DecodeLocator(locator)
	if err != nil {
		return 0, 0, fmt.Errorf("decode telegram locator %q: %w", locator.SessionID, err)
	}
	if !ok {
		return 0, 0, fmt.Errorf("unsupported channel type %q", locator.ChannelType)
	}
	return address.ChatID, address.TopicID, nil
}

func telegramDeliveryProfile(profile deliverycmd.Profile) deliveryfmt.Profile {
	return deliveryfmt.Profile{
		Format:         deliveryfmt.Format(profile.Format),
		TelegramMode:   profile.TelegramMode,
		FormattingMode: profile.FormattingMode,
	}
}

func normalizeTelegramMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case modeRichMarkdown:
		return modeRichMarkdown
	case "rich_html":
		return "rich_html"
	case "markdownv2":
		return "markdownv2"
	case string(deliveryfmt.FormatHTML):
		return string(deliveryfmt.FormatHTML)
	case "none":
		return "none"
	default:
		return modeRichMarkdown
	}
}

func (a *Adapter) telegramFormattingMode() string {
	if a == nil || a.messenger == nil {
		return modeRichMarkdown
	}
	return a.messenger.TelegramFormattingMode()
}

func (a *Adapter) topicIDFromMessage(msg *client.Message) int {
	if msg == nil || msg.MessageThreadId == nil {
		return 0
	}
	if msg.Chat.Type != chatTypePrivate {
		// In public chats, message_thread_id is the routing key for balda job
		// threads even when is_topic_message is omitted or false.
		return *msg.MessageThreadId
	}
	if !isTopicMessage(msg) {
		a.logger.Debug().
			Str("chat_type", msg.Chat.Type).
			Int("message_thread_id", *msg.MessageThreadId).
			Msg("ignoring message_thread_id for non-topic message")
		return 0
	}
	return *msg.MessageThreadId
}

func isTopicMessage(msg *client.Message) bool {
	if msg == nil || msg.MessageThreadId == nil {
		return false
	}
	if msg.IsTopicMessage != nil {
		return *msg.IsTopicMessage
	}
	// Fallback for payloads that omit is_topic_message: if Telegram sent a
	// message_thread_id, treat it as a topic/thread-scoped message.
	return true
}
