package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/tgbotkit/client"
	"github.com/tgbotkit/runtime/events"
)

const QuestionCallbackPrefix = "balda:q:"
const ephemeralProviderMessagePrefix = "ephemeral:"
const (
	telegramQuestionControlHandleClearInlineKeyboard = "telegram:inline:clear"
	telegramQuestionControlHandleDeleteMessage       = "telegram:message:delete"
)

// CallbackContext is the channel-normalized context of a Telegram question
// selection.
type CallbackContext struct {
	Locator           deliverycmd.Locator
	CallbackQueryID   string
	QuestionID        string
	OptionIndex       int
	ProviderMessageID string
	UserID            int64
}

func questionInlineKeyboard(question deliverycmd.Question) (client.InlineKeyboardMarkup, error) {
	rows := make([][]client.InlineKeyboardButton, 0, len(question.Options))
	for index, option := range question.Options {
		payload := QuestionCallbackPrefix + strings.TrimSpace(question.ID) + ":" + strconv.Itoa(index+1)
		if len(payload) > 64 {
			return client.InlineKeyboardMarkup{}, fmt.Errorf("telegram question callback exceeds 64 bytes")
		}
		rows = append(rows, []client.InlineKeyboardButton{{
			Text:         strings.TrimSpace(option.Label),
			CallbackData: &payload,
		}})
	}
	return client.InlineKeyboardMarkup{InlineKeyboard: rows}, nil
}

func questionTextFallback(prompt string, question deliverycmd.Question) string {
	var out strings.Builder
	out.WriteString(strings.TrimSpace(prompt))
	out.WriteString("\n\nChoose:")
	for index, option := range question.Options {
		fmt.Fprintf(&out, "\n%d. %s", index+1, strings.TrimSpace(option.Label))
	}
	out.WriteString("\n\nReply with the number or option name.")
	return out.String()
}

// CallbackContextFromEvent validates and normalizes a Telegram callback into
// a transport-neutral locator plus question selection coordinates.
func (a *Adapter) CallbackContextFromEvent(event *events.CallbackQueryEvent) (CallbackContext, bool) {
	if event == nil || event.CallbackQuery == nil || event.CallbackQuery.Data == nil || event.CallbackQuery.Message == nil {
		return CallbackContext{}, false
	}
	questionID, optionIndex, ok := parseQuestionCallback(*event.CallbackQuery.Data)
	if !ok {
		return CallbackContext{}, false
	}
	var message struct {
		MessageID          int   `json:"message_id"`
		EphemeralMessageID *int  `json:"ephemeral_message_id,omitempty"`
		MessageThreadID    *int  `json:"message_thread_id,omitempty"`
		IsTopicMessage     *bool `json:"is_topic_message,omitempty"`
		ReceiverUser       *struct {
			ID int64 `json:"id"`
		} `json:"receiver_user,omitempty"`
		Chat struct {
			ID   int64  `json:"id"`
			Type string `json:"type"`
		} `json:"chat"`
	}
	raw, err := json.Marshal(event.CallbackQuery.Message)
	if err != nil || json.Unmarshal(raw, &message) != nil || message.Chat.ID == 0 {
		return CallbackContext{}, false
	}
	providerMessageID := ""
	switch {
	case message.MessageID > 0:
		providerMessageID = strconv.Itoa(message.MessageID)
	case message.EphemeralMessageID != nil && *message.EphemeralMessageID > 0 && message.ReceiverUser != nil && message.ReceiverUser.ID == event.CallbackQuery.From.Id:
		providerMessageID = ephemeralProviderMessageID(message.ReceiverUser.ID, *message.EphemeralMessageID)
	default:
		return CallbackContext{}, false
	}
	topicID := telegramTopicID(strings.ToLower(strings.TrimSpace(message.Chat.Type)), message.MessageThreadID, message.IsTopicMessage)
	return CallbackContext{
		Locator:           NewLocator(message.Chat.ID, topicID),
		CallbackQueryID:   strings.TrimSpace(event.CallbackQuery.Id),
		QuestionID:        questionID,
		OptionIndex:       optionIndex,
		ProviderMessageID: providerMessageID,
		UserID:            event.CallbackQuery.From.Id,
	}, true
}

func ephemeralProviderMessageID(receiverUserID int64, ephemeralMessageID int) string {
	return fmt.Sprintf("%s%d:%d", ephemeralProviderMessagePrefix, receiverUserID, ephemeralMessageID)
}

func parseEphemeralProviderMessageID(value string) (int64, int, bool) {
	trimmed := strings.TrimSpace(value)
	if !strings.HasPrefix(trimmed, ephemeralProviderMessagePrefix) {
		return 0, 0, false
	}
	userPart, messagePart, ok := strings.Cut(strings.TrimPrefix(trimmed, ephemeralProviderMessagePrefix), ":")
	if !ok {
		return 0, 0, false
	}
	userID, userErr := strconv.ParseInt(userPart, 10, 64)
	messageID, messageErr := strconv.Atoi(messagePart)
	return userID, messageID, userErr == nil && messageErr == nil && userID > 0 && messageID > 0
}

func parseQuestionCallback(data string) (string, int, bool) {
	if !strings.HasPrefix(data, QuestionCallbackPrefix) {
		return "", 0, false
	}
	remainder := strings.TrimPrefix(data, QuestionCallbackPrefix)
	separator := strings.LastIndexByte(remainder, ':')
	if separator <= 0 || separator == len(remainder)-1 {
		return "", 0, false
	}
	index, err := strconv.Atoi(remainder[separator+1:])
	if err != nil || index <= 0 {
		return "", 0, false
	}
	return strings.TrimSpace(remainder[:separator]), index, strings.TrimSpace(remainder[:separator]) != ""
}

// AnswerQuestionCallback acknowledges a callback selection in Telegram.
func (a *Adapter) AnswerQuestionCallback(ctx context.Context, callbackQueryID, text string, showAlert bool) error {
	if a == nil || a.messenger == nil {
		return fmt.Errorf("telegram messenger is required")
	}
	return a.messenger.AnswerCallbackQuery(ctx, callbackQueryID, text, showAlert)
}
