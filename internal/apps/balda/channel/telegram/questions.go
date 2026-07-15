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
		MessageID       int `json:"message_id"`
		MessageThreadID int `json:"message_thread_id,omitempty"`
		Chat            struct {
			ID   int64  `json:"id"`
			Type string `json:"type"`
		} `json:"chat"`
	}
	raw, err := json.Marshal(event.CallbackQuery.Message)
	if err != nil || json.Unmarshal(raw, &message) != nil || message.MessageID <= 0 || message.Chat.ID == 0 {
		return CallbackContext{}, false
	}
	topicID := message.MessageThreadID
	if strings.EqualFold(strings.TrimSpace(message.Chat.Type), chatTypePrivate) {
		topicID = 0
	}
	return CallbackContext{
		Locator:           NewLocator(message.Chat.ID, topicID),
		CallbackQueryID:   strings.TrimSpace(event.CallbackQuery.Id),
		QuestionID:        questionID,
		OptionIndex:       optionIndex,
		ProviderMessageID: strconv.Itoa(message.MessageID),
		UserID:            event.CallbackQuery.From.Id,
	}, true
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
