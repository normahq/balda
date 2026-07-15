package telegram

import (
	"context"
	"testing"

	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/telegramfmt"
	"github.com/rs/zerolog"
	"github.com/tgbotkit/client"
	"github.com/tgbotkit/runtime/events"
)

type recordingQuestionMessenger struct {
	TelegramMessenger
	keyboard     client.InlineKeyboardMarkup
	fallbackText string
}

func (m *recordingQuestionMessenger) TelegramFormattingMode() string {
	return telegramfmt.ModeRichMarkdown
}

func (m *recordingQuestionMessenger) SendAgentReplyWithInlineKeyboardLastMessageIDAndMode(
	_ context.Context,
	_ int64,
	_ string,
	_ int,
	_ string,
	keyboard client.InlineKeyboardMarkup,
	fallbackText string,
) (int, error) {
	m.keyboard = keyboard
	m.fallbackText = fallbackText
	return 42, nil
}

func TestSendAgentReplyWithQuestionBuildsOneButtonPerRow(t *testing.T) {
	messenger := &recordingQuestionMessenger{}
	adapter := NewAdapter(AdapterParams{Messenger: messenger, Logger: zerolog.Nop()})
	messageID, err := adapter.SendAgentReplyWithQuestion(context.Background(), NewLocator(1, 0), deliverycmd.Profile{}, "Choose", &deliverycmd.Question{
		ID: "question-1",
		Options: []deliverycmd.QuestionOption{
			{ID: "allow", Label: "Allow"},
			{ID: "cancel", Label: "Cancel"},
		},
	})
	if err != nil {
		t.Fatalf("SendAgentReplyWithQuestion() error = %v", err)
	}
	if messageID != "42" {
		t.Fatalf("message id = %q, want 42", messageID)
	}
	if len(messenger.keyboard.InlineKeyboard) != 2 || len(messenger.keyboard.InlineKeyboard[0]) != 1 || len(messenger.keyboard.InlineKeyboard[1]) != 1 {
		t.Fatalf("keyboard = %+v, want one button per row", messenger.keyboard)
	}
	if got := *messenger.keyboard.InlineKeyboard[1][0].CallbackData; got != "balda:q:question-1:2" {
		t.Fatalf("callback data = %q", got)
	}
	if messenger.fallbackText != "Choose\n\nChoose:\n1. Allow\n2. Cancel\n\nReply with the number or option name." {
		t.Fatalf("fallback = %q", messenger.fallbackText)
	}
}

func TestCallbackContextFromEventNormalizesSelection(t *testing.T) {
	data := "balda:q:question-1:2"
	message := client.MaybeInaccessibleMessage{
		"message_id":        42,
		"message_thread_id": 77,
		"chat": map[string]any{
			"id":   int64(-1001),
			"type": "supergroup",
		},
	}
	got, ok := (&Adapter{}).CallbackContextFromEvent(&events.CallbackQueryEvent{CallbackQuery: &client.CallbackQuery{
		Id:      "callback-1",
		Data:    &data,
		From:    client.User{Id: 101},
		Message: &message,
	}})
	if !ok {
		t.Fatal("CallbackContextFromEvent() ok = false")
	}
	if got.QuestionID != "question-1" || got.OptionIndex != 2 || got.ProviderMessageID != "42" || got.UserID != 101 {
		t.Fatalf("callback = %+v", got)
	}
	if got.Locator != NewLocator(-1001, 77) {
		t.Fatalf("locator = %+v", got.Locator)
	}
}

func TestParseQuestionCallbackRejectsMalformedPayload(t *testing.T) {
	for _, data := range []string{"other:question-1:1", "balda:q::1", "balda:q:question-1:0", "balda:q:question-1:x"} {
		if _, _, ok := parseQuestionCallback(data); ok {
			t.Fatalf("parseQuestionCallback(%q) ok = true", data)
		}
	}
}
