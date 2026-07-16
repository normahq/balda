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

const testQuestionCallbackAllowData = "balda:q:question-1:1"

type recordingQuestionMessenger struct {
	TelegramMessenger
	keyboard                client.InlineKeyboardMarkup
	fallbackText            string
	ephemeralChatID         int64
	ephemeralReceiverUserID int64
	ephemeralTopicID        int
	deletedMessageChatID    int64
	deletedMessageID        int
	deletedChatID           int64
	deletedReceiverUserID   int64
	deletedEphemeralID      int
}

func (m *recordingQuestionMessenger) DeleteMessage(_ context.Context, chatID int64, messageID int) error {
	m.deletedMessageChatID = chatID
	m.deletedMessageID = messageID
	return nil
}

func (m *recordingQuestionMessenger) DeleteEphemeralMessage(_ context.Context, chatID, receiverUserID int64, ephemeralMessageID int) error {
	m.deletedChatID = chatID
	m.deletedReceiverUserID = receiverUserID
	m.deletedEphemeralID = ephemeralMessageID
	return nil
}

func (m *recordingQuestionMessenger) TelegramFormattingMode() string {
	return telegramfmt.ModeRichMarkdown
}

func (m *recordingQuestionMessenger) SendEphemeralAgentReplyWithInlineKeyboardLastMessageIDAndMode(
	_ context.Context,
	chatID, receiverUserID int64,
	_ string,
	topicID int,
	_ string,
	keyboard client.InlineKeyboardMarkup,
) (int, error) {
	m.ephemeralChatID = chatID
	m.ephemeralReceiverUserID = receiverUserID
	m.ephemeralTopicID = topicID
	m.keyboard = keyboard
	return 73, nil
}

func TestSendPrivateGroupQuestionUsesEphemeralDelivery(t *testing.T) {
	messenger := &recordingQuestionMessenger{}
	adapter := NewAdapter(AdapterParams{Messenger: messenger, Logger: zerolog.Nop()})
	messageID, err := adapter.SendAgentReplyWithQuestion(context.Background(), NewLocator(-1001, 77), deliverycmd.Profile{}, "Approve?", &deliverycmd.Question{
		ID:       "question-1",
		Audience: deliverycmd.QuestionAudience{Visibility: deliverycmd.QuestionVisibilityPrivate, UserID: "tg-101"},
		Options:  []deliverycmd.QuestionOption{{ID: "allow", Label: "Allow"}, {ID: "deny", Label: "Deny"}},
	})
	if err != nil {
		t.Fatalf("SendAgentReplyWithQuestion() error = %v", err)
	}
	if messageID != "ephemeral:101:73" {
		t.Fatalf("message id = %q", messageID)
	}
	if messenger.ephemeralChatID != -1001 || messenger.ephemeralReceiverUserID != 101 || messenger.ephemeralTopicID != 77 {
		t.Fatalf("ephemeral target = %d/%d/%d", messenger.ephemeralChatID, messenger.ephemeralReceiverUserID, messenger.ephemeralTopicID)
	}
}

func TestClearPrivateGroupQuestionDeletesEphemeralMessage(t *testing.T) {
	messenger := &recordingQuestionMessenger{}
	adapter := NewAdapter(AdapterParams{Messenger: messenger, Logger: zerolog.Nop()})

	if err := adapter.ClearQuestionControls(context.Background(), NewLocator(-1001, 77), "ephemeral:101:73", ""); err != nil {
		t.Fatalf("ClearQuestionControls() error = %v", err)
	}
	if messenger.deletedChatID != -1001 || messenger.deletedReceiverUserID != 101 || messenger.deletedEphemeralID != 73 {
		t.Fatalf("deleted target = %d/%d/%d", messenger.deletedChatID, messenger.deletedReceiverUserID, messenger.deletedEphemeralID)
	}
}

func TestClearPrivateDMQuestionDeletesMessage(t *testing.T) {
	messenger := &recordingQuestionMessenger{}
	adapter := NewAdapter(AdapterParams{Messenger: messenger, Logger: zerolog.Nop()})

	if err := adapter.ClearQuestionControls(context.Background(), NewLocator(101, 0), "42", telegramQuestionControlHandleDeleteMessage); err != nil {
		t.Fatalf("ClearQuestionControls() error = %v", err)
	}
	if messenger.deletedMessageChatID != 101 || messenger.deletedMessageID != 42 {
		t.Fatalf("deleted message = %d/%d", messenger.deletedMessageChatID, messenger.deletedMessageID)
	}
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
	isTopicMessage := false
	message := client.MaybeInaccessibleMessage{
		"message_id":        42,
		"message_thread_id": 77,
		"is_topic_message":  isTopicMessage,
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

func TestCallbackContextFromPrivateTopicPreservesMessageThreadID(t *testing.T) {
	data := testQuestionCallbackAllowData
	message := client.MaybeInaccessibleMessage{
		"message_id":        42,
		"message_thread_id": 523431,
		"is_topic_message":  true,
		"chat":              map[string]any{"id": int64(101), "type": "private"},
	}
	got, ok := (&Adapter{}).CallbackContextFromEvent(&events.CallbackQueryEvent{CallbackQuery: &client.CallbackQuery{
		Id: "callback-1", Data: &data, From: client.User{Id: 101}, Message: &message,
	}})
	if !ok {
		t.Fatal("CallbackContextFromEvent() ok = false")
	}
	if got.Locator != NewLocator(101, 523431) {
		t.Fatalf("locator = %+v", got.Locator)
	}
}

func TestCallbackContextFromPrivateNonTopicIgnoresMessageThreadID(t *testing.T) {
	data := testQuestionCallbackAllowData
	message := client.MaybeInaccessibleMessage{
		"message_id":        42,
		"message_thread_id": 523431,
		"is_topic_message":  false,
		"chat":              map[string]any{"id": int64(101), "type": "private"},
	}
	got, ok := (&Adapter{}).CallbackContextFromEvent(&events.CallbackQueryEvent{CallbackQuery: &client.CallbackQuery{
		Id: "callback-1", Data: &data, From: client.User{Id: 101}, Message: &message,
	}})
	if !ok {
		t.Fatal("CallbackContextFromEvent() ok = false")
	}
	if got.Locator != NewLocator(101, 0) {
		t.Fatalf("locator = %+v", got.Locator)
	}
}

func TestCallbackContextFromPrivateTopicUsesThreadWhenTopicFlagOmitted(t *testing.T) {
	data := testQuestionCallbackAllowData
	message := client.MaybeInaccessibleMessage{
		"message_id":        42,
		"message_thread_id": 523431,
		"chat":              map[string]any{"id": int64(101), "type": "private"},
	}
	got, ok := (&Adapter{}).CallbackContextFromEvent(&events.CallbackQueryEvent{CallbackQuery: &client.CallbackQuery{
		Id: "callback-1", Data: &data, From: client.User{Id: 101}, Message: &message,
	}})
	if !ok {
		t.Fatal("CallbackContextFromEvent() ok = false")
	}
	if got.Locator != NewLocator(101, 523431) {
		t.Fatalf("locator = %+v", got.Locator)
	}
}

func TestCallbackContextFromEphemeralMessageRequiresReceiver(t *testing.T) {
	data := testQuestionCallbackAllowData
	message := client.MaybeInaccessibleMessage{
		"message_id":           0,
		"ephemeral_message_id": 73,
		"receiver_user":        map[string]any{"id": int64(101)},
		"chat":                 map[string]any{"id": int64(-1001), "type": "supergroup"},
	}
	event := &events.CallbackQueryEvent{CallbackQuery: &client.CallbackQuery{Id: "callback-1", Data: &data, From: client.User{Id: 101}, Message: &message}}
	got, ok := (&Adapter{}).CallbackContextFromEvent(event)
	if !ok || got.ProviderMessageID != "ephemeral:101:73" {
		t.Fatalf("callback = %+v ok=%v", got, ok)
	}
	event.CallbackQuery.From.Id = 202
	if _, ok := (&Adapter{}).CallbackContextFromEvent(event); ok {
		t.Fatal("callback from non-receiver accepted")
	}
}

func TestParseQuestionCallbackRejectsMalformedPayload(t *testing.T) {
	for _, data := range []string{"other:question-1:1", "balda:q::1", "balda:q:question-1:0", "balda:q:question-1:x"} {
		if _, _, ok := parseQuestionCallback(data); ok {
			t.Fatalf("parseQuestionCallback(%q) ok = true", data)
		}
	}
}
