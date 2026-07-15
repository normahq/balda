package handlers

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/baldaworks/go-actorlayer"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	"github.com/normahq/balda/internal/apps/balda/questioncmd"
	"github.com/normahq/balda/internal/apps/balda/questions"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/rs/zerolog"
	"github.com/tgbotkit/client"
	"github.com/tgbotkit/runtime/events"
)

type fakeQuestionStore struct {
	record baldastate.QuestionRecord
}

func TestNewBaldaHandlerInitializesClock(t *testing.T) {
	handler, err := newBaldaHandler(baldaHandlerDeps{})
	if err != nil {
		t.Fatalf("newBaldaHandler() error = %v", err)
	}
	if handler.now == nil {
		t.Fatal("newBaldaHandler() clock is nil")
	}
}

func (f *fakeQuestionStore) CreatePendingQuestion(context.Context, baldastate.QuestionRecord) error {
	return nil
}
func (f *fakeQuestionStore) BindQuestionDeliveryRef(context.Context, string, questioncmd.DeliveryRef) error {
	return nil
}
func (f *fakeQuestionStore) GetQuestionByID(context.Context, string) (baldastate.QuestionRecord, bool, error) {
	return f.record, true, nil
}
func (f *fakeQuestionStore) GetPendingQuestionByReplyRef(_ context.Context, provider, conversationKey, replyToMessageID string) (baldastate.QuestionRecord, bool, error) {
	if provider == f.record.Provider && conversationKey == f.record.AddressKey && replyToMessageID == f.record.ProviderMessageID {
		return f.record, true, nil
	}
	return baldastate.QuestionRecord{}, false, nil
}
func (f *fakeQuestionStore) MarkQuestionAnswered(_ context.Context, questionID string, answer questioncmd.Answer) (baldastate.QuestionRecord, bool, error) {
	if questionID != f.record.QuestionID {
		return baldastate.QuestionRecord{}, false, nil
	}
	f.record.Status = questioncmd.StatusAnswered
	encoded, err := json.Marshal(answer)
	if err != nil {
		return baldastate.QuestionRecord{}, false, err
	}
	f.record.AnswerJSON = string(encoded)
	return f.record, true, nil
}
func (f *fakeQuestionStore) MarkQuestionTimedOut(context.Context, string, time.Time) (baldastate.QuestionRecord, bool, error) {
	return baldastate.QuestionRecord{}, false, nil
}

func TestHandleQuestionReplyEnqueuesContinuationTurn(t *testing.T) {
	store := &fakeQuestionStore{
		record: baldastate.QuestionRecord{
			QuestionID:        "question-1",
			SessionID:         "tg-1-0",
			AddressKey:        "1:0",
			Provider:          "telegram",
			ConversationKey:   "1:0",
			ProviderMessageID: "42",
			Status:            questioncmd.StatusPending,
			RequestJSON:       `{"options":[{"id":"opt-1","label":"Allow once"},{"id":"opt-2","label":"Allow"},{"id":"opt-3","label":"Cancel"}]}`,
			InteractionJSON:   `{"session_id":"tg-1-0","channel_kind":"telegram","locator":{"session_id":"tg-1-0","channel_type":"telegram","address_key":"1:0","address_json":"{\"chat_id\":1,\"topic_id\":0}"}}`,
			ResumeJSON:        `{"to":"session:tg-1-0"}`,
		},
	}
	dispatcher := &fakeTurnDispatcher{}
	handler := &BaldaHandler{
		actorDispatcher: dispatcher,
		questionService: questions.New(store, nil, zerolog.Nop()),
		now:             func() time.Time { return time.Date(2026, 7, 14, 6, 0, 0, 0, time.UTC) },
	}
	handled, err := handler.handleQuestionReply(context.Background(), baldatelegram.MessageContext{
		Locator:          baldasession.SessionLocator{SessionID: "tg-1-0", ChannelType: "telegram", AddressKey: "1:0", AddressJSON: `{"chat_id":1,"topic_id":0}`},
		TopicID:          0,
		MessageID:        43,
		ReplyToMessageID: 42,
		UserID:           101,
		Text:             "2",
		ReplyContent:     "Permission required\n1. Allow once\n2. Allow\n3. Cancel",
	})
	if err != nil {
		t.Fatalf("handleQuestionReply() error = %v", err)
	}
	if !handled {
		t.Fatal("handleQuestionReply() handled = false, want true")
	}
	if len(dispatcher.commands) != 1 {
		t.Fatalf("dispatched commands = %d, want 1", len(dispatcher.commands))
	}
	var payload questioncmd.AnsweredContinuation
	if err := actorlayer.UnmarshalPayload(dispatcher.commands[0].Payload, &payload); err != nil {
		t.Fatalf("decode dispatched payload: %v", err)
	}
	if payload.QuestionID != "question-1" {
		t.Fatalf("question_id = %q, want question-1", payload.QuestionID)
	}
	if payload.Answer.Text != "2" || payload.Answer.SelectedOption != "opt-2" {
		t.Fatalf("answer = %+v, want raw reply and opt-2", payload.Answer)
	}
}

type callbackMessenger struct {
	baldatelegram.TelegramMessenger
	answers []string
	alerts  []bool
}

func (m *callbackMessenger) AnswerCallbackQuery(_ context.Context, _ string, text string, showAlert bool) error {
	m.answers = append(m.answers, text)
	m.alerts = append(m.alerts, showAlert)
	return nil
}

func TestHandleQuestionCallbackSettlesAndDispatchesContinuation(t *testing.T) {
	store := &fakeQuestionStore{record: callbackQuestionRecord(questioncmd.StatusPending)}
	dispatcher := &fakeTurnDispatcher{}
	messenger := &callbackMessenger{}
	channel := baldatelegram.NewAdapter(baldatelegram.AdapterParams{Messenger: messenger, Logger: zerolog.Nop()})
	handler := &BaldaHandler{
		channel:         channel,
		actorDispatcher: dispatcher,
		questionService: questions.New(store, nil, zerolog.Nop()),
		now:             func() time.Time { return time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC) },
	}

	if err := handler.onQuestionCallback(context.Background(), questionCallbackEvent("balda:q:question-1:2")); err != nil {
		t.Fatalf("onQuestionCallback() error = %v", err)
	}
	if len(messenger.answers) != 1 || messenger.answers[0] != "Selected." || messenger.alerts[0] {
		t.Fatalf("callback answers = %v alerts = %v", messenger.answers, messenger.alerts)
	}
	if len(dispatcher.commands) != 1 {
		t.Fatalf("dispatched commands = %d, want 1", len(dispatcher.commands))
	}
	var payload questioncmd.AnsweredContinuation
	if err := actorlayer.UnmarshalPayload(dispatcher.commands[0].Payload, &payload); err != nil {
		t.Fatalf("decode continuation: %v", err)
	}
	if payload.Answer.SelectedOption != "cancel" {
		t.Fatalf("selected option = %q, want cancel", payload.Answer.SelectedOption)
	}
}

func TestHandleQuestionCallbackAcknowledgesStaleSelection(t *testing.T) {
	store := &fakeQuestionStore{record: callbackQuestionRecord(questioncmd.StatusAnswered)}
	dispatcher := &fakeTurnDispatcher{}
	messenger := &callbackMessenger{}
	handler := &BaldaHandler{
		channel:         baldatelegram.NewAdapter(baldatelegram.AdapterParams{Messenger: messenger, Logger: zerolog.Nop()}),
		actorDispatcher: dispatcher,
		questionService: questions.New(store, nil, zerolog.Nop()),
	}

	if err := handler.onQuestionCallback(context.Background(), questionCallbackEvent("balda:q:question-1:1")); err != nil {
		t.Fatalf("onQuestionCallback() error = %v", err)
	}
	if len(messenger.answers) != 1 || messenger.answers[0] != "This request has expired." {
		t.Fatalf("callback answers = %v", messenger.answers)
	}
	if len(dispatcher.commands) != 0 {
		t.Fatalf("dispatched commands = %d, want 0", len(dispatcher.commands))
	}
}

func callbackQuestionRecord(status string) baldastate.QuestionRecord {
	return baldastate.QuestionRecord{
		QuestionID:        "question-1",
		SessionID:         "tg-1-0",
		AddressKey:        "1:0",
		Provider:          "telegram",
		ConversationKey:   "1:0",
		ProviderMessageID: "42",
		Status:            status,
		RequestJSON:       `{"options":[{"id":"allow","label":"Allow"},{"id":"cancel","label":"Cancel"}],"responder":"requester"}`,
		InteractionJSON:   `{"session_id":"tg-1-0","requested_by":{"user_id":"tg-101"},"locator":{"session_id":"tg-1-0","channel_type":"telegram","address_key":"1:0","address_json":"{\"chat_id\":1,\"topic_id\":0}"}}`,
		ResumeJSON:        `{"to":"session:tg-1-0"}`,
	}
}

func questionCallbackEvent(data string) *events.CallbackQueryEvent {
	message := client.MaybeInaccessibleMessage{
		"message_id": 42,
		"chat":       map[string]any{"id": int64(1), "type": "private"},
	}
	return &events.CallbackQueryEvent{CallbackQuery: &client.CallbackQuery{
		Id: "callback-1", Data: &data, From: client.User{Id: 101}, Message: &message,
	}}
}
