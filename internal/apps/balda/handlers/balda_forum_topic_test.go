package handlers

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/normahq/balda/internal/apps/balda/actors"
	"github.com/normahq/balda/internal/apps/balda/auth"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	"github.com/normahq/balda/internal/apps/balda/messenger"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/tgbotkit"
	"github.com/normahq/balda/pkg/actorlayer"
	"github.com/rs/zerolog"
	"github.com/tgbotkit/client"
	"github.com/tgbotkit/runtime/eventemitter"
	"github.com/tgbotkit/runtime/events"
	rtHandlers "github.com/tgbotkit/runtime/handlers"
	"github.com/tgbotkit/runtime/messagetype"
)

var _ tgbotkit.Registry = (*fakeBaldaRegistry)(nil)

type fakeBaldaRegistry struct {
	onMessageCalls   int
	messageTypeCalls []messagetype.MessageType
}

func (f *fakeBaldaRegistry) OnUpdate(rtHandlers.UpdateHandler) eventemitter.UnsubscribeFunc {
	return func() {}
}

func (f *fakeBaldaRegistry) OnMessage(rtHandlers.MessageHandler) eventemitter.UnsubscribeFunc {
	f.onMessageCalls++
	return func() {}
}

func (f *fakeBaldaRegistry) OnMessageType(t messagetype.MessageType, _ rtHandlers.MessageHandler) eventemitter.UnsubscribeFunc {
	f.messageTypeCalls = append(f.messageTypeCalls, t)
	return func() {}
}

func (f *fakeBaldaRegistry) OnCommand(rtHandlers.CommandHandler) eventemitter.UnsubscribeFunc {
	return func() {}
}

func TestBaldaHandlerRegister_RegistersForumTopicMessageTypes(t *testing.T) {
	registry := &fakeBaldaRegistry{}
	handler := &BaldaHandler{logger: zerolog.Nop(), channel: newBaldaTestTelegramAdapter()}

	handler.Register(registry)

	if registry.onMessageCalls != 1 {
		t.Fatalf("OnMessage calls = %d, want 1", registry.onMessageCalls)
	}

	want := []messagetype.MessageType{
		messagetype.ForumTopicCreated,
		messagetype.ForumTopicEdited,
		messagetype.ForumTopicClosed,
		messagetype.ForumTopicReopened,
	}
	if len(registry.messageTypeCalls) != len(want) {
		t.Fatalf("OnMessageType calls = %d, want %d", len(registry.messageTypeCalls), len(want))
	}
	for i := range want {
		if registry.messageTypeCalls[i] != want[i] {
			t.Fatalf("OnMessageType[%d] = %q, want %q", i, registry.messageTypeCalls[i], want[i])
		}
	}
}

func TestBaldaHandlerOnForumTopicLifecycle_NonClosingEventsDoNotStopSession(t *testing.T) {
	handler := &BaldaHandler{logger: zerolog.Nop(), channel: newBaldaTestTelegramAdapter()}

	tests := []messagetype.MessageType{
		messagetype.ForumTopicCreated,
		messagetype.ForumTopicEdited,
		messagetype.ForumTopicReopened,
	}

	for _, messageType := range tests {
		t.Run(string(messageType), func(t *testing.T) {
			topicID := 77
			userID := int64(101)
			event := &events.MessageEvent{
				Type: messageType,
				Message: &client.Message{
					MessageId:       42,
					MessageThreadId: &topicID,
					Chat: client.Chat{
						Id:   9001,
						Type: "supergroup",
					},
					From: &client.User{Id: userID},
				},
			}

			if err := handler.onForumTopicLifecycle(context.Background(), event); err != nil {
				t.Fatalf("onForumTopicLifecycle() error = %v", err)
			}
		})
	}
}

func TestBaldaHandlerOnForumTopicLifecycle_ClosedStopsTopicSession(t *testing.T) {
	topicID := 77
	locator := baldatelegram.NewLocator(9001, topicID)
	sessionManager := newBaldaSessionManagerWithSession(t, locator, newBaldaTopicSession(t, locator.SessionID))
	turnDispatcher := &fakeTurnDispatcher{}
	handler := &BaldaHandler{
		logger:          zerolog.Nop(),
		channel:         newBaldaTestTelegramAdapter(),
		sessionManager:  sessionManager,
		turnDispatcher:  turnDispatcher,
		actorDispatcher: turnDispatcher,
	}
	handler.setChatID(9001)

	event := &events.MessageEvent{
		Type: messagetype.ForumTopicClosed,
		Message: &client.Message{
			MessageId:       42,
			MessageThreadId: &topicID,
			Chat: client.Chat{
				Id:   9001,
				Type: "supergroup",
			},
			From: &client.User{Id: 101},
		},
	}

	if err := handler.onForumTopicLifecycle(context.Background(), event); err != nil {
		t.Fatalf("onForumTopicLifecycle() error = %v", err)
	}

	if len(turnDispatcher.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0 before control actor runs", len(turnDispatcher.cancelCalls))
	}
	if len(turnDispatcher.commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(turnDispatcher.commands))
	}
	if turnDispatcher.commands[0].Namespace != baldaexecution.NamespaceJobControl || turnDispatcher.commands[0].Kind != baldaexecution.KindCancel {
		t.Fatalf("published command = %+v, want job control cancel", turnDispatcher.commands[0])
	}
	if _, err := sessionManager.GetSession(locator); err == nil {
		t.Fatal("GetSession() error = nil, want stopped session")
	}
}

func TestBaldaHandlerOnForumTopicLifecycle_IgnoresOtherChatWhenBound(t *testing.T) {
	handler := &BaldaHandler{logger: zerolog.Nop(), channel: newBaldaTestTelegramAdapter()}
	handler.setChatID(9001)

	topicID := 13
	event := &events.MessageEvent{
		Type: messagetype.ForumTopicClosed,
		Message: &client.Message{
			MessageId:       55,
			MessageThreadId: &topicID,
			Chat: client.Chat{
				Id:   9999,
				Type: "supergroup",
			},
		},
	}

	if err := handler.onForumTopicLifecycle(context.Background(), event); err != nil {
		t.Fatalf("onForumTopicLifecycle() error = %v", err)
	}

	if got := handler.getChatID(); got != 9001 {
		t.Fatalf("chatID = %d, want 9001", got)
	}
}

func TestBaldaHandlerOnForumTopicLifecycle_IgnoresEventWithoutTopicID(t *testing.T) {
	handler := &BaldaHandler{logger: zerolog.Nop(), channel: newBaldaTestTelegramAdapter()}

	event := &events.MessageEvent{
		Type: messagetype.ForumTopicClosed,
		Message: &client.Message{
			MessageId: 66,
			Chat: client.Chat{
				Id:   9001,
				Type: "supergroup",
			},
		},
	}

	if err := handler.onForumTopicLifecycle(context.Background(), event); err != nil {
		t.Fatalf("onForumTopicLifecycle() error = %v", err)
	}
}

func TestBaldaHandlerOnMessage_IgnoresNilFrom(t *testing.T) {
	handler := &BaldaHandler{logger: zerolog.Nop(), channel: newBaldaTestTelegramAdapter()}
	handler.setOwner(101, 9001)

	text := "hello"
	event := &events.MessageEvent{
		Type: messagetype.Text,
		Message: &client.Message{
			Chat: client.Chat{
				Id:   9001,
				Type: "private",
			},
			Text: &text,
			From: nil,
		},
	}

	if err := handler.onMessage(context.Background(), event); err != nil {
		t.Fatalf("onMessage() error = %v", err)
	}
}

func TestBaldaHandlerOnMessage_ChannelIgnoresNonMention(t *testing.T) {
	handler, turns, _ := newBaldaMessageHandlerHarness(t, 0)

	text := "hello world"
	event := &events.MessageEvent{
		Type: messagetype.Text,
		Message: &client.Message{
			Chat: client.Chat{
				Id:   9001,
				Type: "supergroup",
			},
			Text: &text,
			From: &client.User{Id: 101},
		},
	}

	if err := handler.onMessage(context.Background(), event); err != nil {
		t.Fatalf("onMessage() error = %v", err)
	}

	if len(turns.commands) != 0 {
		t.Fatalf("published commands = %d, want 0", len(turns.commands))
	}
}

func TestBaldaHandlerOnMessage_ChannelMentionBypassesGate(t *testing.T) {
	handler, turns, locator := newBaldaMessageHandlerHarness(t, 0)

	text := "@testbot hello world"
	entities := []client.MessageEntity{{Type: "mention", Offset: 0, Length: len("@testbot")}}
	event := &events.MessageEvent{
		Type: messagetype.Text,
		Message: &client.Message{
			Chat: client.Chat{
				Id:   9001,
				Type: "supergroup",
			},
			Text:     &text,
			Entities: &entities,
			From:     &client.User{Id: 101},
		},
	}

	if err := handler.onMessage(context.Background(), event); err != nil {
		t.Fatalf("onMessage() error = %v", err)
	}

	if len(turns.commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(turns.commands))
	}
	if turns.commands[0].SessionID != locator.SessionID {
		t.Fatalf("command session = %q, want %q", turns.commands[0].SessionID, locator.SessionID)
	}
}

func TestBaldaHandlerOnMessage_DMNonMentionAllowed(t *testing.T) {
	handler, turns, locator := newBaldaMessageHandlerHarness(t, 0)

	text := "hello from dm"
	event := &events.MessageEvent{
		Type: messagetype.Text,
		Message: &client.Message{
			Chat: client.Chat{
				Id:   9001,
				Type: "private",
			},
			Text: &text,
			From: &client.User{Id: 101},
		},
	}

	if err := handler.onMessage(context.Background(), event); err != nil {
		t.Fatalf("onMessage() error = %v", err)
	}

	if len(turns.commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(turns.commands))
	}
	if turns.commands[0].SessionID != locator.SessionID {
		t.Fatalf("command session = %q, want %q", turns.commands[0].SessionID, locator.SessionID)
	}
}

func TestBaldaHandlerOnMessage_TopicUnknownThreadIgnoresNonMentionNonReply(t *testing.T) {
	handler, turns, _ := newBaldaMessageHandlerHarness(t, 77)

	text := "hello from the topic"
	topicID := 99
	event := &events.MessageEvent{
		Type: messagetype.Text,
		Message: &client.Message{
			Chat: client.Chat{
				Id:   9001,
				Type: "supergroup",
			},
			MessageThreadId: &topicID,
			Text:            &text,
			From:            &client.User{Id: 101},
		},
	}

	if err := handler.onMessage(context.Background(), event); err != nil {
		t.Fatalf("onMessage() error = %v", err)
	}

	if len(turns.commands) != 0 {
		t.Fatalf("published commands = %d, want 0", len(turns.commands))
	}
}

func TestBaldaHandlerOnMessage_TopicKnownThreadStillRequiresMentionOrReply(t *testing.T) {
	handler, turns, _ := newBaldaMessageHandlerHarness(t, 77)

	text := "hello from the topic"
	topicID := 77
	event := &events.MessageEvent{
		Type: messagetype.Text,
		Message: &client.Message{
			Chat: client.Chat{
				Id:   9001,
				Type: "supergroup",
			},
			MessageThreadId: &topicID,
			Text:            &text,
			From:            &client.User{Id: 101},
		},
	}

	if err := handler.onMessage(context.Background(), event); err != nil {
		t.Fatalf("onMessage() error = %v", err)
	}

	if len(turns.commands) != 0 {
		t.Fatalf("published commands = %d, want 0", len(turns.commands))
	}
}

func TestBaldaHandlerOnMessage_RejectsFalsePositiveBotMentionPrefix(t *testing.T) {
	handler, turns, _ := newBaldaMessageHandlerHarness(t, 0)

	text := "@testbotx please ignore this"
	event := &events.MessageEvent{
		Type: messagetype.Text,
		Message: &client.Message{
			Chat: client.Chat{
				Id:   9001,
				Type: "supergroup",
			},
			Text: &text,
			From: &client.User{Id: 101},
		},
	}

	if err := handler.onMessage(context.Background(), event); err != nil {
		t.Fatalf("onMessage() error = %v", err)
	}

	if len(turns.commands) != 0 {
		t.Fatalf("published commands = %d, want 0", len(turns.commands))
	}
}

func TestBaldaHandlerOnMessage_ChannelReplyToBotBypassesMentionGate(t *testing.T) {
	handler, turns, locator := newBaldaMessageHandlerHarness(t, 0)

	text := "following up in channel"
	event := &events.MessageEvent{
		Type: messagetype.Text,
		Message: &client.Message{
			Chat: client.Chat{
				Id:   9001,
				Type: "supergroup",
			},
			Text:           &text,
			From:           &client.User{Id: 101},
			ReplyToMessage: replyToMessageFrom(4242, true),
		},
	}

	if err := handler.onMessage(context.Background(), event); err != nil {
		t.Fatalf("onMessage() error = %v", err)
	}

	if len(turns.commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(turns.commands))
	}
	if turns.commands[0].SessionID != locator.SessionID {
		t.Fatalf("command session = %q, want %q", turns.commands[0].SessionID, locator.SessionID)
	}
}

func TestBaldaHandlerOnMessage_ReplyAddsReplyContextToPublishedCommand(t *testing.T) {
	tests := []struct {
		name          string
		chatType      string
		replyToUserID int64
		replyToIsBot  bool
	}{
		{
			name:          "channel reply to bot",
			chatType:      "supergroup",
			replyToUserID: 4242,
			replyToIsBot:  true,
		},
		{
			name:          "dm reply to non-bot",
			chatType:      "private",
			replyToUserID: 777,
			replyToIsBot:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			handler, turns, _ := newBaldaMessageHandlerHarness(t, 0)

			text := "сделай пр"
			replyText := "проверь этот коммит"
			event := &events.MessageEvent{
				Type: messagetype.Text,
				Message: &client.Message{
					Chat: client.Chat{
						Id:   9001,
						Type: tc.chatType,
					},
					Text:           &text,
					From:           &client.User{Id: 101},
					ReplyToMessage: replyToMessageWithTextFrom(tc.replyToUserID, tc.replyToIsBot, replyText),
				},
			}

			if err := handler.onMessage(context.Background(), event); err != nil {
				t.Fatalf("onMessage() error = %v", err)
			}
			assertPublishedTurnIncludesReplyContext(t, turns.commands, text, replyText)
		})
	}
}

func TestBaldaHandlerOnMessage_TopicReplyToBotBypassesMentionGate(t *testing.T) {
	handler, turns, locator := newBaldaMessageHandlerHarness(t, 77)

	text := "topic follow up"
	topicID := 77
	event := &events.MessageEvent{
		Type: messagetype.Text,
		Message: &client.Message{
			Chat: client.Chat{
				Id:   9001,
				Type: "supergroup",
			},
			MessageThreadId: &topicID,
			Text:            &text,
			From:            &client.User{Id: 101},
			ReplyToMessage:  replyToMessageFrom(4242, true),
		},
	}

	if err := handler.onMessage(context.Background(), event); err != nil {
		t.Fatalf("onMessage() error = %v", err)
	}

	if len(turns.commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(turns.commands))
	}
	if turns.commands[0].SessionID != locator.SessionID {
		t.Fatalf("command session = %q, want %q", turns.commands[0].SessionID, locator.SessionID)
	}
}

func TestBaldaHandlerOnMessage_PublishesDirectSessionTurn(t *testing.T) {
	handler, turns, locator := newBaldaMessageHandlerHarness(t, 0)

	text := "run tests"
	event := &events.MessageEvent{
		Type: messagetype.Text,
		Message: &client.Message{
			Chat: client.Chat{
				Id:   9001,
				Type: "private",
			},
			Text: &text,
			From: &client.User{Id: 101},
		},
	}

	if err := handler.onMessage(context.Background(), event); err != nil {
		t.Fatalf("onMessage() error = %v", err)
	}
	if len(turns.commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(turns.commands))
	}
	if turns.commands[0].To.Target != baldaexecution.ActorTypeSession {
		t.Fatalf("command target = %q, want %q", turns.commands[0].To.Target, baldaexecution.ActorTypeSession)
	}
	if turns.commands[0].SessionID != locator.SessionID {
		t.Fatalf("command session = %q, want %q", turns.commands[0].SessionID, locator.SessionID)
	}
	var payload actors.SessionTurnPayload
	if err := json.Unmarshal([]byte(turns.commands[0].PayloadJSON), &payload); err != nil {
		t.Fatalf("decode session turn payload: %v", err)
	}
	if payload.Source != "telegram" || !payload.Deliver {
		t.Fatalf("session turn payload = %+v, want telegram deliver=true", payload)
	}
}

func TestBaldaHandlerOnMessage_ChannelReplyToDifferentBotIgnored(t *testing.T) {
	handler, turns, _ := newBaldaMessageHandlerHarness(t, 0)

	text := "following up in channel"
	event := &events.MessageEvent{
		Type: messagetype.Text,
		Message: &client.Message{
			Chat: client.Chat{
				Id:   9001,
				Type: "supergroup",
			},
			Text:           &text,
			From:           &client.User{Id: 101},
			ReplyToMessage: replyToMessageFrom(9898, true),
		},
	}

	if err := handler.onMessage(context.Background(), event); err != nil {
		t.Fatalf("onMessage() error = %v", err)
	}

	if len(turns.commands) != 0 {
		t.Fatalf("published commands = %d, want 0", len(turns.commands))
	}
}

func newBaldaTestTelegramAdapter() *baldatelegram.Adapter {
	tgClient := &fakeTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	return baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
}

func newBaldaMessageHandlerHarness(t *testing.T, topicID int) (*BaldaHandler, *fakeTurnDispatcher, baldasession.SessionLocator) {
	t.Helper()

	stateStore := &fakeOwnerKVStore{}
	ownerStore, err := auth.NewOwnerStore(stateStore)
	if err != nil {
		t.Fatalf("NewOwnerStore(): %v", err)
	}
	if _, err := ownerStore.RegisterOwner(101, 9001); err != nil {
		t.Fatalf("RegisterOwner(): %v", err)
	}

	locator := baldatelegram.NewLocator(9001, topicID)
	sessionManager := newBaldaSessionManagerWithSession(t, locator, newBaldaTopicSession(t, locator.SessionID))
	turnDispatcher := &fakeTurnDispatcher{}
	handler := &BaldaHandler{
		ownerStore:      ownerStore,
		channel:         newBaldaTestTelegramAdapter(),
		sessionManager:  sessionManager,
		turnDispatcher:  turnDispatcher,
		actorDispatcher: turnDispatcher,
		logger:          zerolog.Nop(),
	}
	handler.setOwner(101, 9001)
	setUnexportedField(t, handler, "baldaProviderName", "alpha")
	handler.botUsername = testBaldaBotUsername
	handler.botUserID = 4242

	return handler, turnDispatcher, locator
}

func replyToMessageFrom(userID int64, isBot bool) *client.Message {
	return &client.Message{
		MessageId: 7,
		From: &client.User{
			Id:    userID,
			IsBot: isBot,
		},
	}
}

func replyToMessageWithTextFrom(userID int64, isBot bool, text string) *client.Message {
	msg := replyToMessageFrom(userID, isBot)
	msg.Text = &text
	return msg
}

func assertPublishedTurnIncludesReplyContext(t *testing.T, commands []actorlayer.Envelope, text, replyText string) {
	t.Helper()
	if len(commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(commands))
	}
	var payload actors.SessionTurnPayload
	if err := json.Unmarshal([]byte(commands[0].PayloadJSON), &payload); err != nil || strings.TrimSpace(payload.Text) == "" {
		var wrapped struct {
			Kind        string                     `json:"kind"`
			SessionTurn *actors.SessionTurnPayload `json:"session_turn,omitempty"`
		}
		if wrapErr := json.Unmarshal([]byte(commands[0].PayloadJSON), &wrapped); wrapErr != nil {
			t.Fatalf("decode session turn payload: %v", err)
		}
		if wrapped.SessionTurn == nil {
			t.Fatalf("wrapped session turn payload missing: %+v", wrapped)
		}
		payload = *wrapped.SessionTurn
	}
	if !strings.Contains(payload.Text, "Reply context:\n"+replyText) {
		t.Fatalf("payload text = %q, want reply context block", payload.Text)
	}
	if !strings.Contains(payload.Text, "User message:\n"+text) {
		t.Fatalf("payload text = %q, want user message block", payload.Text)
	}
}

func newBaldaSessionManagerWithSession(t *testing.T, locator baldasession.SessionLocator, ts *baldasession.TopicSession) *baldasession.Manager {
	t.Helper()

	m := &baldasession.Manager{}
	setUnexportedField(t, m, "sessions", map[string]*baldasession.TopicSession{locator.SessionID: ts})
	setUnexportedField(t, m, "sessionStore", &fakeBaldaRestoreSessionStore{})
	return m
}

func newBaldaTopicSession(t *testing.T, sessionID string) *baldasession.TopicSession {
	t.Helper()

	ts := &baldasession.TopicSession{}
	setUnexportedField(t, ts, "sessionID", sessionID)
	return ts
}

func setUnexportedField[T any](t *testing.T, target any, fieldName string, value T) {
	t.Helper()

	rv := reflect.ValueOf(target).Elem().FieldByName(fieldName)
	reflect.NewAt(rv.Type(), unsafe.Pointer(rv.UnsafeAddr())).Elem().Set(reflect.ValueOf(value))
}
