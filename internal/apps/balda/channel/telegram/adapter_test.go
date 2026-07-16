package telegram

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	"github.com/normahq/balda/internal/apps/balda/messenger"
	"github.com/normahq/balda/internal/apps/balda/telegramfmt"
	"github.com/rs/zerolog"
	"github.com/tgbotkit/client"
	"github.com/tgbotkit/runtime/events"
	"github.com/tgbotkit/runtime/messagetype"
)

const testMessageText = "hello"

func TestMessageContextFromEvent_SnapshotsDeliveryOptions(t *testing.T) {
	msg := messenger.NewMessenger(nil, zerolog.Nop())
	msg.SetTelegramFormattingMode(telegramfmt.ModeMarkdownV2)
	adapter := NewAdapter(AdapterParams{Messenger: msg, Logger: zerolog.Nop()})

	got, ok := adapter.MessageContextFromEvent(&events.MessageEvent{
		Message: &client.Message{
			MessageId: 42,
			Chat: client.Chat{
				Id:   9001,
				Type: "private",
			},
			From: &client.User{Id: 101},
			Text: textPtr(testMessageText),
		},
	})
	if !ok {
		t.Fatal("MessageContextFromEvent() ok = false, want true")
	}
	if got.DeliveryOptions.Profile.Format != deliveryfmt.FormatAuto {
		t.Fatalf("delivery profile format = %q, want %q", got.DeliveryOptions.Profile.Format, deliveryfmt.FormatAuto)
	}
	if got.DeliveryOptions.Profile.TelegramMode != telegramfmt.ModeMarkdownV2 {
		t.Fatalf("delivery telegram mode = %q, want %q", got.DeliveryOptions.Profile.TelegramMode, telegramfmt.ModeMarkdownV2)
	}
	if !got.DeliveryOptions.ProgressPolicy.Typing || !got.DeliveryOptions.ProgressPolicy.Thinking {
		t.Fatalf("delivery progress policy = %+v, want typing and thinking", got.DeliveryOptions.ProgressPolicy)
	}
}

func TestMessageContextFromEvent_PrivateChatIgnoresMessageThreadID(t *testing.T) {
	topicID := 523431
	isTopicMessage := false

	got, ok := (&Adapter{}).MessageContextFromEvent(&events.MessageEvent{
		Message: &client.Message{
			MessageId:       11,
			MessageThreadId: &topicID,
			IsTopicMessage:  &isTopicMessage,
			Chat: client.Chat{
				Id:   2317500,
				Type: "private",
			},
			From: &client.User{Id: 2317500},
			Text: textPtr(testMessageText),
		},
	})
	if !ok {
		t.Fatal("MessageContextFromEvent() ok = false, want true")
	}
	assertPrivateMessageContext(t, got, 0, "tg-2317500-0")
}

func TestMessageContextFromEvent_SupergroupPreservesMessageThreadID(t *testing.T) {
	topicID := 77

	got, ok := (&Adapter{}).MessageContextFromEvent(&events.MessageEvent{
		Message: &client.Message{
			MessageId:       21,
			MessageThreadId: &topicID,
			Chat: client.Chat{
				Id:   -1009001,
				Type: "supergroup",
			},
			From: &client.User{Id: 101},
			Text: textPtr(testMessageText),
		},
	})
	if !ok {
		t.Fatal("MessageContextFromEvent() ok = false, want true")
	}
	if got.TopicID != 77 {
		t.Fatalf("topic_id = %d, want 77 for supergroup topic", got.TopicID)
	}
	if got.ProgressPolicy.Thinking {
		t.Fatalf("progress_policy.thinking = %v, want false for supergroup chat", got.ProgressPolicy.Thinking)
	}
	if !got.ProgressPolicy.Typing {
		t.Fatalf("progress_policy.typing = %v, want true for supergroup chat", got.ProgressPolicy.Typing)
	}
	if got.Locator.SessionID != "tg--1009001-77" {
		t.Fatalf("session_id = %q, want tg--1009001-77", got.Locator.SessionID)
	}
}

func TestMessageContextFromEvent_SupergroupPreservesMessageThreadIDWhenNonTopicFlagFalse(t *testing.T) {
	topicID := 88
	isTopicMessage := false

	got, ok := (&Adapter{}).MessageContextFromEvent(&events.MessageEvent{
		Message: &client.Message{
			MessageId:       22,
			MessageThreadId: &topicID,
			IsTopicMessage:  &isTopicMessage,
			Chat: client.Chat{
				Id:   -1009001,
				Type: "supergroup",
			},
			From: &client.User{Id: 101},
			Text: textPtr(testMessageText),
		},
	})
	if !ok {
		t.Fatal("MessageContextFromEvent() ok = false, want true")
	}
	if got.TopicID != 88 {
		t.Fatalf("topic_id = %d, want 88 for supergroup thread", got.TopicID)
	}
	if got.Locator.SessionID != "tg--1009001-88" {
		t.Fatalf("session_id = %q, want tg--1009001-88", got.Locator.SessionID)
	}
}

func TestMessageContextFromEvent_PrivateTopicPreservesMessageThreadID(t *testing.T) {
	topicID := 523431
	isTopicMessage := true

	got, ok := (&Adapter{}).MessageContextFromEvent(&events.MessageEvent{
		Message: &client.Message{
			MessageId:       31,
			MessageThreadId: &topicID,
			IsTopicMessage:  &isTopicMessage,
			Chat: client.Chat{
				Id:   2317500,
				Type: "private",
			},
			From: &client.User{Id: 2317500},
			Text: textPtr(testMessageText),
		},
	})
	if !ok {
		t.Fatal("MessageContextFromEvent() ok = false, want true")
	}
	assertPrivateMessageContext(t, got, 523431, "tg-2317500-523431")
}

func assertPrivateMessageContext(t *testing.T, got MessageContext, wantTopicID int, wantSessionID string) {
	t.Helper()

	if got.TopicID != wantTopicID {
		t.Fatalf("topic_id = %d, want %d", got.TopicID, wantTopicID)
	}
	if !got.ProgressPolicy.Thinking {
		t.Fatalf("progress_policy.thinking = %v, want true", got.ProgressPolicy.Thinking)
	}
	if !got.ProgressPolicy.Typing {
		t.Fatalf("progress_policy.typing = %v, want true", got.ProgressPolicy.Typing)
	}
	if got.Locator.SessionID != wantSessionID {
		t.Fatalf("session_id = %q, want %q", got.Locator.SessionID, wantSessionID)
	}
}

func TestMessageContextFromEvent_PopulatesReplyMetadataWhenPresent(t *testing.T) {
	got, ok := (&Adapter{}).MessageContextFromEvent(&events.MessageEvent{
		Message: &client.Message{
			MessageId: 41,
			Chat: client.Chat{
				Id:   -1009001,
				Type: "supergroup",
			},
			From: &client.User{Id: 101},
			Text: textPtr(testMessageText),
			ReplyToMessage: &client.Message{
				MessageId: 7,
				From: &client.User{
					Id:    404,
					IsBot: true,
				},
			},
		},
	})
	if !ok {
		t.Fatal("MessageContextFromEvent() ok = false, want true")
	}
	if !got.IsReply {
		t.Fatalf("is_reply = %v, want true", got.IsReply)
	}
	if got.ReplyToUserID != 404 {
		t.Fatalf("reply_to_user_id = %d, want 404", got.ReplyToUserID)
	}
	if !got.ReplyToIsBot {
		t.Fatalf("reply_to_is_bot = %v, want true", got.ReplyToIsBot)
	}
	if got.ReplyContent != "" {
		t.Fatalf("reply_content = %q, want empty", got.ReplyContent)
	}
}

func TestMessageContextFromEvent_ReplyMetadataEmptyWithoutReply(t *testing.T) {
	got, ok := (&Adapter{}).MessageContextFromEvent(&events.MessageEvent{
		Message: &client.Message{
			MessageId: 42,
			Chat: client.Chat{
				Id:   -1009001,
				Type: "supergroup",
			},
			From: &client.User{Id: 101},
			Text: textPtr(testMessageText),
		},
	})
	if !ok {
		t.Fatal("MessageContextFromEvent() ok = false, want true")
	}
	if got.IsReply {
		t.Fatalf("is_reply = %v, want false", got.IsReply)
	}
	if got.ReplyToUserID != 0 {
		t.Fatalf("reply_to_user_id = %d, want 0", got.ReplyToUserID)
	}
	if got.ReplyToIsBot {
		t.Fatalf("reply_to_is_bot = %v, want false", got.ReplyToIsBot)
	}
	if got.ReplyContent != "" {
		t.Fatalf("reply_content = %q, want empty", got.ReplyContent)
	}
	if got.IsForwarded {
		t.Fatalf("is_forwarded = %v, want false", got.IsForwarded)
	}
}

func TestMessageContextFromEvent_PopulatesForwardMetadataWhenPresent(t *testing.T) {
	text := "forwarded bot announcement"
	got, ok := (&Adapter{}).MessageContextFromEvent(&events.MessageEvent{
		Message: &client.Message{
			MessageId: 50,
			Chat: client.Chat{
				Id:   -1009001,
				Type: "supergroup",
			},
			From: &client.User{Id: 101},
			Text: &text,
			ForwardOrigin: &client.MessageOrigin{
				"type": "user",
				"user": map[string]interface{}{
					"id":     float64(4242),
					"is_bot": true,
				},
			},
		},
	})
	if !ok {
		t.Fatal("MessageContextFromEvent() ok = false, want true")
	}
	if !got.IsForwarded {
		t.Fatalf("is_forwarded = %v, want true", got.IsForwarded)
	}
	if !got.ForwardedFromBot {
		t.Fatalf("forwarded_from_bot = %v, want true", got.ForwardedFromBot)
	}
	if got.ForwardedContent != text {
		t.Fatalf("forwarded_content = %q, want %q", got.ForwardedContent, text)
	}
}

func TestMessageContextFromEvent_PopulatesReplyContentFromReplyText(t *testing.T) {
	replyText := "quoted message"
	got, ok := (&Adapter{}).MessageContextFromEvent(&events.MessageEvent{
		Message: &client.Message{
			MessageId: 43,
			Chat: client.Chat{
				Id:   -1009001,
				Type: "supergroup",
			},
			From: &client.User{Id: 101},
			Text: textPtr(testMessageText),
			ReplyToMessage: &client.Message{
				MessageId: 8,
				Text:      &replyText,
			},
		},
	})
	if !ok {
		t.Fatal("MessageContextFromEvent() ok = false, want true")
	}
	if got.ReplyContent != replyText {
		t.Fatalf("reply_content = %q, want %q", got.ReplyContent, replyText)
	}
}

func TestMessageContextFromEvent_UsesReplyCaptionWhenReplyTextMissing(t *testing.T) {
	replyCaption := "photo caption"
	got, ok := (&Adapter{}).MessageContextFromEvent(&events.MessageEvent{
		Message: &client.Message{
			MessageId: 44,
			Chat: client.Chat{
				Id:   -1009001,
				Type: "supergroup",
			},
			From: &client.User{Id: 101},
			Text: textPtr(testMessageText),
			ReplyToMessage: &client.Message{
				MessageId: 9,
				Caption:   &replyCaption,
			},
		},
	})
	if !ok {
		t.Fatal("MessageContextFromEvent() ok = false, want true")
	}
	if got.ReplyContent != replyCaption {
		t.Fatalf("reply_content = %q, want %q", got.ReplyContent, replyCaption)
	}
}

func TestMessageContextFromEvent_UsesSelectedQuoteBeforeFullReplyContent(t *testing.T) {
	replyText := "full replied message"
	quote := client.TextQuote{Text: "selected quote", Position: 0}

	got, ok := (&Adapter{}).MessageContextFromEvent(&events.MessageEvent{
		Message: &client.Message{
			MessageId: 45,
			Chat: client.Chat{
				Id:   -1009001,
				Type: "supergroup",
			},
			From:  &client.User{Id: 101},
			Text:  textPtr(testMessageText),
			Quote: &quote,
			ReplyToMessage: &client.Message{
				MessageId: 10,
				Text:      &replyText,
			},
		},
	})
	if !ok {
		t.Fatal("MessageContextFromEvent() ok = false, want true")
	}
	if !got.IsReply {
		t.Fatalf("is_reply = %v, want true for quote", got.IsReply)
	}
	if got.ReplyContent != quote.Text {
		t.Fatalf("reply_content = %q, want quote %q", got.ReplyContent, quote.Text)
	}
}

func TestMessageContextFromEvent_QuoteOnlyMarksReplyAndPopulatesContext(t *testing.T) {
	quote := client.TextQuote{Text: "external quoted text", Position: 0}

	got, ok := (&Adapter{}).MessageContextFromEvent(&events.MessageEvent{
		Message: &client.Message{
			MessageId: 46,
			Chat: client.Chat{
				Id:   2317500,
				Type: "private",
			},
			From:  &client.User{Id: 101},
			Text:  textPtr(testMessageText),
			Quote: &quote,
		},
	})
	if !ok {
		t.Fatal("MessageContextFromEvent() ok = false, want true")
	}
	if !got.IsReply {
		t.Fatalf("is_reply = %v, want true for quote-only message", got.IsReply)
	}
	if got.ReplyContent != quote.Text {
		t.Fatalf("reply_content = %q, want %q", got.ReplyContent, quote.Text)
	}
}

func TestMessageContextFromEvent_ExtractsReplyContentFromRichMessage(t *testing.T) {
	got, ok := (&Adapter{}).MessageContextFromEvent(&events.MessageEvent{
		Message: &client.Message{
			MessageId: 47,
			Chat: client.Chat{
				Id:   -1009001,
				Type: "supergroup",
			},
			From: &client.User{Id: 101},
			Text: textPtr(testMessageText),
			ReplyToMessage: &client.Message{
				MessageId: 11,
				RichMessage: &client.RichMessage{Blocks: []client.RichBlock{
					{
						"type": "heading",
						"text": "Release notes",
					},
					{
						"type": "paragraph",
						"text": []interface{}{
							"Ship ",
							map[string]interface{}{"type": "bold", "text": "quote support"},
							".",
						},
					},
					{
						"type": "details",
						"summary": map[string]interface{}{
							"type": "marked",
							"text": "Fallbacks",
						},
						"blocks": []interface{}{
							map[string]interface{}{
								"type": "blockquote",
								"blocks": []interface{}{
									map[string]interface{}{"type": "paragraph", "text": "Nested quote"},
								},
							},
						},
					},
				}},
			},
		},
	})
	if !ok {
		t.Fatal("MessageContextFromEvent() ok = false, want true")
	}
	want := "Release notes\nShip quote support.\nFallbacks\nNested quote"
	if got.ReplyContent != want {
		t.Fatalf("reply_content = %q, want %q", got.ReplyContent, want)
	}
}

func TestMessageContextFromEvent_ExtractsReplyContentFromRichMessageListsTablesAndCaptions(t *testing.T) {
	got, ok := (&Adapter{}).MessageContextFromEvent(&events.MessageEvent{
		Message: &client.Message{
			MessageId: 48,
			Chat: client.Chat{
				Id:   -1009001,
				Type: "supergroup",
			},
			From: &client.User{Id: 101},
			Text: textPtr(testMessageText),
			ReplyToMessage: &client.Message{
				MessageId: 12,
				RichMessage: &client.RichMessage{Blocks: []client.RichBlock{
					{
						"type": "list",
						"items": []interface{}{
							map[string]interface{}{
								"label": "1.",
								"blocks": []interface{}{
									map[string]interface{}{"type": "paragraph", "text": "First item"},
								},
							},
							map[string]interface{}{
								"label": "-",
								"blocks": []interface{}{
									map[string]interface{}{"type": "paragraph", "text": "Second item"},
								},
							},
						},
					},
					{
						"type": "table",
						"cells": []interface{}{
							[]interface{}{
								map[string]interface{}{"text": "Name"},
								map[string]interface{}{"text": "Status"},
							},
							[]interface{}{
								map[string]interface{}{"text": "quote"},
								map[string]interface{}{"text": "covered"},
							},
						},
					},
					{
						"type": "photo",
						"caption": map[string]interface{}{
							"text":   "Screenshot caption",
							"credit": "QA",
						},
					},
				}},
			},
		},
	})
	if !ok {
		t.Fatal("MessageContextFromEvent() ok = false, want true")
	}
	want := "1. First item\n- Second item\nName | Status\nquote | covered\nScreenshot caption\nQA"
	if got.ReplyContent != want {
		t.Fatalf("reply_content = %q, want %q", got.ReplyContent, want)
	}
}

func TestMessageContextFromEvent_NonTextOnlyReplyKeepsEmptyContext(t *testing.T) {
	got, ok := (&Adapter{}).MessageContextFromEvent(&events.MessageEvent{
		Message: &client.Message{
			MessageId: 49,
			Chat: client.Chat{
				Id:   -1009001,
				Type: "supergroup",
			},
			From: &client.User{Id: 101},
			Text: textPtr(testMessageText),
			ReplyToMessage: &client.Message{
				MessageId: 13,
				RichMessage: &client.RichMessage{Blocks: []client.RichBlock{
					{"type": "divider"},
				}},
			},
		},
	})
	if !ok {
		t.Fatal("MessageContextFromEvent() ok = false, want true")
	}
	if got.ReplyContent != "" {
		t.Fatalf("reply_content = %q, want empty", got.ReplyContent)
	}
}

func TestMessageContextFromEvent_CopiesEntities(t *testing.T) {
	entities := []client.MessageEntity{
		{Type: "mention", Offset: 6, Length: 8},
	}
	got, ok := (&Adapter{}).MessageContextFromEvent(&events.MessageEvent{
		Message: &client.Message{
			MessageId: 45,
			Chat: client.Chat{
				Id:   -1009001,
				Type: "supergroup",
			},
			From:     &client.User{Id: 101},
			Text:     textPtr("hello @testbot"),
			Entities: &entities,
		},
	})
	if !ok {
		t.Fatal("MessageContextFromEvent() ok = false, want true")
	}
	if len(got.Entities) != 1 {
		t.Fatalf("entities len = %d, want 1", len(got.Entities))
	}
	if got.Entities[0].Type != "mention" || got.Entities[0].Offset != 6 || got.Entities[0].Length != 8 {
		t.Fatalf("entity = %+v, want mention offset=6 length=8", got.Entities[0])
	}
}

func TestCommandContextFromEvent_PrivateChatIgnoresMessageThreadID(t *testing.T) {
	topicID := 523431
	isTopicMessage := false
	got, ok := (&Adapter{}).CommandContextFromEvent(&events.CommandEvent{
		Command: "topic",
		Args:    "codex",
		Message: &client.Message{
			MessageThreadId: &topicID,
			IsTopicMessage:  &isTopicMessage,
			Chat: client.Chat{
				Id:   2317500,
				Type: "private",
			},
			From: &client.User{Id: 2317500},
		},
	})
	if !ok {
		t.Fatal("CommandContextFromEvent() ok = false, want true")
	}
	if got.TopicID != 0 {
		t.Fatalf("topic_id = %d, want 0 for private chat", got.TopicID)
	}
	if got.Locator.SessionID != "tg-2317500-0" {
		t.Fatalf("session_id = %q, want tg-2317500-0", got.Locator.SessionID)
	}
}

func TestCommandContextFromEvent_PrivateTopicPreservesMessageThreadID(t *testing.T) {
	topicID := 523431
	isTopicMessage := true
	got, ok := (&Adapter{}).CommandContextFromEvent(&events.CommandEvent{
		Command: "topic",
		Args:    "codex",
		Message: &client.Message{
			MessageThreadId: &topicID,
			IsTopicMessage:  &isTopicMessage,
			Chat: client.Chat{
				Id:   2317500,
				Type: "private",
			},
			From: &client.User{Id: 2317500},
		},
	})
	if !ok {
		t.Fatal("CommandContextFromEvent() ok = false, want true")
	}
	if got.TopicID != 523431 {
		t.Fatalf("topic_id = %d, want 523431 for private topic command", got.TopicID)
	}
	if got.Locator.SessionID != "tg-2317500-523431" {
		t.Fatalf("session_id = %q, want tg-2317500-523431", got.Locator.SessionID)
	}
}

func TestTopicLifecycleFromEvent_IgnoresPrivateNonTopicChat(t *testing.T) {
	topicID := 523452
	isTopicMessage := false

	_, ok := (&Adapter{}).TopicLifecycleFromEvent(&events.MessageEvent{
		Type: messagetype.ForumTopicCreated,
		Message: &client.Message{
			MessageId:       218,
			MessageThreadId: &topicID,
			IsTopicMessage:  &isTopicMessage,
			Chat: client.Chat{
				Id:   2317500,
				Type: "private",
			},
			From: &client.User{Id: 2317500},
		},
	})
	if ok {
		t.Fatal("TopicLifecycleFromEvent() ok = true, want false for private non-topic chat")
	}
}

func TestTopicLifecycleFromEvent_AcceptsSupergroup(t *testing.T) {
	topicID := 77

	got, ok := (&Adapter{}).TopicLifecycleFromEvent(&events.MessageEvent{
		Type: messagetype.ForumTopicCreated,
		Message: &client.Message{
			MessageId:       42,
			MessageThreadId: &topicID,
			Chat: client.Chat{
				Id:   9001,
				Type: "supergroup",
			},
			From: &client.User{Id: 101},
		},
	})
	if !ok {
		t.Fatal("TopicLifecycleFromEvent() ok = false, want true for supergroup")
	}
	if got.TopicID != 77 {
		t.Fatalf("topic_id = %d, want 77", got.TopicID)
	}
	if got.Locator.SessionID != "tg-9001-77" {
		t.Fatalf("session_id = %q, want tg-9001-77", got.Locator.SessionID)
	}
}

func TestTopicLifecycleFromEvent_AcceptsPrivateTopic(t *testing.T) {
	topicID := 523452
	isTopicMessage := true

	got, ok := (&Adapter{}).TopicLifecycleFromEvent(&events.MessageEvent{
		Type: messagetype.ForumTopicCreated,
		Message: &client.Message{
			MessageId:       218,
			MessageThreadId: &topicID,
			IsTopicMessage:  &isTopicMessage,
			Chat: client.Chat{
				Id:   2317500,
				Type: "private",
			},
			From: &client.User{Id: 2317500},
		},
	})
	if !ok {
		t.Fatal("TopicLifecycleFromEvent() ok = false, want true for private topic")
	}
	if got.TopicID != 523452 {
		t.Fatalf("topic_id = %d, want 523452", got.TopicID)
	}
	if got.Locator.SessionID != "tg-2317500-523452" {
		t.Fatalf("session_id = %q, want tg-2317500-523452", got.Locator.SessionID)
	}
}

func textPtr(s string) *string {
	return &s
}

type fakeTelegramChatActionClient struct {
	client.ClientWithResponsesInterface
	chatActions []client.SendChatActionJSONRequestBody
}

func (f *fakeTelegramChatActionClient) SendChatActionWithResponse(
	_ context.Context,
	body client.SendChatActionJSONRequestBody,
	_ ...client.RequestEditorFn,
) (*client.SendChatActionResponse, error) {
	f.chatActions = append(f.chatActions, body)
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

func TestSendTyping_ThrottlesRepeatedChatActionsPerSession(t *testing.T) {
	tgClient := &fakeTelegramChatActionClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	adapter := NewAdapter(AdapterParams{Messenger: msg, TGClient: tgClient, Logger: zerolog.Nop()})
	adapter.SetTypingThrottleInterval(4 * time.Second)
	now := time.Unix(100, 0)
	adapter.now = func() time.Time { return now }

	locator := NewLocator(9001, 77)
	if err := adapter.SendTyping(context.Background(), locator); err != nil {
		t.Fatalf("SendTyping(first) error = %v", err)
	}
	if err := adapter.SendTyping(context.Background(), locator); err != nil {
		t.Fatalf("SendTyping(second) error = %v", err)
	}

	if len(tgClient.chatActions) != 1 {
		t.Fatalf("chat action calls = %d, want 1", len(tgClient.chatActions))
	}
}

func TestSendTyping_AllowsChatActionAfterThrottleInterval(t *testing.T) {
	tgClient := &fakeTelegramChatActionClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	adapter := NewAdapter(AdapterParams{Messenger: msg, TGClient: tgClient, Logger: zerolog.Nop()})
	adapter.SetTypingThrottleInterval(4 * time.Second)
	now := time.Unix(100, 0)
	adapter.now = func() time.Time { return now }

	locator := NewLocator(9001, 77)
	if err := adapter.SendTyping(context.Background(), locator); err != nil {
		t.Fatalf("SendTyping(first) error = %v", err)
	}
	now = now.Add(4 * time.Second)
	if err := adapter.SendTyping(context.Background(), locator); err != nil {
		t.Fatalf("SendTyping(second) error = %v", err)
	}

	if len(tgClient.chatActions) != 2 {
		t.Fatalf("chat action calls = %d, want 2", len(tgClient.chatActions))
	}
}
