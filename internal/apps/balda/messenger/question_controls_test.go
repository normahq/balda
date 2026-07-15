package messenger

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/normahq/balda/internal/apps/balda/telegramfmt"
	"github.com/rs/zerolog"
	"github.com/tgbotkit/client"
)

type inlineKeyboardClient struct {
	client.ClientWithResponsesInterface
	richBody      []byte
	richBodyError error
	richFallback  []client.SendRichMessageJSONRequestBody
	editRequests  []client.EditMessageReplyMarkupJSONRequestBody
	answers       []client.AnswerCallbackQueryJSONRequestBody
}

func (f *inlineKeyboardClient) EditMessageReplyMarkupWithResponse(_ context.Context, body client.EditMessageReplyMarkupJSONRequestBody, _ ...client.RequestEditorFn) (*client.EditMessageReplyMarkupResponse, error) {
	f.editRequests = append(f.editRequests, body)
	return &client.EditMessageReplyMarkupResponse{JSON400: &client.ErrorResponse{Description: "Bad Request: message is not modified"}}, nil
}

func (f *inlineKeyboardClient) AnswerCallbackQueryWithResponse(_ context.Context, body client.AnswerCallbackQueryJSONRequestBody, _ ...client.RequestEditorFn) (*client.AnswerCallbackQueryResponse, error) {
	f.answers = append(f.answers, body)
	return &client.AnswerCallbackQueryResponse{JSON200: &struct {
		Ok     client.AnswerCallbackQuery200Ok `json:"ok"`
		Result bool                            `json:"result"`
	}{Ok: true, Result: true}}, nil
}

func (f *inlineKeyboardClient) SendRichMessageWithBodyWithResponse(_ context.Context, _ string, body io.Reader, _ ...client.RequestEditorFn) (*client.SendRichMessageResponse, error) {
	f.richBody, _ = io.ReadAll(body)
	if f.richBodyError != nil {
		return nil, f.richBodyError
	}
	return successfulSendRichMessageResponse(42), nil
}

func (f *inlineKeyboardClient) SendRichMessageWithResponse(_ context.Context, body client.SendRichMessageJSONRequestBody, _ ...client.RequestEditorFn) (*client.SendRichMessageResponse, error) {
	f.richFallback = append(f.richFallback, body)
	return successfulSendRichMessageResponse(43), nil
}

func TestSendAgentReplyWithInlineKeyboardIncludesMarkup(t *testing.T) {
	tgClient := &inlineKeyboardClient{}
	messenger := NewMessenger(tgClient, zerolog.Nop())
	callback := "balda:q:question-1:1"
	keyboard := client.InlineKeyboardMarkup{InlineKeyboard: [][]client.InlineKeyboardButton{{{
		Text: "Allow", CallbackData: &callback,
	}}}}

	messageID, err := messenger.SendAgentReplyWithInlineKeyboardLastMessageIDAndMode(
		context.Background(), 9001, "Choose", 77, telegramfmt.ModeRichMarkdown, keyboard, "fallback",
	)
	if err != nil {
		t.Fatalf("SendAgentReplyWithInlineKeyboardLastMessageIDAndMode() error = %v", err)
	}
	if messageID != 42 {
		t.Fatalf("message id = %d, want 42", messageID)
	}
	var request struct {
		ChatID          int64                       `json:"chat_id"`
		MessageThreadID int                         `json:"message_thread_id"`
		ReplyMarkup     client.InlineKeyboardMarkup `json:"reply_markup"`
	}
	if err := json.Unmarshal(tgClient.richBody, &request); err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if request.ChatID != 9001 || request.MessageThreadID != 77 || len(request.ReplyMarkup.InlineKeyboard) != 1 {
		t.Fatalf("request = %+v", request)
	}
	if got := *request.ReplyMarkup.InlineKeyboard[0][0].CallbackData; got != callback {
		t.Fatalf("callback data = %q", got)
	}
}

func TestSendAgentReplyWithInlineKeyboardFallsBackToTextChoices(t *testing.T) {
	tgClient := &inlineKeyboardClient{richBodyError: errors.New("reply markup rejected")}
	messenger := NewMessenger(tgClient, zerolog.Nop())
	keyboard := client.InlineKeyboardMarkup{InlineKeyboard: [][]client.InlineKeyboardButton{{{Text: "Allow"}}}}

	messageID, err := messenger.SendAgentReplyWithInlineKeyboardLastMessageIDAndMode(
		context.Background(), 9001, "Choose", 0, telegramfmt.ModeRichMarkdown, keyboard, "Choose\n\n1. Allow",
	)
	if err != nil {
		t.Fatalf("SendAgentReplyWithInlineKeyboardLastMessageIDAndMode() error = %v", err)
	}
	if messageID != 43 || len(tgClient.richFallback) != 1 {
		t.Fatalf("message id = %d, fallback requests = %d", messageID, len(tgClient.richFallback))
	}
	markdown := tgClient.richFallback[0].RichMessage.Markdown
	if markdown == nil || *markdown != "Choose\n\n1. Allow" {
		t.Fatalf("fallback markdown = %v", markdown)
	}
}

func TestQuestionControlLifecycleCallsTelegramAPIs(t *testing.T) {
	tgClient := &inlineKeyboardClient{}
	messenger := NewMessenger(tgClient, zerolog.Nop())
	if err := messenger.ClearInlineKeyboard(context.Background(), 9001, 42); err != nil {
		t.Fatalf("ClearInlineKeyboard() error = %v", err)
	}
	if len(tgClient.editRequests) != 1 || tgClient.editRequests[0].ReplyMarkup != nil {
		t.Fatalf("edit requests = %+v", tgClient.editRequests)
	}
	if err := messenger.AnswerCallbackQuery(context.Background(), "callback-1", "Selected.", false); err != nil {
		t.Fatalf("AnswerCallbackQuery() error = %v", err)
	}
	if len(tgClient.answers) != 1 || tgClient.answers[0].Text == nil || *tgClient.answers[0].Text != "Selected." {
		t.Fatalf("callback answers = %+v", tgClient.answers)
	}
}
