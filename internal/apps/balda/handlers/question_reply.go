package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/baldaworks/go-actorlayer"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/questioncmd"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/tgbotkit/runtime/events"
)

func (h *BaldaHandler) onQuestionCallback(ctx context.Context, event *events.CallbackQueryEvent) error {
	if h == nil || h.channel == nil {
		return nil
	}
	callback, ok := h.channel.CallbackContextFromEvent(event)
	if !ok {
		if event != nil && event.CallbackQuery != nil {
			_ = h.channel.AnswerQuestionCallback(ctx, event.CallbackQuery.Id, "This choice is no longer available.", true)
		}
		return nil
	}
	if h.questionService == nil {
		return h.channel.AnswerQuestionCallback(ctx, callback.CallbackQueryID, "This request is unavailable.", true)
	}
	if h.ownerStore != nil && h.collaboratorStore != nil && !h.canAccessCollaboratorScope(ctx, callback.UserID) {
		return h.channel.AnswerQuestionCallback(ctx, callback.CallbackQueryID, "You cannot answer this request.", true)
	}
	receivedAt := time.Now()
	if h.now != nil {
		receivedAt = h.now()
	}
	result, err := h.questionService.ResolveSelectionDetailed(ctx, questioncmd.InboundSelection{
		Provider:          string(deliverycmd.ChannelTypeTelegram),
		SessionID:         callback.Locator.SessionID,
		ConversationKey:   callback.Locator.AddressKey,
		QuestionID:        callback.QuestionID,
		ProviderMessageID: callback.ProviderMessageID,
		User: questioncmd.UserRef{
			UserID: baldatelegram.UserID(callback.UserID),
		},
		OptionIndex: callback.OptionIndex,
		ReceivedAt:  receivedAt,
	})
	if err != nil {
		_ = h.channel.AnswerQuestionCallback(ctx, callback.CallbackQueryID, "Could not process this choice.", true)
		return err
	}
	message, alert := "Selected.", false
	switch {
	case !result.Matched || result.Inactive:
		message = "This request has expired."
	case result.Invalid:
		message, alert = "This choice is not available to you.", true
	case !result.Settled:
		message = "This request has already been answered."
	}
	ackErr := h.channel.AnswerQuestionCallback(ctx, callback.CallbackQueryID, message, alert)
	if !result.Settled {
		return ackErr
	}
	if dispatchErr := dispatchQuestionContinuation(ctx, h.actorDispatcher, result.Record); dispatchErr != nil {
		return dispatchErr
	}
	return ackErr
}

func (h *BaldaHandler) handleQuestionReply(ctx context.Context, messageCtx baldatelegram.MessageContext) (bool, error) {
	text := messageCtx.Text
	if h == nil || h.questionService == nil || messageCtx.ReplyToMessageID <= 0 || strings.TrimSpace(text) == "" {
		return false, nil
	}
	receivedAt := time.Now()
	if h.now != nil {
		receivedAt = h.now()
	}
	result, err := h.questionService.ResolveReplyDetailed(ctx, questioncmd.InboundReply{
		Provider:         "telegram",
		SessionID:        messageCtx.Locator.SessionID,
		ConversationKey:  messageCtx.Locator.AddressKey,
		ReplyToMessageID: strconv.Itoa(messageCtx.ReplyToMessageID),
		MessageID:        strconv.Itoa(messageCtx.MessageID),
		User: questioncmd.UserRef{
			UserID: baldatelegram.UserID(messageCtx.UserID),
		},
		Text:       text,
		ReceivedAt: receivedAt,
	})
	if err != nil || !result.Matched {
		return result.Matched, err
	}
	if !result.Settled {
		return true, nil
	}
	if err := dispatchQuestionContinuation(ctx, h.actorDispatcher, result.Record); err != nil {
		return true, err
	}
	return true, nil
}

func dispatchQuestionContinuation(ctx context.Context, dispatcher actortransport.Dispatcher, record baldastate.QuestionRecord) error {
	var interaction questioncmd.InteractionContext
	if err := json.Unmarshal([]byte(record.InteractionJSON), &interaction); err != nil {
		return err
	}
	var resume questioncmd.ResumeTarget
	if err := json.Unmarshal([]byte(record.ResumeJSON), &resume); err != nil {
		return err
	}
	var answer questioncmd.Answer
	if err := json.Unmarshal([]byte(record.AnswerJSON), &answer); err != nil {
		return err
	}
	env, err := questioncmd.AnsweredEnvelope(resume, interaction, answer, record.QuestionID)
	if err != nil {
		return err
	}
	if dispatcher == nil {
		return actorlayer.TransientError(fmt.Errorf("runtime is unavailable"))
	}
	_, err = dispatcher.Dispatch(ctx, env)
	return err
}
