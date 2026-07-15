package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/baldaworks/go-actorlayer"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	"github.com/normahq/balda/internal/apps/balda/questioncmd"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
)

func (h *BaldaHandler) handleQuestionReply(ctx context.Context, messageCtx baldatelegram.MessageContext, text string) (bool, error) {
	if h == nil || h.questionService == nil || messageCtx.ReplyToMessageID <= 0 || strings.TrimSpace(text) == "" {
		return false, nil
	}
	record, matched, err := h.questionService.ResolveReply(ctx, questioncmd.InboundReply{
		Provider:         "telegram",
		SessionID:        messageCtx.Locator.SessionID,
		ConversationKey:  messageCtx.Locator.AddressKey,
		ReplyToMessageID: strconv.Itoa(messageCtx.ReplyToMessageID),
		MessageID:        strconv.Itoa(messageCtx.MessageID),
		User: questioncmd.UserRef{
			UserID: strconv.FormatInt(messageCtx.UserID, 10),
		},
		Text:       text,
		ReceivedAt: h.now(),
	})
	if err != nil || !matched {
		return matched, err
	}
	if err := dispatchQuestionContinuation(ctx, h.actorDispatcher, record); err != nil {
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
