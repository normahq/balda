package questions

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/baldaworks/go-actorlayer"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/questioncmd"
)

const metadataDefaultOptionID = "default_option_id"

type SessionOption struct {
	ID    string
	Label string
}

type SessionRequest struct {
	Interaction     questioncmd.InteractionContext
	Resume          questioncmd.ResumeTarget
	Prompt          string
	Options         []SessionOption
	DefaultOptionID string
	AllowFreeText   bool
	Timeout         time.Duration
	Audience        deliverycmd.QuestionAudience
	Profile         deliverycmd.Profile
	Metadata        map[string]string
}

type SessionResult struct {
	QuestionID string
	OptionID   string
	Text       string
	Source     string
	AnsweredBy questioncmd.UserRef
	TimedOut   bool
	Canceled   bool
}

func (s *Service) AskSession(ctx context.Context, dispatcher actortransport.Dispatcher, req SessionRequest) (SessionResult, error) {
	fallback := SessionResult{Source: "canceled", Canceled: true}
	if s == nil || s.store == nil || dispatcher == nil {
		return fallback, fmt.Errorf("interactive session question is unavailable")
	}
	if strings.TrimSpace(req.Interaction.SessionID) == "" {
		return fallback, fmt.Errorf("interaction session_id is required")
	}
	if strings.TrimSpace(req.Prompt) == "" {
		return fallback, fmt.Errorf("question prompt is required")
	}
	if strings.TrimSpace(req.Resume.To) == "" {
		return fallback, fmt.Errorf("question resume target is required")
	}
	if !req.AllowFreeText && len(req.Options) == 0 {
		return fallback, fmt.Errorf("question requires options or free text")
	}
	if defaultID := strings.TrimSpace(req.DefaultOptionID); defaultID != "" && !hasSessionOption(req.Options, defaultID) {
		return fallback, fmt.Errorf("default option %q is not present in options", defaultID)
	}

	options := make([]questioncmd.Option, 0, len(req.Options))
	deliveryOptions := make([]deliverycmd.QuestionOption, 0, len(req.Options))
	for _, option := range req.Options {
		id := strings.TrimSpace(option.ID)
		label := strings.TrimSpace(option.Label)
		if id == "" || label == "" {
			return fallback, fmt.Errorf("question options require id and label")
		}
		options = append(options, questioncmd.Option{ID: id, Label: label})
		deliveryOptions = append(deliveryOptions, deliverycmd.QuestionOption{ID: id, Label: label})
	}

	metadata := copySessionMetadata(req.Metadata)
	if defaultID := strings.TrimSpace(req.DefaultOptionID); defaultID != "" {
		metadata[metadataDefaultOptionID] = defaultID
	}
	record, err := s.Ask(ctx, req.Interaction, req.Resume, questioncmd.Request{
		Prompt:        strings.TrimSpace(req.Prompt),
		Options:       options,
		AllowFreeText: req.AllowFreeText,
		Responder:     responderForSessionAudience(req),
		Timeout:       req.Timeout,
		Metadata:      metadata,
	})
	if err != nil {
		return fallback, fmt.Errorf("create session question: %w", err)
	}

	waiter := make(chan SessionResult, 1)
	s.waitMu.Lock()
	s.waiters[record.QuestionID] = waiter
	s.waitMu.Unlock()
	defer func() {
		s.waitMu.Lock()
		delete(s.waiters, record.QuestionID)
		s.waitMu.Unlock()
	}()

	envelope, err := buildSessionDeliveryEnvelope(record.QuestionID, req, deliveryOptions)
	if err != nil {
		_, _, _ = s.Timeout(context.WithoutCancel(ctx), record.QuestionID, s.now().UTC())
		return SessionResult{QuestionID: record.QuestionID, Source: "delivery_failed", Canceled: true}, fmt.Errorf("build session question delivery: %w", err)
	}
	if _, err := dispatcher.Dispatch(ctx, envelope); err != nil {
		_, _, _ = s.Timeout(context.WithoutCancel(ctx), record.QuestionID, s.now().UTC())
		return SessionResult{QuestionID: record.QuestionID, Source: "delivery_failed", Canceled: true}, fmt.Errorf("dispatch session question delivery: %w", err)
	}

	timer := time.NewTimer(req.Timeout)
	defer timer.Stop()
	select {
	case result := <-waiter:
		if strings.TrimSpace(result.QuestionID) == "" {
			result.QuestionID = record.QuestionID
		}
		return result, nil
	case <-timer.C:
		return s.sessionTimeoutResult(record.QuestionID)
	case <-ctx.Done():
		result, err := s.sessionTimeoutResult(record.QuestionID)
		if err != nil {
			return result, fmt.Errorf("session question canceled: %w", err)
		}
		return result, ctx.Err()
	}
}

func (s *Service) ResolveSession(questionID string, result SessionResult) {
	if s == nil {
		return
	}
	s.waitMu.Lock()
	waiter := s.waiters[strings.TrimSpace(questionID)]
	s.waitMu.Unlock()
	if waiter == nil {
		return
	}
	result.QuestionID = strings.TrimSpace(questionID)
	result.OptionID = strings.TrimSpace(result.OptionID)
	result.Text = strings.TrimSpace(result.Text)
	result.Source = strings.TrimSpace(result.Source)
	select {
	case waiter <- result:
	default:
	}
}

func (s *Service) sessionTimeoutResult(questionID string) (SessionResult, error) {
	record, settled, err := s.Timeout(context.Background(), questionID, s.now().UTC())
	if err != nil {
		return SessionResult{QuestionID: questionID, Source: "timeout", TimedOut: true, Canceled: true}, err
	}
	result := SessionResult{QuestionID: questionID, Source: "timeout", TimedOut: true, Canceled: true}
	if strings.TrimSpace(record.AnswerJSON) == "" {
		return result, nil
	}

	var answer questioncmd.Answer
	if err := json.Unmarshal([]byte(record.AnswerJSON), &answer); err != nil {
		return result, nil
	}
	if settled || strings.TrimSpace(answer.SelectedOption) != "" || strings.TrimSpace(answer.Text) != "" {
		return SessionResult{
			QuestionID: questionID,
			OptionID:   strings.TrimSpace(answer.SelectedOption),
			Text:       strings.TrimSpace(answer.Text),
			Source:     "user",
			AnsweredBy: answer.AnsweredBy,
		}, nil
	}
	return result, nil
}

func buildSessionDeliveryEnvelope(questionID string, req SessionRequest, options []deliverycmd.QuestionOption) (actorlayer.Envelope, error) {
	from, err := questioncmd.ParseResumeAddress(req.Resume.To)
	if err != nil {
		return actorlayer.Envelope{}, err
	}
	dedupeSuffix := "question:" + questionID
	if len(options) > 0 {
		return deliverycmd.QuestionEnvelope("", from, req.Interaction.Locator, req.Profile, deliverycmd.SettlementOutbox, strings.TrimSpace(req.Prompt), questionID, dedupeSuffix, options, req.Audience)
	}
	return deliverycmd.AgentReplyEnvelopeWithProfileAndSettlementAndRefs("", from, req.Interaction.Locator, req.Profile, deliverycmd.SettlementOutbox, strings.TrimSpace(req.Prompt), dedupeSuffix, map[string]string{"question_id": questionID})
}

func responderForSessionAudience(req SessionRequest) string {
	if strings.EqualFold(string(req.Audience.Visibility), string(deliverycmd.QuestionVisibilityPrivate)) &&
		strings.TrimSpace(req.Audience.UserID) != "" {
		return questioncmd.ResponderRequester
	}
	if strings.TrimSpace(req.Interaction.RequestedBy.UserID) != "" {
		return questioncmd.ResponderRequester
	}
	return questioncmd.ResponderAny
}

func hasSessionOption(options []SessionOption, optionID string) bool {
	for _, option := range options {
		if strings.TrimSpace(option.ID) == strings.TrimSpace(optionID) {
			return true
		}
	}
	return false
}

func copySessionMetadata(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		trimmedKey := strings.TrimSpace(key)
		trimmedValue := strings.TrimSpace(value)
		if trimmedKey == "" || trimmedValue == "" {
			continue
		}
		out[trimmedKey] = trimmedValue
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
