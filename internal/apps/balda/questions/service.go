package questions

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/baldaworks/go-actorlayer"
	"github.com/google/uuid"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/questioncmd"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/rs/zerolog"
)

type Store interface {
	CreatePendingQuestion(ctx context.Context, record baldastate.QuestionRecord) error
	BindQuestionDeliveryRef(ctx context.Context, questionID string, ref questioncmd.DeliveryRef) error
	GetQuestionByID(ctx context.Context, questionID string) (baldastate.QuestionRecord, bool, error)
	GetPendingQuestionByReplyRef(ctx context.Context, provider, conversationKey, replyToMessageID string) (baldastate.QuestionRecord, bool, error)
	MarkQuestionAnswered(ctx context.Context, questionID string, answer questioncmd.Answer) (baldastate.QuestionRecord, bool, error)
	MarkQuestionTimedOut(ctx context.Context, questionID string, timedOutAt time.Time) (baldastate.QuestionRecord, bool, error)
}

type failureStore interface {
	MarkQuestionFailed(ctx context.Context, questionID string, failure questioncmd.Failure) (baldastate.QuestionRecord, bool, error)
}

type ScheduledJobStore interface {
	Upsert(ctx context.Context, record baldastate.ScheduledJobRecord) error
}

// ControlClearRequest asks the delivery boundary to remove interactive
// controls from a question that is no longer pending.
type ControlClearRequest struct {
	QuestionID        string
	Locator           deliverycmd.Locator
	ProviderMessageID string
	ControlHandle     string
}

// ControlPublisher projects question lifecycle changes to delivery channels.
type ControlPublisher interface {
	ClearQuestionControls(ctx context.Context, request ControlClearRequest) error
}

type Service struct {
	store     Store
	scheduled ScheduledJobStore
	controls  ControlPublisher
	logger    zerolog.Logger
	now       func() time.Time
	waitMu    sync.Mutex
	waiters   map[string]chan SessionResult
}

// SetControlPublisher attaches the optional delivery-side projection used to
// remove channel-native controls after settlement.
func (s *Service) SetControlPublisher(controls ControlPublisher) {
	if s != nil {
		s.controls = controls
	}
}

func New(store Store, scheduled ScheduledJobStore, logger zerolog.Logger) *Service {
	return &Service{
		store:     store,
		scheduled: scheduled,
		logger:    logger,
		now:       time.Now,
		waiters:   make(map[string]chan SessionResult),
	}
}

func (s *Service) Ask(ctx context.Context, interaction questioncmd.InteractionContext, resume questioncmd.ResumeTarget, req questioncmd.Request) (baldastate.QuestionRecord, error) {
	if s.store == nil {
		return baldastate.QuestionRecord{}, fmt.Errorf("question store is required")
	}
	if strings.TrimSpace(interaction.SessionID) == "" {
		return baldastate.QuestionRecord{}, fmt.Errorf("interaction session_id is required")
	}
	if strings.TrimSpace(interaction.Locator.SessionID) == "" {
		return baldastate.QuestionRecord{}, fmt.Errorf("interaction locator session_id is required")
	}
	if strings.TrimSpace(req.Prompt) == "" {
		return baldastate.QuestionRecord{}, fmt.Errorf("question prompt is required")
	}
	if strings.TrimSpace(resume.To) == "" {
		return baldastate.QuestionRecord{}, fmt.Errorf("resume target is required")
	}
	questionID := "question-" + uuid.NewString()
	now := s.now().UTC()
	record := baldastate.QuestionRecord{
		QuestionID:      questionID,
		SessionID:       strings.TrimSpace(interaction.SessionID),
		ChannelKind:     firstNonEmpty(interaction.ChannelKind, interaction.Locator.ChannelType),
		AddressKey:      strings.TrimSpace(interaction.Locator.AddressKey),
		AddressJSON:     strings.TrimSpace(interaction.Locator.AddressJSON),
		Prompt:          strings.TrimSpace(req.Prompt),
		Status:          questioncmd.StatusPending,
		InteractionJSON: mustJSON(interaction),
		ResumeJSON:      mustJSON(resume),
		RequestJSON:     mustJSON(req),
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if req.Timeout > 0 {
		expiresAt := now.Add(req.Timeout)
		record.ExpiresAt = expiresAt
	}
	if err := s.store.CreatePendingQuestion(ctx, record); err != nil {
		return baldastate.QuestionRecord{}, err
	}
	if !record.ExpiresAt.IsZero() && s.scheduled != nil {
		content, err := questioncmd.TimeoutScheduledContent(questionID)
		if err != nil {
			return baldastate.QuestionRecord{}, err
		}
		if err := s.scheduled.Upsert(ctx, baldastate.ScheduledJobRecord{
			JobID:        "question-timeout-" + questionID,
			SessionID:    strings.TrimSpace(interaction.Locator.SessionID),
			ChannelType:  strings.TrimSpace(interaction.Locator.ChannelType),
			AddressKey:   strings.TrimSpace(interaction.Locator.AddressKey),
			AddressJSON:  strings.TrimSpace(interaction.Locator.AddressJSON),
			Content:      content,
			ScheduleSpec: "@once",
			Timezone:     "UTC",
			Status:       baldastate.ScheduledJobStatusActive,
			MaxRetries:   0,
			NextRunAt:    record.ExpiresAt.UTC(),
		}); err != nil {
			return baldastate.QuestionRecord{}, err
		}
	}
	return record, nil
}

func (s *Service) BindDelivery(ctx context.Context, questionID string, ref questioncmd.DeliveryRef) error {
	if s.store == nil {
		return fmt.Errorf("question store is required")
	}
	if strings.TrimSpace(questionID) == "" {
		return fmt.Errorf("question id is required")
	}
	if err := s.store.BindQuestionDeliveryRef(ctx, questionID, ref); err != nil {
		return err
	}
	record, ok, err := s.store.GetQuestionByID(ctx, strings.TrimSpace(questionID))
	if err != nil || !ok {
		return err
	}
	if record.Status != questioncmd.StatusPending {
		s.clearControls(ctx, record)
	}
	return nil
}

func (s *Service) ResolveReply(ctx context.Context, in questioncmd.InboundReply) (baldastate.QuestionRecord, bool, error) {
	result, err := s.ResolveReplyDetailed(ctx, in)
	return result.Record, result.Matched, err
}

type ReplyResolution struct {
	Record  baldastate.QuestionRecord
	Matched bool
	Settled bool
	Invalid bool
}

func (s *Service) ResolveReplyDetailed(ctx context.Context, in questioncmd.InboundReply) (ReplyResolution, error) {
	if s.store == nil {
		return ReplyResolution{}, fmt.Errorf("question store is required")
	}
	record, ok, err := s.store.GetPendingQuestionByReplyRef(ctx, strings.TrimSpace(in.Provider), strings.TrimSpace(in.ConversationKey), strings.TrimSpace(in.ReplyToMessageID))
	if err != nil || !ok {
		return ReplyResolution{Record: record, Matched: ok}, err
	}
	request, err := decodeRequest(record)
	if err != nil {
		return ReplyResolution{Record: record, Matched: true}, fmt.Errorf("decode question request: %w", err)
	}
	allowed, err := responderAllowed(record, request, in.User)
	if err != nil {
		return ReplyResolution{Record: record, Matched: true}, err
	}
	if !allowed {
		return ReplyResolution{Record: record, Matched: true, Invalid: true}, nil
	}
	selected, valid := selectedOption(request, in.Text)
	if !valid {
		return ReplyResolution{Record: record, Matched: true, Invalid: true}, nil
	}
	answer := questioncmd.Answer{
		Text:           strings.TrimSpace(in.Text),
		SelectedOption: selected,
		AnsweredBy:     in.User,
		AnsweredAt:     zeroOrNow(in.ReceivedAt, s.now().UTC()),
		ProviderMsgID:  strings.TrimSpace(in.MessageID),
	}
	updated, settled, err := s.store.MarkQuestionAnswered(ctx, record.QuestionID, answer)
	if err == nil && settled {
		s.clearControls(ctx, updated)
	}
	return ReplyResolution{Record: updated, Matched: true, Settled: settled}, err
}

// SelectionResolution reports whether a structured selection matched and
// durably settled a question.
type SelectionResolution struct {
	Record   baldastate.QuestionRecord
	Matched  bool
	Settled  bool
	Invalid  bool
	Inactive bool
}

// ResolveSelectionDetailed validates and settles a channel-independent option
// selection. It never trusts the channel callback as the source of option IDs;
// the persisted question remains authoritative.
func (s *Service) ResolveSelectionDetailed(ctx context.Context, in questioncmd.InboundSelection) (SelectionResolution, error) {
	if s.store == nil {
		return SelectionResolution{}, fmt.Errorf("question store is required")
	}
	record, ok, err := s.store.GetQuestionByID(ctx, strings.TrimSpace(in.QuestionID))
	if err != nil || !ok {
		return SelectionResolution{Record: record, Matched: ok}, err
	}
	if !selectionContextMatches(record, in) {
		return SelectionResolution{Record: record, Matched: true, Invalid: true}, nil
	}
	if record.Status != questioncmd.StatusPending {
		s.clearControls(ctx, record)
		return SelectionResolution{Record: record, Matched: true, Inactive: true}, nil
	}
	request, err := decodeRequest(record)
	if err != nil {
		return SelectionResolution{Record: record, Matched: true}, fmt.Errorf("decode question request: %w", err)
	}
	allowed, err := responderAllowed(record, request, in.User)
	if err != nil {
		return SelectionResolution{Record: record, Matched: true}, err
	}
	if !allowed {
		return SelectionResolution{Record: record, Matched: true, Invalid: true}, nil
	}
	option, valid := selectedStructuredOption(request, in.OptionID, in.OptionIndex)
	if !valid {
		return SelectionResolution{Record: record, Matched: true, Invalid: true}, nil
	}
	answer := questioncmd.Answer{
		Text:           strings.TrimSpace(option.Label),
		SelectedOption: strings.TrimSpace(option.ID),
		AnsweredBy:     in.User,
		AnsweredAt:     zeroOrNow(in.ReceivedAt, s.now().UTC()),
		ProviderMsgID:  strings.TrimSpace(in.ProviderMessageID),
	}
	updated, settled, err := s.store.MarkQuestionAnswered(ctx, record.QuestionID, answer)
	if err == nil {
		if settled {
			s.clearControls(ctx, updated)
		} else {
			s.clearControls(ctx, record)
		}
	}
	return SelectionResolution{Record: updated, Matched: true, Settled: settled, Inactive: err == nil && !settled}, err
}

func decodeRequest(record baldastate.QuestionRecord) (questioncmd.Request, error) {
	var request questioncmd.Request
	if strings.TrimSpace(record.RequestJSON) == "" {
		request.AllowFreeText = true
		return request, nil
	}
	if err := json.Unmarshal([]byte(record.RequestJSON), &request); err != nil {
		return questioncmd.Request{}, err
	}
	return request, nil
}

func responderAllowed(record baldastate.QuestionRecord, request questioncmd.Request, user questioncmd.UserRef) (bool, error) {
	if !strings.EqualFold(strings.TrimSpace(request.Responder), questioncmd.ResponderRequester) {
		return true, nil
	}
	var interaction questioncmd.InteractionContext
	if err := json.Unmarshal([]byte(record.InteractionJSON), &interaction); err != nil {
		return false, fmt.Errorf("decode question interaction: %w", err)
	}
	requesterID := strings.TrimSpace(interaction.RequestedBy.UserID)
	return requesterID != "" && strings.TrimSpace(user.UserID) == requesterID, nil
}

func selectionContextMatches(record baldastate.QuestionRecord, in questioncmd.InboundSelection) bool {
	if strings.TrimSpace(in.SessionID) != "" && strings.TrimSpace(in.SessionID) != strings.TrimSpace(record.SessionID) {
		return false
	}
	if strings.TrimSpace(in.Provider) != "" && strings.TrimSpace(record.Provider) != "" && !strings.EqualFold(strings.TrimSpace(in.Provider), strings.TrimSpace(record.Provider)) {
		return false
	}
	if strings.TrimSpace(in.ConversationKey) != "" && strings.TrimSpace(in.ConversationKey) != firstNonEmpty(record.ConversationKey, record.AddressKey) {
		return false
	}
	return strings.TrimSpace(in.ProviderMessageID) == "" || strings.TrimSpace(record.ProviderMessageID) == "" || strings.TrimSpace(in.ProviderMessageID) == strings.TrimSpace(record.ProviderMessageID)
}

func selectedStructuredOption(request questioncmd.Request, optionID string, optionIndex int) (questioncmd.Option, bool) {
	optionID = strings.TrimSpace(optionID)
	if optionID != "" {
		for _, option := range request.Options {
			if optionID == strings.TrimSpace(option.ID) {
				return option, true
			}
		}
		return questioncmd.Option{}, false
	}
	if optionIndex <= 0 || optionIndex > len(request.Options) {
		return questioncmd.Option{}, false
	}
	return request.Options[optionIndex-1], true
}

func selectedOption(request questioncmd.Request, raw string) (string, bool) {
	text := strings.TrimSpace(raw)
	if len(request.Options) == 0 {
		return "", request.AllowFreeText && text != ""
	}
	for _, option := range request.Options {
		if strings.EqualFold(text, strings.TrimSpace(option.ID)) || strings.EqualFold(text, strings.TrimSpace(option.Label)) {
			return strings.TrimSpace(option.ID), true
		}
	}
	index, err := strconv.Atoi(text)
	if err == nil && index > 0 && index <= len(request.Options) {
		return strings.TrimSpace(request.Options[index-1].ID), true
	}
	if request.AllowFreeText && text != "" {
		return "", true
	}
	return "", false
}

func (s *Service) Timeout(ctx context.Context, questionID string, timedOutAt time.Time) (baldastate.QuestionRecord, bool, error) {
	if s.store == nil {
		return baldastate.QuestionRecord{}, false, fmt.Errorf("question store is required")
	}
	record, settled, err := s.store.MarkQuestionTimedOut(ctx, strings.TrimSpace(questionID), zeroOrNow(timedOutAt, s.now().UTC()))
	if err == nil && (settled || record.Status != questioncmd.StatusPending) {
		s.clearControls(ctx, record)
	}
	return record, settled, err
}

// FailedDeliveryContinuation returns the deterministic continuation for a
// question already failed by delivery. The bool is false for every other
// lifecycle state.
func (s *Service) FailedDeliveryContinuation(ctx context.Context, questionID string) (actorlayer.Envelope, bool, error) {
	if s == nil || s.store == nil {
		return actorlayer.Envelope{}, false, fmt.Errorf("question store is required")
	}
	record, ok, err := s.store.GetQuestionByID(ctx, strings.TrimSpace(questionID))
	if err != nil || !ok || record.Status != questioncmd.StatusFailed {
		return actorlayer.Envelope{}, false, err
	}
	envelope, err := failedEnvelopeFromRecord(record)
	return envelope, err == nil, err
}

// FailDelivery atomically fails a pending question and builds its generic
// continuation. Repeated calls return the same lifecycle continuation shape
// so actor deduplication can safely complete an interrupted publication.
func (s *Service) FailDelivery(ctx context.Context, questionID string, failure questioncmd.Failure) (actorlayer.Envelope, bool, error) {
	if s == nil || s.store == nil {
		return actorlayer.Envelope{}, false, fmt.Errorf("question store is required")
	}
	store, ok := s.store.(failureStore)
	if !ok {
		return actorlayer.Envelope{}, false, fmt.Errorf("question failure store is required")
	}
	if failure.FailedAt.IsZero() {
		failure.FailedAt = s.now().UTC()
	}
	record, settled, err := store.MarkQuestionFailed(ctx, strings.TrimSpace(questionID), failure)
	if err != nil {
		return actorlayer.Envelope{}, false, err
	}
	if !settled && record.Status != questioncmd.StatusFailed {
		return actorlayer.Envelope{}, false, nil
	}
	if !settled && strings.TrimSpace(record.FailureJSON) != "" {
		if err := json.Unmarshal([]byte(record.FailureJSON), &failure); err != nil {
			return actorlayer.Envelope{}, false, fmt.Errorf("decode question failure: %w", err)
		}
	}
	envelope, err := failedEnvelopeFromRecordWithFailure(record, failure)
	return envelope, err == nil, err
}

func failedEnvelopeFromRecord(record baldastate.QuestionRecord) (actorlayer.Envelope, error) {
	var failure questioncmd.Failure
	if err := json.Unmarshal([]byte(record.FailureJSON), &failure); err != nil {
		return actorlayer.Envelope{}, fmt.Errorf("decode question failure: %w", err)
	}
	return failedEnvelopeFromRecordWithFailure(record, failure)
}

func failedEnvelopeFromRecordWithFailure(record baldastate.QuestionRecord, failure questioncmd.Failure) (actorlayer.Envelope, error) {
	var interaction questioncmd.InteractionContext
	if err := json.Unmarshal([]byte(record.InteractionJSON), &interaction); err != nil {
		return actorlayer.Envelope{}, fmt.Errorf("decode failed question interaction: %w", err)
	}
	var resume questioncmd.ResumeTarget
	if err := json.Unmarshal([]byte(record.ResumeJSON), &resume); err != nil {
		return actorlayer.Envelope{}, fmt.Errorf("decode failed question resume: %w", err)
	}
	return questioncmd.FailedEnvelope(resume, interaction, record.QuestionID, failure)
}

func (s *Service) clearControls(ctx context.Context, record baldastate.QuestionRecord) {
	if s == nil || s.controls == nil || strings.TrimSpace(record.ProviderMessageID) == "" {
		return
	}
	var interaction questioncmd.InteractionContext
	if err := json.Unmarshal([]byte(record.InteractionJSON), &interaction); err != nil {
		s.logger.Warn().Err(err).Str("question_id", record.QuestionID).Msg("decode question interaction for control cleanup")
		return
	}
	if err := s.controls.ClearQuestionControls(ctx, ControlClearRequest{
		QuestionID:        strings.TrimSpace(record.QuestionID),
		Locator:           interaction.Locator,
		ProviderMessageID: strings.TrimSpace(record.ProviderMessageID),
		ControlHandle:     strings.TrimSpace(record.ControlHandle),
	}); err != nil {
		s.logger.Warn().Err(err).Str("question_id", record.QuestionID).Msg("clear settled question controls")
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func zeroOrNow(v time.Time, fallback time.Time) time.Time {
	if v.IsZero() {
		return fallback
	}
	return v.UTC()
}
