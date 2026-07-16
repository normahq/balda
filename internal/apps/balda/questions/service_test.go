package questions

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/baldaworks/go-actorlayer"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/questioncmd"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/rs/zerolog"
)

type fakeStore struct {
	record     baldastate.QuestionRecord
	replyMatch baldastate.QuestionRecord
}

type fakeScheduledStore struct {
	record baldastate.ScheduledJobRecord
}

func (f *fakeScheduledStore) Upsert(_ context.Context, record baldastate.ScheduledJobRecord) error {
	f.record = record
	return nil
}

func (f *fakeStore) CreatePendingQuestion(_ context.Context, record baldastate.QuestionRecord) error {
	f.record = record
	return nil
}
func (f *fakeStore) BindQuestionDeliveryRef(_ context.Context, questionID string, ref questioncmd.DeliveryRef) error {
	f.record.QuestionID = questionID
	f.record.Provider = ref.Provider
	f.record.ConversationKey = ref.ConversationKey
	f.record.ProviderMessageID = ref.ProviderMessageID
	f.record.ControlHandle = ref.ControlHandle
	return nil
}
func (f *fakeStore) GetQuestionByID(_ context.Context, questionID string) (baldastate.QuestionRecord, bool, error) {
	return f.record, f.record.QuestionID == questionID, nil
}
func (f *fakeStore) GetPendingQuestionByReplyRef(_ context.Context, provider, conversationKey, replyToMessageID string) (baldastate.QuestionRecord, bool, error) {
	if f.replyMatch.Provider == provider && f.replyMatch.ConversationKey == conversationKey && f.replyMatch.ProviderMessageID == replyToMessageID {
		return f.replyMatch, true, nil
	}
	return baldastate.QuestionRecord{}, false, nil
}
func (f *fakeStore) MarkQuestionAnswered(_ context.Context, questionID string, answer questioncmd.Answer) (baldastate.QuestionRecord, bool, error) {
	target := &f.replyMatch
	if target.QuestionID != questionID {
		target = &f.record
	}
	if target.QuestionID != questionID || target.Status != questioncmd.StatusPending {
		return baldastate.QuestionRecord{}, false, nil
	}
	target.Status = questioncmd.StatusAnswered
	target.AnswerJSON = mustJSON(answer)
	return *target, true, nil
}

type fakeControlPublisher struct {
	requests []ControlClearRequest
}

func (f *fakeControlPublisher) ClearQuestionControls(_ context.Context, request ControlClearRequest) error {
	f.requests = append(f.requests, request)
	return nil
}
func (f *fakeStore) MarkQuestionTimedOut(_ context.Context, questionID string, timedOutAt time.Time) (baldastate.QuestionRecord, bool, error) {
	if f.record.QuestionID != questionID {
		return baldastate.QuestionRecord{}, false, nil
	}
	f.record.Status = questioncmd.StatusTimedOut
	f.record.AnsweredAt = timedOutAt
	return f.record, true, nil
}

func (f *fakeStore) MarkQuestionFailed(_ context.Context, questionID string, failure questioncmd.Failure) (baldastate.QuestionRecord, bool, error) {
	if f.record.QuestionID != questionID {
		return baldastate.QuestionRecord{}, false, nil
	}
	if f.record.Status != questioncmd.StatusPending {
		return f.record, false, nil
	}
	f.record.Status = questioncmd.StatusFailed
	f.record.FailureJSON = mustJSON(failure)
	f.record.FailedAt = failure.FailedAt
	return f.record, true, nil
}

func TestServiceFailDeliverySettlesAndBuildsContinuation(t *testing.T) {
	store := &fakeStore{record: selectableQuestionRecord()}
	store.record.ResumeJSON = mustJSON(questioncmd.ResumeTarget{To: "permission:review-1"})
	service := New(store, nil, zerolog.Nop())
	failure := questioncmd.Failure{Code: "delivery_failed", Message: "ephemeral unavailable", FailedAt: time.Date(2026, 7, 16, 1, 0, 0, 0, time.UTC)}

	envelope, failed, err := service.FailDelivery(context.Background(), "question-1", failure)
	if err != nil {
		t.Fatalf("FailDelivery() error = %v", err)
	}
	if !failed || store.record.Status != questioncmd.StatusFailed {
		t.Fatalf("failed = %v record = %+v", failed, store.record)
	}
	if envelope.DedupeKey != "question:question-1:failed" {
		t.Fatalf("dedupe key = %q", envelope.DedupeKey)
	}
	var continuation questioncmd.FailedContinuation
	if err := actorlayer.UnmarshalPayload(envelope.Payload, &continuation); err != nil {
		t.Fatalf("decode continuation: %v", err)
	}
	if continuation.Failure.Code != "delivery_failed" || continuation.Resume.To != "permission:review-1" {
		t.Fatalf("continuation = %+v", continuation)
	}
}

func TestServiceAskCreatesPendingRecord(t *testing.T) {
	store := &fakeStore{}
	scheduled := &fakeScheduledStore{}
	svc := New(store, scheduled, zerolog.Nop())
	now := time.Date(2026, 7, 14, 5, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	record, err := svc.Ask(context.Background(), questioncmd.InteractionContext{
		SessionID:   "tg-1-0",
		ChannelKind: "telegram",
		Locator:     deliverycmd.Locator{SessionID: "tg-1-0", ChannelType: "telegram", AddressKey: "1:0", AddressJSON: `{"chat_id":1,"topic_id":0}`},
	}, questioncmd.ResumeTarget{To: "goalkeeper:job-1"}, questioncmd.Request{
		Prompt:  "continue?",
		Timeout: time.Minute,
	})
	if err != nil {
		t.Fatalf("Ask() error = %v", err)
	}
	if record.Status != questioncmd.StatusPending {
		t.Fatalf("status = %q, want pending", record.Status)
	}
	if record.ExpiresAt.IsZero() {
		t.Fatal("expires_at = zero, want timeout recorded")
	}
	if scheduled.record.JobID == "" {
		t.Fatal("scheduled timeout job = empty, want one-shot scheduled job")
	}
}

func TestServiceResolveReplySettlesPendingQuestion(t *testing.T) {
	store := &fakeStore{
		replyMatch: baldastate.QuestionRecord{
			QuestionID:        "question-1",
			Status:            questioncmd.StatusPending,
			Provider:          "telegram",
			ConversationKey:   "1:0",
			ProviderMessageID: "42",
		},
	}
	svc := New(store, nil, zerolog.Nop())
	record, ok, err := svc.ResolveReply(context.Background(), questioncmd.InboundReply{
		Provider:         "telegram",
		ConversationKey:  "1:0",
		ReplyToMessageID: "42",
		MessageID:        "43",
		Text:             "yes",
	})
	if err != nil {
		t.Fatalf("ResolveReply() error = %v", err)
	}
	if !ok {
		t.Fatal("ResolveReply() matched = false, want true")
	}
	if record.Status != questioncmd.StatusAnswered {
		t.Fatalf("status = %q, want answered", record.Status)
	}
}

func TestServiceResolveReplyRejectsDifferentRequester(t *testing.T) {
	store := &fakeStore{replyMatch: baldastate.QuestionRecord{
		QuestionID:        "question-1",
		Status:            questioncmd.StatusPending,
		Provider:          "telegram",
		ConversationKey:   "1:0",
		ProviderMessageID: "42",
		RequestJSON:       mustJSON(questioncmd.Request{Responder: questioncmd.ResponderRequester, AllowFreeText: true}),
		InteractionJSON:   mustJSON(questioncmd.InteractionContext{RequestedBy: questioncmd.UserRef{UserID: "tg-101"}}),
	}}
	result, err := New(store, nil, zerolog.Nop()).ResolveReplyDetailed(context.Background(), questioncmd.InboundReply{
		Provider:         "telegram",
		ConversationKey:  "1:0",
		ReplyToMessageID: "42",
		User:             questioncmd.UserRef{UserID: "tg-202"},
		Text:             "yes",
	})
	if err != nil {
		t.Fatalf("ResolveReplyDetailed() error = %v", err)
	}
	if !result.Matched || !result.Invalid || result.Settled {
		t.Fatalf("resolution = %+v, want matched invalid unsettled", result)
	}
	if store.replyMatch.Status != questioncmd.StatusPending {
		t.Fatalf("status = %q, want pending", store.replyMatch.Status)
	}
}

func TestServiceResolveReplyMapsOptionNumber(t *testing.T) {
	store := &fakeStore{replyMatch: baldastate.QuestionRecord{
		QuestionID:        "question-1",
		Status:            questioncmd.StatusPending,
		Provider:          "telegram",
		ConversationKey:   "1:0",
		ProviderMessageID: "42",
		RequestJSON: mustJSON(questioncmd.Request{Options: []questioncmd.Option{
			{ID: "allow", Label: "Allow once"},
			{ID: "reject", Label: "Reject once"},
		}}),
	}}
	result, err := New(store, nil, zerolog.Nop()).ResolveReplyDetailed(context.Background(), questioncmd.InboundReply{
		Provider:         "telegram",
		ConversationKey:  "1:0",
		ReplyToMessageID: "42",
		Text:             "2",
	})
	if err != nil {
		t.Fatalf("ResolveReplyDetailed() error = %v", err)
	}
	if !result.Settled {
		t.Fatalf("resolution = %+v, want settled", result)
	}
	var answer questioncmd.Answer
	if err := json.Unmarshal([]byte(result.Record.AnswerJSON), &answer); err != nil {
		t.Fatalf("decode answer: %v", err)
	}
	if answer.SelectedOption != "reject" {
		t.Fatalf("selected option = %q, want reject", answer.SelectedOption)
	}
}

func TestServiceResolveSelectionSettlesPersistedOptionAndClearsControls(t *testing.T) {
	store := &fakeStore{record: selectableQuestionRecord()}
	controls := &fakeControlPublisher{}
	service := New(store, nil, zerolog.Nop())
	service.SetControlPublisher(controls)

	result, err := service.ResolveSelectionDetailed(context.Background(), questioncmd.InboundSelection{
		Provider:          "telegram",
		SessionID:         "tg-1-0",
		ConversationKey:   "1:0",
		QuestionID:        "question-1",
		ProviderMessageID: "42",
		User:              questioncmd.UserRef{UserID: "tg-101"},
		OptionIndex:       2,
	})
	if err != nil {
		t.Fatalf("ResolveSelectionDetailed() error = %v", err)
	}
	if !result.Matched || !result.Settled || result.Invalid || result.Inactive {
		t.Fatalf("resolution = %+v, want matched settled", result)
	}
	var answer questioncmd.Answer
	if err := json.Unmarshal([]byte(result.Record.AnswerJSON), &answer); err != nil {
		t.Fatalf("decode answer: %v", err)
	}
	if answer.SelectedOption != "cancel" || answer.Text != "Cancel" {
		t.Fatalf("answer = %+v, want cancel selection", answer)
	}
	if len(controls.requests) != 1 || controls.requests[0].QuestionID != "question-1" || controls.requests[0].ProviderMessageID != "42" {
		t.Fatalf("control cleanup = %+v", controls.requests)
	}
}

func TestServiceResolveSelectionRejectsWrongRequesterAndContext(t *testing.T) {
	for _, test := range []struct {
		name      string
		selection questioncmd.InboundSelection
	}{
		{
			name: "requester",
			selection: questioncmd.InboundSelection{
				Provider: "telegram", SessionID: "tg-1-0", ConversationKey: "1:0", QuestionID: "question-1", ProviderMessageID: "42",
				User: questioncmd.UserRef{UserID: "tg-202"}, OptionIndex: 1,
			},
		},
		{
			name: "conversation",
			selection: questioncmd.InboundSelection{
				Provider: "telegram", SessionID: "tg-2-0", ConversationKey: "2:0", QuestionID: "question-1", ProviderMessageID: "42",
				User: questioncmd.UserRef{UserID: "tg-101"}, OptionIndex: 1,
			},
		},
		{
			name: "option index",
			selection: questioncmd.InboundSelection{
				Provider: "telegram", SessionID: "tg-1-0", ConversationKey: "1:0", QuestionID: "question-1", ProviderMessageID: "42",
				User: questioncmd.UserRef{UserID: "tg-101"}, OptionIndex: 99,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &fakeStore{record: selectableQuestionRecord()}
			result, err := New(store, nil, zerolog.Nop()).ResolveSelectionDetailed(context.Background(), test.selection)
			if err != nil {
				t.Fatalf("ResolveSelectionDetailed() error = %v", err)
			}
			if !result.Matched || !result.Invalid || result.Settled {
				t.Fatalf("resolution = %+v, want invalid unsettled", result)
			}
		})
	}
}

func TestServiceResolveSelectionReportsInactiveAndRetriesCleanup(t *testing.T) {
	record := selectableQuestionRecord()
	record.Status = questioncmd.StatusAnswered
	store := &fakeStore{record: record}
	controls := &fakeControlPublisher{}
	service := New(store, nil, zerolog.Nop())
	service.SetControlPublisher(controls)

	result, err := service.ResolveSelectionDetailed(context.Background(), questioncmd.InboundSelection{
		QuestionID: "question-1", SessionID: "tg-1-0", ConversationKey: "1:0", ProviderMessageID: "42", OptionIndex: 1,
	})
	if err != nil {
		t.Fatalf("ResolveSelectionDetailed() error = %v", err)
	}
	if !result.Matched || !result.Inactive || result.Settled {
		t.Fatalf("resolution = %+v, want inactive", result)
	}
	if len(controls.requests) != 1 {
		t.Fatalf("cleanup requests = %d, want 1", len(controls.requests))
	}
}

func TestServiceBindDeliveryCleansControlsIfSelectionWonDeliveryRace(t *testing.T) {
	record := selectableQuestionRecord()
	record.Status = questioncmd.StatusAnswered
	record.Provider = ""
	record.ProviderMessageID = ""
	store := &fakeStore{record: record}
	controls := &fakeControlPublisher{}
	service := New(store, nil, zerolog.Nop())
	service.SetControlPublisher(controls)

	err := service.BindDelivery(context.Background(), "question-1", questioncmd.DeliveryRef{
		Provider: "telegram", ConversationKey: "1:0", ProviderMessageID: "42", ControlHandle: "telegram:message:delete",
	})
	if err != nil {
		t.Fatalf("BindDelivery() error = %v", err)
	}
	if len(controls.requests) != 1 || controls.requests[0].ProviderMessageID != "42" || controls.requests[0].ControlHandle != "telegram:message:delete" {
		t.Fatalf("cleanup requests = %+v", controls.requests)
	}
}

func selectableQuestionRecord() baldastate.QuestionRecord {
	interaction := questioncmd.InteractionContext{
		SessionID:   "tg-1-0",
		ChannelKind: "telegram",
		Locator: deliverycmd.Locator{
			SessionID: "tg-1-0", ChannelType: "telegram", AddressKey: "1:0", AddressJSON: `{"chat_id":1,"topic_id":0}`,
		},
		RequestedBy: questioncmd.UserRef{UserID: "tg-101"},
	}
	return baldastate.QuestionRecord{
		QuestionID:        "question-1",
		SessionID:         "tg-1-0",
		Status:            questioncmd.StatusPending,
		Provider:          "telegram",
		ConversationKey:   "1:0",
		ProviderMessageID: "42",
		InteractionJSON:   mustJSON(interaction),
		ResumeJSON:        mustJSON(questioncmd.ResumeTarget{To: "permission:review-1"}),
		RequestJSON: mustJSON(questioncmd.Request{
			Responder: questioncmd.ResponderRequester,
			Options:   []questioncmd.Option{{ID: "allow", Label: "Allow"}, {ID: "cancel", Label: "Cancel"}},
		}),
	}
}
