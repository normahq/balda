package goalkeeper

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/baldaworks/go-actorlayer"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/goalcmd"
	"github.com/normahq/balda/internal/apps/balda/goalresultcmd"
	"github.com/normahq/balda/internal/apps/balda/questioncmd"
	"github.com/normahq/balda/internal/apps/balda/questions"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/rs/zerolog"
)

type fakeQuestionDispatcher struct {
	commands []actorlayer.Envelope
}

type fakeQuestionStore struct {
	record baldastate.QuestionRecord
}

func (f *fakeQuestionDispatcher) Dispatch(_ context.Context, env actorlayer.Envelope) (*actortransport.DispatchReceipt, error) {
	f.commands = append(f.commands, env)
	return &actortransport.DispatchReceipt{}, nil
}

func (f *fakeQuestionStore) CreatePendingQuestion(_ context.Context, record baldastate.QuestionRecord) error {
	f.record = record
	return nil
}

func (f *fakeQuestionStore) BindQuestionDeliveryRef(_ context.Context, questionID string, ref questioncmd.DeliveryRef) error {
	f.record.QuestionID = questionID
	f.record.Provider = ref.Provider
	f.record.ConversationKey = ref.ConversationKey
	f.record.ProviderMessageID = ref.ProviderMessageID
	return nil
}

func (f *fakeQuestionStore) GetQuestionByID(_ context.Context, questionID string) (baldastate.QuestionRecord, bool, error) {
	return f.record, f.record.QuestionID == questionID, nil
}

func (*fakeQuestionStore) GetPendingQuestionByReplyRef(_ context.Context, _, _, _ string) (baldastate.QuestionRecord, bool, error) {
	return baldastate.QuestionRecord{}, false, nil
}

func (*fakeQuestionStore) MarkQuestionAnswered(_ context.Context, _ string, _ questioncmd.Answer) (baldastate.QuestionRecord, bool, error) {
	return baldastate.QuestionRecord{}, false, nil
}

func (*fakeQuestionStore) MarkQuestionTimedOut(_ context.Context, _ string, _ time.Time) (baldastate.QuestionRecord, bool, error) {
	return baldastate.QuestionRecord{}, false, nil
}

func TestCoordinatorAskQuestionPersistsAndDispatchesDelivery(t *testing.T) {
	ctx := context.Background()
	store := &fakeQuestionStore{}
	dispatcher := &fakeQuestionDispatcher{}
	jobs := &fakeQuestionJobStore{
		job: baldastate.JobRecord{
			ID:        "goal-tg-1-0-123",
			SessionID: "tg-1-0",
			Status:    baldastate.JobStatusRunning,
		},
	}
	coord := &coordinator{
		jobs:       jobs,
		events:     jobs,
		dispatcher: dispatcher,
		questions:  questions.New(store, nil, zerolog.Nop()),
		logger:     zerolog.Nop(),
	}

	record, err := coord.askQuestion(ctx, goalJobPayload{
		JobID:           "goal-tg-1-0-123",
		Locator:         baldasession.SessionLocator{SessionID: "tg-1-0", ChannelType: "telegram", AddressKey: "1:0", AddressJSON: `{"chat_id":1,"topic_id":0}`},
		TransportUserID: "tg-user-1",
	}, "Нужен доступ?", nil, 0)
	if err != nil {
		t.Fatalf("askQuestion() error = %v", err)
	}
	if record.QuestionID == "" {
		t.Fatal("question id = empty, want persisted record")
	}
	if jobs.job.Status != baldastate.JobStatusWaitingForUser {
		t.Fatalf("job status = %q, want %q", jobs.job.Status, baldastate.JobStatusWaitingForUser)
	}
	if len(dispatcher.commands) != 1 {
		t.Fatalf("dispatch count = %d, want 1", len(dispatcher.commands))
	}
	var payload deliverycmd.Payload
	if err := actorlayer.UnmarshalPayload(dispatcher.commands[0].Payload, &payload); err != nil {
		t.Fatalf("decode delivery payload: %v", err)
	}
	if payload.Refs["question_id"] != record.QuestionID {
		t.Fatalf("refs.question_id = %q, want %q", payload.Refs["question_id"], record.QuestionID)
	}
	if payload.Settlement != deliverycmd.SettlementOutbox {
		t.Fatalf("settlement = %q, want %q", payload.Settlement, deliverycmd.SettlementOutbox)
	}
	if got := store.record.ResumeJSON; !strings.Contains(got, "\"goal_payload\"") {
		t.Fatalf("resume json = %q, want goal payload snapshot metadata", got)
	}
}

func TestCoordinatorAskQuestionWithOptionsDispatchesStructuredQuestion(t *testing.T) {
	ctx := context.Background()
	store := &fakeQuestionStore{}
	dispatcher := &fakeQuestionDispatcher{}
	coord := &coordinator{
		dispatcher: dispatcher,
		questions:  questions.New(store, nil, zerolog.Nop()),
		logger:     zerolog.Nop(),
	}

	record, err := coord.askQuestion(ctx, goalJobPayload{
		JobID:           "goal-tg-1-0-123",
		Locator:         baldasession.SessionLocator{SessionID: "tg-1-0", ChannelType: "telegram", AddressKey: "1:0", AddressJSON: `{"chat_id":1,"topic_id":0}`},
		TransportUserID: "tg-user-1",
	}, "Продолжаем?", []goalresultcmd.WorkerResultOption{{ID: "yes", Label: "Да"}, {ID: "no", Label: "Нет"}}, 0)
	if err != nil {
		t.Fatalf("askQuestion() error = %v", err)
	}
	if record.QuestionID == "" || len(dispatcher.commands) != 1 {
		t.Fatalf("record/dispatch = %+v / %d", record, len(dispatcher.commands))
	}
	var payload deliverycmd.Payload
	if err := actorlayer.UnmarshalPayload(dispatcher.commands[0].Payload, &payload); err != nil {
		t.Fatalf("decode delivery payload: %v", err)
	}
	if payload.Question == nil || len(payload.Question.Options) != 2 {
		t.Fatalf("question = %+v", payload.Question)
	}
}

func TestAppendGoalClarification(t *testing.T) {
	got := appendGoalClarification("ship release", "нужен доступ")
	if !strings.Contains(got, "User clarification:\nнужен доступ") {
		t.Fatalf("clarified objective = %q, want appended clarification", got)
	}
}

func TestGoalResumeEnvelope(t *testing.T) {
	env, err := goalcmd.ResumeEnvelope(goalcmd.JobPayload{
		JobID:           "goal-tg-1-0-123",
		Locator:         baldasession.SessionLocator{SessionID: "tg-1-0"},
		Objective:       "ship release",
		TransportUserID: "101",
		MaxIterations:   2,
	})
	if err != nil {
		t.Fatalf("ResumeEnvelope() error = %v", err)
	}
	if env.To.Key != "goal-tg-1-0-123" {
		t.Fatalf("to.key = %q, want goal-tg-1-0-123", env.To.Key)
	}
}
