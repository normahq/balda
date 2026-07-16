package goalkeeper

import (
	"context"
	"testing"

	"github.com/baldaworks/go-actorlayer"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	"github.com/normahq/balda/internal/apps/balda/goalkeepercmd"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/rs/zerolog"
)

type fakeQuestionJobStore struct {
	job    baldastate.JobRecord
	events []string
}

type fakeContinuationDispatcher struct {
	commands []actorlayer.Envelope
}

func (f *fakeContinuationDispatcher) Dispatch(_ context.Context, env actorlayer.Envelope) (*actortransport.DispatchReceipt, error) {
	f.commands = append(f.commands, env)
	return &actortransport.DispatchReceipt{}, nil
}

func (f *fakeQuestionJobStore) Create(_ context.Context, record baldastate.JobRecord, _ string, _ any) (bool, error) {
	f.job = record
	return true, nil
}

func (f *fakeQuestionJobStore) Get(_ context.Context, jobID string) (baldastate.JobRecord, bool, error) {
	return f.job, f.job.ID == jobID, nil
}

func (f *fakeQuestionJobStore) ListActiveGoalJobsBySession(_ context.Context, sessionID string) ([]baldastate.JobRecord, error) {
	if f.job.SessionID == sessionID {
		return []baldastate.JobRecord{f.job}, nil
	}
	return nil, nil
}

func (f *fakeQuestionJobStore) MarkStatus(_ context.Context, jobID string, status string, _ string, _ string, _ string, _ any) error {
	if f.job.ID == jobID {
		f.job.Status = status
	}
	return nil
}

func (f *fakeQuestionJobStore) SetResult(_ context.Context, jobID string, _ any, status string, _ string, _ string) error {
	if f.job.ID == jobID {
		f.job.Status = status
	}
	return nil
}

func (f *fakeQuestionJobStore) AppendEvent(_ context.Context, jobID string, eventType string, _ string, _ string, _ any) error {
	if f.job.ID == jobID {
		f.events = append(f.events, eventType)
	}
	return nil
}

func TestGoalkeeperAcceptsQuestionContinuation(t *testing.T) {
	ctx := context.Background()
	store := &fakeQuestionJobStore{
		job: baldastate.JobRecord{
			ID:         "goal-tg-1-0-123",
			SessionID:  "tg-1-0",
			Title:      "goal",
			Objective:  "ship release",
			Status:     baldastate.JobStatusRunning,
			OwnerActor: "goalkeeper:goal-tg-1-0-123",
		},
	}
	dispatcher := &fakeContinuationDispatcher{}
	actor := NewActor(ActorParams{
		JobLifecycle:    store,
		JobEvents:       store,
		Dispatcher:      dispatcher,
		SessionManager:  nil,
		GoalRunPreparer: nil,
		JobRuns:         nil,
		MaxIterations:   1,
		Logger:          zerolog.Nop(),
	})
	env, err := goalkeepercmd.QuestionAnsweredEnvelope("goal-tg-1-0-123", "question-1", "да", "2026-07-14T07:00:00Z")
	if err != nil {
		t.Fatalf("QuestionAnsweredEnvelope() error = %v", err)
	}
	env.Meta["goal_payload"] = `{"job_id":"goal-tg-1-0-123","locator":{"session_id":"tg-1-0","channel_type":"telegram","address_key":"1:0","address_json":"{\"chat_id\":1,\"topic_id\":0}"},"objective":"ship release","transport_user_id":"101","max_iterations":1}`
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if store.job.Status != baldastate.JobStatusWaitingForUser {
		t.Fatalf("job status = %q, want %q", store.job.Status, baldastate.JobStatusWaitingForUser)
	}
	if len(store.events) != 1 || store.events[0] != "goal.question.answered" {
		t.Fatalf("events = %v, want [goal.question.answered]", store.events)
	}
	if len(dispatcher.commands) != 1 {
		t.Fatalf("resumed commands = %d, want 1", len(dispatcher.commands))
	}
	var resumed goalkeepercmd.EnvelopePayload
	if err := actorlayer.UnmarshalPayload(dispatcher.commands[0].Payload, &resumed); err != nil {
		t.Fatalf("decode resumed goalkeeper payload: %v", err)
	}
	if resumed.Goal == nil {
		t.Fatal("resumed goalkeeper payload = nil, want goalkeeper payload")
	}
	if resumed.Goal.Objective == "ship release" {
		t.Fatalf("objective = %q, want clarification appended", resumed.Goal.Objective)
	}
}
