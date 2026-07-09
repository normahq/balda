package actors

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/pkg/actorlayer"
)

func TestSessionActorInterruptQueueModeCancelsSessionBeforeEnqueue(t *testing.T) {
	t.Parallel()

	turns := &fakeTurnDispatcher{}
	exec := &sessionActorExecutor{
		turns:  turns,
		runner: fakeSessionTurnRunner{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := exec.enqueueTurn(ctx, testSessionTurnEnvelope(t, map[string]string{"queue_mode": baldaexecution.QueueModeInterrupt}))
	if err == nil {
		t.Fatal("enqueueTurn() error = nil, want canceled context after enqueue")
	}
	if len(turns.cancelCalls) != 1 {
		t.Fatalf("CancelSession calls = %d, want 1", len(turns.cancelCalls))
	}
	if got := turns.cancelCalls[0]; got.SessionID != "tg-9001-77" || !got.ClearQueued {
		t.Fatalf("CancelSession call = %+v, want session=tg-9001-77 clearQueued=true", got)
	}
	if len(turns.enqueueCalls) != 1 {
		t.Fatalf("Enqueue calls = %d, want 1", len(turns.enqueueCalls))
	}
}

func TestSessionActorDefaultQueueModeDoesNotCancelSession(t *testing.T) {
	t.Parallel()

	turns := &fakeTurnDispatcher{}
	exec := &sessionActorExecutor{
		turns:  turns,
		runner: fakeSessionTurnRunner{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := exec.enqueueTurn(ctx, testSessionTurnEnvelope(t, nil))
	if err == nil {
		t.Fatal("enqueueTurn() error = nil, want canceled context after enqueue")
	}
	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0", len(turns.cancelCalls))
	}
	if len(turns.enqueueCalls) != 1 {
		t.Fatalf("Enqueue calls = %d, want 1", len(turns.enqueueCalls))
	}
}

func TestSessionActorRejectsMismatchedEnvelopeAndPayloadJobID(t *testing.T) {
	t.Parallel()

	exec := &sessionActorExecutor{}
	env := testSessionTurnEnvelopeWithJobID(t, nil, "task-payload", sessionTurnSourceWebhook)
	env.Namespace = baldaexecution.NamespaceWebhookInbound
	env.Meta = baldaexecution.WithJobIDMeta(nil, "task-envelope")

	err := exec.enqueueTurn(context.Background(), env)
	if err == nil {
		t.Fatal("enqueueTurn() error = nil, want policy error")
	}
	if got, want := actorlayer.ClassifyError(err), actorlayer.ErrorKindPolicy; got != want {
		t.Fatalf("enqueueTurn() error kind = %q, want %q (err=%v)", got, want, err)
	}
}

func TestSessionActorSettleSessionTurnResultMarksTaskFailedWithoutRetry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	provider, bus, dispatcher, tasks, allocator := newTaskActorRuntimeServices(t, ctx)
	_ = provider
	_ = bus
	_ = dispatcher
	_ = allocator
	created, err := tasks.Create(ctx, baldastate.JobRecord{
		ID:        "task-session-failed",
		SessionID: "tg-9001-77",
		Objective: "run session task",
		Status:    baldastate.JobStatusRunning,
	}, "test", nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if !created {
		t.Fatal("Create() created = false, want true")
	}

	exec := &sessionActorExecutor{tasks: tasks}
	runErr := errors.New("runner failed")
	env := testSessionTurnEnvelopeWithJobID(t, nil, "task-session-failed", sessionTurnSourceWebhook)
	env.Namespace = baldaexecution.NamespaceWebhookInbound
	payload := SessionTurnPayload{JobID: "task-session-failed", Source: sessionTurnSourceWebhook}

	if err := exec.settleSessionTurnResult(ctx, env, payload, runErr); err != nil {
		t.Fatalf("settleSessionTurnResult() error = %v, want nil after recording task failure", err)
	}

	task, ok, err := tasks.Get(ctx, baldaexecution.EnvelopeJobID(env))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatalf("Get() found = false for task %q", baldaexecution.EnvelopeJobID(env))
	}
	if task.Status != baldastate.JobStatusFailed {
		t.Fatalf("task status = %q, want %q", task.Status, baldastate.JobStatusFailed)
	}
}

func TestSessionActorSettleSessionTurnResultMarksTaskCanceledWithoutRetry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	provider, bus, dispatcher, tasks, allocator := newTaskActorRuntimeServices(t, ctx)
	_ = provider
	_ = bus
	_ = dispatcher
	_ = allocator
	created, err := tasks.Create(ctx, baldastate.JobRecord{
		ID:        "task-session-canceled",
		SessionID: "tg-9001-77",
		Objective: "run session task",
		Status:    baldastate.JobStatusRunning,
	}, "test", nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if !created {
		t.Fatal("Create() created = false, want true")
	}

	exec := &sessionActorExecutor{tasks: tasks}
	env := testSessionTurnEnvelopeWithJobID(t, nil, "task-session-canceled", sessionTurnSourceWebhook)
	env.Namespace = baldaexecution.NamespaceWebhookInbound
	payload := SessionTurnPayload{JobID: "task-session-canceled", Source: sessionTurnSourceWebhook}

	if err := exec.settleSessionTurnResult(ctx, env, payload, context.Canceled); err != nil {
		t.Fatalf("settleSessionTurnResult() error = %v, want nil after recording task cancellation", err)
	}

	task, ok, err := tasks.Get(ctx, baldaexecution.EnvelopeJobID(env))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatalf("Get() found = false for task %q", baldaexecution.EnvelopeJobID(env))
	}
	if task.Status != baldastate.JobStatusCanceled {
		t.Fatalf("task status = %q, want %q", task.Status, baldastate.JobStatusCanceled)
	}
}

func TestSessionActorSettleSessionTurnResultKeepsNonTaskErrorsRetryable(t *testing.T) {
	t.Parallel()

	exec := &sessionActorExecutor{}
	runErr := errors.New("runner failed")

	err := exec.settleSessionTurnResult(context.Background(), testSessionTurnEnvelope(t, nil), SessionTurnPayload{}, runErr)
	if !errors.Is(err, runErr) {
		t.Fatalf("settleSessionTurnResult() error = %v, want original run error", err)
	}
}

func TestSessionActorSettleSessionTurnResultKeepsHumanTurnErrorsRetryableEvenWithJobID(t *testing.T) {
	t.Parallel()

	exec := &sessionActorExecutor{}
	env := testSessionTurnEnvelopeWithJobID(t, nil, "turn-legacy-1", sessionTurnSourceTelegram)
	runErr := errors.New("runner failed")

	err := exec.settleSessionTurnResult(context.Background(), env, SessionTurnPayload{Source: sessionTurnSourceTelegram}, runErr)
	if !errors.Is(err, runErr) {
		t.Fatalf("settleSessionTurnResult() error = %v, want original run error", err)
	}
}

func TestSessionActorSettleSessionTurnResultRecordsScheduledTaskOutcome(t *testing.T) {
	t.Parallel()

	recorder := &fakeScheduledTaskRecorder{}
	exec := &sessionActorExecutor{scheduler: recorder}
	payload := SessionTurnPayload{JobID: "runtime-task-1", ScheduledJobID: "scheduled-1"}
	env := testSessionTurnEnvelopeWithJobID(t, nil, "runtime-task-1", sessionTurnSourceSchedule)

	if err := exec.settleSessionTurnResult(context.Background(), env, payload, nil); err != nil {
		t.Fatalf("settleSessionTurnResult(success) error = %v", err)
	}
	if len(recorder.successes) != 1 || recorder.successes[0] != "scheduled-1" {
		t.Fatalf("successes = %#v, want [scheduled-1]", recorder.successes)
	}
	if len(recorder.failures) != 0 {
		t.Fatalf("failures = %d, want 0", len(recorder.failures))
	}

	runErr := errors.New("scheduled run failed")
	if err := exec.settleSessionTurnResult(context.Background(), env, payload, runErr); err != nil {
		t.Fatalf("settleSessionTurnResult(failure) error = %v, want nil after recording scheduled task failure", err)
	}
	if len(recorder.failures) != 1 {
		t.Fatalf("failures = %d, want 1", len(recorder.failures))
	}
	if got := recorder.failures[0]; got.taskID != "scheduled-1" || !errors.Is(got.cause, runErr) {
		t.Fatalf("failure = %+v, want task scheduled-1 with original error", got)
	}
}

func TestSessionActorEnqueueTurnSkipsDeadLetteredTask(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	provider, bus, dispatcher, tasks, allocator := newTaskActorRuntimeServices(t, ctx)
	_ = provider
	_ = bus
	_ = dispatcher
	_ = allocator
	if _, err := tasks.Create(ctx, baldastate.JobRecord{
		ID:        "task-session-deadlettered",
		SessionID: "tg-9001-77",
		Objective: "run session task",
		Status:    baldastate.JobStatusDeadLettered,
	}, "test", nil); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	turns := &fakeTurnDispatcher{}
	exec := &sessionActorExecutor{
		turns:  turns,
		runner: fakeSessionTurnRunner{},
		tasks:  tasks,
	}
	env := testSessionTurnEnvelopeWithJobID(t, nil, "task-session-deadlettered", sessionTurnSourceWebhook)
	env.Namespace = baldaexecution.NamespaceWebhookInbound

	if err := exec.enqueueTurn(ctx, env); err != nil {
		t.Fatalf("enqueueTurn() error = %v, want nil noop for deadlettered task", err)
	}
	if len(turns.enqueueCalls) != 0 {
		t.Fatalf("Enqueue calls = %d, want 0 for deadlettered task", len(turns.enqueueCalls))
	}
}

func testSessionTurnEnvelope(t *testing.T, meta map[string]string) actorlayer.Envelope {
	t.Helper()

	locator := baldasession.SessionLocator{
		ChannelType: "telegram",
		AddressKey:  "tg-9001-77",
		AddressJSON: `{"chat_id":9001,"topic_id":77}`,
		SessionID:   "tg-9001-77",
	}
	payload, err := json.Marshal(SessionTurnPayload{
		Text:    "run this",
		Locator: locator,
		Deliver: false,
		Source:  sessionTurnSourceTelegram,
	})
	if err != nil {
		t.Fatalf("Marshal(SessionTurnPayload) error = %v", err)
	}
	return actorlayer.Envelope{
		ID:          "session-command-1",
		Namespace:   baldaexecution.NamespaceHumanInbound,
		Kind:        baldaexecution.KindMessage,
		From:        actorlayer.ActorAddress{Target: "telegram", Key: "101"},
		To:          actorlayer.ActorAddress{Target: baldaexecution.ActorTypeSession, Key: locator.SessionID},
		SessionID:   locator.SessionID,
		PayloadJSON: string(payload),
		Meta:        meta,
	}
}

func testSessionTurnEnvelopeWithJobID(t *testing.T, meta map[string]string, jobID string, source string) actorlayer.Envelope {
	t.Helper()
	env := testSessionTurnEnvelope(t, meta)
	var payload SessionTurnPayload
	if err := json.Unmarshal([]byte(env.PayloadJSON), &payload); err != nil {
		t.Fatalf("Unmarshal(SessionTurnPayload) error = %v", err)
	}
	payload.JobID = jobID
	env.Meta = baldaexecution.WithJobIDMeta(env.Meta, jobID)
	if strings.TrimSpace(source) != "" {
		payload.Source = source
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal(SessionTurnPayload with JobID) error = %v", err)
	}
	env.PayloadJSON = string(data)
	return env
}

type fakeSessionTurnRunner struct{}

func (fakeSessionTurnRunner) RunSessionTurnPayload(context.Context, SessionTurnPayload) error {
	return nil
}

type fakeScheduledTaskRecorder struct {
	successes []string
	failures  []scheduledTaskFailure
}

type scheduledTaskFailure struct {
	taskID string
	cause  error
}

func (f *fakeScheduledTaskRecorder) MarkSuccess(_ context.Context, taskID string) error {
	f.successes = append(f.successes, taskID)
	return nil
}

func (f *fakeScheduledTaskRecorder) RecordExecutionFailure(_ context.Context, taskID string, cause error) error {
	f.failures = append(f.failures, scheduledTaskFailure{taskID: taskID, cause: cause})
	return nil
}
