package jobs

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/baldaworks/go-actorlayer"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
)

type recordingJobCommandBus struct {
	mu       sync.Mutex
	subjects []string
	envs     []actorlayer.Envelope
	errs     []error
}

func (b *recordingJobCommandBus) PublishEvent(_ context.Context, subject string, env actorlayer.Envelope) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subjects = append(b.subjects, subject)
	b.envs = append(b.envs, env)
	if len(b.errs) > 0 {
		err := b.errs[0]
		b.errs = b.errs[1:]
		return err
	}
	return nil
}

func TestJobEventsServiceAppendEventPublishesDurableEvent(t *testing.T) {
	ctx := context.Background()
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	bus := &recordingJobCommandBus{}
	service, err := NewJobEventsService(jobEventsServiceParams{Store: provider.Jobs(), Bus: bus})
	if err != nil {
		t.Fatalf("NewJobEventsService() error = %v", err)
	}
	if err := service.AppendEvent(ctx, "task-1", JobEventAgentProgress, "agent:executor", "msg-1", map[string]any{"text": "working"}); err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}
	if len(bus.subjects) != 1 || bus.subjects[0] != baldaexecution.SubjectEventJobUpdated {
		t.Fatalf("subjects = %+v, want %q", bus.subjects, baldaexecution.SubjectEventJobUpdated)
	}
	if len(bus.envs) != 1 || baldaexecution.EnvelopeJobID(bus.envs[0]) != "task-1" || bus.envs[0].Meta["event_type"] != JobEventAgentProgress {
		t.Fatalf("envs = %+v, want job event envelope", bus.envs)
	}
	events, err := provider.Jobs().ListJobEvents(ctx, "task-1")
	if err != nil {
		t.Fatalf("ListJobEvents() error = %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("projected events = %+v, want no direct SQLite projection writes", events)
	}
}

func TestJobEventsServiceAppendEventPublishesDeliveryFailedSubject(t *testing.T) {
	ctx := context.Background()
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	bus := &recordingJobCommandBus{}
	service, err := NewJobEventsService(jobEventsServiceParams{Store: provider.Jobs(), Bus: bus})
	if err != nil {
		t.Fatalf("NewJobEventsService() error = %v", err)
	}
	if err := service.AppendEvent(ctx, "task-1", JobEventDeliveryFailed, "delivery.actor", "msg-1", map[string]any{"reason": "telegram send failed"}); err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}
	if len(bus.subjects) != 1 || bus.subjects[0] != baldaexecution.SubjectEventDeliveryFailed {
		t.Fatalf("subjects = %+v, want %q", bus.subjects, baldaexecution.SubjectEventDeliveryFailed)
	}
}

func TestJobEventsServiceAppendEventUsesDeterministicIDsExceptProgress(t *testing.T) {
	ctx := context.Background()
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	bus := &recordingJobCommandBus{}
	service, err := NewJobEventsService(jobEventsServiceParams{Store: provider.Jobs(), Bus: bus})
	if err != nil {
		t.Fatalf("NewJobEventsService() error = %v", err)
	}

	payload := map[string]any{"step": "worker", "iteration": 1}
	if err := service.AppendEvent(ctx, "task-1", JobEventAgentStarted, "goal.actor", "task-1:goal:worker:1", payload); err != nil {
		t.Fatalf("AppendEvent(first started) error = %v", err)
	}
	if err := service.AppendEvent(ctx, "task-1", JobEventAgentStarted, "goal.actor", "task-1:goal:worker:1", payload); err != nil {
		t.Fatalf("AppendEvent(second started) error = %v", err)
	}
	if got := bus.envs[0].ID; got == "" || got != bus.envs[1].ID {
		t.Fatalf("started event ids = %q/%q, want deterministic duplicate id", bus.envs[0].ID, bus.envs[1].ID)
	}

	if err := service.AppendEvent(ctx, "task-1", JobEventAgentProgress, "agent:planner", "", map[string]any{"text": "working"}); err != nil {
		t.Fatalf("AppendEvent(first progress) error = %v", err)
	}
	if err := service.AppendEvent(ctx, "task-1", JobEventAgentProgress, "agent:planner", "", map[string]any{"text": "working"}); err != nil {
		t.Fatalf("AppendEvent(second progress) error = %v", err)
	}
	if got := bus.envs[2].ID; got == "" || got == bus.envs[3].ID {
		t.Fatalf("progress event ids = %q/%q, want append-only unique ids", bus.envs[2].ID, bus.envs[3].ID)
	}
}

func TestJobLifecycleServiceCreateIgnoresEventPublishFailureAfterStateMutation(t *testing.T) {
	ctx := context.Background()
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	bus := &recordingJobCommandBus{errs: []error{errors.New("event stream unavailable")}}
	service, err := NewJobLifecycleService(jobLifecycleServiceParams{Store: provider.Jobs(), Bus: bus})
	if err != nil {
		t.Fatalf("NewJobLifecycleService() error = %v", err)
	}
	record := baldastate.JobRecord{ID: "task-created", SessionID: "s-1", Objective: "create task"}
	created, err := service.Create(ctx, record, "task.actor", map[string]any{"objective": record.Objective})
	if err != nil {
		t.Fatalf("Create(first) error = %v, want nil because job event publication is visibility-only", err)
	}
	if !created {
		t.Fatal("Create(first) created = false, want new task")
	}
	if task, ok, err := service.Get(ctx, record.ID); err != nil || !ok || task.ID != record.ID {
		t.Fatalf("task after failed event publish = %+v found=%v err=%v, want persisted task", task, ok, err)
	}
	created, err = service.Create(ctx, record, "task.actor", map[string]any{"objective": record.Objective})
	if err != nil {
		t.Fatalf("Create(retry) error = %v", err)
	}
	if created {
		t.Fatal("Create(retry) created = true, want existing task")
	}
	if len(bus.envs) != 2 {
		t.Fatalf("published created events = %d, want 2 attempts", len(bus.envs))
	}
	wantEventID := "job:" + record.ID + ":event:created"
	if bus.envs[0].ID != bus.envs[1].ID || bus.envs[1].ID != wantEventID {
		t.Fatalf("event ids = %q/%q, want deterministic created event id", bus.envs[0].ID, bus.envs[1].ID)
	}
}

func TestOutboxPublisherRetriesFailedJobEvent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	bus := &recordingJobCommandBus{errs: []error{errors.New("event stream unavailable")}}
	service, err := NewJobLifecycleService(jobLifecycleServiceParams{Store: provider.Jobs(), Bus: bus})
	if err != nil {
		t.Fatalf("NewJobLifecycleService() error = %v", err)
	}
	record := baldastate.JobRecord{ID: "job-retry", SessionID: "s-1", Objective: "retry event"}
	if _, err := service.Create(ctx, record, "job.actor", nil); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	pending, err := provider.Jobs().ListPendingJobEvents(ctx, 10)
	if err != nil {
		t.Fatalf("ListPendingJobEvents() error = %v", err)
	}
	if len(pending) != 1 || pending[0].Attempts != 1 {
		t.Fatalf("pending events = %+v, want one failed attempt", pending)
	}

	publisher := &OutboxPublisher{store: provider.Jobs(), bus: bus, batchSize: 10}
	if err := publisher.Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	pending, err = provider.Jobs().ListPendingJobEvents(ctx, 10)
	if err != nil {
		t.Fatalf("ListPendingJobEvents(after retry) error = %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending events after retry = %+v, want none", pending)
	}
	if len(bus.envs) != 2 || bus.envs[0].ID != bus.envs[1].ID {
		t.Fatalf("published events = %+v, want two attempts with stable event id", bus.envs)
	}
}

func TestJobLifecycleServiceMarkStatusIgnoresEventPublishFailureAfterStateMutation(t *testing.T) {
	ctx := context.Background()
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	if _, err := provider.Jobs().CreateJob(ctx, baldastate.JobRecord{
		ID:        "task-running",
		SessionID: "s-1",
		Objective: "run task",
		Status:    baldastate.JobStatusCreated,
	}); err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	bus := &recordingJobCommandBus{errs: []error{errors.New("event stream unavailable")}}
	service, err := NewJobLifecycleService(jobLifecycleServiceParams{Store: provider.Jobs(), Bus: bus})
	if err != nil {
		t.Fatalf("NewJobLifecycleService() error = %v", err)
	}

	if err := service.MarkStatus(ctx, "task-running", baldastate.JobStatusRunning, "task.actor", "msg-1", "", map[string]any{"step": "start"}); err != nil {
		t.Fatalf("MarkStatus() error = %v, want nil because job event publication is visibility-only", err)
	}
	task, ok, err := service.Get(ctx, "task-running")
	if err != nil || !ok {
		t.Fatalf("Get() job = %+v found=%v err=%v", task, ok, err)
	}
	if task.Status != baldastate.JobStatusRunning {
		t.Fatalf("job status = %q, want %q", task.Status, baldastate.JobStatusRunning)
	}
	if len(bus.envs) != 1 || bus.envs[0].Meta["event_type"] != JobEventStarted {
		t.Fatalf("published events = %+v, want one job.started visibility attempt", bus.envs)
	}
}

func TestJobLifecycleServiceSetResultIgnoresEventPublishFailureAfterStateMutation(t *testing.T) {
	ctx := context.Background()
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	if _, err := provider.Jobs().CreateJob(ctx, baldastate.JobRecord{
		ID:        "task-completed",
		SessionID: "s-1",
		Objective: "complete task",
		Status:    baldastate.JobStatusRunning,
	}); err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	bus := &recordingJobCommandBus{errs: []error{errors.New("event stream unavailable")}}
	service, err := NewJobLifecycleService(jobLifecycleServiceParams{Store: provider.Jobs(), Bus: bus})
	if err != nil {
		t.Fatalf("NewJobLifecycleService() error = %v", err)
	}

	result := map[string]any{"summary": "done"}
	if err := service.SetResult(ctx, "task-completed", result, baldastate.JobStatusCompleted, "task.actor", ""); err != nil {
		t.Fatalf("SetResult() error = %v, want nil because job event publication is visibility-only", err)
	}
	task, ok, err := service.Get(ctx, "task-completed")
	if err != nil || !ok {
		t.Fatalf("Get() job = %+v found=%v err=%v", task, ok, err)
	}
	if task.Status != baldastate.JobStatusCompleted {
		t.Fatalf("job status = %q, want %q", task.Status, baldastate.JobStatusCompleted)
	}
	if task.Result == "" {
		t.Fatal("job result json is empty, want persisted result despite event publish failure")
	}
	if len(bus.envs) != 1 || bus.envs[0].Meta["event_type"] != JobEventCompleted {
		t.Fatalf("published events = %+v, want one job.completed visibility attempt", bus.envs)
	}
}

func TestJobLifecycleServiceMarkStatusIgnoresStaleTerminalTransition(t *testing.T) {
	ctx := context.Background()
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	if _, err := provider.Jobs().CreateJob(ctx, baldastate.JobRecord{
		ID:        "task-deadlettered",
		SessionID: "s-1",
		Objective: "deadlettered task",
		Status:    baldastate.JobStatusDeadLettered,
	}); err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	bus := &recordingJobCommandBus{}
	service, err := NewJobLifecycleService(jobLifecycleServiceParams{Store: provider.Jobs(), Bus: bus})
	if err != nil {
		t.Fatalf("NewJobLifecycleService() error = %v", err)
	}

	if err := service.MarkStatus(ctx, "task-deadlettered", baldastate.JobStatusFailed, "task.actor", "msg-1", "runner failed", nil); err != nil {
		t.Fatalf("MarkStatus() error = %v, want nil for stale terminal finalization", err)
	}
	task, ok, err := service.Get(ctx, "task-deadlettered")
	if err != nil || !ok {
		t.Fatalf("Get() job = %+v found=%v err=%v", task, ok, err)
	}
	if task.Status != baldastate.JobStatusDeadLettered {
		t.Fatalf("job status = %q, want %q", task.Status, baldastate.JobStatusDeadLettered)
	}
	if len(bus.envs) != 0 {
		t.Fatalf("published events = %+v, want no visibility event for stale terminal finalization", bus.envs)
	}
}

func TestJobLifecycleServiceSetResultIgnoresStaleTerminalTransition(t *testing.T) {
	ctx := context.Background()
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	if _, err := provider.Jobs().CreateJob(ctx, baldastate.JobRecord{
		ID:        "task-deadlettered-result",
		SessionID: "s-1",
		Objective: "deadlettered task",
		Status:    baldastate.JobStatusDeadLettered,
		Result:    `{"status":"deadlettered"}`,
	}); err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	bus := &recordingJobCommandBus{}
	service, err := NewJobLifecycleService(jobLifecycleServiceParams{Store: provider.Jobs(), Bus: bus})
	if err != nil {
		t.Fatalf("NewJobLifecycleService() error = %v", err)
	}

	if err := service.SetResult(ctx, "task-deadlettered-result", map[string]any{"status": "failed"}, baldastate.JobStatusFailed, "task.actor", "runner failed"); err != nil {
		t.Fatalf("SetResult() error = %v, want nil for stale terminal finalization", err)
	}
	task, ok, err := service.Get(ctx, "task-deadlettered-result")
	if err != nil || !ok {
		t.Fatalf("Get() job = %+v found=%v err=%v", task, ok, err)
	}
	if task.Status != baldastate.JobStatusDeadLettered {
		t.Fatalf("job status = %q, want %q", task.Status, baldastate.JobStatusDeadLettered)
	}
	if task.Result != `{"status":"deadlettered"}` {
		t.Fatalf("job result = %q, want original deadlettered result preserved", task.Result)
	}
	if len(bus.envs) != 0 {
		t.Fatalf("published events = %+v, want no visibility event for stale terminal finalization", bus.envs)
	}
}
