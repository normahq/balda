package swarm

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	baldastate "github.com/normahq/balda/internal/apps/balda/state"
)

type recordingTaskCommandBus struct {
	mu       sync.Mutex
	subjects []string
	envs     []Envelope
	errs     []error
}

func (b *recordingTaskCommandBus) PublishEvent(_ context.Context, subject string, env Envelope) error {
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

func TestTaskServiceAppendEventPublishesDurableEvent(t *testing.T) {
	ctx := context.Background()
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	bus := &recordingTaskCommandBus{}
	service, err := NewTaskService(taskServiceParams{StateProvider: provider, Bus: bus})
	if err != nil {
		t.Fatalf("NewTaskService() error = %v", err)
	}
	if err := service.AppendEvent(ctx, "task-1", TaskEventAgentProgress, "agent:executor", "msg-1", map[string]any{"text": "working"}); err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}
	if len(bus.subjects) != 1 || bus.subjects[0] != SubjectEventTaskUpdated {
		t.Fatalf("subjects = %+v, want %q", bus.subjects, SubjectEventTaskUpdated)
	}
	if len(bus.envs) != 1 || bus.envs[0].TaskID != "task-1" || bus.envs[0].Meta["event_type"] != TaskEventAgentProgress {
		t.Fatalf("envs = %+v, want task event envelope", bus.envs)
	}
	events, err := provider.Swarm().ListTaskEvents(ctx, "task-1")
	if err != nil {
		t.Fatalf("ListTaskEvents() error = %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("projected events = %+v, want no direct SQLite projection writes", events)
	}
}

func TestTaskServiceAppendEventPublishesDeliveryFailedSubject(t *testing.T) {
	ctx := context.Background()
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	bus := &recordingTaskCommandBus{}
	service, err := NewTaskService(taskServiceParams{StateProvider: provider, Bus: bus})
	if err != nil {
		t.Fatalf("NewTaskService() error = %v", err)
	}
	if err := service.AppendEvent(ctx, "task-1", TaskEventDeliveryFailed, "delivery.actor", "msg-1", map[string]any{"reason": "telegram send failed"}); err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}
	if len(bus.subjects) != 1 || bus.subjects[0] != SubjectEventDeliveryFailed {
		t.Fatalf("subjects = %+v, want %q", bus.subjects, SubjectEventDeliveryFailed)
	}
}

func TestTaskServiceAppendEventUsesDeterministicIDsExceptProgress(t *testing.T) {
	ctx := context.Background()
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	bus := &recordingTaskCommandBus{}
	service, err := NewTaskService(taskServiceParams{StateProvider: provider, Bus: bus})
	if err != nil {
		t.Fatalf("NewTaskService() error = %v", err)
	}

	payload := map[string]any{"step": "worker", "iteration": 1}
	if err := service.AppendEvent(ctx, "task-1", TaskEventAgentStarted, "goal.actor", "task-1:goal:worker:1", payload); err != nil {
		t.Fatalf("AppendEvent(first started) error = %v", err)
	}
	if err := service.AppendEvent(ctx, "task-1", TaskEventAgentStarted, "goal.actor", "task-1:goal:worker:1", payload); err != nil {
		t.Fatalf("AppendEvent(second started) error = %v", err)
	}
	if got := bus.envs[0].ID; got == "" || got != bus.envs[1].ID {
		t.Fatalf("started event ids = %q/%q, want deterministic duplicate id", bus.envs[0].ID, bus.envs[1].ID)
	}

	if err := service.AppendEvent(ctx, "task-1", TaskEventAgentProgress, "agent:planner", "", map[string]any{"text": "working"}); err != nil {
		t.Fatalf("AppendEvent(first progress) error = %v", err)
	}
	if err := service.AppendEvent(ctx, "task-1", TaskEventAgentProgress, "agent:planner", "", map[string]any{"text": "working"}); err != nil {
		t.Fatalf("AppendEvent(second progress) error = %v", err)
	}
	if got := bus.envs[2].ID; got == "" || got == bus.envs[3].ID {
		t.Fatalf("progress event ids = %q/%q, want append-only unique ids", bus.envs[2].ID, bus.envs[3].ID)
	}
}

func TestTaskServiceCreateIgnoresEventPublishFailureAfterStateMutation(t *testing.T) {
	ctx := context.Background()
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	bus := &recordingTaskCommandBus{errs: []error{errors.New("event stream unavailable")}}
	service, err := NewTaskService(taskServiceParams{StateProvider: provider, Bus: bus})
	if err != nil {
		t.Fatalf("NewTaskService() error = %v", err)
	}
	record := baldastate.SwarmTaskRecord{ID: "task-created", SessionID: "s-1", Objective: "create task"}
	created, err := service.Create(ctx, record, "task.actor", map[string]any{"objective": record.Objective})
	if err != nil {
		t.Fatalf("Create(first) error = %v, want nil because task event publication is visibility-only", err)
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
	if bus.envs[0].ID != bus.envs[1].ID || bus.envs[1].ID != taskCreatedEventID(record.ID) {
		t.Fatalf("event ids = %q/%q, want deterministic created event id", bus.envs[0].ID, bus.envs[1].ID)
	}
}

func TestTaskServiceMarkStatusIgnoresEventPublishFailureAfterStateMutation(t *testing.T) {
	ctx := context.Background()
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	if _, err := provider.Swarm().CreateTask(ctx, baldastate.SwarmTaskRecord{
		ID:        "task-running",
		SessionID: "s-1",
		Objective: "run task",
		Status:    baldastate.SwarmTaskStatusCreated,
	}); err != nil {
		t.Fatalf("CreateTask() error = %v", err)
	}
	bus := &recordingTaskCommandBus{errs: []error{errors.New("event stream unavailable")}}
	service, err := NewTaskService(taskServiceParams{StateProvider: provider, Bus: bus})
	if err != nil {
		t.Fatalf("NewTaskService() error = %v", err)
	}

	if err := service.MarkStatus(ctx, "task-running", baldastate.SwarmTaskStatusRunning, "task.actor", "msg-1", "", map[string]any{"step": "start"}); err != nil {
		t.Fatalf("MarkStatus() error = %v, want nil because task event publication is visibility-only", err)
	}
	task, ok, err := service.Get(ctx, "task-running")
	if err != nil || !ok {
		t.Fatalf("Get() task = %+v found=%v err=%v", task, ok, err)
	}
	if task.Status != baldastate.SwarmTaskStatusRunning {
		t.Fatalf("task status = %q, want %q", task.Status, baldastate.SwarmTaskStatusRunning)
	}
	if len(bus.envs) != 1 || bus.envs[0].Meta["event_type"] != TaskEventTaskStarted {
		t.Fatalf("published events = %+v, want one task.started visibility attempt", bus.envs)
	}
}

func TestTaskServiceSetResultIgnoresEventPublishFailureAfterStateMutation(t *testing.T) {
	ctx := context.Background()
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	if _, err := provider.Swarm().CreateTask(ctx, baldastate.SwarmTaskRecord{
		ID:        "task-completed",
		SessionID: "s-1",
		Objective: "complete task",
		Status:    baldastate.SwarmTaskStatusRunning,
	}); err != nil {
		t.Fatalf("CreateTask() error = %v", err)
	}
	bus := &recordingTaskCommandBus{errs: []error{errors.New("event stream unavailable")}}
	service, err := NewTaskService(taskServiceParams{StateProvider: provider, Bus: bus})
	if err != nil {
		t.Fatalf("NewTaskService() error = %v", err)
	}

	result := map[string]any{"summary": "done"}
	if err := service.SetResult(ctx, "task-completed", result, baldastate.SwarmTaskStatusCompleted, "task.actor", ""); err != nil {
		t.Fatalf("SetResult() error = %v, want nil because task event publication is visibility-only", err)
	}
	task, ok, err := service.Get(ctx, "task-completed")
	if err != nil || !ok {
		t.Fatalf("Get() task = %+v found=%v err=%v", task, ok, err)
	}
	if task.Status != baldastate.SwarmTaskStatusCompleted {
		t.Fatalf("task status = %q, want %q", task.Status, baldastate.SwarmTaskStatusCompleted)
	}
	if task.ResultJSON == "" {
		t.Fatal("task result json is empty, want persisted result despite event publish failure")
	}
	if len(bus.envs) != 1 || bus.envs[0].Meta["event_type"] != TaskEventTaskCompleted {
		t.Fatalf("published events = %+v, want one task.completed visibility attempt", bus.envs)
	}
}
