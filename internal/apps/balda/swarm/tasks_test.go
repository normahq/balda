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

func (*recordingTaskCommandBus) PublishCommand(context.Context, Envelope) (*CommandPublishResult, error) {
	return nil, nil
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
func (*recordingTaskCommandBus) PublishDLQ(context.Context, Envelope, string) error { return nil }
func (*recordingTaskCommandBus) RunCommandConsumer(ctx context.Context, _ CommandHandler) error {
	<-ctx.Done()
	return ctx.Err()
}
func (*recordingTaskCommandBus) Drain(context.Context) error { return nil }

func TestTaskServiceAppendEventPublishesJetStreamEvent(t *testing.T) {
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

	payload := map[string]any{"role": AgentNamePlanner, "iteration": 1}
	if err := service.AppendEvent(ctx, "task-1", TaskEventAgentStarted, "task.actor", "task-1:agent:planner:planner:1", payload); err != nil {
		t.Fatalf("AppendEvent(first started) error = %v", err)
	}
	if err := service.AppendEvent(ctx, "task-1", TaskEventAgentStarted, "task.actor", "task-1:agent:planner:planner:1", payload); err != nil {
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

func TestTaskServiceCreateReemitsCreatedEventForExistingTask(t *testing.T) {
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
	if _, err := service.Create(ctx, record, "task.actor", map[string]any{"objective": record.Objective}); err == nil {
		t.Fatal("Create(first) error = nil, want event publish error")
	}
	if task, ok, err := service.Get(ctx, record.ID); err != nil || !ok || task.ID != record.ID {
		t.Fatalf("task after failed event publish = %+v found=%v err=%v, want persisted task", task, ok, err)
	}
	created, err := service.Create(ctx, record, "task.actor", map[string]any{"objective": record.Objective})
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
