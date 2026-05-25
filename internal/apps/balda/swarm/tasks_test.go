package swarm

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	baldastate "github.com/normahq/balda/internal/apps/balda/state"
)

type recordingEventBus struct {
	mu       sync.Mutex
	subjects []string
	envs     []Envelope
}

func (b *recordingEventBus) Publish(_ context.Context, subject string, env Envelope) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subjects = append(b.subjects, subject)
	b.envs = append(b.envs, env)
	return nil
}

func (*recordingEventBus) Subscribe(context.Context, string, EventHandler) (Subscription, error) {
	return noopSubscription{}, nil
}

func (*recordingEventBus) Request(context.Context, string, Envelope, time.Duration) (*Envelope, error) {
	return nil, nil
}

func (*recordingEventBus) Drain(context.Context) error { return nil }

func TestTaskServiceAppendEventPublishesEventBusEvent(t *testing.T) {
	ctx := context.Background()
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	bus := &recordingEventBus{}
	service, err := NewTaskService(taskServiceParams{StateProvider: provider, Bus: bus})
	if err != nil {
		t.Fatalf("NewTaskService() error = %v", err)
	}
	if err := service.AppendEvent(ctx, "task-1", TaskEventAgentProgress, "agent:executor", "msg-1", map[string]any{"text": "working"}); err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}
	if len(bus.subjects) != 1 || bus.subjects[0] != SubjectEventAgent {
		t.Fatalf("subjects = %+v, want %q", bus.subjects, SubjectEventAgent)
	}
	if len(bus.envs) != 1 || bus.envs[0].TaskID != "task-1" || bus.envs[0].Meta["event_type"] != TaskEventAgentProgress {
		t.Fatalf("envs = %+v, want task event envelope", bus.envs)
	}
}
