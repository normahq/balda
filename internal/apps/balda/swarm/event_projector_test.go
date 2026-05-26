package swarm

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/rs/zerolog"
	"go.uber.org/fx/fxtest"
)

type recordingEventConsumer struct {
	runCalls int
}

func (c *recordingEventConsumer) RunEventConsumer(ctx context.Context, _ EventHandler) error {
	c.runCalls++
	<-ctx.Done()
	return ctx.Err()
}

func TestNewEventProjectorRequiresEventConsumerWhenEnabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	provider := newEventProjectorStateProvider(t, ctx)

	projector, err := NewEventProjector(eventProjectorParams{
		LC:            fxtest.NewLifecycle(t),
		Bus:           UnsupportedCommandBus{},
		Config:        Config{Enabled: true},
		StateProvider: provider,
		Logger:        zerolog.Nop(),
	})
	if err == nil || !strings.Contains(err.Error(), "event-consumer command bus") {
		t.Fatalf("NewEventProjector() = (%v, %v), want event consumer error", projector, err)
	}
}

func TestNewEventProjectorAllowsMissingEventConsumerWhenDisabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	provider := newEventProjectorStateProvider(t, ctx)

	projector, err := NewEventProjector(eventProjectorParams{
		LC:            fxtest.NewLifecycle(t),
		Bus:           UnsupportedCommandBus{},
		Config:        Config{Enabled: false},
		StateProvider: provider,
		Logger:        zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewEventProjector() error = %v, want nil", err)
	}
	if projector == nil || projector.enabled {
		t.Fatalf("projector = %+v, want disabled projector", projector)
	}
}

func TestEventProjectorStartDisabledDoesNotRunConsumer(t *testing.T) {
	consumer := &recordingEventConsumer{}
	projector := &EventProjector{consumer: consumer, enabled: false, logger: zerolog.Nop()}
	if err := projector.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if consumer.runCalls != 0 {
		t.Fatalf("RunEventConsumer calls = %d, want 0", consumer.runCalls)
	}
}

func TestEventProjectorProjectsTaskEventIdempotently(t *testing.T) {
	ctx := context.Background()
	provider := newEventProjectorStateProvider(t, ctx)
	projector := &EventProjector{store: provider.Swarm(), logger: zerolog.Nop()}
	env := Envelope{
		ID:          "event-1",
		Namespace:   NamespaceTelemetry,
		Kind:        "task_event",
		From:        SystemAddress("task-events"),
		To:          ActorAddress{Target: ActorTypeTask, Key: "task-1"},
		TaskID:      "task-1",
		PayloadJSON: `{"text":"working"}`,
		Meta:        map[string]string{"event_type": TaskEventAgentProgress, "actor": "agent:executor", "message_id": "msg-1"},
	}
	if err := projector.Project(ctx, SubjectEventTaskUpdated, env); err != nil {
		t.Fatalf("Project() error = %v", err)
	}
	if err := projector.Project(ctx, SubjectEventTaskUpdated, env); err != nil {
		t.Fatalf("Project(duplicate) error = %v", err)
	}
	events, err := provider.Swarm().ListTaskEvents(ctx, "task-1")
	if err != nil {
		t.Fatalf("ListTaskEvents() error = %v", err)
	}
	if len(events) != 1 || events[0].EventType != TaskEventAgentProgress || events[0].Actor != "agent:executor" {
		t.Fatalf("events = %+v, want one projected task event", events)
	}
}

func TestEventProjectorProjectsCommandEventForTask(t *testing.T) {
	ctx := context.Background()
	provider := newEventProjectorStateProvider(t, ctx)
	projector := &EventProjector{store: provider.Swarm(), logger: zerolog.Nop()}
	env := Envelope{
		ID:          "cmd-1:event:deadlettered",
		Namespace:   NamespaceTelemetry,
		Kind:        "command_event",
		From:        SystemAddress("jetstream"),
		To:          ActorAddress{Target: ActorTypeTask, Key: "task-1"},
		TaskID:      "task-1",
		PayloadJSON: `{"reason":"retry exhausted"}`,
	}
	if err := projector.Project(ctx, SubjectEventCommandDeadLettered, env); err != nil {
		t.Fatalf("Project() error = %v", err)
	}
	events, err := provider.Swarm().ListTaskEvents(ctx, "task-1")
	if err != nil {
		t.Fatalf("ListTaskEvents() error = %v", err)
	}
	if len(events) != 1 || events[0].EventType != "command.deadlettered" {
		t.Fatalf("events = %+v, want command.deadlettered projection", events)
	}
}

func newEventProjectorStateProvider(t *testing.T, ctx context.Context) baldastate.Provider {
	t.Helper()

	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	return provider
}
