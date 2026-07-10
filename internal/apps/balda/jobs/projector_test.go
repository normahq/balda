package jobs

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/pkg/actorlayer"
	"github.com/rs/zerolog"
	"go.uber.org/fx/fxtest"
)

func TestNewEventProjectorRequiresConsumer(t *testing.T) {
	t.Parallel()

	projector, err := NewEventProjector(eventProjectorParams{
		LC:            fxtest.NewLifecycle(t),
		StateProvider: newEventProjectorStateProvider(t, context.Background()),
		Logger:        zerolog.Nop(),
	})
	if err == nil || !strings.Contains(err.Error(), "actor runtime event consumer") {
		t.Fatalf("NewEventProjector() = (%v, %v), want consumer error", projector, err)
	}
}

func TestEventProjectorProjectsJobEventIdempotently(t *testing.T) {
	ctx := context.Background()
	provider := newEventProjectorStateProvider(t, ctx)
	projector := &EventProjector{store: provider.Jobs(), logger: zerolog.Nop()}
	env := actorlayer.Envelope{
		ID:          "event-1",
		Namespace:   baldaexecution.NamespaceTelemetry,
		Kind:        "job_event",
		From:        actorlayer.SystemAddress("job-events"),
		To:          actorlayer.ActorAddress{Target: baldaexecution.ActorTypeJob, Key: "task-1"},
		PayloadJSON: `{"text":"working"}`,
		Meta:        map[string]string{baldaexecution.JobIDMetaKey: "task-1", "event_type": JobEventAgentProgress, "actor": "agent:executor", "message_id": "msg-1"},
	}
	if err := projector.Project(ctx, baldaexecution.SubjectEventJobUpdated, env); err != nil {
		t.Fatalf("Project() error = %v", err)
	}
	if err := projector.Project(ctx, baldaexecution.SubjectEventJobUpdated, env); err != nil {
		t.Fatalf("Project(duplicate) error = %v", err)
	}
	events, err := provider.Jobs().ListJobEvents(ctx, "task-1")
	if err != nil {
		t.Fatalf("ListJobEvents() error = %v", err)
	}
	if len(events) != 1 || events[0].EventType != JobEventAgentProgress || events[0].Actor != "agent:executor" {
		t.Fatalf("events = %+v, want one projected job event", events)
	}
}

func TestEventProjectorProjectsCommandEventForTask(t *testing.T) {
	testEventProjectorProjectsCommandEventForTask(
		t,
		baldaexecution.SubjectEventCommandDeadLettered,
		"cmd-1:event:deadlettered",
		`{"reason":"retry exhausted"}`,
		"command.deadlettered",
	)
}

func TestEventProjectorProjectsCommandDecodeFailedEventForTask(t *testing.T) {
	testEventProjectorProjectsCommandEventForTask(
		t,
		baldaexecution.SubjectEventCommandDecodeFailed,
		"cmd-1:event:decode_failed",
		`{"reason":"decode failed: invalid json"}`,
		"command.decode_failed",
	)
}

func testEventProjectorProjectsCommandEventForTask(t *testing.T, subject string, envelopeID string, payloadJSON string, eventType string) {
	t.Helper()

	ctx := context.Background()
	provider := newEventProjectorStateProvider(t, ctx)
	projector := &EventProjector{store: provider.Jobs(), logger: zerolog.Nop()}
	env := actorlayer.Envelope{
		ID:          envelopeID,
		Namespace:   baldaexecution.NamespaceTelemetry,
		Kind:        "command_event",
		From:        actorlayer.SystemAddress("transport"),
		To:          actorlayer.ActorAddress{Target: baldaexecution.ActorTypeJob, Key: "task-1"},
		PayloadJSON: payloadJSON,
		Meta:        map[string]string{baldaexecution.JobIDMetaKey: "task-1"},
	}
	if err := projector.Project(ctx, subject, env); err != nil {
		t.Fatalf("Project() error = %v", err)
	}
	events, err := provider.Jobs().ListJobEvents(ctx, "task-1")
	if err != nil {
		t.Fatalf("ListJobEvents() error = %v", err)
	}
	if len(events) != 1 || events[0].EventType != eventType {
		t.Fatalf("events = %+v, want %s projection", events, eventType)
	}
}

func TestEventProjectorProjectsDeliveryFailedEventForTask(t *testing.T) {
	ctx := context.Background()
	provider := newEventProjectorStateProvider(t, ctx)
	projector := &EventProjector{store: provider.Jobs(), logger: zerolog.Nop()}
	env := actorlayer.Envelope{
		ID:          "delivery-1:event:failed",
		Namespace:   baldaexecution.NamespaceTelemetry,
		Kind:        "job_event",
		From:        actorlayer.SystemAddress("job-events"),
		To:          actorlayer.ActorAddress{Target: baldaexecution.ActorTypeJob, Key: "task-1"},
		PayloadJSON: `{"reason":"telegram send failed"}`,
		Meta:        map[string]string{baldaexecution.JobIDMetaKey: "task-1"},
	}
	if err := projector.Project(ctx, baldaexecution.SubjectEventDeliveryFailed, env); err != nil {
		t.Fatalf("Project() error = %v", err)
	}
	events, err := provider.Jobs().ListJobEvents(ctx, "task-1")
	if err != nil {
		t.Fatalf("ListJobEvents() error = %v", err)
	}
	if len(events) != 1 || events[0].EventType != JobEventDeliveryFailed {
		t.Fatalf("events = %+v, want delivery.failed projection", events)
	}
}

func TestEventProjectorReplayAfterRestartRemainsIdempotent(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")

	providerA := newEventProjectorStateProviderAtPath(t, ctx, dbPath)
	projectorA := &EventProjector{store: providerA.Jobs(), logger: zerolog.Nop()}
	eventCreated := actorlayer.Envelope{
		ID:          "evt-task-created",
		Namespace:   baldaexecution.NamespaceTelemetry,
		Kind:        "job_event",
		From:        actorlayer.SystemAddress("job-events"),
		To:          actorlayer.ActorAddress{Target: baldaexecution.ActorTypeJob, Key: "task-replay"},
		PayloadJSON: `{"status":"created"}`,
		Meta:        map[string]string{baldaexecution.JobIDMetaKey: "task-replay", "event_type": JobEventCreated, "actor": "task:actor", "message_id": "m-1"},
	}
	eventProgress := actorlayer.Envelope{
		ID:          "evt-task-progress",
		Namespace:   baldaexecution.NamespaceTelemetry,
		Kind:        "job_event",
		From:        actorlayer.SystemAddress("job-events"),
		To:          actorlayer.ActorAddress{Target: baldaexecution.ActorTypeJob, Key: "task-replay"},
		PayloadJSON: `{"status":"running"}`,
		Meta:        map[string]string{baldaexecution.JobIDMetaKey: "task-replay", "event_type": JobEventAgentProgress, "actor": "agent:executor", "message_id": "m-2"},
	}
	if err := projectorA.Project(ctx, baldaexecution.SubjectEventJobCreated, eventCreated); err != nil {
		t.Fatalf("Project(created) error = %v", err)
	}
	if err := projectorA.Project(ctx, baldaexecution.SubjectEventJobUpdated, eventProgress); err != nil {
		t.Fatalf("Project(progress) error = %v", err)
	}
	if err := providerA.Close(); err != nil {
		t.Fatalf("providerA.Close() error = %v", err)
	}

	providerB := newEventProjectorStateProviderAtPath(t, ctx, dbPath)
	projectorB := &EventProjector{store: providerB.Jobs(), logger: zerolog.Nop()}
	eventCompleted := actorlayer.Envelope{
		ID:          "evt-task-completed",
		Namespace:   baldaexecution.NamespaceTelemetry,
		Kind:        "job_event",
		From:        actorlayer.SystemAddress("job-events"),
		To:          actorlayer.ActorAddress{Target: baldaexecution.ActorTypeJob, Key: "task-replay"},
		PayloadJSON: `{"status":"completed"}`,
		Meta:        map[string]string{baldaexecution.JobIDMetaKey: "task-replay", "event_type": JobEventCompleted, "actor": "task:actor", "message_id": "m-3"},
	}
	if err := projectorB.Project(ctx, baldaexecution.SubjectEventJobCreated, eventCreated); err != nil {
		t.Fatalf("Project(replay created) error = %v", err)
	}
	if err := projectorB.Project(ctx, baldaexecution.SubjectEventJobUpdated, eventProgress); err != nil {
		t.Fatalf("Project(replay progress) error = %v", err)
	}
	if err := projectorB.Project(ctx, baldaexecution.SubjectEventJobCompleted, eventCompleted); err != nil {
		t.Fatalf("Project(completed) error = %v", err)
	}

	events, err := providerB.Jobs().ListJobEvents(ctx, "task-replay")
	if err != nil {
		t.Fatalf("ListJobEvents() error = %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("projected replay events len = %d, want 3", len(events))
	}
	if events[0].ID != eventCreated.ID || events[1].ID != eventProgress.ID || events[2].ID != eventCompleted.ID {
		t.Fatalf("projected replay event IDs = [%s %s %s], want [%s %s %s]", events[0].ID, events[1].ID, events[2].ID, eventCreated.ID, eventProgress.ID, eventCompleted.ID)
	}
	if events[0].EventType != JobEventCreated || events[1].EventType != JobEventAgentProgress || events[2].EventType != JobEventCompleted {
		t.Fatalf("projected replay event types = [%s %s %s], want [%s %s %s]", events[0].EventType, events[1].EventType, events[2].EventType, JobEventCreated, JobEventAgentProgress, JobEventCompleted)
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

func newEventProjectorStateProviderAtPath(t *testing.T, ctx context.Context, path string) baldastate.Provider {
	t.Helper()

	provider, err := baldastate.NewSQLiteProvider(ctx, path)
	if err != nil {
		t.Fatalf("NewSQLiteProvider(%s) error = %v", path, err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	return provider
}
