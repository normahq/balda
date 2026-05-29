package handlers

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func TestCommandHandlerTaskVisibilityCommands(t *testing.T) {
	ctx := context.Background()
	handler, sm, _, tgClient := newCommandHandlerTestHarness(t)
	provider, bus, coordinator, tasks, registry := newTaskVisibilitySwarmServices(t, ctx)
	handler.swarmConfig = swarm.Config{Enabled: true}
	handler.swarmCoordinator = coordinator
	handler.swarmRuntime = fakeSwarmRuntimeStatusProvider{}
	handler.commandBus = bus
	handler.tasks = tasks
	handler.agentRegistry = registry
	handler.taskRuns = newTaskRunRegistry()
	sm.sessionInfos = map[string]baldasession.TopicSessionInfo{
		"tg-9001-0": {SessionID: "tg-9001-0", Locator: baldasession.SessionLocator{SessionID: "tg-9001-0", ChannelType: "telegram", AddressKey: "9001:0"}, BranchName: "norma/balda/tg-9001-0"},
	}

	createTaskRecord(t, ctx, tasks, baldastate.SwarmTaskRecord{ID: "task-active", SessionID: "tg-9001-0", Title: "Goal: active", Objective: "active work", Status: baldastate.SwarmTaskStatusCreated, CreatedFrom: "goal"})
	if err := tasks.MarkStatus(ctx, "task-active", baldastate.SwarmTaskStatusQueued, "test", "", "", nil); err != nil {
		t.Fatalf("MarkStatus(active) error = %v", err)
	}
	createTaskRecord(t, ctx, tasks, baldastate.SwarmTaskRecord{ID: "task-done", SessionID: "tg-9001-0", Title: "Goal: done", Objective: "ship reviewable outcome", Status: baldastate.SwarmTaskStatusCreated, CreatedFrom: "goal"})
	if err := tasks.SetResult(ctx, "task-done", map[string]any{"goal_reached": true, "executor_output": "Implemented task visibility commands.", "reviewer_output": "verdict: pass\nCommand tests passed."}, baldastate.SwarmTaskStatusCompleted, "test", ""); err != nil {
		t.Fatalf("SetResult(done) error = %v", err)
	}
	if err := provider.Swarm().AppendTaskEvent(ctx, baldastate.SwarmTaskEventRecord{
		ID:        "task-done-completed",
		TaskID:    "task-done",
		EventType: swarm.TaskEventTaskCompleted,
		Actor:     "test",
	}); err != nil {
		t.Fatalf("Append projected task event: %v", err)
	}

	if err := handler.onCommand(ctx, newCommandEvent("tasks", "", 101, 9001, nil)); err != nil {
		t.Fatalf("/tasks error = %v", err)
	}
	assertLastSentContains(t, tgClient, "task-active")
	assertLastSentNotContains(t, tgClient, "task-done")

	if err := handler.onCommand(ctx, newCommandEvent("task", "task-done", 101, 9001, nil)); err != nil {
		t.Fatalf("/task error = %v", err)
	}
	assertLastSentContains(t, tgClient, "Result")
	assertLastSentContains(t, tgClient, "Artifacts")
	assertLastSentContains(t, tgClient, "Confidence")
	assertLastSentContains(t, tgClient, "Next action")

	if err := handler.onCommand(ctx, newCommandEvent("task", "task-done events", 101, 9001, nil)); err != nil {
		t.Fatalf("/task events error = %v", err)
	}
	assertLastSentContains(t, tgClient, swarm.TaskEventTaskCompleted)

	if err := handler.onCommand(ctx, newCommandEvent("task", "task-active cancel", 101, 9001, nil)); err != nil {
		t.Fatalf("/task cancel error = %v", err)
	}
	assertLastSentContains(t, tgClient, "Cancel requested for task task-active")
	if len(bus.commands) != 1 || bus.commands[0].Namespace != swarm.NamespaceTaskControl || bus.commands[0].TaskID != "task-active" {
		t.Fatalf("published cancel commands = %+v, want one task control command", bus.commands)
	}
	got, ok, err := provider.Swarm().GetTask(ctx, "task-active")
	if err != nil {
		t.Fatalf("GetTask(active) error = %v", err)
	}
	if !ok || got.Status != baldastate.SwarmTaskStatusQueued {
		t.Fatalf("task-active = %+v, found=%v, want queued until control actor runs", got, ok)
	}
}

func TestCommandHandlerTaskVisibilityShowsTaskStatusWithoutProjectedEvents(t *testing.T) {
	ctx := context.Background()
	handler, _, _, tgClient := newCommandHandlerTestHarness(t)
	_, bus, coordinator, tasks, registry := newTaskVisibilitySwarmServices(t, ctx)
	handler.swarmConfig = swarm.Config{Enabled: true}
	handler.swarmCoordinator = coordinator
	handler.swarmRuntime = fakeSwarmRuntimeStatusProvider{}
	handler.commandBus = bus
	handler.tasks = tasks
	handler.agentRegistry = registry

	createTaskRecord(t, ctx, tasks, baldastate.SwarmTaskRecord{
		ID:          "task-no-events",
		SessionID:   "tg-9001-0",
		Title:       "Goal: no events",
		Objective:   "show status from task row",
		Status:      baldastate.SwarmTaskStatusRunning,
		CreatedFrom: "goal",
	})
	if err := tasks.MarkStatus(ctx, "task-no-events", baldastate.SwarmTaskStatusRunning, "test", "", "", nil); err != nil {
		t.Fatalf("MarkStatus() error = %v", err)
	}

	if err := handler.onCommand(ctx, newCommandEvent("task", "task-no-events", 101, 9001, nil)); err != nil {
		t.Fatalf("/task error = %v", err)
	}
	assertLastSentContains(t, tgClient, "Status: running")

	if err := handler.onCommand(ctx, newCommandEvent("task", "task-no-events events", 101, 9001, nil)); err != nil {
		t.Fatalf("/task events error = %v", err)
	}
	assertLastSentContains(t, tgClient, "No events for task task-no-events.")
}

func TestCommandHandlerTaskVisibilityRedactsSecrets(t *testing.T) {
	ctx := context.Background()
	handler, _, _, tgClient := newCommandHandlerTestHarness(t)
	_, bus, coordinator, tasks, registry := newTaskVisibilitySwarmServices(t, ctx)
	handler.swarmConfig = swarm.Config{Enabled: true}
	handler.swarmCoordinator = coordinator
	handler.swarmRuntime = fakeSwarmRuntimeStatusProvider{}
	handler.commandBus = bus
	handler.tasks = tasks
	handler.agentRegistry = registry

	createTaskRecord(t, ctx, tasks, baldastate.SwarmTaskRecord{
		ID:          "task-redact",
		SessionID:   "tg-9001-0",
		Title:       "Goal: redact",
		Objective:   "verify redaction",
		Status:      baldastate.SwarmTaskStatusCreated,
		CreatedFrom: "goal",
	})
	if err := tasks.SetResult(
		ctx,
		"task-redact",
		map[string]any{
			"goal_reached":    true,
			"executor_output": "token=abc123",
			"reviewer_output": "Authorization: Bearer super-secret",
		},
		baldastate.SwarmTaskStatusCompleted,
		"test",
		"password=hidden",
	); err != nil {
		t.Fatalf("SetResult(redact) error = %v", err)
	}

	if err := handler.onCommand(ctx, newCommandEvent("task", "task-redact", 101, 9001, nil)); err != nil {
		t.Fatalf("/task redact error = %v", err)
	}
	assertLastSentContains(t, tgClient, "[REDACTED]")
	assertLastSentNotContains(t, tgClient, "abc123")
	assertLastSentNotContains(t, tgClient, "super-secret")
	assertLastSentNotContains(t, tgClient, "password=hidden")
}

func TestCommandHandlerSwarmQueueAndMailboxStatusCommands(t *testing.T) {
	ctx := context.Background()
	handler, _, _, tgClient := newCommandHandlerTestHarness(t)
	_, bus, coordinator, tasks, registry := newTaskVisibilitySwarmServices(t, ctx)
	handler.swarmConfig = swarm.Config{Enabled: true}
	handler.swarmCoordinator = coordinator
	handler.swarmRuntime = fakeSwarmRuntimeStatusProvider{
		status: swarm.RuntimeLaneStatus{
			Active: 2,
			Keys:   []string{"session:tg-9001-0", "task:task-status"},
		},
	}
	handler.commandBus = bus
	handler.tasks = tasks
	handler.agentRegistry = registry

	createTaskRecord(t, ctx, tasks, baldastate.SwarmTaskRecord{ID: "task-status", SessionID: "tg-9001-0", Title: "Goal: status", Objective: "status", Status: baldastate.SwarmTaskStatusCreated})
	delivery, created, err := tasks.ReserveDelivery(ctx, baldastate.SwarmDeliveryRecord{
		ID:          "delivery-status-1",
		DeliveryKey: "task-status:delivery:final",
		TaskID:      "task-status",
		SessionID:   "tg-9001-0",
		Channel:     "telegram",
		AddressKey:  "9001:0",
		Kind:        "delivery",
		PayloadJSON: `{"text":"status"}`,
		PayloadHash: "hash-status-1",
		Status:      baldastate.SwarmDeliveryStatusPending,
	})
	if err != nil {
		t.Fatalf("ReserveDelivery(status) error = %v", err)
	}
	if !created || delivery.ID == "" {
		t.Fatalf("ReserveDelivery(status) = %+v created=%v, want created", delivery, created)
	}
	if err := tasks.MarkDeliveryFailed(ctx, delivery.DeliveryKey, "provider timeout"); err != nil {
		t.Fatalf("MarkDeliveryFailed(status) error = %v", err)
	}

	if err := handler.onCommand(ctx, newCommandEvent("swarm", "status", 101, 9001, nil)); err != nil {
		t.Fatalf("/swarm status error = %v", err)
	}
	assertLastSentContains(t, tgClient, "Swarm status")
	assertLastSentContains(t, tgClient, "command_bus: jetstream")
	assertLastSentContains(t, tgClient, "command_event_publishing_mode: best_effort_visibility")
	assertLastSentNotContains(t, tgClient, "sqlite_command_bus")
	assertLastSentNotContains(t, tgClient, "shadow_mode")
	assertLastSentNotContains(t, tgClient, "legacy_direct_path")
	assertLastSentContains(t, tgClient, "planner")
	assertLastSentContains(t, tgClient, "shell_policy=workspace_write")
	assertLastSentContains(t, tgClient, "workspace_access=read_write")
	assertLastSentContains(t, tgClient, "allowed_tools=workspace,shell,mcp")
	assertLastSentContains(t, tgClient, "state_source_of_truth: sqlite")
	assertLastSentContains(t, tgClient, "event_publishing_mode: best_effort_visibility")
	assertLastSentContains(t, tgClient, "created: 1")
	assertLastSentContains(t, tgClient, "BALDA_EVENT_PROJECTOR_lag: 2")
	assertLastSentContains(t, tgClient, "Metrics")
	assertLastSentContains(t, tgClient, "commands_backlog: 0")
	assertLastSentContains(t, tgClient, "commands_published_total: 0")
	assertLastSentContains(t, tgClient, "commands_running_total: 0")
	assertLastSentContains(t, tgClient, "commands_acked_total: 0")
	assertLastSentContains(t, tgClient, "commands_retrying_total: 0")
	assertLastSentContains(t, tgClient, "commands_deadlettered_total: 0")
	assertLastSentContains(t, tgClient, "command_duration_seconds: 0.000")
	assertLastSentContains(t, tgClient, "actor_duration_seconds: 0.000")
	assertLastSentContains(t, tgClient, "commands_redelivered_total: 0")
	assertLastSentContains(t, tgClient, "dlq_messages_total: 0")
	assertLastSentContains(t, tgClient, "projection_lag_total: 2")
	assertLastSentContains(t, tgClient, "projection_lag_seconds: 2")
	assertLastSentContains(t, tgClient, "delivery_duplicate_suppressed_total: 0")
	assertLastSentContains(t, tgClient, "delivery_outbox_failed: 1")
	assertLastSentNotContains(t, tgClient, "delivery_outbox: none")
	assertLastSentContains(t, tgClient, "active_actor_lanes: 2")
	assertLastSentContains(t, tgClient, "active_actor_lane_keys: session:tg-9001-0,task:task-status")

	if err := handler.onCommand(ctx, newCommandEvent("mailbox", "status", 101, 9001, nil)); err != nil {
		t.Fatalf("/mailbox status error = %v", err)
	}
	assertLastSentContains(t, tgClient, "BALDA_WORKER_COMMANDS")

	if err := handler.onCommand(ctx, newCommandEvent("queue", "status", 101, 9001, nil)); err != nil {
		t.Fatalf("/queue status error = %v", err)
	}
	assertLastSentContains(t, tgClient, "BALDA_WORKER_COMMANDS")

	if err := handler.onCommand(ctx, newCommandEvent("dlq", "", 101, 9001, nil)); err != nil {
		t.Fatalf("/dlq error = %v", err)
	}
	assertLastSentContains(t, tgClient, "DLQ status")
	assertLastSentContains(t, tgClient, "stream: BALDA_DLQ")

	if err := handler.onCommand(ctx, newCommandEvent("dlq", "7", 101, 9001, nil)); err != nil {
		t.Fatalf("/dlq <id> error = %v", err)
	}
	assertLastSentContains(t, tgClient, "DLQ entry")
	assertLastSentContains(t, tgClient, "sequence: 7")
	assertLastSentContains(t, tgClient, "envelope_id: dlq-entry-7")
	assertLastSentContains(t, tgClient, "reason: token=[REDACTED]")

	if err := handler.onCommand(ctx, newCommandEvent("projection", "status", 101, 9001, nil)); err != nil {
		t.Fatalf("/projection status error = %v", err)
	}
	assertLastSentContains(t, tgClient, "Projection status")
	assertLastSentContains(t, tgClient, "BALDA_EVENT_PROJECTOR_lag: 2")
	assertLastSentContains(t, tgClient, "projection_lag_seconds: 2")

	if err := handler.onCommand(ctx, newCommandEvent("actors", "status", 101, 9001, nil)); err != nil {
		t.Fatalf("/actors status error = %v", err)
	}
	assertLastSentContains(t, tgClient, "Actors status")
	assertLastSentContains(t, tgClient, "planner")
	assertLastSentContains(t, tgClient, "shell_policy=read_only")
	assertLastSentContains(t, tgClient, "workspace_access=read_only")
	assertLastSentContains(t, tgClient, "allowed_tools=none")
}

func TestCommandHandlerSwarmStatusShowsDisabledModeContract(t *testing.T) {
	ctx := context.Background()
	handler, _, _, tgClient := newCommandHandlerTestHarness(t)
	handler.swarmConfig = swarm.Config{Enabled: false}
	handler.swarmCoordinator = nil
	handler.commandBus = swarm.UnsupportedCommandBus{}

	if err := handler.onCommand(ctx, newCommandEvent("swarm", "status", 101, 9001, nil)); err != nil {
		t.Fatalf("/swarm status error = %v", err)
	}
	assertLastSentContains(t, tgClient, "enabled: false")
	assertLastSentContains(t, tgClient, "runtime enabled: false")
	assertLastSentContains(t, tgClient, "command_bus: unavailable")
	assertLastSentContains(t, tgClient, "disabled_mode_contract: runtime_unavailable_no_fallback")
}

func TestCommandHandlerDLQEntryUsageAndNotFound(t *testing.T) {
	ctx := context.Background()
	handler, _, _, tgClient := newCommandHandlerTestHarness(t)
	_, bus, coordinator, tasks, registry := newTaskVisibilitySwarmServices(t, ctx)
	handler.swarmConfig = swarm.Config{Enabled: true}
	handler.swarmCoordinator = coordinator
	handler.swarmRuntime = fakeSwarmRuntimeStatusProvider{}
	handler.commandBus = bus
	handler.tasks = tasks
	handler.agentRegistry = registry

	if err := handler.onCommand(ctx, newCommandEvent("dlq", "abc", 101, 9001, nil)); err != nil {
		t.Fatalf("/dlq usage error = %v", err)
	}
	assertLastSentContains(t, tgClient, "Usage: /dlq <stream_seq>")

	if err := handler.onCommand(ctx, newCommandEvent("dlq", "999", 101, 9001, nil)); err != nil {
		t.Fatalf("/dlq not found error = %v", err)
	}
	assertLastSentContains(t, tgClient, "DLQ entry 999 not found.")
}

func newTaskVisibilitySwarmServices(t *testing.T, ctx context.Context) (baldastate.Provider, *statusCommandBus, *swarm.Coordinator, *swarm.TaskService, *swarm.AgentRegistry) {
	t.Helper()
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	bus := &statusCommandBus{}
	cfg, err := (swarm.Config{Enabled: true}).Normalized()
	if err != nil {
		t.Fatalf("Normalize swarm config: %v", err)
	}
	var coordinator *swarm.Coordinator
	var tasks *swarm.TaskService
	var registry *swarm.AgentRegistry
	app := fxtest.New(t,
		fx.Supply(
			fx.Annotate(provider, fx.As(new(baldastate.Provider))),
			cfg,
		),
		fx.Provide(
			func() swarm.CoordinatorBus { return bus },
			func() swarm.EventPublisher { return bus },
		),
		fx.Provide(swarm.NewTaskService, swarm.NewAgentRegistry, swarm.NewCoordinator),
		fx.Populate(&coordinator, &tasks, &registry),
	)
	app.RequireStart()
	t.Cleanup(func() { app.RequireStop() })
	return provider, bus, coordinator, tasks, registry
}

type statusCommandBus struct{ recordingHandlerCommandBus }

func (*statusCommandBus) Status(context.Context) (swarm.CommandBusStatus, error) {
	return swarm.CommandBusStatus{
		CommandBus: "jetstream",
		Embedded:   true,
		Running:    true,
		JetStream:  true,
		Commands:   swarm.StreamStatus{Name: swarm.DefaultCommandStream, Messages: 1, FirstSeq: 1, LastSeq: 1},
		Events:     swarm.StreamStatus{Name: swarm.DefaultEventStream, Messages: 2, FirstSeq: 1, LastSeq: 2},
		DLQ:        swarm.StreamStatus{Name: swarm.DefaultDLQStream},
		Worker:     swarm.ConsumerStatus{Name: swarm.DefaultCommandConsumer},
		ProjectionLag: map[string]uint64{
			swarm.DefaultEventProjectorConsumer: 2,
		},
	}, nil
}

func (*statusCommandBus) GetDLQEntry(_ context.Context, sequence uint64) (swarm.DLQEntry, error) {
	if sequence != 7 {
		return swarm.DLQEntry{}, fmt.Errorf("%w: sequence=%d", swarm.ErrDLQEntryNotFound, sequence)
	}
	return swarm.DLQEntry{
		Stream:      swarm.DefaultDLQStream,
		Sequence:    7,
		Subject:     swarm.SubjectDLQCommand,
		PublishedAt: time.Date(2026, time.May, 27, 10, 0, 0, 0, time.UTC),
		Reason:      "token=secret",
		Envelope: swarm.Envelope{
			ID:            "dlq-entry-7",
			Namespace:     swarm.NamespaceTaskControl,
			Kind:          "cancel_task",
			TaskID:        "task-active",
			SessionID:     "tg-9001-0",
			CorrelationID: "corr-7",
			CausationID:   "cause-7",
		},
	}, nil
}

func createTaskRecord(t *testing.T, ctx context.Context, tasks *swarm.TaskService, record baldastate.SwarmTaskRecord) {
	t.Helper()
	created, err := tasks.Create(ctx, record, "test", nil)
	if err != nil {
		t.Fatalf("Create(%s) error = %v", record.ID, err)
	}
	if !created {
		t.Fatalf("Create(%s) created = false, want true", record.ID)
	}
}

type fakeSwarmRuntimeStatusProvider struct {
	status swarm.RuntimeLaneStatus
}

func (f fakeSwarmRuntimeStatusProvider) LaneStatus() swarm.RuntimeLaneStatus {
	return f.status
}
