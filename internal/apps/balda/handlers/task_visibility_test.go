package handlers

import (
	"context"
	"path/filepath"
	"testing"

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

func TestCommandHandlerSwarmQueueAndMailboxStatusCommands(t *testing.T) {
	ctx := context.Background()
	handler, _, _, tgClient := newCommandHandlerTestHarness(t)
	_, bus, coordinator, tasks, registry := newTaskVisibilitySwarmServices(t, ctx)
	handler.swarmConfig = swarm.Config{Enabled: true}
	handler.swarmCoordinator = coordinator
	handler.commandBus = bus
	handler.tasks = tasks
	handler.agentRegistry = registry

	createTaskRecord(t, ctx, tasks, baldastate.SwarmTaskRecord{ID: "task-status", SessionID: "tg-9001-0", Title: "Goal: status", Objective: "status", Status: baldastate.SwarmTaskStatusCreated})

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
	assertLastSentContains(t, tgClient, "state_source_of_truth: sqlite")
	assertLastSentContains(t, tgClient, "event_publishing_mode: best_effort_visibility")
	assertLastSentContains(t, tgClient, "created: 1")
	assertLastSentContains(t, tgClient, "BALDA_EVENT_PROJECTOR_lag: 2")

	if err := handler.onCommand(ctx, newCommandEvent("mailbox", "status", 101, 9001, nil)); err != nil {
		t.Fatalf("/mailbox status error = %v", err)
	}
	assertLastSentContains(t, tgClient, "BALDA_WORKER_COMMANDS")

	if err := handler.onCommand(ctx, newCommandEvent("queue", "status", 101, 9001, nil)); err != nil {
		t.Fatalf("/queue status error = %v", err)
	}
	assertLastSentContains(t, tgClient, "BALDA_WORKER_COMMANDS")
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
