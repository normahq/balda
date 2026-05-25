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
	provider, mailboxes, coordinator, tasks, registry := newTaskVisibilitySwarmServices(t, ctx)
	handler.swarmConfig = swarm.Config{Enabled: true, Mode: swarm.ModeMailbox}
	handler.swarmCoordinator = coordinator
	handler.mailboxes = mailboxes
	handler.tasks = tasks
	handler.agentRegistry = registry
	handler.taskRuns = newTaskRunRegistry()
	sm.sessionInfos = map[string]baldasession.TopicSessionInfo{
		"tg-9001-0": {
			SessionID:  "tg-9001-0",
			Locator:    baldasession.SessionLocator{SessionID: "tg-9001-0", ChannelType: "telegram", AddressKey: "9001:0"},
			BranchName: "norma/balda/tg-9001-0",
		},
	}

	createTaskRecord(t, ctx, tasks, baldastate.SwarmTaskRecord{
		ID:          "task-active",
		SessionID:   "tg-9001-0",
		Title:       "Goal: active",
		Objective:   "active work",
		Status:      baldastate.SwarmTaskStatusCreated,
		CreatedFrom: "goal",
	})
	if err := tasks.MarkStatus(ctx, "task-active", baldastate.SwarmTaskStatusQueued, "test", "", "", nil); err != nil {
		t.Fatalf("MarkStatus(active) error = %v", err)
	}
	createTaskRecord(t, ctx, tasks, baldastate.SwarmTaskRecord{
		ID:          "task-done",
		SessionID:   "tg-9001-0",
		Title:       "Goal: done",
		Objective:   "ship reviewable outcome",
		Status:      baldastate.SwarmTaskStatusCreated,
		CreatedFrom: "goal",
	})
	if err := tasks.SetResult(ctx, "task-done", map[string]any{
		"goal_reached":    true,
		"executor_output": "Implemented task visibility commands.",
		"reviewer_output": "verdict: pass\nCommand tests passed.",
	}, baldastate.SwarmTaskStatusCompleted, "test", ""); err != nil {
		t.Fatalf("SetResult(done) error = %v", err)
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
	assertLastSentContains(t, tgClient, "Canceled task task-active")
	got, ok, err := provider.Swarm().GetTask(ctx, "task-active")
	if err != nil {
		t.Fatalf("GetTask(active) error = %v", err)
	}
	if !ok || got.Status != baldastate.SwarmTaskStatusCanceled {
		t.Fatalf("task-active = %+v, found=%v, want canceled", got, ok)
	}
}

func TestCommandHandlerSwarmAndMailboxStatusCommands(t *testing.T) {
	ctx := context.Background()
	handler, _, _, tgClient := newCommandHandlerTestHarness(t)
	_, mailboxes, coordinator, tasks, registry := newTaskVisibilitySwarmServices(t, ctx)
	handler.swarmConfig = swarm.Config{Enabled: true, Mode: swarm.ModeMailbox, WebhookMode: swarm.ModeShadow, SchedulerMode: swarm.ModeShadow}
	handler.swarmCoordinator = coordinator
	handler.mailboxes = mailboxes
	handler.tasks = tasks
	handler.agentRegistry = registry

	createTaskRecord(t, ctx, tasks, baldastate.SwarmTaskRecord{
		ID:        "task-status",
		SessionID: "tg-9001-0",
		Title:     "Goal: status",
		Objective: "status",
		Status:    baldastate.SwarmTaskStatusCreated,
	})
	if _, err := mailboxes.Publish(ctx, swarm.Envelope{
		ID:          "status-message",
		Namespace:   swarm.NamespaceHumanInbound,
		Kind:        swarm.KindMessage,
		From:        swarm.ActorAddress{Target: swarm.ActorTypeSystem, Key: "test"},
		To:          swarm.ActorAddress{Target: swarm.ActorTypeSession, Key: "tg-9001-0"},
		SessionID:   "tg-9001-0",
		PayloadJSON: `{}`,
	}); err != nil {
		t.Fatalf("Publish(status message) error = %v", err)
	}

	if err := handler.onCommand(ctx, newCommandEvent("swarm", "status", 101, 9001, nil)); err != nil {
		t.Fatalf("/swarm status error = %v", err)
	}
	assertLastSentContains(t, tgClient, "Swarm status")
	assertLastSentContains(t, tgClient, "planner")
	assertLastSentContains(t, tgClient, "created: 1")

	if err := handler.onCommand(ctx, newCommandEvent("mailbox", "status", 101, 9001, nil)); err != nil {
		t.Fatalf("/mailbox status error = %v", err)
	}
	assertLastSentContains(t, tgClient, "Mailbox status")
	assertLastSentContains(t, tgClient, "session:tg-9001-0 [queued]: 1")
}

func newTaskVisibilitySwarmServices(
	t *testing.T,
	ctx context.Context,
) (baldastate.Provider, *swarm.MailboxService, *swarm.Coordinator, *swarm.TaskService, *swarm.AgentRegistry) {
	t.Helper()

	cfg, err := (swarm.Config{Enabled: true, Mode: swarm.ModeMailbox}).Normalized()
	if err != nil {
		t.Fatalf("Normalize swarm config: %v", err)
	}
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() {
		if err := provider.Close(); err != nil {
			t.Fatalf("provider.Close() error = %v", err)
		}
	})

	var mailboxes *swarm.MailboxService
	var coordinator *swarm.Coordinator
	var tasks *swarm.TaskService
	var registry *swarm.AgentRegistry
	app := fxtest.New(t,
		fx.Supply(
			fx.Annotate(provider, fx.As(new(baldastate.Provider))),
			fx.Annotate(handlerShadowWakeBus{}, fx.As(new(swarm.WakeBus))),
			cfg,
		),
		fx.Provide(
			swarm.NewShadowMetrics,
			swarm.NewMailboxService,
			swarm.NewTaskService,
			swarm.NewAgentRegistry,
			swarm.NewCoordinator,
		),
		fx.Populate(&mailboxes, &coordinator, &tasks, &registry),
	)
	app.RequireStart()
	t.Cleanup(func() { app.RequireStop() })
	return provider, mailboxes, coordinator, tasks, registry
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
