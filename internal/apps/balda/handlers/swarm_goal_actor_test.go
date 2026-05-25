package handlers

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func TestTaskActorStartGoalDispatchesPlannerFirst(t *testing.T) {
	ctx := context.Background()
	_, mailboxes, coordinator, tasks, allocator := newTaskActorSwarmServices(t, ctx)
	exec := &taskActorExecutor{tasks: tasks, coordinator: coordinator, agents: allocator, maxIters: 3}
	locator := taskActorTestLocator()
	env, goal := taskActorGoalEnvelope(t, locator, "fix failing tests", 3)

	if err := exec.startGoalTask(ctx, env, goal); err != nil {
		t.Fatalf("startGoalTask() error = %v", err)
	}

	claimed, err := mailboxes.Claim(ctx, "agent:planner", "test-worker", 1, time.Minute)
	if err != nil {
		t.Fatalf("Claim(planner) error = %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("planner claimed messages = %d, want 1", len(claimed))
	}
	var command taskAgentCommandPayload
	if err := json.Unmarshal([]byte(claimed[0].PayloadJSON), &command); err != nil {
		t.Fatalf("decode planner command: %v", err)
	}
	if command.Role != taskAgentRolePlanner || command.AgentName != swarm.AgentNamePlanner {
		t.Fatalf("planner command role/name = %q/%q, want planner/planner", command.Role, command.AgentName)
	}
	if command.Plan != "" {
		t.Fatalf("planner command plan = %q, want empty", command.Plan)
	}
}

func TestTaskActorPlannerResultStoresPlanAndDispatchesExecutor(t *testing.T) {
	ctx := context.Background()
	_, mailboxes, coordinator, tasks, allocator := newTaskActorSwarmServices(t, ctx)
	exec := &taskActorExecutor{tasks: tasks, coordinator: coordinator, agents: allocator, maxIters: 3}
	locator := taskActorTestLocator()
	env, goal := taskActorGoalEnvelope(t, locator, "fix failing tests", 3)
	if err := exec.startGoalTask(ctx, env, goal); err != nil {
		t.Fatalf("startGoalTask() error = %v", err)
	}

	plannerText := "1. inspect tests\n2. patch code\n3. run go test ./..."
	resultEnv := swarm.Envelope{
		ID:          "planner-result-1",
		Namespace:   swarm.NamespaceAgentResult,
		Kind:        swarm.KindGoal,
		From:        swarm.ActorAddress{Target: swarm.ActorTypeAgent, Key: swarm.AgentNamePlanner},
		To:          swarm.ActorAddress{Target: swarm.ActorTypeTask, Key: goal.TaskID},
		SessionID:   locator.SessionID,
		TaskID:      goal.TaskID,
		PayloadJSON: `{}`,
	}
	if err := exec.handleAgentResult(ctx, resultEnv, taskAgentResultPayload{
		TaskID:          goal.TaskID,
		AgentName:       swarm.AgentNamePlanner,
		Role:            taskAgentRolePlanner,
		Iteration:       1,
		Locator:         locator,
		Objective:       goal.Objective,
		TransportUserID: goal.TransportUserID,
		Text:            plannerText,
		MaxIterations:   goal.MaxIterations,
	}); err != nil {
		t.Fatalf("handleAgentResult(planner) error = %v", err)
	}

	claimed, err := mailboxes.Claim(ctx, "agent:executor", "test-worker", 1, time.Minute)
	if err != nil {
		t.Fatalf("Claim(executor) error = %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("executor claimed messages = %d, want 1", len(claimed))
	}
	var command taskAgentCommandPayload
	if err := json.Unmarshal([]byte(claimed[0].PayloadJSON), &command); err != nil {
		t.Fatalf("decode executor command: %v", err)
	}
	if command.Role != taskAgentRoleExecutor || command.AgentName != swarm.AgentNameExecutor {
		t.Fatalf("executor command role/name = %q/%q, want executor/executor", command.Role, command.AgentName)
	}
	if command.Plan != plannerText || command.PlannerOutput != plannerText {
		t.Fatalf("executor plan/planner_output = %q/%q, want planner text", command.Plan, command.PlannerOutput)
	}

	task, ok, err := tasks.Get(ctx, goal.TaskID)
	if err != nil {
		t.Fatalf("Get(task) error = %v", err)
	}
	if !ok {
		t.Fatal("task not found")
	}
	if !strings.Contains(task.PlanJSON, "inspect tests") {
		t.Fatalf("task plan json = %q, want planner output", task.PlanJSON)
	}
}

func newTaskActorSwarmServices(
	t *testing.T,
	ctx context.Context,
) (baldastate.Provider, *swarm.MailboxService, *swarm.Coordinator, *swarm.TaskService, *swarm.AgentAllocator) {
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
	var allocator *swarm.AgentAllocator
	app := fxtest.New(t,
		fx.Supply(
			fx.Annotate(provider, fx.As(new(baldastate.Provider))),
			fx.Annotate(handlerShadowWakeBus{}, fx.As(new(swarm.EventBus))),
			cfg,
		),
		fx.Provide(
			swarm.NewShadowMetrics,
			swarm.NewMailboxService,
			swarm.NewTaskService,
			swarm.NewAgentRegistry,
			swarm.NewAgentAllocator,
			swarm.NewCoordinator,
		),
		fx.Populate(&mailboxes, &coordinator, &tasks, &allocator),
	)
	app.RequireStart()
	t.Cleanup(func() { app.RequireStop() })
	return provider, mailboxes, coordinator, tasks, allocator
}

func taskActorTestLocator() baldasession.SessionLocator {
	return baldasession.SessionLocator{
		SessionID:   "tg-9001-99",
		ChannelType: "telegram",
		AddressKey:  "tg-9001-99",
	}
}

func taskActorGoalEnvelope(
	t *testing.T,
	locator baldasession.SessionLocator,
	objective string,
	maxIterations int,
) (swarm.Envelope, goalTaskPayload) {
	t.Helper()
	env, err := goalTaskEnvelope(locator, objective, testTelegramUserID101, maxIterations)
	if err != nil {
		t.Fatalf("goalTaskEnvelope() error = %v", err)
	}
	var payload taskEnvelopePayload
	if err := json.Unmarshal([]byte(env.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode goal payload: %v", err)
	}
	if payload.Goal == nil {
		t.Fatal("goal payload is nil")
	}
	return env, *payload.Goal
}
