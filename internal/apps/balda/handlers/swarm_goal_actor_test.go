package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func TestTaskActorStartGoalDispatchesPlannerBeforeProgress(t *testing.T) {
	ctx := context.Background()
	_, bus, coordinator, tasks, allocator := newTaskActorSwarmServices(t, ctx)
	exec := &taskActorExecutor{tasks: tasks, coordinator: coordinator, agents: allocator, maxIters: 3}
	locator := taskActorTestLocator()
	env, goal := taskActorGoalEnvelope(t, locator, "fix failing tests", 3)

	if err := exec.startGoalTask(ctx, env, goal); err != nil {
		t.Fatalf("startGoalTask() error = %v", err)
	}

	if len(bus.commands) != 3 {
		t.Fatalf("published commands = %d, want planner + start delivery + planner-start delivery", len(bus.commands))
	}
	planner := bus.commands[0]
	if planner.To.Target != swarm.ActorTypeAgent || planner.To.Key != swarm.AgentNamePlanner {
		t.Fatalf("planner command target = %+v, want agent:planner", planner.To)
	}
	var command taskAgentCommandPayload
	if err := json.Unmarshal([]byte(planner.PayloadJSON), &command); err != nil {
		t.Fatalf("decode planner command: %v", err)
	}
	if command.Role != taskAgentRolePlanner || command.AgentName != swarm.AgentNamePlanner {
		t.Fatalf("planner command role/name = %q/%q, want planner/planner", command.Role, command.AgentName)
	}
}

func TestTaskActorAgentPublishFailureDoesNotAdvanceTask(t *testing.T) {
	ctx := context.Background()
	_, bus, coordinator, tasks, allocator := newTaskActorSwarmServices(t, ctx)
	exec := &taskActorExecutor{tasks: tasks, coordinator: coordinator, agents: allocator, maxIters: 3}
	locator := taskActorTestLocator()
	const taskID = "goal-publish-failure"
	if _, err := tasks.Create(ctx, baldastate.SwarmTaskRecord{
		ID:          taskID,
		SessionID:   locator.SessionID,
		Title:       "Goal",
		Objective:   "fix ordering",
		Status:      baldastate.SwarmTaskStatusRunning,
		CreatedFrom: "goal",
	}, "test", nil); err != nil {
		t.Fatalf("Create task: %v", err)
	}
	bus.commandErrs = []error{errors.New("jetstream unavailable")}

	err := exec.dispatchAgent(ctx, taskAgentCommandPayload{
		TaskID:          taskID,
		Role:            taskAgentRoleReviewer,
		Iteration:       1,
		Locator:         locator,
		Objective:       "fix ordering",
		TransportUserID: "tg-101",
		MaxIterations:   3,
	})
	if err == nil {
		t.Fatal("dispatchAgent() error = nil, want publish failure")
	}
	if len(bus.commands) != 0 {
		t.Fatalf("published commands = %d, want 0 after failed publish", len(bus.commands))
	}
	task, ok, err := tasks.Get(ctx, taskID)
	if err != nil {
		t.Fatalf("Get(task) error = %v", err)
	}
	if !ok || task.Status != baldastate.SwarmTaskStatusRunning {
		t.Fatalf("task = %+v found=%v, want status to remain running", task, ok)
	}
}

func TestTaskActorPlannerResultStoresPlanAndDispatchesExecutor(t *testing.T) {
	ctx := context.Background()
	_, bus, coordinator, tasks, allocator := newTaskActorSwarmServices(t, ctx)
	exec := &taskActorExecutor{tasks: tasks, coordinator: coordinator, agents: allocator, maxIters: 3}
	locator := taskActorTestLocator()
	env, goal := taskActorGoalEnvelope(t, locator, "fix failing tests", 3)
	if err := exec.startGoalTask(ctx, env, goal); err != nil {
		t.Fatalf("startGoalTask() error = %v", err)
	}

	plannerText := "1. inspect tests\n2. patch code\n3. run go test ./..."
	resultEnv := swarm.Envelope{ID: "planner-result-1", Namespace: swarm.NamespaceAgentResult, Kind: swarm.KindGoal, From: swarm.ActorAddress{Target: swarm.ActorTypeAgent, Key: swarm.AgentNamePlanner}, To: swarm.ActorAddress{Target: swarm.ActorTypeTask, Key: goal.TaskID}, SessionID: locator.SessionID, TaskID: goal.TaskID, PayloadJSON: `{}`}
	if err := exec.handleAgentResult(ctx, resultEnv, taskAgentResultPayload{
		TaskID: goal.TaskID, AgentName: swarm.AgentNamePlanner, Role: taskAgentRolePlanner, Iteration: 1, Locator: locator, Objective: goal.Objective, TransportUserID: goal.TransportUserID, Text: plannerText, MaxIterations: goal.MaxIterations,
	}); err != nil {
		t.Fatalf("handleAgentResult(planner) error = %v", err)
	}

	appended := bus.commands[3:]
	if len(appended) != 3 {
		t.Fatalf("planner appended commands = %d, want executor + finished delivery + executor-start delivery", len(appended))
	}
	if appended[0].To.Target != swarm.ActorTypeAgent || appended[0].To.Key != swarm.AgentNameExecutor {
		t.Fatalf("first planner follow-up = %+v, want executor command before visible finish", appended[0].To)
	}
	if !strings.Contains(appended[1].DedupeKey, ":delivery:finished:planner:1") {
		t.Fatalf("second planner follow-up dedupe = %q, want planner finished delivery", appended[1].DedupeKey)
	}
	executor := lastPublishedCommandTo(t, bus, swarm.ActorTypeAgent, swarm.AgentNameExecutor)
	if executor.To.Target != swarm.ActorTypeAgent || executor.To.Key != swarm.AgentNameExecutor {
		t.Fatalf("executor command target = %+v, want agent:executor", executor.To)
	}
	var command taskAgentCommandPayload
	if err := json.Unmarshal([]byte(executor.PayloadJSON), &command); err != nil {
		t.Fatalf("decode executor command: %v", err)
	}
	if command.Role != taskAgentRoleExecutor || command.Plan != plannerText || command.PlannerOutput != plannerText {
		t.Fatalf("executor command = %+v, want executor with planner text", command)
	}

	task, ok, err := tasks.Get(ctx, goal.TaskID)
	if err != nil {
		t.Fatalf("Get(task) error = %v", err)
	}
	if !ok || !strings.Contains(task.PlanJSON, "inspect tests") {
		t.Fatalf("task = %+v found=%v, want planner output", task, ok)
	}
}

func TestTaskActorExecutorResultDispatchesReviewerBeforeProgress(t *testing.T) {
	ctx := context.Background()
	_, bus, coordinator, tasks, allocator := newTaskActorSwarmServices(t, ctx)
	exec := &taskActorExecutor{tasks: tasks, coordinator: coordinator, agents: allocator, maxIters: 3}
	locator := taskActorTestLocator()
	env, goal := taskActorGoalEnvelope(t, locator, "fix failing tests", 3)
	if err := exec.startGoalTask(ctx, env, goal); err != nil {
		t.Fatalf("startGoalTask() error = %v", err)
	}
	plannerText := "1. inspect tests\n2. patch code"
	if err := exec.handleAgentResult(ctx, swarm.Envelope{ID: "planner-result-1", Namespace: swarm.NamespaceAgentResult, Kind: swarm.KindGoal, From: swarm.ActorAddress{Target: swarm.ActorTypeAgent, Key: swarm.AgentNamePlanner}, To: swarm.ActorAddress{Target: swarm.ActorTypeTask, Key: goal.TaskID}, SessionID: locator.SessionID, TaskID: goal.TaskID, PayloadJSON: `{}`}, taskAgentResultPayload{
		TaskID: goal.TaskID, AgentName: swarm.AgentNamePlanner, Role: taskAgentRolePlanner, Iteration: 1, Locator: locator, Objective: goal.Objective, TransportUserID: goal.TransportUserID, Text: plannerText, MaxIterations: goal.MaxIterations,
	}); err != nil {
		t.Fatalf("handleAgentResult(planner) error = %v", err)
	}
	beforeExecutor := len(bus.commands)

	executorText := "patched code and ran tests"
	if err := exec.handleAgentResult(ctx, swarm.Envelope{ID: "executor-result-1", Namespace: swarm.NamespaceAgentResult, Kind: swarm.KindGoal, From: swarm.ActorAddress{Target: swarm.ActorTypeAgent, Key: swarm.AgentNameExecutor}, To: swarm.ActorAddress{Target: swarm.ActorTypeTask, Key: goal.TaskID}, SessionID: locator.SessionID, TaskID: goal.TaskID, PayloadJSON: `{}`}, taskAgentResultPayload{
		TaskID: goal.TaskID, AgentName: swarm.AgentNameExecutor, Role: taskAgentRoleExecutor, Iteration: 1, Locator: locator, Objective: goal.Objective, Plan: plannerText, PlannerOutput: plannerText, TransportUserID: goal.TransportUserID, Text: executorText, MaxIterations: goal.MaxIterations,
	}); err != nil {
		t.Fatalf("handleAgentResult(executor) error = %v", err)
	}

	appended := bus.commands[beforeExecutor:]
	if len(appended) != 3 {
		t.Fatalf("executor appended commands = %d, want reviewer + finished delivery + validator-start delivery", len(appended))
	}
	if appended[0].To.Target != swarm.ActorTypeAgent || appended[0].To.Key != swarm.AgentNameReviewer {
		t.Fatalf("first executor follow-up = %+v, want reviewer command before visible finish", appended[0].To)
	}
	if !strings.Contains(appended[1].DedupeKey, ":delivery:finished:executor:1") {
		t.Fatalf("second executor follow-up dedupe = %q, want executor finished delivery", appended[1].DedupeKey)
	}
}

func TestTaskActorEnsureGoalTaskIgnoresCreatedEventPublishFailure(t *testing.T) {
	ctx := context.Background()
	_, bus, coordinator, tasks, allocator := newTaskActorSwarmServices(t, ctx)
	exec := &taskActorExecutor{tasks: tasks, coordinator: coordinator, agents: allocator, maxIters: 3}
	locator := taskActorTestLocator()
	_, goal := taskActorGoalEnvelope(t, locator, "repair task event", 3)
	bus.eventErrs = []error{errors.New("event stream unavailable")}

	if err := exec.ensureGoalTask(ctx, goal.TaskID, goal, goal.Objective); err != nil {
		t.Fatalf("ensureGoalTask(first) error = %v, want nil because task events are visibility-only", err)
	}
	if len(bus.eventEnvs) != 2 {
		t.Fatalf("event publish attempts = %d, want created + queued visibility attempts", len(bus.eventEnvs))
	}
	wantCreatedEventID := "task:" + goal.TaskID + ":event:created"
	if bus.eventEnvs[0].ID != wantCreatedEventID {
		t.Fatalf("created event id = %q, want deterministic task.created event id", bus.eventEnvs[0].ID)
	}
	task, ok, err := tasks.Get(ctx, goal.TaskID)
	if err != nil {
		t.Fatalf("Get(task) error = %v", err)
	}
	if !ok || task.Status != baldastate.SwarmTaskStatusQueued {
		t.Fatalf("task = %+v found=%v, want queued after repaired ensure", task, ok)
	}
}

func TestSubmitWebhookTaskUsesStableTaskAndDistinctDedupeKeys(t *testing.T) {
	ctx := context.Background()
	bus := &recordingHandlerCommandBus{}
	handler := &BaldaHandler{swarmCoordinator: swarm.NewCoordinator(bus, swarm.Config{Enabled: true})}
	locator := taskActorTestLocator()
	payload := sessionTurnPayload{
		Text:      "release event",
		Locator:   locator,
		UserID:    "tg-101",
		Source:    "webhook",
		DedupeKey: "webhook:release:req-1",
	}

	result, taskID, err := handler.submitWebhookTask(ctx, payload, "release", "req-1")
	if err != nil {
		t.Fatalf("submitWebhookTask() error = %v", err)
	}
	_, duplicateTaskID, err := handler.submitWebhookTask(ctx, payload, "release", "req-1")
	if err != nil {
		t.Fatalf("submitWebhookTask(duplicate) error = %v", err)
	}
	if taskID == "" || duplicateTaskID != taskID {
		t.Fatalf("task ids = %q/%q, want stable non-empty task id", taskID, duplicateTaskID)
	}
	if result.MsgID != "webhook:release:req-1:task" {
		t.Fatalf("msg id = %q, want task-scoped dedupe", result.MsgID)
	}
	if got := len(bus.commands); got != 2 {
		t.Fatalf("published commands = %d, want 2 recording bus calls", got)
	}
	parent := bus.commands[0]
	if parent.DedupeKey != "webhook:release:req-1:task" {
		t.Fatalf("parent dedupe = %q, want task-scoped key", parent.DedupeKey)
	}
	var body taskEnvelopePayload
	if err := json.Unmarshal([]byte(parent.PayloadJSON), &body); err != nil {
		t.Fatalf("decode parent payload: %v", err)
	}
	if body.SessionTurn == nil || body.SessionTurn.DedupeKey != "webhook:release:req-1:session" {
		t.Fatalf("child dedupe in payload = %+v, want session-scoped key", body.SessionTurn)
	}
}

func TestTaskActorDispatchSessionTurnKeepsTaskRunningUntilSessionCompletes(t *testing.T) {
	ctx := context.Background()
	provider, bus, coordinator, tasks, allocator := newTaskActorSwarmServices(t, ctx)
	_ = provider
	_ = coordinator
	_ = allocator
	exec := &taskActorExecutor{tasks: tasks, coordinator: swarm.NewCoordinator(bus, swarm.Config{Enabled: true})}
	locator := taskActorTestLocator()
	taskID := "webhook-release-abc"
	payload := sessionTurnPayload{
		Text:      "release event",
		Locator:   locator,
		UserID:    "tg-101",
		Source:    "webhook",
		DedupeKey: "webhook:release:req-1:session",
	}
	env := swarm.Envelope{
		ID:          "task-command-1",
		Namespace:   swarm.NamespaceWebhookInbound,
		Kind:        swarm.KindWebhookEvent,
		From:        swarm.ActorAddress{Target: "webhook", Key: "release"},
		To:          swarm.ActorAddress{Target: swarm.ActorTypeTask, Key: taskID},
		SessionID:   locator.SessionID,
		TaskID:      taskID,
		PayloadJSON: `{}`,
	}

	if err := exec.dispatchSessionTurn(ctx, env, payload); err != nil {
		t.Fatalf("dispatchSessionTurn() error = %v", err)
	}
	task, ok, err := tasks.Get(ctx, taskID)
	if err != nil {
		t.Fatalf("Get(task) error = %v", err)
	}
	if !ok || task.Status != baldastate.SwarmTaskStatusRunning {
		t.Fatalf("task = %+v found=%v, want running until SessionActor completes it", task, ok)
	}
	last := bus.commands[len(bus.commands)-1]
	if last.To.Target != swarm.ActorTypeSession || last.DedupeKey != payload.DedupeKey {
		t.Fatalf("child session command = %+v, want session command with child dedupe", last)
	}
}

func TestTaskActorStartScheduledTaskDispatchesSessionTurn(t *testing.T) {
	ctx := context.Background()
	provider, bus, coordinator, tasks, allocator := newTaskActorSwarmServices(t, ctx)
	_ = provider
	_ = allocator
	exec := &taskActorExecutor{tasks: tasks, coordinator: coordinator}
	locator := taskActorTestLocator()
	taskID := "scheduled-daily-review-slot-1"
	payload := scheduledTaskPayload{
		TaskID:  "daily-review",
		Content: "review open work",
		Locator: locator,
		UserID:  "tg-101",
	}
	data, err := json.Marshal(taskEnvelopePayload{Kind: taskPayloadKindScheduledTask, ScheduledTask: &payload})
	if err != nil {
		t.Fatalf("encode scheduled payload: %v", err)
	}
	env := swarm.Envelope{
		ID:          "scheduled-command-1",
		Namespace:   swarm.NamespaceScheduleInbound,
		Kind:        swarm.KindScheduledTask,
		From:        swarm.ActorAddress{Target: "schedule", Key: "daily-review"},
		To:          swarm.ActorAddress{Target: swarm.ActorTypeTask, Key: taskID},
		SessionID:   locator.SessionID,
		TaskID:      taskID,
		DedupeKey:   "schedule:daily-review:slot-1",
		PayloadJSON: string(data),
	}

	if err := exec.startScheduledTaskTask(ctx, env, payload); err != nil {
		t.Fatalf("startScheduledTaskTask() error = %v", err)
	}
	task, ok, err := tasks.Get(ctx, taskID)
	if err != nil {
		t.Fatalf("Get(task) error = %v", err)
	}
	if !ok || task.Status != baldastate.SwarmTaskStatusRunning || task.CreatedFrom != "schedule" {
		t.Fatalf("task = %+v found=%v, want running scheduled task", task, ok)
	}
	last := bus.commands[len(bus.commands)-1]
	if last.To.Target != swarm.ActorTypeSession || last.DedupeKey != "schedule:daily-review:slot-1:session" {
		t.Fatalf("child command = %+v, want session command with child dedupe", last)
	}
	var child sessionTurnPayload
	if err := json.Unmarshal([]byte(last.PayloadJSON), &child); err != nil {
		t.Fatalf("decode child session payload: %v", err)
	}
	if child.ScheduledTaskID != "daily-review" || child.Deliver || child.Source != "schedule" {
		t.Fatalf("child payload = %+v, want scheduled no-delivery turn", child)
	}
}

func TestSessionActorCompletesTaskAfterTurnSuccess(t *testing.T) {
	ctx := context.Background()
	provider, bus, coordinator, tasks, allocator := newTaskActorSwarmServices(t, ctx)
	_ = provider
	_ = bus
	_ = coordinator
	_ = allocator
	taskID := "webhook-release-complete"
	_, err := tasks.Create(ctx, baldastate.SwarmTaskRecord{
		ID:          taskID,
		SessionID:   "tg-9001-99",
		Title:       "Webhook task",
		Objective:   "release event",
		Status:      baldastate.SwarmTaskStatusRunning,
		CreatedFrom: "webhook",
	}, "test", nil)
	if err != nil {
		t.Fatalf("Create task: %v", err)
	}
	exec := &sessionActorExecutor{tasks: tasks}
	if err := exec.recordSessionTaskResult(ctx, swarm.Envelope{
		ID:        "session-command-1",
		Namespace: swarm.NamespaceWebhookInbound,
		Kind:      swarm.KindWebhookEvent,
		TaskID:    taskID,
	}, sessionTurnPayload{}, nil); err != nil {
		t.Fatalf("recordSessionTaskResult() error = %v", err)
	}

	task, ok, err := tasks.Get(ctx, taskID)
	if err != nil {
		t.Fatalf("Get(task) error = %v", err)
	}
	if !ok || task.Status != baldastate.SwarmTaskStatusCompleted {
		t.Fatalf("task = %+v found=%v, want completed", task, ok)
	}
}

func TestSessionActorIgnoresTaskResultEventPublishFailure(t *testing.T) {
	ctx := context.Background()
	provider, bus, coordinator, tasks, allocator := newTaskActorSwarmServices(t, ctx)
	_ = provider
	_ = coordinator
	_ = allocator
	const taskID = "webhook-release-persist-fails"
	if _, err := tasks.Create(ctx, baldastate.SwarmTaskRecord{
		ID:          taskID,
		SessionID:   "tg-9001-99",
		Title:       "Webhook task",
		Objective:   "release event",
		Status:      baldastate.SwarmTaskStatusRunning,
		CreatedFrom: "webhook",
	}, "test", nil); err != nil {
		t.Fatalf("Create task: %v", err)
	}
	bus.eventErrs = []error{errors.New("event stream unavailable")}
	exec := &sessionActorExecutor{tasks: tasks}

	err := exec.recordSessionTaskResult(ctx, swarm.Envelope{
		ID:        "session-command-1",
		Namespace: swarm.NamespaceWebhookInbound,
		Kind:      swarm.KindWebhookEvent,
		TaskID:    taskID,
	}, sessionTurnPayload{}, nil)
	if err != nil {
		t.Fatalf("recordSessionTaskResult() error = %v, want nil because task events are visibility-only", err)
	}
	task, ok, err := tasks.Get(ctx, taskID)
	if err != nil || !ok {
		t.Fatalf("Get(task) = %+v found=%v err=%v", task, ok, err)
	}
	if task.Status != baldastate.SwarmTaskStatusCompleted {
		t.Fatalf("task status = %q, want %q", task.Status, baldastate.SwarmTaskStatusCompleted)
	}
}

func TestTaskAgentResolveSessionEnsuresMissingSession(t *testing.T) {
	ctx := context.Background()
	locator := taskActorTestLocator()
	locator.AddressJSON = `{"chat_id":9001,"topic_id":99}`
	store := &fakeBaldaRestoreSessionStore{}
	manager := newBaldaRestoreSessionManager(t, &fakeBaldaRestoreAgentBuilder{}, &fakeBaldaRestoreRuntimeManager{providerID: "balda-provider"}, store)
	actor := &taskAgentActor{sessions: manager}

	ts, err := actor.resolveSession(ctx, taskAgentCommandPayload{
		Locator:         locator,
		TransportUserID: "tg-101",
	})
	if err != nil {
		t.Fatalf("resolveSession() error = %v", err)
	}
	if ts.GetSessionID() != locator.SessionID {
		t.Fatalf("session id = %q, want %q", ts.GetSessionID(), locator.SessionID)
	}
	if store.lastUpsert.SessionID != locator.SessionID || store.lastUpsert.UserID != "tg-101" {
		t.Fatalf("last upsert = %+v, want ensured session for tg-101", store.lastUpsert)
	}
}

func newTaskActorSwarmServices(t *testing.T, ctx context.Context) (baldastate.Provider, *recordingHandlerCommandBus, *swarm.Coordinator, *swarm.TaskService, *swarm.AgentAllocator) {
	t.Helper()
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	bus := &recordingHandlerCommandBus{}
	cfg, err := (swarm.Config{Enabled: true}).Normalized()
	if err != nil {
		t.Fatalf("Normalize swarm config: %v", err)
	}
	var coordinator *swarm.Coordinator
	var tasks *swarm.TaskService
	var allocator *swarm.AgentAllocator
	app := fxtest.New(t,
		fx.Supply(
			fx.Annotate(provider, fx.As(new(baldastate.Provider))),
			cfg,
		),
		fx.Provide(
			func() swarm.CoordinatorBus { return bus },
			func() swarm.EventPublisher { return bus },
		),
		fx.Provide(swarm.NewTaskService, swarm.NewAgentRegistry, swarm.NewAgentAllocator, swarm.NewCoordinator),
		fx.Populate(&coordinator, &tasks, &allocator),
	)
	app.RequireStart()
	t.Cleanup(func() { app.RequireStop() })
	return provider, bus, coordinator, tasks, allocator
}

func taskActorTestLocator() baldasession.SessionLocator {
	return baldasession.SessionLocator{SessionID: "tg-9001-99", ChannelType: "telegram", AddressKey: "9001:99"}
}

func taskActorGoalEnvelope(t *testing.T, locator baldasession.SessionLocator, objective string, maxIterations int) (swarm.Envelope, goalTaskPayload) {
	t.Helper()
	env, err := goalTaskEnvelope(locator, objective, "tg-101", maxIterations)
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

func lastPublishedCommandTo(t *testing.T, bus *recordingHandlerCommandBus, target string, key string) swarm.Envelope {
	t.Helper()
	for i := len(bus.commands) - 1; i >= 0; i-- {
		env := bus.commands[i]
		if env.To.Target == target && env.To.Key == key {
			return env
		}
	}
	t.Fatalf("no command to %s:%s in %+v", target, key, bus.commands)
	return swarm.Envelope{}
}
