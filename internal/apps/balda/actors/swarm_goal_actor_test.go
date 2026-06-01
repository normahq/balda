package actors

import (
	"context"
	"encoding/json"
	"iter"
	"slices"
	"strings"
	"testing"

	"github.com/normahq/balda/internal/apps/balda/actors/goalkeeper"
	"github.com/normahq/balda/internal/apps/balda/progress"
	"github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/rs/zerolog"
	adkagent "google.golang.org/adk/agent"
	adkrunner "google.golang.org/adk/runner"
	adksession "google.golang.org/adk/session"
	"google.golang.org/genai"
)

func TestGoalKeeperActorCompletesPassingRun(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, bus, dispatcher, tasks, _ := newTaskActorSwarmServices(t, ctx)
	locator := session.SessionLocator{SessionID: "tg-101-202", AddressKey: "101"}
	ts := newBaldaTopicSession(t, locator.SessionID)
	setUnexportedField(t, ts, "userID", "101")
	setUnexportedField(t, ts, "agentSessionID", "adk-session-1")
	setUnexportedField(t, ts, "workspaceDir", t.TempDir())
	manager := newBaldaSessionManagerWithSession(t, locator, ts)
	runtimeBuilder := &fakeGoalRuntimeBuilder{t: t, finalValidatorText: "verdict: pass\nvalidated"}
	actor := goalkeeper.NewActor(goalkeeper.ActorParams{
		TaskService:        tasks,
		Dispatcher:         dispatcher,
		SessionManager:     manager,
		RuntimeBuilder:     runtimeBuilder,
		TaskRuns:           NewTaskRunRegistry(),
		MaxIterations:      3,
		PlanUpdatesEnabled: false,
		Logger:             zerolog.Nop(),
	})
	env, err := goalkeeper.GoalTaskEnvelope(locator, "ship release", "101", 3)
	if err != nil {
		t.Fatalf("GoalTaskEnvelope() error = %v", err)
	}
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	task, ok, err := tasks.Get(ctx, env.TaskID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatalf("task %q not found", env.TaskID)
	}
	if task.Status != baldastate.SwarmTaskStatusCompleted {
		t.Fatalf("task status = %q, want %q", task.Status, baldastate.SwarmTaskStatusCompleted)
	}
	if !strings.Contains(task.ResultJSON, `"goal_reached":true`) {
		t.Fatalf("task result = %s, want goal_reached true", task.ResultJSON)
	}
	if runtimeBuilder.exportedMessage == "" {
		t.Fatal("exportedMessage = empty, want generated commit message")
	}
	if runtimeBuilder.cleanupCalls != 1 {
		t.Fatalf("cleanupCalls = %d, want 1", runtimeBuilder.cleanupCalls)
	}
	if got := lastPublishedCommandTo(t, bus, swarm.ActorTypeDelivery, locator.AddressKey); got.Kind != taskPayloadKindDelivery {
		t.Fatalf("last delivery = %+v, want delivery command", got)
	}
}

func TestGoalKeeperActorRejectsSecondActiveGoalInSession(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, bus, dispatcher, tasks, _ := newTaskActorSwarmServices(t, ctx)
	locator := session.SessionLocator{SessionID: "tg-101-202", AddressKey: "101"}
	ts := newBaldaTopicSession(t, locator.SessionID)
	setUnexportedField(t, ts, "userID", "101")
	setUnexportedField(t, ts, "agentSessionID", "adk-session-1")
	setUnexportedField(t, ts, "workspaceDir", t.TempDir())
	manager := newBaldaSessionManagerWithSession(t, locator, ts)
	if _, err := tasks.Create(ctx, baldastate.SwarmTaskRecord{
		ID:            "goal-existing",
		SessionID:     locator.SessionID,
		Objective:     "existing goal",
		Status:        baldastate.SwarmTaskStatusRunning,
		OwnerActor:    swarm.ActorTypeGoalkeeper + ":goal-existing",
		AssignedActor: swarm.ActorTypeGoalkeeper + ":goal-existing",
	}, "test", nil); err != nil {
		t.Fatalf("Create existing goal task: %v", err)
	}
	runtimeBuilder := &fakeGoalRuntimeBuilder{t: t}
	actor := goalkeeper.NewActor(goalkeeper.ActorParams{
		TaskService:        tasks,
		Dispatcher:         dispatcher,
		SessionManager:     manager,
		RuntimeBuilder:     runtimeBuilder,
		TaskRuns:           NewTaskRunRegistry(),
		MaxIterations:      3,
		PlanUpdatesEnabled: false,
		Logger:             zerolog.Nop(),
	})
	env, err := goalkeeper.GoalTaskEnvelope(locator, "run tests", "101", 3)
	if err != nil {
		t.Fatalf("GoalTaskEnvelope() error = %v", err)
	}
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	task, ok, err := tasks.Get(ctx, env.TaskID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatalf("task %q not found", env.TaskID)
	}
	if task.Status != baldastate.SwarmTaskStatusCanceled {
		t.Fatalf("task status = %q, want %q", task.Status, baldastate.SwarmTaskStatusCanceled)
	}
	if runtimeBuilder.buildCalls != 0 {
		t.Fatalf("buildCalls = %d, want 0", runtimeBuilder.buildCalls)
	}
	texts := deliveryTextsForTask(t, bus, env.TaskID)
	if got := countMatches(texts, "A goal run is already active for this session."); got != 1 {
		t.Fatalf("already-active deliveries = %d, want 1\n%v", got, texts)
	}
}

func TestGoalKeeperActorDeliversWorkerProgressAndDedupesRepeatedOutput(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, bus, dispatcher, tasks, _ := newTaskActorSwarmServices(t, ctx)
	locator := session.SessionLocator{SessionID: "tg-101-202", AddressKey: "101"}
	ts := newBaldaTopicSession(t, locator.SessionID)
	setUnexportedField(t, ts, "userID", "101")
	setUnexportedField(t, ts, "agentSessionID", "adk-session-1")
	setUnexportedField(t, ts, "workspaceDir", t.TempDir())
	manager := newBaldaSessionManagerWithSession(t, locator, ts)
	runtimeBuilder := &fakeGoalRuntimeBuilder{
		t: t,
		events: []goalTestEvent{
			{kind: "step", step: goalkeeper.WorkerStep, eventType: goalkeeper.StepStarted},
			{kind: "text", text: "worker completed"},
			{kind: "text", text: "worker completed"},
			{kind: "step", step: goalkeeper.WorkerStep, eventType: goalkeeper.StepCompleted},
			{kind: "step", step: goalkeeper.ValidatorStep, eventType: goalkeeper.StepStarted},
			{kind: "text", text: "verdict: pass\nvalidated"},
			{kind: "step", step: goalkeeper.ValidatorStep, eventType: goalkeeper.StepCompleted, turnComplete: true},
		},
	}
	actor := goalkeeper.NewActor(goalkeeper.ActorParams{
		TaskService:        tasks,
		Dispatcher:         dispatcher,
		SessionManager:     manager,
		RuntimeBuilder:     runtimeBuilder,
		TaskRuns:           NewTaskRunRegistry(),
		MaxIterations:      3,
		PlanUpdatesEnabled: false,
		Logger:             zerolog.Nop(),
	})
	env, err := goalkeeper.GoalTaskEnvelope(locator, "run tests", "101", 3)
	if err != nil {
		t.Fatalf("GoalTaskEnvelope() error = %v", err)
	}
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	texts := deliveryTextsForTask(t, bus, env.TaskID)
	joined := strings.Join(texts, "\n---\n")
	for _, want := range []string{
		"Goal iteration 1/3: worker started.",
		"Goal iteration 1/3: worker update.\n\nworker completed",
		"Goal iteration 1/3: worker completed.",
		"Goal iteration 1/3: validator started.",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("delivery texts missing %q\n%s", want, joined)
		}
	}
	if got := countMatches(texts, "Goal iteration 1/3: worker update.\n\nworker completed"); got != 1 {
		t.Fatalf("worker update deliveries = %d, want 1\n%v", got, texts)
	}
	var progressKinds []string
	for _, event := range bus.eventEnvs {
		if event.Meta["event_type"] != swarm.TaskEventAgentProgress || event.TaskID != env.TaskID {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
			t.Fatalf("progress payload decode: %v", err)
		}
		progressKinds = append(progressKinds, strings.TrimSpace(payload["kind"].(string)))
	}
	for _, want := range []string{"output", "completed"} {
		if !slices.Contains(progressKinds, want) {
			t.Fatalf("progress kinds = %v, want %q", progressKinds, want)
		}
	}
}

func TestGoalKeeperActorDeliversPlanUpdatesWhenEnabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, bus, dispatcher, tasks, _ := newTaskActorSwarmServices(t, ctx)
	locator := session.SessionLocator{SessionID: "tg-101-202", AddressKey: "101"}
	ts := newBaldaTopicSession(t, locator.SessionID)
	setUnexportedField(t, ts, "userID", "101")
	setUnexportedField(t, ts, "agentSessionID", "adk-session-1")
	setUnexportedField(t, ts, "workspaceDir", t.TempDir())
	manager := newBaldaSessionManagerWithSession(t, locator, ts)
	runtimeBuilder := &fakeGoalRuntimeBuilder{
		t: t,
		events: []goalTestEvent{
			{kind: "step", step: goalkeeper.WorkerStep, eventType: goalkeeper.StepStarted},
			{kind: "plan", planEntries: []map[string]any{{"content": "Run tests", "status": "in_progress"}}},
			{kind: "plan", planEntries: []map[string]any{{"content": "Run tests", "status": "in_progress"}}},
			{kind: "text", text: "worker completed"},
			{kind: "step", step: goalkeeper.WorkerStep, eventType: goalkeeper.StepCompleted},
			{kind: "step", step: goalkeeper.ValidatorStep, eventType: goalkeeper.StepStarted},
			{kind: "text", text: "verdict: pass\nvalidated"},
			{kind: "step", step: goalkeeper.ValidatorStep, eventType: goalkeeper.StepCompleted, turnComplete: true},
		},
	}
	actor := goalkeeper.NewActor(goalkeeper.ActorParams{
		TaskService:        tasks,
		Dispatcher:         dispatcher,
		SessionManager:     manager,
		RuntimeBuilder:     runtimeBuilder,
		TaskRuns:           NewTaskRunRegistry(),
		MaxIterations:      3,
		PlanUpdatesEnabled: true,
		Logger:             zerolog.Nop(),
	})
	env, err := goalkeeper.GoalTaskEnvelope(locator, "run tests", "101", 3)
	if err != nil {
		t.Fatalf("GoalTaskEnvelope() error = %v", err)
	}
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	texts := deliveryTextsForTask(t, bus, env.TaskID)
	planText := "Goal iteration 1/3: worker plan update.\n\nPlan update\n- [in progress] Run tests"
	if got := countMatches(texts, planText); got != 1 {
		t.Fatalf("plan update deliveries = %d, want 1\n%v", got, texts)
	}
}

type fakeGoalRuntimeBuilder struct {
	t                  *testing.T
	finalValidatorText string
	commitMessage      string
	commitErr          error
	exportErr          error
	cleanupCalls       int
	buildCalls         int
	exportedMessage    string
	events             []goalTestEvent
}

func (b *fakeGoalRuntimeBuilder) BuildGoalRuntime(ctx context.Context, cfg goalkeeper.GoalRuntimeConfig) (goalkeeper.GoalRuntime, error) {
	b.t.Helper()
	b.buildCalls++
	if cfg.UserID == "" || cfg.SourceSessionID == "" || cfg.TaskID == "" {
		b.t.Fatalf("BuildGoalRuntime() cfg = %+v, want user/source session/task", cfg)
	}
	workspaceDir := b.t.TempDir()
	svc := adksession.InMemoryService()
	if _, err := svc.Create(ctx, &adksession.CreateRequest{
		AppName:   "goal-test",
		UserID:    cfg.UserID,
		SessionID: cfg.TaskID,
	}); err != nil {
		return nil, err
	}
	ag, err := adkagent.New(adkagent.Config{
		Name:        "Goal",
		Description: "test goal",
		Run: func(inv adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
			return func(yield func(*adksession.Event, error) bool) {
				events := b.events
				if len(events) == 0 {
					events = []goalTestEvent{
						{kind: "step", step: goalkeeper.WorkerStep, eventType: goalkeeper.StepStarted},
						{kind: "text", text: "worker completed"},
						{kind: "step", step: goalkeeper.WorkerStep, eventType: goalkeeper.StepCompleted},
						{kind: "step", step: goalkeeper.ValidatorStep, eventType: goalkeeper.StepStarted},
						{kind: "text", text: b.finalValidatorText},
						{kind: "step", step: goalkeeper.ValidatorStep, eventType: goalkeeper.StepCompleted, turnComplete: true},
					}
				}
				for _, event := range events {
					ev := event.build(inv.InvocationID(), b.finalValidatorText)
					if !yield(ev, nil) {
						return
					}
				}
			}
		},
	})
	if err != nil {
		return nil, err
	}
	r, err := adkrunner.New(adkrunner.Config{AppName: "goal-test", Agent: ag, SessionService: svc})
	if err != nil {
		return nil, err
	}
	return fakeGoalRuntime{
		runner:       r,
		sessionID:    cfg.TaskID,
		workspaceDir: workspaceDir,
		branchName:   "norma/balda/goal/" + cfg.TaskID,
		buildCommitMessageFn: func(context.Context, string, string, string) (string, error) {
			return b.commitMessage, b.commitErr
		},
		exportWorkspaceFn: func(_ context.Context, commitMessage string) error {
			b.exportedMessage = commitMessage
			return b.exportErr
		},
		cleanupResourcesFn: func(context.Context) error {
			b.cleanupCalls++
			return nil
		},
	}, nil
}

func TestGoalKeeperActorPreservesWorkspaceOnExportFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	provider, bus, dispatcher, tasks, allocator := newTaskActorSwarmServices(t, ctx)
	_ = provider
	_ = bus
	_ = allocator
	locator := session.SessionLocator{SessionID: "tg-101-202", AddressKey: "101"}
	ts := newBaldaTopicSession(t, locator.SessionID)
	setUnexportedField(t, ts, "userID", "101")
	setUnexportedField(t, ts, "agentSessionID", "adk-session-1")
	setUnexportedField(t, ts, "workspaceDir", t.TempDir())
	manager := newBaldaSessionManagerWithSession(t, locator, ts)
	runtimeBuilder := &fakeGoalRuntimeBuilder{
		t:                  t,
		finalValidatorText: "verdict: pass\nvalidated",
		exportErr:          context.DeadlineExceeded,
	}
	actor := goalkeeper.NewActor(goalkeeper.ActorParams{
		TaskService:        tasks,
		Dispatcher:         dispatcher,
		SessionManager:     manager,
		RuntimeBuilder:     runtimeBuilder,
		TaskRuns:           NewTaskRunRegistry(),
		MaxIterations:      3,
		PlanUpdatesEnabled: false,
		Logger:             zerolog.Nop(),
	})
	env, err := goalkeeper.GoalTaskEnvelope(locator, "ship release", "101", 3)
	if err != nil {
		t.Fatalf("GoalTaskEnvelope() error = %v", err)
	}
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	task, ok, err := tasks.Get(ctx, env.TaskID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatalf("task %q not found", env.TaskID)
	}
	if task.Status != baldastate.SwarmTaskStatusFailed {
		t.Fatalf("task status = %q, want %q", task.Status, baldastate.SwarmTaskStatusFailed)
	}
	if !strings.Contains(task.ResultJSON, `"status":"export_failed"`) {
		t.Fatalf("task result = %s, want export_failed status", task.ResultJSON)
	}
	if runtimeBuilder.cleanupCalls != 0 {
		t.Fatalf("cleanupCalls = %d, want 0 when export fails", runtimeBuilder.cleanupCalls)
	}
}

type goalTestEvent struct {
	kind         string
	step         string
	eventType    string
	text         string
	turnComplete bool
	planEntries  []map[string]any
}

func (e goalTestEvent) build(invocationID string, fallbackValidatorText string) *adksession.Event {
	ev := adksession.NewEvent(invocationID)
	switch e.kind {
	case "step":
		ev.CustomMetadata = map[string]any{
			goalkeeper.MetadataEventKey: e.eventType,
			goalkeeper.MetadataStepKey:  e.step,
		}
	case "text":
		text := e.text
		if text == "" {
			text = fallbackValidatorText
		}
		ev.Content = genai.NewContentFromText(text, genai.RoleModel)
	case "plan":
		ev.Actions.StateDelta = map[string]any{}
		ev.Actions.StateDelta[progress.ACPPlanMetadataKey] = map[string]any{"entries": e.planEntries}
	}
	ev.TurnComplete = e.turnComplete
	return ev
}

type fakeGoalRuntime struct {
	runner               goalkeeper.GoalRunner
	sessionID            string
	workspaceDir         string
	branchName           string
	buildCommitMessageFn func(context.Context, string, string, string) (string, error)
	exportWorkspaceFn    func(context.Context, string) error
	cleanupResourcesFn   func(context.Context) error
}

func (r fakeGoalRuntime) Runner() goalkeeper.GoalRunner { return r.runner }
func (r fakeGoalRuntime) SessionID() string             { return r.sessionID }
func (r fakeGoalRuntime) WorkspaceDir() string          { return r.workspaceDir }
func (r fakeGoalRuntime) BranchName() string            { return r.branchName }
func (r fakeGoalRuntime) Close() error                  { return nil }

func (r fakeGoalRuntime) CleanupResources(ctx context.Context) error {
	if r.cleanupResourcesFn == nil {
		return nil
	}
	return r.cleanupResourcesFn(ctx)
}

func (r fakeGoalRuntime) BuildCommitMessage(ctx context.Context, objective string, workerOutput string, validatorOutput string) (string, error) {
	if r.buildCommitMessageFn == nil {
		return "chore(goal): complete goal", nil
	}
	message, err := r.buildCommitMessageFn(ctx, objective, workerOutput, validatorOutput)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(message) == "" {
		return "chore(goal): complete goal", nil
	}
	return message, nil
}

func (r fakeGoalRuntime) ExportWorkspace(ctx context.Context, commitMessage string) error {
	if r.exportWorkspaceFn == nil {
		return nil
	}
	return r.exportWorkspaceFn(ctx, commitMessage)
}

func deliveryTextsForTask(t *testing.T, bus *recordingHandlerCommandBus, taskID string) []string {
	t.Helper()
	var texts []string
	for _, env := range bus.commands {
		if env.TaskID != taskID || env.To.Target != swarm.ActorTypeDelivery {
			continue
		}
		var payload struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal([]byte(env.PayloadJSON), &payload); err != nil {
			t.Fatalf("decode delivery payload: %v", err)
		}
		texts = append(texts, payload.Text)
	}
	return texts
}

func countMatches(values []string, want string) int {
	count := 0
	for _, value := range values {
		if value == want {
			count++
		}
	}
	return count
}
