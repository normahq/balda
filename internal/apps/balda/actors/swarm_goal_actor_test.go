package actors

import (
	"context"
	"iter"
	"strings"
	"testing"

	baldaagent "github.com/normahq/balda/internal/apps/balda/agent"
	"github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/rs/zerolog"
	adkagent "google.golang.org/adk/agent"
	adkrunner "google.golang.org/adk/runner"
	adksession "google.golang.org/adk/session"
	"google.golang.org/genai"
)

func TestGoalActorCompletesPassingRun(t *testing.T) {
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
	actor := &goalActor{
		tasks:          tasks,
		dispatcher:     dispatcher,
		sessions:       manager,
		runtimeBuilder: runtimeBuilder,
		taskRuns:       NewTaskRunRegistry(),
		maxIters:       3,
		logger:         zerolog.Nop(),
	}
	env, err := GoalTaskEnvelope(locator, "ship release", "101", 3)
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

type fakeGoalRuntimeBuilder struct {
	t                  *testing.T
	finalValidatorText string
	commitMessage      string
	commitErr          error
	exportErr          error
	cleanupCalls       int
	exportedMessage    string
}

func (b *fakeGoalRuntimeBuilder) BuildGoalRuntime(ctx context.Context, cfg baldaagent.GoalRuntimeConfig) (*baldaagent.GoalRuntime, error) {
	b.t.Helper()
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
				workerStarted := goalTestMetadataEvent(inv.InvocationID(), goalWorkerStep, goalStepStarted)
				if !yield(workerStarted, nil) {
					return
				}
				if !yield(goalTestTextEvent(inv.InvocationID(), "worker completed"), nil) {
					return
				}
				if !yield(goalTestMetadataEvent(inv.InvocationID(), goalWorkerStep, goalStepCompleted), nil) {
					return
				}
				if !yield(goalTestMetadataEvent(inv.InvocationID(), goalValidatorStep, goalStepStarted), nil) {
					return
				}
				if !yield(goalTestTextEvent(inv.InvocationID(), b.finalValidatorText), nil) {
					return
				}
				completed := goalTestMetadataEvent(inv.InvocationID(), goalValidatorStep, goalStepCompleted)
				completed.TurnComplete = true
				yield(completed, nil)
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
	return &baldaagent.GoalRuntime{
		Agent:        ag,
		Runner:       r,
		SessionID:    cfg.TaskID,
		WorkspaceDir: workspaceDir,
		BranchName:   "norma/balda/goal/" + cfg.TaskID,
		BuildCommitMessageFn: func(context.Context, string, string, string) (string, error) {
			return b.commitMessage, b.commitErr
		},
		ExportWorkspaceFn: func(_ context.Context, commitMessage string) error {
			b.exportedMessage = commitMessage
			return b.exportErr
		},
		CleanupResourcesFn: func(context.Context) error {
			b.cleanupCalls++
			return nil
		},
	}, nil
}

func TestGoalActor_PreservesWorkspaceOnExportFailure(t *testing.T) {
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
	actor := &goalActor{
		tasks:          tasks,
		dispatcher:     dispatcher,
		sessions:       manager,
		runtimeBuilder: runtimeBuilder,
		taskRuns:       NewTaskRunRegistry(),
		maxIters:       3,
		logger:         zerolog.Nop(),
	}
	env, err := GoalTaskEnvelope(locator, "ship release", "101", 3)
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
	if !strings.Contains(task.ResultJSON, `"status":"`+goalExportStatusFailed+`"`) {
		t.Fatalf("task result = %s, want export_failed status", task.ResultJSON)
	}
	if runtimeBuilder.cleanupCalls != 0 {
		t.Fatalf("cleanupCalls = %d, want 0 when export fails", runtimeBuilder.cleanupCalls)
	}
}

func goalTestMetadataEvent(invocationID string, step string, eventType string) *adksession.Event {
	ev := adksession.NewEvent(invocationID)
	ev.CustomMetadata = map[string]any{
		goalMetadataEventKey: eventType,
		goalMetadataStepKey:  step,
	}
	return ev
}

func goalTestTextEvent(invocationID string, text string) *adksession.Event {
	ev := adksession.NewEvent(invocationID)
	ev.Content = genai.NewContentFromText(text, genai.RoleModel)
	return ev
}
