package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"strconv"
	"strings"
	"testing"

	baldaagent "github.com/normahq/balda/internal/apps/balda/agent"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	adkagent "google.golang.org/adk/agent"
	"google.golang.org/adk/runner"
	adksession "google.golang.org/adk/session"
	"google.golang.org/genai"
)

func TestTaskAgentActorHandleSkipsDuplicateRunningStep(t *testing.T) {
	ctx := context.Background()
	provider, bus, coordinator, tasks, allocator := newTaskActorSwarmServices(t, ctx)
	_ = provider
	_ = bus
	_ = coordinator
	_ = allocator
	manager := newBaldaRestoreSessionManager(
		t,
		&fakeBaldaRestoreAgentBuilder{},
		&fakeBaldaRestoreRuntimeManager{providerID: "balda-provider"},
		&fakeBaldaRestoreSessionStore{},
	)
	payload, env := taskAgentCommandForTest(t, "task-running-duplicate", taskAgentRoleExecutor, 1)
	if _, err := manager.EnsureSession(ctx, baldasession.SessionContext{
		Locator: payload.Locator,
		UserID:  payload.TransportUserID,
	}, ownerSessionLabel); err != nil {
		t.Fatalf("EnsureSession() error = %v", err)
	}
	stepKey := taskAgentStepKey(payload)
	if _, _, err := tasks.ReserveAgentStep(ctx, baldastate.SwarmAgentStepRecord{
		ID:          "step-running-duplicate",
		StepKey:     stepKey,
		TaskID:      payload.TaskID,
		AgentName:   payload.AgentName,
		Role:        payload.Role,
		Iteration:   payload.Iteration,
		PayloadHash: hashTaskAgentCommandPayload(payload),
		Status:      baldastate.SwarmAgentStepStatusRunning,
	}); err != nil {
		t.Fatalf("ReserveAgentStep() error = %v", err)
	}

	actor := &taskAgentActor{
		sessions:       manager,
		runtimeBuilder: &recordingTaskAgentRuntimeBuilder{t: t},
		tasks:          tasks,
	}
	err := actor.Handle(ctx, env)
	if err == nil {
		t.Fatal("Handle() error = nil, want duplicate running step")
	}
	if swarm.ClassifyError(err) != swarm.ErrorKindTransient {
		t.Fatalf("ClassifyError(%v) = %s, want transient", err, swarm.ClassifyError(err))
	}
	if !strings.Contains(err.Error(), "already running") {
		t.Fatalf("Handle() error = %v, want already running marker", err)
	}
}

func TestTaskAgentActorHandleReplaysStoredSucceededResult(t *testing.T) {
	ctx := context.Background()
	_, bus, coordinator, tasks, _ := newTaskActorSwarmServices(t, ctx)
	manager := newBaldaRestoreSessionManager(
		t,
		&fakeBaldaRestoreAgentBuilder{},
		&fakeBaldaRestoreRuntimeManager{providerID: "balda-provider"},
		&fakeBaldaRestoreSessionStore{},
	)
	payload, env := taskAgentCommandForTest(t, "task-replay-succeeded", taskAgentRoleExecutor, 1)
	if _, err := manager.EnsureSession(ctx, baldasession.SessionContext{
		Locator: payload.Locator,
		UserID:  payload.TransportUserID,
	}, ownerSessionLabel); err != nil {
		t.Fatalf("EnsureSession() error = %v", err)
	}
	stepKey := taskAgentStepKey(payload)
	resultJSON, err := marshalTaskAgentResult(payload, "done", nil)
	if err != nil {
		t.Fatalf("marshalTaskAgentResult() error = %v", err)
	}
	if _, _, err := tasks.ReserveAgentStep(ctx, baldastate.SwarmAgentStepRecord{
		ID:          "step-replay-succeeded",
		StepKey:     stepKey,
		TaskID:      payload.TaskID,
		AgentName:   payload.AgentName,
		Role:        payload.Role,
		Iteration:   payload.Iteration,
		PayloadHash: hashTaskAgentCommandPayload(payload),
		Status:      baldastate.SwarmAgentStepStatusRunning,
	}); err != nil {
		t.Fatalf("ReserveAgentStep() error = %v", err)
	}
	if err := tasks.CompleteAgentStep(ctx, stepKey, resultJSON); err != nil {
		t.Fatalf("CompleteAgentStep() error = %v", err)
	}

	actor := &taskAgentActor{
		sessions:       manager,
		runtimeBuilder: &recordingTaskAgentRuntimeBuilder{t: t},
		tasks:          tasks,
		coordinator:    coordinator,
	}
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	resultEnv := lastPublishedCommandTo(t, bus, swarm.ActorTypeTask, payload.TaskID)
	if resultEnv.DedupeKey != taskAgentResultDedupeKey(payload) {
		t.Fatalf("result dedupe key = %q, want %q", resultEnv.DedupeKey, taskAgentResultDedupeKey(payload))
	}
	if strings.TrimSpace(resultEnv.PayloadJSON) != strings.TrimSpace(resultJSON) {
		t.Fatalf("result payload mismatch = %q", resultEnv.PayloadJSON)
	}
}

func TestTaskAgentActorHandleReplaysStoredFailedResult(t *testing.T) {
	ctx := context.Background()
	_, bus, coordinator, tasks, _ := newTaskActorSwarmServices(t, ctx)
	manager := newBaldaRestoreSessionManager(
		t,
		&fakeBaldaRestoreAgentBuilder{},
		&fakeBaldaRestoreRuntimeManager{providerID: "balda-provider"},
		&fakeBaldaRestoreSessionStore{},
	)
	payload, env := taskAgentCommandForTest(t, "task-replay-failed", taskAgentRoleReviewer, 2)
	if _, err := manager.EnsureSession(ctx, baldasession.SessionContext{
		Locator: payload.Locator,
		UserID:  payload.TransportUserID,
	}, ownerSessionLabel); err != nil {
		t.Fatalf("EnsureSession() error = %v", err)
	}
	stepKey := taskAgentStepKey(payload)
	resultJSON, err := marshalTaskAgentResult(payload, "", errors.New("agent failed"))
	if err != nil {
		t.Fatalf("marshalTaskAgentResult() error = %v", err)
	}
	if _, _, err := tasks.ReserveAgentStep(ctx, baldastate.SwarmAgentStepRecord{
		ID:          "step-replay-failed",
		StepKey:     stepKey,
		TaskID:      payload.TaskID,
		AgentName:   payload.AgentName,
		Role:        payload.Role,
		Iteration:   payload.Iteration,
		PayloadHash: hashTaskAgentCommandPayload(payload),
		Status:      baldastate.SwarmAgentStepStatusRunning,
	}); err != nil {
		t.Fatalf("ReserveAgentStep() error = %v", err)
	}
	if err := tasks.FailAgentStep(ctx, stepKey, resultJSON, "agent failed"); err != nil {
		t.Fatalf("FailAgentStep() error = %v", err)
	}

	actor := &taskAgentActor{
		sessions:       manager,
		runtimeBuilder: &recordingTaskAgentRuntimeBuilder{t: t},
		tasks:          tasks,
		coordinator:    coordinator,
	}
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	resultEnv := lastPublishedCommandTo(t, bus, swarm.ActorTypeTask, payload.TaskID)
	if resultEnv.DedupeKey != taskAgentResultDedupeKey(payload) {
		t.Fatalf("result dedupe key = %q, want %q", resultEnv.DedupeKey, taskAgentResultDedupeKey(payload))
	}
}

func TestTaskAgentActorHandleRejectsStepPayloadHashMismatch(t *testing.T) {
	ctx := context.Background()
	provider, bus, coordinator, tasks, allocator := newTaskActorSwarmServices(t, ctx)
	_ = provider
	_ = bus
	_ = coordinator
	_ = allocator
	manager := newBaldaRestoreSessionManager(
		t,
		&fakeBaldaRestoreAgentBuilder{},
		&fakeBaldaRestoreRuntimeManager{providerID: "balda-provider"},
		&fakeBaldaRestoreSessionStore{},
	)
	payload, env := taskAgentCommandForTest(t, "task-hash-mismatch", taskAgentRolePlanner, 1)
	if _, err := manager.EnsureSession(ctx, baldasession.SessionContext{
		Locator: payload.Locator,
		UserID:  payload.TransportUserID,
	}, ownerSessionLabel); err != nil {
		t.Fatalf("EnsureSession() error = %v", err)
	}
	stepKey := taskAgentStepKey(payload)
	if _, _, err := tasks.ReserveAgentStep(ctx, baldastate.SwarmAgentStepRecord{
		ID:          "step-hash-mismatch",
		StepKey:     stepKey,
		TaskID:      payload.TaskID,
		AgentName:   payload.AgentName,
		Role:        payload.Role,
		Iteration:   payload.Iteration,
		PayloadHash: "different",
		Status:      baldastate.SwarmAgentStepStatusRunning,
	}); err != nil {
		t.Fatalf("ReserveAgentStep() error = %v", err)
	}

	actor := &taskAgentActor{
		sessions:       manager,
		runtimeBuilder: &recordingTaskAgentRuntimeBuilder{t: t},
		tasks:          tasks,
	}
	err := actor.Handle(ctx, env)
	if err == nil {
		t.Fatal("Handle() error = nil, want payload mismatch")
	}
	if swarm.ClassifyError(err) != swarm.ErrorKindPermanent {
		t.Fatalf("ClassifyError(%v) = %s, want permanent", err, swarm.ClassifyError(err))
	}
	if !strings.Contains(err.Error(), "different payload") {
		t.Fatalf("Handle() error = %v, want payload mismatch message", err)
	}
}

func TestTaskAgentADKSessionIDDeterministicPerTaskAndRole(t *testing.T) {
	command := taskAgentCommandPayload{
		TaskID:  "goal-tg-9001-99-123",
		Role:    taskAgentRoleExecutor,
		Locator: taskActorTestLocator(),
	}
	gotOne := taskAgentADKSessionID("tg-9001-99", command)
	gotTwo := taskAgentADKSessionID("tg-9001-99", command)
	if gotOne != gotTwo {
		t.Fatalf("taskAgentADKSessionID() = %q then %q, want deterministic value", gotOne, gotTwo)
	}
	if !strings.HasPrefix(gotOne, "tg-9001-99-task-executor-") {
		t.Fatalf("taskAgentADKSessionID() = %q, want executor-prefixed session id", gotOne)
	}
}

func TestTaskAgentADKSessionIDDiffersAcrossRolesAndTasks(t *testing.T) {
	base := "tg-9001-99"
	locator := taskActorTestLocator()
	executorTaskOne := taskAgentADKSessionID(base, taskAgentCommandPayload{
		TaskID:  "goal-1",
		Role:    taskAgentRoleExecutor,
		Locator: locator,
	})
	reviewerTaskOne := taskAgentADKSessionID(base, taskAgentCommandPayload{
		TaskID:  "goal-1",
		Role:    taskAgentRoleReviewer,
		Locator: locator,
	})
	executorTaskTwo := taskAgentADKSessionID(base, taskAgentCommandPayload{
		TaskID:  "goal-2",
		Role:    taskAgentRoleExecutor,
		Locator: locator,
	})

	if executorTaskOne == reviewerTaskOne {
		t.Fatalf("executor and reviewer session ids are equal: %q", executorTaskOne)
	}
	if executorTaskOne == executorTaskTwo {
		t.Fatalf("session ids for different tasks are equal: %q", executorTaskOne)
	}
}

func TestTaskAgentADKSessionIDFallsBackToLocatorSession(t *testing.T) {
	command := taskAgentCommandPayload{
		TaskID:  "goal-1",
		Role:    taskAgentRoleExecutor,
		Locator: taskActorTestLocator(),
	}
	got := taskAgentADKSessionID("", command)
	if !strings.HasPrefix(got, command.Locator.SessionID+"-task-executor-") {
		t.Fatalf("taskAgentADKSessionID() = %q, want locator session fallback", got)
	}
}

func TestTaskAgentActorHandleUsesDerivedADKSessionID(t *testing.T) {
	ctx := context.Background()
	_, bus, coordinator, tasks, _ := newTaskActorSwarmServices(t, ctx)
	manager := newBaldaRestoreSessionManager(
		t,
		&fakeBaldaRestoreAgentBuilder{},
		&fakeBaldaRestoreRuntimeManager{providerID: "balda-provider"},
		&fakeBaldaRestoreSessionStore{},
	)
	locator := taskActorTestLocator()
	if _, err := manager.EnsureSession(ctx, baldasession.SessionContext{
		Locator: locator,
		UserID:  "tg-101",
	}, ownerSessionLabel); err != nil {
		t.Fatalf("EnsureSession() error = %v", err)
	}

	runtimeBuilder := &recordingTaskAgentRuntimeBuilder{t: t}
	actor := &taskAgentActor{
		sessions:       manager,
		runtimeBuilder: runtimeBuilder,
		coordinator:    coordinator,
		tasks:          tasks,
	}

	payload, env := taskAgentCommandForTest(t, "task-derived-session", taskAgentRoleExecutor, 1)
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(runtimeBuilder.cfgs) != 1 {
		t.Fatalf("BuildTaskAgentRuntime() calls = %d, want 1", len(runtimeBuilder.cfgs))
	}
	if runtimeBuilder.cfgs[0].SessionID == locator.SessionID {
		t.Fatalf("runtime session id = %q, want derived task session (not base session id)", runtimeBuilder.cfgs[0].SessionID)
	}
	if runtimeBuilder.cfgs[0].UserID != "tg-101" {
		t.Fatalf("runtime user id = %q, want tg-101", runtimeBuilder.cfgs[0].UserID)
	}
	if !strings.HasPrefix(runtimeBuilder.cfgs[0].SessionID, locator.SessionID+"-a") {
		t.Fatalf("runtime session id = %q, want derived id based on active agent session id", runtimeBuilder.cfgs[0].SessionID)
	}
	if !strings.Contains(runtimeBuilder.cfgs[0].SessionID, "-task-executor-") {
		t.Fatalf("runtime session id = %q, want task-role marker", runtimeBuilder.cfgs[0].SessionID)
	}

	resultEnv := lastPublishedCommandTo(t, bus, swarm.ActorTypeTask, payload.TaskID)
	var resultPayload taskEnvelopePayload
	if err := json.Unmarshal([]byte(resultEnv.PayloadJSON), &resultPayload); err != nil {
		t.Fatalf("decode result payload: %v", err)
	}
	if resultPayload.AgentResult == nil {
		t.Fatal("result payload agent_result is nil")
	}
	if strings.TrimSpace(resultPayload.AgentResult.ADKSessionID) != runtimeBuilder.cfgs[0].SessionID {
		t.Fatalf("result adk_session_id = %q, want %q", resultPayload.AgentResult.ADKSessionID, runtimeBuilder.cfgs[0].SessionID)
	}
	if strings.TrimSpace(resultPayload.AgentResult.BranchName) != strings.TrimSpace(runtimeBuilder.cfgs[0].BranchName) {
		t.Fatalf("result branch_name = %q, want %q", resultPayload.AgentResult.BranchName, runtimeBuilder.cfgs[0].BranchName)
	}
	if strings.TrimSpace(resultPayload.AgentResult.WorkspaceDir) != strings.TrimSpace(runtimeBuilder.cfgs[0].WorkspaceDir) {
		t.Fatalf("result workspace_dir = %q, want %q", resultPayload.AgentResult.WorkspaceDir, runtimeBuilder.cfgs[0].WorkspaceDir)
	}
}

func TestTaskAgentActorHandleRuntimeBootstrapFailureDoesNotReserveRunningStep(t *testing.T) {
	ctx := context.Background()
	_, bus, coordinator, tasks, _ := newTaskActorSwarmServices(t, ctx)
	manager := newBaldaRestoreSessionManager(
		t,
		&fakeBaldaRestoreAgentBuilder{},
		&fakeBaldaRestoreRuntimeManager{providerID: "balda-provider"},
		&fakeBaldaRestoreSessionStore{},
	)
	locator := taskActorTestLocator()
	if _, err := manager.EnsureSession(ctx, baldasession.SessionContext{
		Locator: locator,
		UserID:  "tg-101",
	}, ownerSessionLabel); err != nil {
		t.Fatalf("EnsureSession() error = %v", err)
	}

	runtimeBuilder := &failingTaskAgentRuntimeBuilder{
		t:           t,
		failBuilds:  1,
		errOnBuild:  errors.New("runtime bootstrap failed"),
	}
	actor := &taskAgentActor{
		sessions:       manager,
		runtimeBuilder: runtimeBuilder,
		coordinator:    coordinator,
		tasks:          tasks,
	}
	payload, env := taskAgentCommandForTest(t, "task-bootstrap-retry", taskAgentRoleExecutor, 1)

	err := actor.Handle(ctx, env)
	if err == nil {
		t.Fatal("Handle() error = nil, want transient bootstrap error")
	}
	if swarm.ClassifyError(err) != swarm.ErrorKindTransient {
		t.Fatalf("ClassifyError(%v) = %s, want transient", err, swarm.ClassifyError(err))
	}
	if len(runtimeBuilder.cfgs) != 1 {
		t.Fatalf("BuildTaskAgentRuntime() calls after first attempt = %d, want 1", len(runtimeBuilder.cfgs))
	}

	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() retry error = %v", err)
	}
	if len(runtimeBuilder.cfgs) != 2 {
		t.Fatalf("BuildTaskAgentRuntime() calls after retry = %d, want 2", len(runtimeBuilder.cfgs))
	}
	if runtimeBuilder.cfgs[0].SessionID != runtimeBuilder.cfgs[1].SessionID {
		t.Fatalf("runtime session ids differ across retry: %q vs %q", runtimeBuilder.cfgs[0].SessionID, runtimeBuilder.cfgs[1].SessionID)
	}
	resultEnv := lastPublishedCommandTo(t, bus, swarm.ActorTypeTask, payload.TaskID)
	var resultPayload taskEnvelopePayload
	if err := json.Unmarshal([]byte(resultEnv.PayloadJSON), &resultPayload); err != nil {
		t.Fatalf("decode result payload: %v", err)
	}
	if resultPayload.AgentResult == nil {
		t.Fatal("result payload agent_result is nil")
	}
	if strings.TrimSpace(resultPayload.AgentResult.ADKSessionID) != runtimeBuilder.cfgs[1].SessionID {
		t.Fatalf("result adk_session_id = %q, want %q", resultPayload.AgentResult.ADKSessionID, runtimeBuilder.cfgs[1].SessionID)
	}
}

type recordingTaskAgentRuntimeBuilder struct {
	t    *testing.T
	cfgs []baldaagent.TaskAgentRuntimeConfig
}

func (b *recordingTaskAgentRuntimeBuilder) BuildTaskAgentRuntime(
	ctx context.Context,
	cfg baldaagent.TaskAgentRuntimeConfig,
) (*baldaagent.TaskAgentRuntime, error) {
	b.t.Helper()
	b.cfgs = append(b.cfgs, cfg)
	return newTaskAgentRuntimeForTest(ctx, cfg)
}

type failingTaskAgentRuntimeBuilder struct {
	t          *testing.T
	failBuilds int
	errOnBuild error
	cfgs       []baldaagent.TaskAgentRuntimeConfig
}

func (b *failingTaskAgentRuntimeBuilder) BuildTaskAgentRuntime(
	ctx context.Context,
	cfg baldaagent.TaskAgentRuntimeConfig,
) (*baldaagent.TaskAgentRuntime, error) {
	b.t.Helper()
	b.cfgs = append(b.cfgs, cfg)
	if len(b.cfgs) <= b.failBuilds {
		if b.errOnBuild != nil {
			return nil, b.errOnBuild
		}
		return nil, errors.New("task runtime build failed")
	}
	return newTaskAgentRuntimeForTest(ctx, cfg)
}

func newTaskAgentRuntimeForTest(
	ctx context.Context,
	cfg baldaagent.TaskAgentRuntimeConfig,
) (*baldaagent.TaskAgentRuntime, error) {
	ag, err := adkagent.New(adkagent.Config{
		Name:        "TaskAgentRuntimeBuilderTestAgent",
		Description: "Emits one final response",
		Run: func(ctx adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
			return func(yield func(*adksession.Event, error) bool) {
				reply := adksession.NewEvent(ctx.InvocationID())
				reply.Content = genai.NewContentFromText("ok", genai.RoleModel)
				if !yield(reply, nil) {
					return
				}
				done := adksession.NewEvent(ctx.InvocationID())
				done.TurnComplete = true
				yield(done, nil)
			}
		},
	})
	if err != nil {
		return nil, err
	}

	sessionService := adksession.InMemoryService()
	adkRunner, err := runner.New(runner.Config{
		AppName:        "task-agent-runtime-builder-test",
		Agent:          ag,
		SessionService: sessionService,
	})
	if err != nil {
		return nil, err
	}
	if _, err := sessionService.Create(ctx, &adksession.CreateRequest{
		AppName:   "task-agent-runtime-builder-test",
		UserID:    cfg.UserID,
		SessionID: cfg.SessionID,
	}); err != nil {
		return nil, err
	}

	return &baldaagent.TaskAgentRuntime{
		Agent:  ag,
		Runner: adkRunner,
	}, nil
}

func taskAgentCommandForTest(t *testing.T, taskID string, role string, iteration int) (taskAgentCommandPayload, swarm.Envelope) {
	t.Helper()
	locator := taskActorTestLocator()
	payload := taskAgentCommandPayload{
		TaskID:          taskID,
		AgentName:       role,
		Role:            role,
		Iteration:       iteration,
		Locator:         locator,
		Objective:       "test objective",
		TransportUserID: "tg-101",
		MaxIterations:   3,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal(payload) error = %v", err)
	}
	env := swarm.Envelope{
		ID:            taskID + ":command:" + role,
		Namespace:     swarm.NamespaceAgentCommand,
		Kind:          swarm.KindGoal,
		From:          swarm.ActorAddress{Target: swarm.ActorTypeTask, Key: taskID},
		To:            swarm.ActorAddress{Target: swarm.ActorTypeAgent, Key: role},
		SessionID:     locator.SessionID,
		TaskID:        taskID,
		CorrelationID: taskID,
		DedupeKey:     taskID + ":agent:" + role + ":" + role + ":" + strconv.Itoa(iteration),
		PayloadJSON:   string(data),
	}
	return payload, env
}
