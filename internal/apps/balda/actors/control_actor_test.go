package actors

import (
	"context"
	"testing"
	"time"

	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
)

func TestTaskControlActorCancelsSessionWork(t *testing.T) {
	ctx := context.Background()
	provider, bus, dispatcher, tasks, allocator := newTaskActorRuntimeServices(t, ctx)
	_ = provider
	_ = bus
	_ = dispatcher
	_ = allocator
	locator := baldatelegram.NewLocator(9001, 0)
	_, err := tasks.Create(ctx, baldastate.JobRecord{
		ID:        "task-session",
		SessionID: locator.SessionID,
		Objective: "active",
		Status:    baldastate.JobStatusRunning,
	}, "test", nil)
	if err != nil {
		t.Fatalf("Create task: %v", err)
	}
	turns := &fakeTurnDispatcher{}
	actor := &jobControlActor{
		turnDispatcher: turns,
		dispatcher:     dispatcher,
		tasks:          tasks,
		taskRuns:       NewJobRunRegistry(),
	}
	env, err := ControlCancelEnvelope(locator, "", testTelegramUserID101, "session canceled by user")
	if err != nil {
		t.Fatalf("ControlCancelEnvelope() error = %v", err)
	}
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(turns.cancelCalls) != 1 || turns.cancelCalls[0].SessionID != locator.SessionID {
		t.Fatalf("CancelSession calls = %+v, want one session cancel", turns.cancelCalls)
	}
	task, ok, err := tasks.Get(ctx, "task-session")
	if err != nil {
		t.Fatalf("Get task: %v", err)
	}
	if !ok || task.Status != baldastate.JobStatusCanceled {
		t.Fatalf("task = %+v found=%v, want canceled", task, ok)
	}
}

func TestTaskControlActorCancelsSessionTurnOnly(t *testing.T) {
	ctx := context.Background()
	provider, bus, dispatcher, tasks, allocator := newTaskActorRuntimeServices(t, ctx)
	_ = provider
	_ = bus
	_ = dispatcher
	_ = allocator
	locator := baldatelegram.NewLocator(9001, 0)
	_, err := tasks.Create(ctx, baldastate.JobRecord{
		ID:            "goal-task",
		SessionID:     locator.SessionID,
		Objective:     "active goal",
		Status:        baldastate.JobStatusRunning,
		OwnerActor:    baldaexecution.ActorTypeGoalkeeper + ":goal-task",
		AssignedActor: baldaexecution.ActorTypeGoalkeeper + ":goal-task",
	}, "test", nil)
	if err != nil {
		t.Fatalf("Create task: %v", err)
	}
	turns := &fakeTurnDispatcher{}
	actor := &jobControlActor{
		turnDispatcher: turns,
		dispatcher:     dispatcher,
		tasks:          tasks,
		taskRuns:       NewJobRunRegistry(),
	}
	env, err := ControlCancelTurnEnvelopeWithNotify(locator, testTelegramUserID101, "session turn canceled by user", true)
	if err != nil {
		t.Fatalf("ControlCancelTurnEnvelopeWithNotify() error = %v", err)
	}
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(turns.cancelCalls) != 1 || turns.cancelCalls[0].SessionID != locator.SessionID {
		t.Fatalf("CancelSession calls = %+v, want one session cancel", turns.cancelCalls)
	}
	task, ok, err := tasks.Get(ctx, "goal-task")
	if err != nil {
		t.Fatalf("Get task: %v", err)
	}
	if !ok || task.Status != baldastate.JobStatusRunning {
		t.Fatalf("task = %+v found=%v, want still running", task, ok)
	}
}

func TestTaskControlActorCancelsTaskWork(t *testing.T) {
	ctx := context.Background()
	provider, bus, dispatcher, tasks, allocator := newTaskActorRuntimeServices(t, ctx)
	_ = provider
	_ = bus
	_ = dispatcher
	_ = allocator
	locator := baldatelegram.NewLocator(9001, 0)
	_, err := tasks.Create(ctx, baldastate.JobRecord{
		ID:        "task-one",
		SessionID: locator.SessionID,
		Objective: "active",
		Status:    baldastate.JobStatusRunning,
	}, "test", nil)
	if err != nil {
		t.Fatalf("Create task: %v", err)
	}
	actor := &jobControlActor{
		turnDispatcher: &fakeTurnDispatcher{},
		dispatcher:     dispatcher,
		tasks:          tasks,
		taskRuns:       NewJobRunRegistry(),
	}
	env, err := ControlCancelEnvelope(locator, "task-one", testTelegramUserID101, "task canceled by user")
	if err != nil {
		t.Fatalf("ControlCancelEnvelope() error = %v", err)
	}
	if env.Namespace != baldaexecution.NamespaceJobControl || baldaexecution.EnvelopeJobID(env) != "task-one" {
		t.Fatalf("control env = %+v, want job control for task-one", env)
	}
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	task, ok, err := tasks.Get(ctx, "task-one")
	if err != nil {
		t.Fatalf("Get task: %v", err)
	}
	if !ok || task.Status != baldastate.JobStatusCanceled {
		t.Fatalf("task = %+v found=%v, want canceled", task, ok)
	}
}

func TestTaskControlActorCancelsAllRegisteredTaskRuns(t *testing.T) {
	ctx := context.Background()
	provider, bus, dispatcher, tasks, allocator := newTaskActorRuntimeServices(t, ctx)
	_ = provider
	_ = bus
	_ = dispatcher
	_ = allocator

	locator := baldatelegram.NewLocator(9001, 0)
	_, err := tasks.Create(ctx, baldastate.JobRecord{
		ID:        "task-multi-run",
		SessionID: locator.SessionID,
		Objective: "active",
		Status:    baldastate.JobStatusRunning,
	}, "test", nil)
	if err != nil {
		t.Fatalf("Create task: %v", err)
	}

	registry := NewJobRunRegistry()
	runCtxOne, cancelOne := context.WithCancel(context.Background())
	defer cancelOne()
	runCtxTwo, cancelTwo := context.WithCancel(context.Background())
	defer cancelTwo()
	registry.Register("task-multi-run", cancelOne)
	registry.Register("task-multi-run", cancelTwo)

	actor := &jobControlActor{
		turnDispatcher: &fakeTurnDispatcher{},
		dispatcher:     dispatcher,
		tasks:          tasks,
		taskRuns:       registry,
	}

	env, err := ControlCancelEnvelope(locator, "task-multi-run", testTelegramUserID101, "task canceled by user")
	if err != nil {
		t.Fatalf("ControlCancelEnvelope() error = %v", err)
	}
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	waitCancelDone(t, runCtxOne, "run one")
	waitCancelDone(t, runCtxTwo, "run two")

	task, ok, err := tasks.Get(ctx, "task-multi-run")
	if err != nil {
		t.Fatalf("Get task: %v", err)
	}
	if !ok || task.Status != baldastate.JobStatusCanceled {
		t.Fatalf("task = %+v found=%v, want canceled", task, ok)
	}
}

func TestTaskControlActorClearsGoalTasksOnly(t *testing.T) {
	ctx := context.Background()
	provider, bus, dispatcher, tasks, allocator := newTaskActorRuntimeServices(t, ctx)
	_ = provider
	_ = bus
	_ = dispatcher
	_ = allocator

	locator := baldatelegram.NewLocator(9001, 0)
	for _, task := range []baldastate.JobRecord{
		{
			ID:            "goal-task",
			SessionID:     locator.SessionID,
			Objective:     "goal",
			Status:        baldastate.JobStatusRunning,
			OwnerActor:    baldaexecution.ActorTypeGoalkeeper + ":goal-task",
			AssignedActor: baldaexecution.ActorTypeGoalkeeper + ":goal-task",
		},
		{
			ID:            "non-goal-task",
			SessionID:     locator.SessionID,
			Objective:     "turn",
			Status:        baldastate.JobStatusRunning,
			OwnerActor:    baldaexecution.ActorTypeSession + ":non-goal-task",
			AssignedActor: baldaexecution.ActorTypeSession + ":non-goal-task",
		},
	} {
		if _, err := tasks.Create(ctx, task, "test", nil); err != nil {
			t.Fatalf("Create task %s: %v", task.ID, err)
		}
	}

	turns := &fakeTurnDispatcher{}
	actor := &jobControlActor{
		turnDispatcher: turns,
		dispatcher:     dispatcher,
		tasks:          tasks,
		taskRuns:       NewJobRunRegistry(),
	}
	env, err := ControlClearGoalEnvelopeWithNotify(locator, testTelegramUserID101, "goal cleared by user", true)
	if err != nil {
		t.Fatalf("ControlClearGoalEnvelopeWithNotify() error = %v", err)
	}
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %+v, want 0", turns.cancelCalls)
	}

	goalTask, ok, err := tasks.Get(ctx, "goal-task")
	if err != nil {
		t.Fatalf("Get goal task: %v", err)
	}
	if !ok || goalTask.Status != baldastate.JobStatusCanceled {
		t.Fatalf("goal task = %+v found=%v, want canceled", goalTask, ok)
	}
	nonGoalTask, ok, err := tasks.Get(ctx, "non-goal-task")
	if err != nil {
		t.Fatalf("Get non-goal task: %v", err)
	}
	if !ok || nonGoalTask.Status != baldastate.JobStatusRunning {
		t.Fatalf("non-goal task = %+v found=%v, want still running", nonGoalTask, ok)
	}
}

func waitCancelDone(t *testing.T, runCtx context.Context, label string) {
	t.Helper()
	select {
	case <-runCtx.Done():
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for %s cancellation", label)
	}
}
