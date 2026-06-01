package actors

import (
	"context"
	"testing"
	"time"

	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
)

func TestTaskControlActorCancelsSessionWork(t *testing.T) {
	ctx := context.Background()
	provider, bus, dispatcher, tasks, allocator := newTaskActorSwarmServices(t, ctx)
	_ = provider
	_ = bus
	_ = dispatcher
	_ = allocator
	locator := baldatelegram.NewLocator(9001, 0)
	_, err := tasks.Create(ctx, baldastate.SwarmTaskRecord{
		ID:        "task-session",
		SessionID: locator.SessionID,
		Objective: "active",
		Status:    baldastate.SwarmTaskStatusRunning,
	}, "test", nil)
	if err != nil {
		t.Fatalf("Create task: %v", err)
	}
	turns := &fakeTurnDispatcher{}
	actor := &taskControlActor{
		turnDispatcher: turns,
		tasks:          tasks,
		taskRuns:       NewTaskRunRegistry(),
		channel:        newBaldaTestTelegramAdapter(),
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
	if !ok || task.Status != baldastate.SwarmTaskStatusCanceled {
		t.Fatalf("task = %+v found=%v, want canceled", task, ok)
	}
}

func TestTaskControlActorCancelsSessionTurnOnly(t *testing.T) {
	ctx := context.Background()
	provider, bus, dispatcher, tasks, allocator := newTaskActorSwarmServices(t, ctx)
	_ = provider
	_ = bus
	_ = dispatcher
	_ = allocator
	locator := baldatelegram.NewLocator(9001, 0)
	_, err := tasks.Create(ctx, baldastate.SwarmTaskRecord{
		ID:            "goal-task",
		SessionID:     locator.SessionID,
		Objective:     "active goal",
		Status:        baldastate.SwarmTaskStatusRunning,
		OwnerActor:    swarm.ActorTypeGoalkeeper + ":goal-task",
		AssignedActor: swarm.ActorTypeGoalkeeper + ":goal-task",
	}, "test", nil)
	if err != nil {
		t.Fatalf("Create task: %v", err)
	}
	turns := &fakeTurnDispatcher{}
	actor := &taskControlActor{
		turnDispatcher: turns,
		tasks:          tasks,
		taskRuns:       NewTaskRunRegistry(),
		channel:        newBaldaTestTelegramAdapter(),
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
	if !ok || task.Status != baldastate.SwarmTaskStatusRunning {
		t.Fatalf("task = %+v found=%v, want still running", task, ok)
	}
}

func TestTaskControlActorCancelsTaskWork(t *testing.T) {
	ctx := context.Background()
	provider, bus, dispatcher, tasks, allocator := newTaskActorSwarmServices(t, ctx)
	_ = provider
	_ = bus
	_ = dispatcher
	_ = allocator
	locator := baldatelegram.NewLocator(9001, 0)
	_, err := tasks.Create(ctx, baldastate.SwarmTaskRecord{
		ID:        "task-one",
		SessionID: locator.SessionID,
		Objective: "active",
		Status:    baldastate.SwarmTaskStatusRunning,
	}, "test", nil)
	if err != nil {
		t.Fatalf("Create task: %v", err)
	}
	actor := &taskControlActor{
		turnDispatcher: &fakeTurnDispatcher{},
		tasks:          tasks,
		taskRuns:       NewTaskRunRegistry(),
		channel:        newBaldaTestTelegramAdapter(),
	}
	env, err := ControlCancelEnvelope(locator, "task-one", testTelegramUserID101, "task canceled by user")
	if err != nil {
		t.Fatalf("ControlCancelEnvelope() error = %v", err)
	}
	if env.Namespace != swarm.NamespaceTaskControl || env.TaskID != "task-one" {
		t.Fatalf("control env = %+v, want task control for task-one", env)
	}
	if err := actor.Handle(ctx, env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	task, ok, err := tasks.Get(ctx, "task-one")
	if err != nil {
		t.Fatalf("Get task: %v", err)
	}
	if !ok || task.Status != baldastate.SwarmTaskStatusCanceled {
		t.Fatalf("task = %+v found=%v, want canceled", task, ok)
	}
}

func TestTaskControlActorCancelsAllRegisteredTaskRuns(t *testing.T) {
	ctx := context.Background()
	provider, bus, dispatcher, tasks, allocator := newTaskActorSwarmServices(t, ctx)
	_ = provider
	_ = bus
	_ = dispatcher
	_ = allocator

	locator := baldatelegram.NewLocator(9001, 0)
	_, err := tasks.Create(ctx, baldastate.SwarmTaskRecord{
		ID:        "task-multi-run",
		SessionID: locator.SessionID,
		Objective: "active",
		Status:    baldastate.SwarmTaskStatusRunning,
	}, "test", nil)
	if err != nil {
		t.Fatalf("Create task: %v", err)
	}

	registry := NewTaskRunRegistry()
	runCtxOne, cancelOne := context.WithCancel(context.Background())
	defer cancelOne()
	runCtxTwo, cancelTwo := context.WithCancel(context.Background())
	defer cancelTwo()
	registry.Register("task-multi-run", cancelOne)
	registry.Register("task-multi-run", cancelTwo)

	actor := &taskControlActor{
		turnDispatcher: &fakeTurnDispatcher{},
		tasks:          tasks,
		taskRuns:       registry,
		channel:        newBaldaTestTelegramAdapter(),
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
	if !ok || task.Status != baldastate.SwarmTaskStatusCanceled {
		t.Fatalf("task = %+v found=%v, want canceled", task, ok)
	}
}

func TestTaskControlActorClearsGoalTasksOnly(t *testing.T) {
	ctx := context.Background()
	provider, bus, dispatcher, tasks, allocator := newTaskActorSwarmServices(t, ctx)
	_ = provider
	_ = bus
	_ = dispatcher
	_ = allocator

	locator := baldatelegram.NewLocator(9001, 0)
	for _, task := range []baldastate.SwarmTaskRecord{
		{
			ID:            "goal-task",
			SessionID:     locator.SessionID,
			Objective:     "goal",
			Status:        baldastate.SwarmTaskStatusRunning,
			OwnerActor:    swarm.ActorTypeGoalkeeper + ":goal-task",
			AssignedActor: swarm.ActorTypeGoalkeeper + ":goal-task",
		},
		{
			ID:            "non-goal-task",
			SessionID:     locator.SessionID,
			Objective:     "turn",
			Status:        baldastate.SwarmTaskStatusRunning,
			OwnerActor:    swarm.ActorTypeSession + ":non-goal-task",
			AssignedActor: swarm.ActorTypeSession + ":non-goal-task",
		},
	} {
		if _, err := tasks.Create(ctx, task, "test", nil); err != nil {
			t.Fatalf("Create task %s: %v", task.ID, err)
		}
	}

	turns := &fakeTurnDispatcher{}
	actor := &taskControlActor{
		turnDispatcher: turns,
		tasks:          tasks,
		taskRuns:       NewTaskRunRegistry(),
		channel:        newBaldaTestTelegramAdapter(),
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
	if !ok || goalTask.Status != baldastate.SwarmTaskStatusCanceled {
		t.Fatalf("goal task = %+v found=%v, want canceled", goalTask, ok)
	}
	nonGoalTask, ok, err := tasks.Get(ctx, "non-goal-task")
	if err != nil {
		t.Fatalf("Get non-goal task: %v", err)
	}
	if !ok || nonGoalTask.Status != baldastate.SwarmTaskStatusRunning {
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
