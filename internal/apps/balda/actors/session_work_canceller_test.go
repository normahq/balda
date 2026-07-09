package actors

import (
	"context"
	"testing"

	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/rs/zerolog"
)

func TestSessionWorkCancellerCancelsQueueTasksAndRuns(t *testing.T) {
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
	registry := NewJobRunRegistry()
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry.Register("task-session", cancel)
	canceller := &SessionWorkCanceller{
		turnDispatcher: turns,
		tasks:          tasks,
		taskRuns:       registry,
		logger:         zerolog.Nop(),
	}

	if err := canceller.CancelWork(ctx, locator, "command.reset", "session canceled by reset command"); err != nil {
		t.Fatalf("CancelWork() error = %v", err)
	}

	if len(turns.cancelCalls) != 1 || turns.cancelCalls[0].SessionID != locator.SessionID || !turns.cancelCalls[0].ClearQueued {
		t.Fatalf("CancelSession calls = %+v, want one queued session cancel", turns.cancelCalls)
	}
	waitCancelDone(t, runCtx, "session task run")
	task, ok, err := tasks.Get(ctx, "task-session")
	if err != nil {
		t.Fatalf("Get task: %v", err)
	}
	if !ok || task.Status != baldastate.JobStatusCanceled {
		t.Fatalf("task = %+v found=%v, want canceled", task, ok)
	}
}
