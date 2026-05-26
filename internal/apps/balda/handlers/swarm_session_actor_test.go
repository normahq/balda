package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/rs/zerolog"
)

func TestSessionActorInterruptQueueModeCancelsSessionBeforeEnqueue(t *testing.T) {
	t.Parallel()

	turns := &fakeTurnDispatcher{}
	exec := &sessionActorExecutor{
		handler: &BaldaHandler{
			turnDispatcher: turns,
			logger:         zerolog.Nop(),
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := exec.enqueueTurn(ctx, testSessionTurnEnvelope(t, map[string]string{"queue_mode": swarm.QueueModeInterrupt}))
	if err == nil {
		t.Fatal("enqueueTurn() error = nil, want canceled context after enqueue")
	}
	if len(turns.cancelCalls) != 1 {
		t.Fatalf("CancelSession calls = %d, want 1", len(turns.cancelCalls))
	}
	if got := turns.cancelCalls[0]; got.SessionID != "tg-9001-77" || !got.ClearQueued {
		t.Fatalf("CancelSession call = %+v, want session=tg-9001-77 clearQueued=true", got)
	}
	if len(turns.enqueueCalls) != 1 {
		t.Fatalf("Enqueue calls = %d, want 1", len(turns.enqueueCalls))
	}
}

func TestSessionActorDefaultQueueModeDoesNotCancelSession(t *testing.T) {
	t.Parallel()

	turns := &fakeTurnDispatcher{}
	exec := &sessionActorExecutor{
		handler: &BaldaHandler{
			turnDispatcher: turns,
			logger:         zerolog.Nop(),
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := exec.enqueueTurn(ctx, testSessionTurnEnvelope(t, nil))
	if err == nil {
		t.Fatal("enqueueTurn() error = nil, want canceled context after enqueue")
	}
	if len(turns.cancelCalls) != 0 {
		t.Fatalf("CancelSession calls = %d, want 0", len(turns.cancelCalls))
	}
	if len(turns.enqueueCalls) != 1 {
		t.Fatalf("Enqueue calls = %d, want 1", len(turns.enqueueCalls))
	}
}

func TestSessionActorSettleSessionTurnResultMarksTaskFailedWithoutRetry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	provider, bus, coordinator, tasks, allocator := newTaskActorSwarmServices(t, ctx)
	_ = provider
	_ = bus
	_ = coordinator
	_ = allocator
	created, err := tasks.Create(ctx, baldastate.SwarmTaskRecord{
		ID:        "task-session-failed",
		SessionID: "tg-9001-77",
		Objective: "run session task",
		Status:    baldastate.SwarmTaskStatusRunning,
	}, "test", nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if !created {
		t.Fatal("Create() created = false, want true")
	}

	exec := &sessionActorExecutor{tasks: tasks}
	runErr := errors.New("runner failed")
	env := testSessionTurnEnvelope(t, nil)
	env.TaskID = "task-session-failed"

	if err := exec.settleSessionTurnResult(ctx, env, sessionTurnPayload{}, runErr); err != nil {
		t.Fatalf("settleSessionTurnResult() error = %v, want nil after recording task failure", err)
	}

	task, ok, err := tasks.Get(ctx, env.TaskID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatalf("Get() found = false for task %q", env.TaskID)
	}
	if task.Status != baldastate.SwarmTaskStatusFailed {
		t.Fatalf("task status = %q, want %q", task.Status, baldastate.SwarmTaskStatusFailed)
	}
}

func TestSessionActorSettleSessionTurnResultKeepsNonTaskErrorsRetryable(t *testing.T) {
	t.Parallel()

	exec := &sessionActorExecutor{}
	runErr := errors.New("runner failed")

	err := exec.settleSessionTurnResult(context.Background(), testSessionTurnEnvelope(t, nil), sessionTurnPayload{}, runErr)
	if !errors.Is(err, runErr) {
		t.Fatalf("settleSessionTurnResult() error = %v, want original run error", err)
	}
}

func testSessionTurnEnvelope(t *testing.T, meta map[string]string) swarm.Envelope {
	t.Helper()

	locator := baldasession.SessionLocator{
		ChannelType: "telegram",
		AddressKey:  "tg-9001-77",
		AddressJSON: `{"chat_id":9001,"topic_id":77}`,
		SessionID:   "tg-9001-77",
	}
	payload, err := json.Marshal(sessionTurnPayload{
		Text:    "run this",
		Locator: locator,
		Deliver: false,
		Source:  sessionTurnSourceTelegram,
	})
	if err != nil {
		t.Fatalf("Marshal(sessionTurnPayload) error = %v", err)
	}
	return swarm.Envelope{
		ID:          "session-command-1",
		Namespace:   swarm.NamespaceHumanInbound,
		Kind:        swarm.KindMessage,
		From:        swarm.ActorAddress{Target: "telegram", Key: "101"},
		To:          swarm.ActorAddress{Target: swarm.ActorTypeSession, Key: locator.SessionID},
		SessionID:   locator.SessionID,
		PayloadJSON: string(payload),
		Meta:        meta,
	}
}
