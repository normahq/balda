package state

import (
	"context"
	"strings"
	"testing"
)

func TestSQLiteSwarmStore_TaskLifecycle(t *testing.T) {
	provider := newTestProvider(t)
	defer closeProvider(t, provider)

	ctx := context.Background()
	store := provider.Swarm()

	created, err := store.CreateTask(ctx, SwarmTaskRecord{
		ID:            "task-1",
		SessionID:     "session-1",
		Title:         "Goal: test",
		Objective:     "test",
		Status:        SwarmTaskStatusCreated,
		AssignedActor: "agent:executor",
		CreatedBy:     "tg-101",
		CreatedFrom:   "goal",
	})
	if err != nil {
		t.Fatalf("CreateTask() error = %v", err)
	}
	if !created {
		t.Fatal("CreateTask() created = false, want true")
	}
	created, err = store.CreateTask(ctx, SwarmTaskRecord{
		ID:        "task-1",
		Objective: "duplicate",
	})
	if err != nil {
		t.Fatalf("CreateTask(duplicate) error = %v", err)
	}
	if created {
		t.Fatal("CreateTask(duplicate) created = true, want false")
	}

	if err := store.UpdateTaskStatus(ctx, "task-1", SwarmTaskStatusWaitingForAgent, "waiting"); err != nil {
		t.Fatalf("UpdateTaskStatus(waiting) error = %v", err)
	}
	if err := store.SetTaskPlan(ctx, "task-1", `{"steps":["run"]}`); err != nil {
		t.Fatalf("SetTaskPlan() error = %v", err)
	}
	if err := store.AppendTaskEvent(ctx, SwarmTaskEventRecord{
		ID:          "event-1",
		TaskID:      "task-1",
		EventType:   "agent.started",
		Actor:       "task.actor",
		PayloadJSON: `{"role":"executor"}`,
	}); err != nil {
		t.Fatalf("AppendTaskEvent() error = %v", err)
	}

	active, err := store.ListActiveTasksBySession(ctx, "session-1")
	if err != nil {
		t.Fatalf("ListActiveTasksBySession() error = %v", err)
	}
	if len(active) != 1 || active[0].ID != "task-1" {
		t.Fatalf("active tasks = %+v, want task-1", active)
	}

	if err := store.SetTaskResult(ctx, "task-1", `{"ok":true}`, SwarmTaskStatusCompleted, ""); err != nil {
		t.Fatalf("SetTaskResult() error = %v", err)
	}
	got, ok, err := store.GetTask(ctx, "task-1")
	if err != nil {
		t.Fatalf("GetTask() error = %v", err)
	}
	if !ok || got.Status != SwarmTaskStatusCompleted || got.PlanJSON == "" || got.ResultJSON == "" || got.StartedAt.IsZero() || got.CompletedAt.IsZero() {
		t.Fatalf("task = %+v, found=%v, want completed with plan/result/timestamps", got, ok)
	}

	active, err = store.ListActiveTasksBySession(ctx, "session-1")
	if err != nil {
		t.Fatalf("ListActiveTasksBySession(after complete) error = %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("active tasks after complete = %+v, want none", active)
	}
	events, err := store.ListTaskEvents(ctx, "task-1")
	if err != nil {
		t.Fatalf("ListTaskEvents() error = %v", err)
	}
	if len(events) != 1 || events[0].EventType != "agent.started" {
		t.Fatalf("events = %+v, want agent.started", events)
	}
	counts, err := store.ListTaskStatusCounts(ctx)
	if err != nil {
		t.Fatalf("ListTaskStatusCounts() error = %v", err)
	}
	if len(counts) != 1 || counts[0].Status != SwarmTaskStatusCompleted || counts[0].Count != 1 {
		t.Fatalf("task status counts = %+v, want completed=1", counts)
	}
}

func TestSQLiteSwarmStore_TaskStatusTransitionsAreGuarded(t *testing.T) {
	provider := newTestProvider(t)
	defer closeProvider(t, provider)

	ctx := context.Background()
	store := provider.Swarm()

	_, err := store.CreateTask(ctx, SwarmTaskRecord{
		ID:        "task-guarded",
		SessionID: "session-1",
		Objective: "guard transitions",
		Status:    SwarmTaskStatusRunning,
	})
	if err != nil {
		t.Fatalf("CreateTask() error = %v", err)
	}
	if err := store.UpdateTaskStatus(ctx, "task-guarded", SwarmTaskStatusCompleted, "done"); err != nil {
		t.Fatalf("UpdateTaskStatus(completed) error = %v", err)
	}

	err = store.UpdateTaskStatus(ctx, "task-guarded", SwarmTaskStatusRunning, "reopen")
	if err == nil || !strings.Contains(err.Error(), "invalid swarm task transition") {
		t.Fatalf("UpdateTaskStatus(reopen) error = %v, want invalid transition", err)
	}
	got, ok, err := store.GetTask(ctx, "task-guarded")
	if err != nil {
		t.Fatalf("GetTask() error = %v", err)
	}
	if !ok || got.Status != SwarmTaskStatusCompleted {
		t.Fatalf("task = %+v found=%v, want status completed", got, ok)
	}

	if err := store.UpdateTaskStatus(ctx, "task-guarded", SwarmTaskStatusCompleted, "idempotent"); err != nil {
		t.Fatalf("UpdateTaskStatus(idempotent terminal) error = %v", err)
	}
}

func TestSQLiteSwarmStore_SetTaskResultTransitionGuarded(t *testing.T) {
	provider := newTestProvider(t)
	defer closeProvider(t, provider)

	ctx := context.Background()
	store := provider.Swarm()

	_, err := store.CreateTask(ctx, SwarmTaskRecord{
		ID:        "task-canceled",
		SessionID: "session-1",
		Objective: "canceled",
		Status:    SwarmTaskStatusCanceled,
	})
	if err != nil {
		t.Fatalf("CreateTask() error = %v", err)
	}

	err = store.SetTaskResult(ctx, "task-canceled", `{"ok":true}`, SwarmTaskStatusCompleted, "should fail")
	if err == nil || !strings.Contains(err.Error(), "invalid swarm task transition") {
		t.Fatalf("SetTaskResult() error = %v, want invalid transition", err)
	}
	got, ok, err := store.GetTask(ctx, "task-canceled")
	if err != nil {
		t.Fatalf("GetTask() error = %v", err)
	}
	if !ok || got.Status != SwarmTaskStatusCanceled {
		t.Fatalf("task = %+v found=%v, want status canceled", got, ok)
	}
}

func TestSQLiteSwarmStore_DeliveryOutboxLifecycle(t *testing.T) {
	provider := newTestProvider(t)
	defer closeProvider(t, provider)

	ctx := context.Background()
	store := provider.Swarm()

	record, created, err := store.ReserveDelivery(ctx, SwarmDeliveryRecord{
		ID:          "delivery-1",
		DeliveryKey: "task-1:delivery:started",
		TaskID:      "task-1",
		SessionID:   "session-1",
		Channel:     "telegram",
		AddressKey:  "9001:1",
		Kind:        "delivery",
		PayloadJSON: `{"text":"hello"}`,
		PayloadHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("ReserveDelivery() error = %v", err)
	}
	if !created || record.Status != SwarmDeliveryStatusPending {
		t.Fatalf("ReserveDelivery() = %+v created=%v, want pending created", record, created)
	}

	again, created, err := store.ReserveDelivery(ctx, SwarmDeliveryRecord{
		ID:          "delivery-duplicate",
		DeliveryKey: "task-1:delivery:started",
		TaskID:      "task-1",
		SessionID:   "session-1",
		Channel:     "telegram",
		AddressKey:  "9001:1",
		Kind:        "delivery",
		PayloadJSON: `{"text":"hello"}`,
		PayloadHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("ReserveDelivery(duplicate) error = %v", err)
	}
	if created || again.ID != "delivery-1" {
		t.Fatalf("ReserveDelivery(duplicate) = %+v created=%v, want existing", again, created)
	}

	if err := store.MarkDeliverySent(ctx, record.DeliveryKey, "tg-42"); err != nil {
		t.Fatalf("MarkDeliverySent() error = %v", err)
	}
	sent, created, err := store.ReserveDelivery(ctx, SwarmDeliveryRecord{
		ID:          "delivery-after-sent",
		DeliveryKey: record.DeliveryKey,
		TaskID:      "task-1",
		SessionID:   "session-1",
		Channel:     "telegram",
		AddressKey:  "9001:1",
		Kind:        "delivery",
		PayloadJSON: `{"text":"hello"}`,
		PayloadHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("ReserveDelivery(after sent) error = %v", err)
	}
	if created || sent.Status != SwarmDeliveryStatusSent || sent.ProviderMessageID != "tg-42" || sent.SentAt.IsZero() {
		t.Fatalf("ReserveDelivery(after sent) = %+v created=%v, want sent existing", sent, created)
	}

	if err := store.MarkDeliveryFailed(ctx, record.DeliveryKey, "provider timeout"); err != nil {
		t.Fatalf("MarkDeliveryFailed() error = %v", err)
	}
	deliveryCounts, err := store.ListDeliveryStatusCounts(ctx)
	if err != nil {
		t.Fatalf("ListDeliveryStatusCounts() error = %v", err)
	}
	if len(deliveryCounts) != 1 || deliveryCounts[0].Status != SwarmDeliveryStatusFailed || deliveryCounts[0].Count != 1 {
		t.Fatalf("delivery status counts = %+v, want failed=1", deliveryCounts)
	}
}

func TestSQLiteSwarmStore_AgentStepLifecycle(t *testing.T) {
	provider := newTestProvider(t)
	defer closeProvider(t, provider)

	ctx := context.Background()
	store := provider.Swarm()

	record, created, err := store.ReserveAgentStep(ctx, SwarmAgentStepRecord{
		ID:          "step-1",
		StepKey:     "task-1:agent:executor:executor:1",
		TaskID:      "task-1",
		AgentName:   "executor",
		Role:        "executor",
		Iteration:   1,
		PayloadHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("ReserveAgentStep() error = %v", err)
	}
	if !created || record.Status != SwarmAgentStepStatusRunning {
		t.Fatalf("ReserveAgentStep() = %+v created=%v, want running created", record, created)
	}

	again, created, err := store.ReserveAgentStep(ctx, SwarmAgentStepRecord{
		ID:          "step-duplicate",
		StepKey:     record.StepKey,
		TaskID:      "task-1",
		AgentName:   "executor",
		Role:        "executor",
		Iteration:   1,
		PayloadHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("ReserveAgentStep(duplicate) error = %v", err)
	}
	if created || again.ID != "step-1" || again.Status != SwarmAgentStepStatusRunning {
		t.Fatalf("ReserveAgentStep(duplicate) = %+v created=%v, want existing running", again, created)
	}

	if err := store.CompleteAgentStep(ctx, record.StepKey, `{"kind":"agent_result"}`); err != nil {
		t.Fatalf("CompleteAgentStep() error = %v", err)
	}
	completed, created, err := store.ReserveAgentStep(ctx, SwarmAgentStepRecord{
		ID:          "step-after-complete",
		StepKey:     record.StepKey,
		TaskID:      "task-1",
		AgentName:   "executor",
		Role:        "executor",
		Iteration:   1,
		PayloadHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("ReserveAgentStep(after complete) error = %v", err)
	}
	if created || completed.Status != SwarmAgentStepStatusSucceeded || completed.ResultJSON == "" || completed.CompletedAt.IsZero() {
		t.Fatalf("ReserveAgentStep(after complete) = %+v created=%v, want stored succeeded result", completed, created)
	}
}
