package state

import (
	"context"
	"strings"
	"testing"
)

func TestSQLiteJobStore_TaskLifecycle(t *testing.T) {
	provider := newTestProvider(t)
	defer closeProvider(t, provider)

	ctx := context.Background()
	store := provider.Jobs()

	created, err := store.CreateJob(ctx, JobRecord{
		ID:            "task-1",
		SessionID:     "session-1",
		Title:         "Goal: test",
		Objective:     "test",
		Status:        JobStatusCreated,
		AssignedActor: "agent:executor",
		CreatedBy:     "tg-101",
	})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	if !created {
		t.Fatal("CreateJob() created = false, want true")
	}
	created, err = store.CreateJob(ctx, JobRecord{
		ID:        "task-1",
		Objective: "duplicate",
	})
	if err != nil {
		t.Fatalf("CreateJob(duplicate) error = %v", err)
	}
	if created {
		t.Fatal("CreateJob(duplicate) created = true, want false")
	}

	if err := store.UpdateJobStatus(ctx, "task-1", JobStatusWaitingForAgent, "waiting"); err != nil {
		t.Fatalf("UpdateJobStatus(waiting) error = %v", err)
	}
	if err := store.AppendJobEvent(ctx, JobEventRecord{
		ID:          "event-1",
		JobID:       "task-1",
		EventType:   "agent.started",
		Actor:       "task.actor",
		PayloadJSON: `{"role":"executor"}`,
	}); err != nil {
		t.Fatalf("AppendJobEvent() error = %v", err)
	}

	active, err := store.ListActiveJobsBySession(ctx, "session-1")
	if err != nil {
		t.Fatalf("ListActiveJobsBySession() error = %v", err)
	}
	if len(active) != 1 || active[0].ID != "task-1" {
		t.Fatalf("active tasks = %+v, want task-1", active)
	}

	if err := store.SetJobResult(ctx, "task-1", `{"ok":true}`, JobStatusCompleted, ""); err != nil {
		t.Fatalf("SetJobResult() error = %v", err)
	}
	got, ok, err := store.GetJob(ctx, "task-1")
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}
	if !ok || got.Status != JobStatusCompleted || got.ResultJSON == "" || got.StartedAt.IsZero() || got.CompletedAt.IsZero() {
		t.Fatalf("task = %+v, found=%v, want completed with result/timestamps", got, ok)
	}

	active, err = store.ListActiveJobsBySession(ctx, "session-1")
	if err != nil {
		t.Fatalf("ListActiveJobsBySession(after complete) error = %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("active tasks after complete = %+v, want none", active)
	}
	events, err := store.ListJobEvents(ctx, "task-1")
	if err != nil {
		t.Fatalf("ListJobEvents() error = %v", err)
	}
	if len(events) != 1 || events[0].EventType != "agent.started" {
		t.Fatalf("events = %+v, want agent.started", events)
	}
}

func TestSQLiteJobStore_TaskStatusTransitionsAreGuarded(t *testing.T) {
	provider := newTestProvider(t)
	defer closeProvider(t, provider)

	ctx := context.Background()
	store := provider.Jobs()

	_, err := store.CreateJob(ctx, JobRecord{
		ID:        "task-guarded",
		SessionID: "session-1",
		Objective: "guard transitions",
		Status:    JobStatusRunning,
	})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	if err := store.UpdateJobStatus(ctx, "task-guarded", JobStatusCompleted, "done"); err != nil {
		t.Fatalf("UpdateJobStatus(completed) error = %v", err)
	}

	err = store.UpdateJobStatus(ctx, "task-guarded", JobStatusRunning, "reopen")
	if err == nil || !strings.Contains(err.Error(), "invalid runtime job transition") {
		t.Fatalf("UpdateJobStatus(reopen) error = %v, want invalid transition", err)
	}
	got, ok, err := store.GetJob(ctx, "task-guarded")
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}
	if !ok || got.Status != JobStatusCompleted {
		t.Fatalf("task = %+v found=%v, want status completed", got, ok)
	}

	if err := store.UpdateJobStatus(ctx, "task-guarded", JobStatusCompleted, "idempotent"); err != nil {
		t.Fatalf("UpdateJobStatus(idempotent terminal) error = %v", err)
	}
}

func TestSQLiteJobStore_SetTaskResultTransitionGuarded(t *testing.T) {
	provider := newTestProvider(t)
	defer closeProvider(t, provider)

	ctx := context.Background()
	store := provider.Jobs()

	_, err := store.CreateJob(ctx, JobRecord{
		ID:        "task-canceled",
		SessionID: "session-1",
		Objective: "canceled",
		Status:    JobStatusCanceled,
	})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	err = store.SetJobResult(ctx, "task-canceled", `{"ok":true}`, JobStatusCompleted, "should fail")
	if err == nil || !strings.Contains(err.Error(), "invalid runtime job transition") {
		t.Fatalf("SetJobResult() error = %v, want invalid transition", err)
	}
	got, ok, err := store.GetJob(ctx, "task-canceled")
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}
	if !ok || got.Status != JobStatusCanceled {
		t.Fatalf("task = %+v found=%v, want status canceled", got, ok)
	}
}

func TestSQLiteJobStore_DeliveryOutboxLifecycle(t *testing.T) {
	provider := newTestProvider(t)
	defer closeProvider(t, provider)

	ctx := context.Background()
	store := provider.Jobs()

	record, created, err := store.ReserveDelivery(ctx, DeliveryRecord{
		ID:          "delivery-1",
		DeliveryKey: "task-1:delivery:started",
		JobID:       "task-1",
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
	if !created || record.Status != DeliveryStatusPending {
		t.Fatalf("ReserveDelivery() = %+v created=%v, want pending created", record, created)
	}

	again, created, err := store.ReserveDelivery(ctx, DeliveryRecord{
		ID:          "delivery-duplicate",
		DeliveryKey: "task-1:delivery:started",
		JobID:       "task-1",
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
	sent, created, err := store.ReserveDelivery(ctx, DeliveryRecord{
		ID:          "delivery-after-sent",
		DeliveryKey: record.DeliveryKey,
		JobID:       "task-1",
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
	if created || sent.Status != DeliveryStatusSent || sent.ProviderMessageID != "tg-42" || sent.SentAt.IsZero() {
		t.Fatalf("ReserveDelivery(after sent) = %+v created=%v, want sent existing", sent, created)
	}

	if err := store.MarkDeliveryFailed(ctx, record.DeliveryKey, "provider timeout"); err != nil {
		t.Fatalf("MarkDeliveryFailed() error = %v", err)
	}
	if delivery, created, err := store.ReserveDelivery(ctx, record); err != nil {
		t.Fatalf("ReserveDelivery(after failed) error = %v", err)
	} else if created || delivery.Status != DeliveryStatusFailed || delivery.Error != "provider timeout" {
		t.Fatalf("ReserveDelivery(after failed) = %+v created=%v, want failed existing", delivery, created)
	}
}

func TestSQLiteJobStore_AgentStepLifecycle(t *testing.T) {
	provider := newTestProvider(t)
	defer closeProvider(t, provider)

	ctx := context.Background()
	store := provider.Jobs()

	record, created, err := store.ReserveAgentStep(ctx, AgentStepRecord{
		ID:          "step-1",
		StepKey:     "task-1:agent:executor:executor:1",
		JobID:       "task-1",
		AgentName:   "executor",
		Role:        "executor",
		Iteration:   1,
		PayloadHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("ReserveAgentStep() error = %v", err)
	}
	if !created || record.Status != AgentStepStatusRunning {
		t.Fatalf("ReserveAgentStep() = %+v created=%v, want running created", record, created)
	}

	again, created, err := store.ReserveAgentStep(ctx, AgentStepRecord{
		ID:          "step-duplicate",
		StepKey:     record.StepKey,
		JobID:       "task-1",
		AgentName:   "executor",
		Role:        "executor",
		Iteration:   1,
		PayloadHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("ReserveAgentStep(duplicate) error = %v", err)
	}
	if created || again.ID != "step-1" || again.Status != AgentStepStatusRunning {
		t.Fatalf("ReserveAgentStep(duplicate) = %+v created=%v, want existing running", again, created)
	}

	if err := store.CompleteAgentStep(ctx, record.StepKey, `{"kind":"agent_result"}`); err != nil {
		t.Fatalf("CompleteAgentStep() error = %v", err)
	}
	completed, created, err := store.ReserveAgentStep(ctx, AgentStepRecord{
		ID:          "step-after-complete",
		StepKey:     record.StepKey,
		JobID:       "task-1",
		AgentName:   "executor",
		Role:        "executor",
		Iteration:   1,
		PayloadHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("ReserveAgentStep(after complete) error = %v", err)
	}
	if created || completed.Status != AgentStepStatusSucceeded || completed.ResultJSON == "" || completed.CompletedAt.IsZero() {
		t.Fatalf("ReserveAgentStep(after complete) = %+v created=%v, want stored succeeded result", completed, created)
	}
}
