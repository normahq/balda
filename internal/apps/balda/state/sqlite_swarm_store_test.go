package state

import (
	"context"
	"testing"
	"time"
)

func TestSQLiteSwarmStore_PublishClaimAckPriority(t *testing.T) {
	provider := newTestProvider(t)
	defer closeProvider(t, provider)

	ctx := context.Background()
	store := provider.Swarm()

	if _, err := store.Publish(ctx, swarmRecord("low", "session:alpha", 0)); err != nil {
		t.Fatalf("Publish(low) error = %v", err)
	}
	if _, err := store.Publish(ctx, swarmRecord("high", "session:alpha", 10)); err != nil {
		t.Fatalf("Publish(high) error = %v", err)
	}

	claimed, err := store.Claim(ctx, "session:alpha", "worker-1", 8, time.Minute)
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("Claim() len = %d, want 2", len(claimed))
	}
	if got := claimed[0].ID; got != "high" {
		t.Fatalf("first claimed id = %q, want high", got)
	}
	if claimed[0].Status != SwarmMessageStatusLeased || claimed[0].Attempt != 1 || claimed[0].LeaseOwner != "worker-1" || claimed[0].LeaseUntil.IsZero() {
		t.Fatalf("claimed high = %+v, want leased attempt=1 with lease", claimed[0])
	}

	if err := store.Ack(ctx, "session:alpha", "high"); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	acked, ok, err := store.GetMessage(ctx, "high")
	if err != nil {
		t.Fatalf("GetMessage(high) error = %v", err)
	}
	if !ok || acked.Status != SwarmMessageStatusAcked || acked.CompletedAt.IsZero() {
		t.Fatalf("acked high = %+v, found=%v, want acked", acked, ok)
	}
}

func TestSQLiteSwarmStore_RetryDeadLetterAndRecovery(t *testing.T) {
	provider := newTestProvider(t)
	defer closeProvider(t, provider)

	ctx := context.Background()
	store := provider.Swarm()

	if _, err := store.Publish(ctx, swarmRecord("retry", "task:alpha", 0)); err != nil {
		t.Fatalf("Publish(retry) error = %v", err)
	}
	claimed, err := store.Claim(ctx, "task:alpha", "worker-1", 1, time.Millisecond)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim(retry) = len %d, err %v, want one", len(claimed), err)
	}
	if err := store.Retry(ctx, "task:alpha", "retry", time.Now().Add(-time.Second), "temporary"); err != nil {
		t.Fatalf("Retry() error = %v", err)
	}
	claimed, err = store.Claim(ctx, "task:alpha", "worker-2", 1, time.Minute)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim(after retry) = len %d, err %v, want one", len(claimed), err)
	}
	if claimed[0].Attempt != 2 {
		t.Fatalf("attempt after retry claim = %d, want 2", claimed[0].Attempt)
	}
	if err := store.DeadLetter(ctx, "task:alpha", "retry", "permanent"); err != nil {
		t.Fatalf("DeadLetter() error = %v", err)
	}
	dead, ok, err := store.GetMessage(ctx, "retry")
	if err != nil {
		t.Fatalf("GetMessage(retry) error = %v", err)
	}
	if !ok || dead.Status != SwarmMessageStatusDead || dead.Error != "permanent" {
		t.Fatalf("dead message = %+v, found=%v, want dead permanent", dead, ok)
	}

	if _, err := store.Publish(ctx, swarmRecord("lease", "task:alpha", 0)); err != nil {
		t.Fatalf("Publish(lease) error = %v", err)
	}
	if _, err := store.Claim(ctx, "task:alpha", "worker-1", 1, time.Millisecond); err != nil {
		t.Fatalf("Claim(lease) error = %v", err)
	}
	recovered, err := store.Recover(ctx, time.Now().Add(2*time.Second).UTC())
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if recovered.RetriedLeases != 1 {
		t.Fatalf("RetriedLeases = %d, want 1", recovered.RetriedLeases)
	}
}

func TestSQLiteSwarmStore_DedupeAndCancel(t *testing.T) {
	provider := newTestProvider(t)
	defer closeProvider(t, provider)

	ctx := context.Background()
	store := provider.Swarm()

	record := swarmRecord("first", "session:dedupe", 0)
	record.DedupeKey = "webhook:req-1"
	first, err := store.Publish(ctx, record)
	if err != nil {
		t.Fatalf("Publish(first) error = %v", err)
	}
	if !first.Published {
		t.Fatal("Publish(first) Published = false, want true")
	}
	duplicate := swarmRecord("second", "session:dedupe", 0)
	duplicate.DedupeKey = "webhook:req-1"
	second, err := store.Publish(ctx, duplicate)
	if err != nil {
		t.Fatalf("Publish(second duplicate) error = %v", err)
	}
	if second.Published {
		t.Fatal("Publish(second duplicate) Published = true, want false")
	}
	claimed, err := store.Claim(ctx, "session:dedupe", "worker-1", 8, time.Minute)
	if err != nil {
		t.Fatalf("Claim(dedupe) error = %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != "first" {
		t.Fatalf("claimed dedupe = %+v, want only first", claimed)
	}

	if _, err := store.Publish(ctx, swarmRecordWithSessionTask("cancel-session", "session:cancel", "s-1", "")); err != nil {
		t.Fatalf("Publish(cancel-session) error = %v", err)
	}
	if _, err := store.Publish(ctx, swarmRecordWithSessionTask("cancel-task", "task:cancel", "", "t-1")); err != nil {
		t.Fatalf("Publish(cancel-task) error = %v", err)
	}
	count, err := store.CancelBySession(ctx, "s-1", "stop session")
	if err != nil || count != 1 {
		t.Fatalf("CancelBySession() = %d, err %v, want 1 nil", count, err)
	}
	count, err = store.CancelByTask(ctx, "t-1", "stop task")
	if err != nil || count != 1 {
		t.Fatalf("CancelByTask() = %d, err %v, want 1 nil", count, err)
	}
}

func TestSQLiteSwarmStore_PendingCountAndCancelDroppable(t *testing.T) {
	provider := newTestProvider(t)
	defer closeProvider(t, provider)

	ctx := context.Background()
	store := provider.Swarm()

	if _, err := store.Publish(ctx, swarmRecord("newer-high", "session:drop", 20)); err != nil {
		t.Fatalf("Publish(newer-high) error = %v", err)
	}
	if _, err := store.Publish(ctx, swarmRecord("older-low", "session:drop", 1)); err != nil {
		t.Fatalf("Publish(older-low) error = %v", err)
	}
	if _, err := store.Publish(ctx, swarmRecord("newer-low", "session:drop", 1)); err != nil {
		t.Fatalf("Publish(newer-low) error = %v", err)
	}

	count, err := store.PendingCount(ctx, "session:drop")
	if err != nil {
		t.Fatalf("PendingCount() error = %v", err)
	}
	if count != 3 {
		t.Fatalf("PendingCount() = %d, want 3", count)
	}

	dropped, err := store.CancelDroppable(ctx, "session:drop", 2, "cap")
	if err != nil {
		t.Fatalf("CancelDroppable() error = %v", err)
	}
	if len(dropped) != 2 {
		t.Fatalf("CancelDroppable() len = %d, want 2", len(dropped))
	}
	if dropped[0].ID != "older-low" || dropped[1].ID != "newer-low" {
		t.Fatalf("dropped ids = %q, %q; want older-low, newer-low", dropped[0].ID, dropped[1].ID)
	}
	count, err = store.PendingCount(ctx, "session:drop")
	if err != nil {
		t.Fatalf("PendingCount(after) error = %v", err)
	}
	if count != 1 {
		t.Fatalf("PendingCount(after) = %d, want 1", count)
	}
	statusCounts, err := store.ListMailboxStatusCounts(ctx, 10)
	if err != nil {
		t.Fatalf("ListMailboxStatusCounts() error = %v", err)
	}
	if len(statusCounts) != 2 {
		t.Fatalf("mailbox status counts = %+v, want queued and canceled", statusCounts)
	}
}

func TestSQLiteSwarmStore_ShadowMessagesAreNotClaimable(t *testing.T) {
	provider := newTestProvider(t)
	defer closeProvider(t, provider)

	ctx := context.Background()
	store := provider.Swarm()
	record := swarmRecord("shadow", "session:shadow", 0)
	record.Status = SwarmMessageStatusShadow
	record.DedupeKey = "event-1"
	if _, err := store.Publish(ctx, record); err != nil {
		t.Fatalf("Publish(shadow) error = %v", err)
	}
	queued := swarmRecord("queued", "session:shadow", 0)
	queued.DedupeKey = "event-1"
	published, err := store.Publish(ctx, queued)
	if err != nil {
		t.Fatalf("Publish(queued) error = %v", err)
	}
	if !published.Published {
		t.Fatal("Publish(queued) Published = false, want true despite matching shadow dedupe")
	}

	claimed, err := store.Claim(ctx, "session:shadow", "worker-1", 8, time.Minute)
	if err != nil {
		t.Fatalf("Claim(shadow) error = %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != "queued" {
		t.Fatalf("claimed messages = %+v, want queued only", claimed)
	}

	got, ok, err := store.GetMessage(ctx, "shadow")
	if err != nil {
		t.Fatalf("GetMessage(shadow) error = %v", err)
	}
	if !ok || got.Status != SwarmMessageStatusShadow {
		t.Fatalf("shadow message = %+v, found=%v, want status shadow", got, ok)
	}
}

func TestSQLiteSwarmStore_ExpiresMessages(t *testing.T) {
	provider := newTestProvider(t)
	defer closeProvider(t, provider)

	ctx := context.Background()
	store := provider.Swarm()
	record := swarmRecord("expired", "session:expire", 0)
	record.ExpiresAt = time.Now().Add(-time.Second)
	if _, err := store.Publish(ctx, record); err != nil {
		t.Fatalf("Publish(expired) error = %v", err)
	}
	recovered, err := store.Recover(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if recovered.Expired != 1 {
		t.Fatalf("Expired = %d, want 1", recovered.Expired)
	}
	got, ok, err := store.GetMessage(ctx, "expired")
	if err != nil {
		t.Fatalf("GetMessage(expired) error = %v", err)
	}
	if !ok || got.Status != SwarmMessageStatusExpired {
		t.Fatalf("expired message = %+v, found=%v, want expired", got, ok)
	}
}

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

func swarmRecord(id, mailbox string, priority int) SwarmMessageRecord {
	record := swarmRecordWithSessionTask(id, mailbox, "", "")
	record.Priority = priority
	return record
}

func swarmRecordWithSessionTask(id, mailbox, sessionID, taskID string) SwarmMessageRecord {
	return SwarmMessageRecord{
		ID:          id,
		Mailbox:     mailbox,
		Namespace:   "test.inbound",
		Kind:        "test",
		FromAddr:    "test:source",
		ToAddr:      mailbox,
		SessionID:   sessionID,
		TaskID:      taskID,
		Priority:    0,
		PayloadJSON: `{"ok":true}`,
	}
}
