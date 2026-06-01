package swarm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
)

const (
	TaskEventTaskCreated    = "task.created"
	TaskEventTaskAssigned   = "task.assigned"
	TaskEventTaskStarted    = "task.started"
	TaskEventAgentStarted   = "agent.started"
	TaskEventAgentProgress  = "agent.progress"
	TaskEventAgentResult    = "agent.result"
	TaskEventTaskValidating = "task.validating"
	TaskEventTaskCompleted  = "task.completed"
	TaskEventTaskFailed     = "task.failed"
	TaskEventTaskCanceled   = "task.canceled"
	TaskEventDeliverySent   = "delivery.sent"
	TaskEventDeliveryFailed = "delivery.failed"
)

type TaskService struct {
	store baldastate.SwarmStore
	bus   EventPublisher
}

type taskServiceParams struct {
	fx.In

	StateProvider baldastate.Provider
	Bus           EventPublisher `optional:"true"`
}

func NewTaskService(params taskServiceParams) (*TaskService, error) {
	if params.StateProvider == nil {
		return nil, fmt.Errorf("balda state provider is required")
	}
	return &TaskService{store: params.StateProvider.Swarm(), bus: params.Bus}, nil
}

func (s *TaskService) Create(ctx context.Context, record baldastate.SwarmTaskRecord, actor string, payload any) (bool, error) {
	if s == nil {
		return false, nil
	}
	payloadJSON, err := marshalPayload(payload)
	if err != nil {
		return false, err
	}
	if strings.TrimSpace(payloadJSON) == "" {
		payloadJSON = "{}"
	}
	// Contract: task state is authoritative in SQLite; event publication is visibility-only.
	created, err := s.store.CreateTask(ctx, record)
	if err != nil {
		return false, err
	}
	taskID := strings.TrimSpace(record.ID)
	s.publishEventRecordBestEffort(ctx, baldastate.SwarmTaskEventRecord{
		ID:          "task:" + taskID + ":event:created",
		TaskID:      taskID,
		EventType:   TaskEventTaskCreated,
		Actor:       strings.TrimSpace(actor),
		PayloadJSON: payloadJSON,
	})
	return created, nil
}

func (s *TaskService) Get(ctx context.Context, taskID string) (baldastate.SwarmTaskRecord, bool, error) {
	if s == nil {
		return baldastate.SwarmTaskRecord{}, false, nil
	}
	return s.store.GetTask(ctx, taskID)
}

func (s *TaskService) ListActiveTasksBySession(ctx context.Context, sessionID string) ([]baldastate.SwarmTaskRecord, error) {
	if s == nil {
		return nil, nil
	}
	return s.store.ListActiveTasksBySession(ctx, sessionID)
}

func (s *TaskService) ListActiveGoalTasksBySession(ctx context.Context, sessionID string) ([]baldastate.SwarmTaskRecord, error) {
	if s == nil {
		return nil, nil
	}
	tasks, err := s.store.ListActiveTasksBySession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	out := make([]baldastate.SwarmTaskRecord, 0, len(tasks))
	for _, task := range tasks {
		if IsGoalTask(task) {
			out = append(out, task)
		}
	}
	return out, nil
}

func (s *TaskService) MarkStatus(ctx context.Context, taskID string, status string, actor string, messageID string, reason string, payload any) error {
	if s == nil {
		return nil
	}
	// Contract: persist lifecycle transition first, then best-effort event emission.
	if err := s.store.UpdateTaskStatus(ctx, taskID, status, reason); err != nil {
		return err
	}
	eventType := ""
	switch strings.TrimSpace(status) {
	case baldastate.SwarmTaskStatusQueued, baldastate.SwarmTaskStatusWaitingForAgent, baldastate.SwarmTaskStatusWaitingForUser:
		eventType = TaskEventTaskAssigned
	case baldastate.SwarmTaskStatusRunning:
		eventType = TaskEventTaskStarted
	case baldastate.SwarmTaskStatusValidating:
		eventType = TaskEventTaskValidating
	case baldastate.SwarmTaskStatusCompleted:
		eventType = TaskEventTaskCompleted
	case baldastate.SwarmTaskStatusFailed, baldastate.SwarmTaskStatusDeadLettered:
		eventType = TaskEventTaskFailed
	case baldastate.SwarmTaskStatusCanceled:
		eventType = TaskEventTaskCanceled
	}
	if eventType == "" {
		return nil
	}
	return s.appendEventBestEffort(ctx, taskID, eventType, actor, messageID, mergePayload(payload, map[string]any{
		"status": status,
		"reason": reason,
	}))
}

func (s *TaskService) SetResult(ctx context.Context, taskID string, result any, status string, actor string, reason string) error {
	if s == nil {
		return nil
	}
	data, err := marshalPayload(result)
	if err != nil {
		return err
	}
	// Contract: result/state write is authoritative; event emission is best-effort visibility.
	if err := s.store.SetTaskResult(ctx, taskID, data, status, reason); err != nil {
		return err
	}
	eventType := ""
	switch strings.TrimSpace(status) {
	case baldastate.SwarmTaskStatusQueued, baldastate.SwarmTaskStatusWaitingForAgent, baldastate.SwarmTaskStatusWaitingForUser:
		eventType = TaskEventTaskAssigned
	case baldastate.SwarmTaskStatusRunning:
		eventType = TaskEventTaskStarted
	case baldastate.SwarmTaskStatusValidating:
		eventType = TaskEventTaskValidating
	case baldastate.SwarmTaskStatusCompleted:
		eventType = TaskEventTaskCompleted
	case baldastate.SwarmTaskStatusFailed, baldastate.SwarmTaskStatusDeadLettered:
		eventType = TaskEventTaskFailed
	case baldastate.SwarmTaskStatusCanceled:
		eventType = TaskEventTaskCanceled
	}
	return s.appendEventBestEffort(ctx, taskID, eventType, actor, "", mergePayload(result, map[string]any{
		"status": status,
		"reason": reason,
	}))
}

func (s *TaskService) AppendEvent(ctx context.Context, taskID string, eventType string, actor string, messageID string, payload any) error {
	if s == nil {
		return nil
	}
	event, err := taskEventRecord(taskID, eventType, actor, messageID, payload)
	if err != nil {
		return err
	}
	return s.publishEventRecord(ctx, event)
}

func (s *TaskService) appendEventBestEffort(ctx context.Context, taskID string, eventType string, actor string, messageID string, payload any) error {
	if s == nil {
		return nil
	}
	event, err := taskEventRecord(taskID, eventType, actor, messageID, payload)
	if err != nil {
		return err
	}
	s.publishEventRecordBestEffort(ctx, event)
	return nil
}

func taskEventRecord(taskID string, eventType string, actor string, messageID string, payload any) (baldastate.SwarmTaskEventRecord, error) {
	data, err := marshalPayload(payload)
	if err != nil {
		return baldastate.SwarmTaskEventRecord{}, err
	}
	eventID := ""
	if strings.TrimSpace(eventType) == TaskEventAgentProgress {
		eventID = uuid.NewString()
	} else {
		parts := []string{
			strings.TrimSpace(taskID),
			strings.TrimSpace(eventType),
			strings.TrimSpace(actor),
			strings.TrimSpace(messageID),
			strings.TrimSpace(data),
		}
		sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
		eventTypePart := strings.ToLower(strings.TrimSpace(eventType))
		var eventTypeID strings.Builder
		lastDash := false
		for _, r := range eventTypePart {
			switch {
			case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
				eventTypeID.WriteRune(r)
				lastDash = false
			default:
				if eventTypeID.Len() > 0 && !lastDash {
					eventTypeID.WriteByte('-')
					lastDash = true
				}
			}
			if eventTypeID.Len() >= 48 {
				break
			}
		}
		eventTypePart = strings.Trim(eventTypeID.String(), "-")
		if eventTypePart == "" {
			eventTypePart = "event"
		}
		eventID = "task:" + strings.TrimSpace(taskID) + ":event:" + eventTypePart + ":" + hex.EncodeToString(sum[:])[:16]
	}
	return baldastate.SwarmTaskEventRecord{
		ID:          eventID,
		TaskID:      strings.TrimSpace(taskID),
		EventType:   strings.TrimSpace(eventType),
		Actor:       strings.TrimSpace(actor),
		MessageID:   strings.TrimSpace(messageID),
		PayloadJSON: data,
	}, nil
}

func (s *TaskService) CancelBySession(ctx context.Context, sessionID string, actor string, reason string) ([]string, error) {
	if s == nil {
		return nil, nil
	}
	tasks, err := s.store.ListActiveTasksBySession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(tasks))
	for _, task := range tasks {
		if err := s.MarkStatus(ctx, task.ID, baldastate.SwarmTaskStatusCanceled, actor, "", reason, nil); err != nil {
			return ids, err
		}
		ids = append(ids, task.ID)
	}
	return ids, nil
}

func (s *TaskService) CancelTask(ctx context.Context, taskID string, actor string, reason string) error {
	if s == nil {
		return nil
	}
	return s.MarkStatus(ctx, taskID, baldastate.SwarmTaskStatusCanceled, actor, "", reason, nil)
}

func (s *TaskService) DeadLetter(ctx context.Context, taskID string, actor string, messageID string, reason string) error {
	return s.MarkStatus(ctx, taskID, baldastate.SwarmTaskStatusDeadLettered, actor, messageID, reason, nil)
}

func (s *TaskService) ReserveDelivery(ctx context.Context, record baldastate.SwarmDeliveryRecord) (baldastate.SwarmDeliveryRecord, bool, error) {
	if s == nil {
		return baldastate.SwarmDeliveryRecord{}, false, nil
	}
	return s.store.ReserveDelivery(ctx, record)
}

func (s *TaskService) MarkDeliverySent(ctx context.Context, deliveryKey string, providerMessageID string) error {
	if s == nil {
		return nil
	}
	return s.store.MarkDeliverySent(ctx, deliveryKey, providerMessageID)
}

func (s *TaskService) MarkDeliverySending(ctx context.Context, deliveryKey string) error {
	if s == nil {
		return nil
	}
	return s.store.MarkDeliverySending(ctx, deliveryKey)
}

func (s *TaskService) MarkDeliveryFailed(ctx context.Context, deliveryKey string, reason string) error {
	if s == nil {
		return nil
	}
	return s.store.MarkDeliveryFailed(ctx, deliveryKey, reason)
}

func (s *TaskService) ReserveAgentStep(ctx context.Context, record baldastate.SwarmAgentStepRecord) (baldastate.SwarmAgentStepRecord, bool, error) {
	if s == nil {
		return baldastate.SwarmAgentStepRecord{}, false, nil
	}
	return s.store.ReserveAgentStep(ctx, record)
}

func (s *TaskService) CompleteAgentStep(ctx context.Context, stepKey string, resultJSON string) error {
	if s == nil {
		return nil
	}
	return s.store.CompleteAgentStep(ctx, stepKey, resultJSON)
}

func (s *TaskService) FailAgentStep(ctx context.Context, stepKey string, resultJSON string, reason string) error {
	if s == nil {
		return nil
	}
	return s.store.FailAgentStep(ctx, stepKey, resultJSON, reason)
}

func IsGoalTask(task baldastate.SwarmTaskRecord) bool {
	owner := strings.TrimSpace(task.OwnerActor)
	assigned := strings.TrimSpace(task.AssignedActor)
	for _, prefix := range []string{"goalkeeper:", "goal:"} {
		if strings.HasPrefix(owner, prefix) || strings.HasPrefix(assigned, prefix) {
			return true
		}
	}
	return false
}

func (s *TaskService) publishTaskEvent(ctx context.Context, event baldastate.SwarmTaskEventRecord) error {
	if s == nil || s.bus == nil {
		return fmt.Errorf("event bus is required")
	}
	payload := strings.TrimSpace(event.PayloadJSON)
	if payload == "" {
		payload = "{}"
	}
	subject := SubjectEventTaskUpdated
	switch strings.TrimSpace(event.EventType) {
	case TaskEventDeliverySent:
		subject = SubjectEventDeliverySent
	case TaskEventDeliveryFailed:
		subject = SubjectEventDeliveryFailed
	case TaskEventTaskCreated:
		subject = SubjectEventTaskCreated
	case TaskEventTaskCompleted:
		subject = SubjectEventTaskCompleted
	}
	env := Envelope{
		ID:          event.ID,
		Namespace:   NamespaceTelemetry,
		Kind:        "task_event",
		From:        SystemAddress("task-events"),
		To:          ActorAddress{Target: ActorTypeTask, Key: event.TaskID},
		TaskID:      event.TaskID,
		PayloadJSON: payload,
		Meta: map[string]string{
			"event_type": event.EventType,
			"actor":      event.Actor,
			"message_id": event.MessageID,
		},
	}
	return s.bus.PublishEvent(ctx, subject, env)
}

func (s *TaskService) publishEventRecord(ctx context.Context, event baldastate.SwarmTaskEventRecord) error {
	if s == nil {
		return nil
	}
	return s.publishTaskEvent(ctx, event)
}

func (s *TaskService) publishEventRecordBestEffort(ctx context.Context, event baldastate.SwarmTaskEventRecord) {
	if err := s.publishEventRecord(ctx, event); err != nil {
		log.Ctx(ctx).Warn().
			Err(err).
			Str("task_id", event.TaskID).
			Str("event_type", event.EventType).
			Str("event_id", event.ID).
			Msg("failed to publish task event")
	}
}

func marshalPayload(payload any) (string, error) {
	if payload == nil {
		return "", nil
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode task payload: %w", err)
	}
	return string(data), nil
}

func mergePayload(payload any, extra map[string]any) any {
	out := make(map[string]any, len(extra)+1)
	if payload != nil {
		out["payload"] = payload
	}
	for key, value := range extra {
		if strings.TrimSpace(key) != "" && value != "" {
			out[key] = value
		}
	}
	return out
}
