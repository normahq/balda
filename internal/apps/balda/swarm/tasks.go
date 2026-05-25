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
)

type TaskService struct {
	store baldastate.SwarmStore
	bus   CommandBus
}

type taskServiceParams struct {
	fx.In

	StateProvider baldastate.Provider
	Bus           CommandBus `optional:"true"`
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
	created, err := s.store.CreateTask(ctx, record)
	if err != nil {
		return false, err
	}
	if err := s.publishEventRecord(ctx, baldastate.SwarmTaskEventRecord{
		ID:          taskCreatedEventID(record.ID),
		TaskID:      strings.TrimSpace(record.ID),
		EventType:   TaskEventTaskCreated,
		Actor:       strings.TrimSpace(actor),
		PayloadJSON: payloadJSON,
	}); err != nil {
		return false, err
	}
	return created, nil
}

func (s *TaskService) Get(ctx context.Context, taskID string) (baldastate.SwarmTaskRecord, bool, error) {
	if s == nil {
		return baldastate.SwarmTaskRecord{}, false, nil
	}
	return s.store.GetTask(ctx, taskID)
}

func (s *TaskService) ListActiveBySession(ctx context.Context, sessionID string) ([]baldastate.SwarmTaskRecord, error) {
	if s == nil {
		return nil, nil
	}
	return s.store.ListActiveTasksBySession(ctx, sessionID)
}

func (s *TaskService) ListEvents(ctx context.Context, taskID string) ([]baldastate.SwarmTaskEventRecord, error) {
	if s == nil {
		return nil, nil
	}
	return s.store.ListTaskEvents(ctx, taskID)
}

func (s *TaskService) StatusCounts(ctx context.Context) ([]baldastate.SwarmStatusCount, error) {
	if s == nil {
		return nil, nil
	}
	return s.store.ListTaskStatusCounts(ctx)
}

func (s *TaskService) MarkStatus(ctx context.Context, taskID string, status string, actor string, messageID string, reason string, payload any) error {
	if s == nil {
		return nil
	}
	if err := s.store.UpdateTaskStatus(ctx, taskID, status, reason); err != nil {
		return err
	}
	eventType := taskEventForStatus(status)
	if eventType == "" {
		return nil
	}
	return s.AppendEvent(ctx, taskID, eventType, actor, messageID, mergePayload(payload, map[string]any{
		"status": status,
		"reason": reason,
	}))
}

func (s *TaskService) SetPlan(ctx context.Context, taskID string, actor string, plan any) error {
	if s == nil {
		return nil
	}
	data, err := marshalPayload(plan)
	if err != nil {
		return err
	}
	if err := s.store.SetTaskPlan(ctx, taskID, data); err != nil {
		return err
	}
	return nil
}

func (s *TaskService) SetResult(ctx context.Context, taskID string, result any, status string, actor string, reason string) error {
	if s == nil {
		return nil
	}
	data, err := marshalPayload(result)
	if err != nil {
		return err
	}
	if err := s.store.SetTaskResult(ctx, taskID, data, status, reason); err != nil {
		return err
	}
	return s.AppendEvent(ctx, taskID, taskEventForStatus(status), actor, "", mergePayload(result, map[string]any{
		"status": status,
		"reason": reason,
	}))
}

func (s *TaskService) AppendEvent(ctx context.Context, taskID string, eventType string, actor string, messageID string, payload any) error {
	if s == nil {
		return nil
	}
	data, err := marshalPayload(payload)
	if err != nil {
		return err
	}
	eventID := taskEventID(taskID, eventType, actor, messageID, data)
	event := baldastate.SwarmTaskEventRecord{
		ID:          eventID,
		TaskID:      strings.TrimSpace(taskID),
		EventType:   strings.TrimSpace(eventType),
		Actor:       strings.TrimSpace(actor),
		MessageID:   strings.TrimSpace(messageID),
		PayloadJSON: data,
	}
	if s.bus != nil {
		return s.publishEventRecord(ctx, event)
	}
	return fmt.Errorf("jetstream event bus is required")
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

func taskEventForStatus(status string) string {
	switch strings.TrimSpace(status) {
	case baldastate.SwarmTaskStatusQueued, baldastate.SwarmTaskStatusWaitingForAgent, baldastate.SwarmTaskStatusWaitingForUser:
		return TaskEventTaskAssigned
	case baldastate.SwarmTaskStatusRunning:
		return TaskEventTaskStarted
	case baldastate.SwarmTaskStatusValidating:
		return TaskEventTaskValidating
	case baldastate.SwarmTaskStatusCompleted:
		return TaskEventTaskCompleted
	case baldastate.SwarmTaskStatusFailed, baldastate.SwarmTaskStatusDeadLettered:
		return TaskEventTaskFailed
	case baldastate.SwarmTaskStatusCanceled:
		return TaskEventTaskCanceled
	default:
		return ""
	}
}

func (s *TaskService) publishTaskEvent(ctx context.Context, event baldastate.SwarmTaskEventRecord) error {
	if s == nil || s.bus == nil {
		return fmt.Errorf("jetstream event bus is required")
	}
	payload := strings.TrimSpace(event.PayloadJSON)
	if payload == "" {
		payload = "{}"
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
	return s.bus.PublishEvent(ctx, subjectForTaskEvent(event.EventType), env)
}

func (s *TaskService) publishEventRecord(ctx context.Context, event baldastate.SwarmTaskEventRecord) error {
	if s == nil {
		return nil
	}
	return s.publishTaskEvent(ctx, event)
}

func taskCreatedEventID(taskID string) string {
	return "task:" + strings.TrimSpace(taskID) + ":event:created"
}

func taskEventID(taskID string, eventType string, actor string, messageID string, payloadJSON string) string {
	if strings.TrimSpace(eventType) == TaskEventAgentProgress {
		return uuid.NewString()
	}
	parts := []string{
		strings.TrimSpace(taskID),
		strings.TrimSpace(eventType),
		strings.TrimSpace(actor),
		strings.TrimSpace(messageID),
		strings.TrimSpace(payloadJSON),
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return "task:" + strings.TrimSpace(taskID) + ":event:" + safeEventIDPart(eventType) + ":" + hex.EncodeToString(sum[:])[:16]
}

func safeEventIDPart(raw string) string {
	trimmed := strings.ToLower(strings.TrimSpace(raw))
	var out strings.Builder
	lastDash := false
	for _, r := range trimmed {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			out.WriteRune(r)
			lastDash = false
		default:
			if out.Len() > 0 && !lastDash {
				out.WriteByte('-')
				lastDash = true
			}
		}
		if out.Len() >= 48 {
			break
		}
	}
	part := strings.Trim(out.String(), "-")
	if part == "" {
		return "event"
	}
	return part
}

func subjectForTaskEvent(eventType string) string {
	switch strings.TrimSpace(eventType) {
	case TaskEventAgentStarted, TaskEventAgentProgress, TaskEventAgentResult:
		return SubjectEventTaskUpdated
	case TaskEventDeliverySent:
		return SubjectEventDeliverySent
	case TaskEventTaskCreated:
		return SubjectEventTaskCreated
	case TaskEventTaskCompleted:
		return SubjectEventTaskCompleted
	default:
		return SubjectEventTaskUpdated
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
