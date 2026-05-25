package swarm

import (
	"context"
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
	bus   EventBus
}

type taskServiceParams struct {
	fx.In

	StateProvider baldastate.Provider
	Bus           EventBus `optional:"true"`
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
	created, err := s.store.CreateTask(ctx, record)
	if err != nil {
		return false, err
	}
	if created {
		if err := s.AppendEvent(ctx, record.ID, TaskEventTaskCreated, actor, "", payload); err != nil {
			return false, err
		}
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
	eventID := uuid.NewString()
	event := baldastate.SwarmTaskEventRecord{
		ID:          eventID,
		TaskID:      strings.TrimSpace(taskID),
		EventType:   strings.TrimSpace(eventType),
		Actor:       strings.TrimSpace(actor),
		MessageID:   strings.TrimSpace(messageID),
		PayloadJSON: data,
	}
	if err := s.store.AppendTaskEvent(ctx, event); err != nil {
		return err
	}
	s.publishTaskEvent(ctx, event)
	return nil
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

func (s *TaskService) publishTaskEvent(ctx context.Context, event baldastate.SwarmTaskEventRecord) {
	if s == nil || s.bus == nil {
		return
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
	_ = s.bus.Publish(ctx, subjectForTaskEvent(event.EventType), env)
}

func subjectForTaskEvent(eventType string) string {
	switch strings.TrimSpace(eventType) {
	case TaskEventAgentStarted, TaskEventAgentProgress, TaskEventAgentResult:
		return SubjectEventAgent
	case TaskEventDeliverySent:
		return SubjectEventDelivery
	default:
		return SubjectEventTask
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
