package jobs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/pkg/actorlayer"
	actortransport "github.com/normahq/balda/pkg/actorlayer/transport"
	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
)

const (
	JobEventCreated        = "job.created"
	JobEventAssigned       = "job.assigned"
	JobEventStarted        = "job.started"
	JobEventAgentStarted   = "agent.started"
	JobEventAgentProgress  = "agent.progress"
	JobEventAgentResult    = "agent.result"
	JobEventValidating     = "job.validating"
	JobEventCompleted      = "job.completed"
	JobEventFailed         = "job.failed"
	JobEventCanceled       = "job.canceled"
	JobEventDeliverySent   = "delivery.sent"
	JobEventDeliveryFailed = "delivery.failed"

	TaskEventTaskCreated    = JobEventCreated
	TaskEventTaskAssigned   = JobEventAssigned
	TaskEventTaskStarted    = JobEventStarted
	TaskEventAgentStarted   = JobEventAgentStarted
	TaskEventAgentProgress  = JobEventAgentProgress
	TaskEventAgentResult    = JobEventAgentResult
	TaskEventTaskValidating = JobEventValidating
	TaskEventTaskCompleted  = JobEventCompleted
	TaskEventTaskFailed     = JobEventFailed
	TaskEventTaskCanceled   = JobEventCanceled
	TaskEventDeliverySent   = JobEventDeliverySent
	TaskEventDeliveryFailed = JobEventDeliveryFailed
)

type JobService struct {
	store baldastate.JobStore
	bus   actortransport.EventPublisher
}

type jobServiceParams struct {
	fx.In

	StateProvider baldastate.Provider
	Bus           actortransport.EventPublisher `optional:"true"`
}

func NewJobService(params jobServiceParams) (*JobService, error) {
	if params.StateProvider == nil {
		return nil, fmt.Errorf("balda state provider is required")
	}
	return &JobService{store: params.StateProvider.Jobs(), bus: params.Bus}, nil
}

func (s *JobService) Create(ctx context.Context, record baldastate.JobRecord, actor string, payload any) (bool, error) {
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
	created, err := s.store.CreateJob(ctx, record)
	if err != nil {
		return false, err
	}
	taskID := strings.TrimSpace(record.ID)
	s.publishEventRecordBestEffort(ctx, baldastate.JobEventRecord{
		ID:          "task:" + taskID + ":event:created",
		JobID:       taskID,
		EventType:   JobEventCreated,
		Actor:       strings.TrimSpace(actor),
		PayloadJSON: payloadJSON,
	})
	return created, nil
}

func (s *JobService) Get(ctx context.Context, taskID string) (baldastate.JobRecord, bool, error) {
	if s == nil {
		return baldastate.JobRecord{}, false, nil
	}
	return s.store.GetJob(ctx, taskID)
}

func (s *JobService) ListActiveJobsBySession(ctx context.Context, sessionID string) ([]baldastate.JobRecord, error) {
	if s == nil {
		return nil, nil
	}
	return s.store.ListActiveJobsBySession(ctx, sessionID)
}

func (s *JobService) ListActiveGoalJobsBySession(ctx context.Context, sessionID string) ([]baldastate.JobRecord, error) {
	if s == nil {
		return nil, nil
	}
	tasks, err := s.store.ListActiveJobsBySession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	out := make([]baldastate.JobRecord, 0, len(tasks))
	for _, task := range tasks {
		if IsGoalJob(task) {
			out = append(out, task)
		}
	}
	return out, nil
}

func (s *JobService) MarkStatus(ctx context.Context, taskID string, status string, actor string, messageID string, reason string, payload any) error {
	if s == nil {
		return nil
	}
	// Contract: persist lifecycle transition first, then best-effort event emission.
	if err := s.store.UpdateJobStatus(ctx, taskID, status, reason); err != nil {
		return s.suppressStaleTerminalTransition(ctx, taskID, status, err)
	}
	eventType := ""
	switch strings.TrimSpace(status) {
	case baldastate.JobStatusQueued, baldastate.JobStatusWaitingForAgent, baldastate.JobStatusWaitingForUser:
		eventType = JobEventAssigned
	case baldastate.JobStatusRunning:
		eventType = JobEventStarted
	case baldastate.JobStatusValidating:
		eventType = JobEventValidating
	case baldastate.JobStatusCompleted:
		eventType = JobEventCompleted
	case baldastate.JobStatusFailed, baldastate.JobStatusDeadLettered:
		eventType = JobEventFailed
	case baldastate.JobStatusCanceled:
		eventType = JobEventCanceled
	}
	if eventType == "" {
		return nil
	}
	return s.appendEventBestEffort(ctx, taskID, eventType, actor, messageID, mergePayload(payload, map[string]any{
		"status": status,
		"reason": reason,
	}))
}

func (s *JobService) SetResult(ctx context.Context, taskID string, result any, status string, actor string, reason string) error {
	if s == nil {
		return nil
	}
	data, err := marshalPayload(result)
	if err != nil {
		return err
	}
	// Contract: result/state write is authoritative; event emission is best-effort visibility.
	if err := s.store.SetJobResult(ctx, taskID, data, status, reason); err != nil {
		return s.suppressStaleTerminalTransition(ctx, taskID, status, err)
	}
	eventType := ""
	switch strings.TrimSpace(status) {
	case baldastate.JobStatusQueued, baldastate.JobStatusWaitingForAgent, baldastate.JobStatusWaitingForUser:
		eventType = JobEventAssigned
	case baldastate.JobStatusRunning:
		eventType = JobEventStarted
	case baldastate.JobStatusValidating:
		eventType = JobEventValidating
	case baldastate.JobStatusCompleted:
		eventType = JobEventCompleted
	case baldastate.JobStatusFailed, baldastate.JobStatusDeadLettered:
		eventType = JobEventFailed
	case baldastate.JobStatusCanceled:
		eventType = JobEventCanceled
	}
	return s.appendEventBestEffort(ctx, taskID, eventType, actor, "", mergePayload(result, map[string]any{
		"status": status,
		"reason": reason,
	}))
}

func (s *JobService) suppressStaleTerminalTransition(ctx context.Context, taskID string, status string, err error) error {
	if err == nil {
		return nil
	}
	if !strings.Contains(err.Error(), "invalid runtime task transition") {
		return err
	}
	if !isTerminalTaskStatus(status) {
		return err
	}
	task, ok, getErr := s.Get(ctx, taskID)
	if getErr != nil || !ok {
		return err
	}
	if !isTerminalTaskStatus(task.Status) {
		return err
	}
	return nil
}

func isTerminalTaskStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case baldastate.JobStatusCompleted,
		baldastate.JobStatusFailed,
		baldastate.JobStatusCanceled,
		baldastate.JobStatusDeadLettered:
		return true
	default:
		return false
	}
}

func (s *JobService) AppendEvent(ctx context.Context, taskID string, eventType string, actor string, messageID string, payload any) error {
	if s == nil {
		return nil
	}
	event, err := jobEventRecord(taskID, eventType, actor, messageID, payload)
	if err != nil {
		return err
	}
	return s.publishEventRecord(ctx, event)
}

func (s *JobService) appendEventBestEffort(ctx context.Context, taskID string, eventType string, actor string, messageID string, payload any) error {
	if s == nil {
		return nil
	}
	event, err := jobEventRecord(taskID, eventType, actor, messageID, payload)
	if err != nil {
		return err
	}
	s.publishEventRecordBestEffort(ctx, event)
	return nil
}

func jobEventRecord(taskID string, eventType string, actor string, messageID string, payload any) (baldastate.JobEventRecord, error) {
	data, err := marshalPayload(payload)
	if err != nil {
		return baldastate.JobEventRecord{}, err
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
	return baldastate.JobEventRecord{
		ID:          eventID,
		JobID:       strings.TrimSpace(taskID),
		EventType:   strings.TrimSpace(eventType),
		Actor:       strings.TrimSpace(actor),
		MessageID:   strings.TrimSpace(messageID),
		PayloadJSON: data,
	}, nil
}

func (s *JobService) CancelBySession(ctx context.Context, sessionID string, actor string, reason string) ([]string, error) {
	if s == nil {
		return nil, nil
	}
	tasks, err := s.store.ListActiveJobsBySession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(tasks))
	for _, task := range tasks {
		if err := s.MarkStatus(ctx, task.ID, baldastate.JobStatusCanceled, actor, "", reason, nil); err != nil {
			return ids, err
		}
		ids = append(ids, task.ID)
	}
	return ids, nil
}

func (s *JobService) CancelJob(ctx context.Context, taskID string, actor string, reason string) error {
	if s == nil {
		return nil
	}
	return s.MarkStatus(ctx, taskID, baldastate.JobStatusCanceled, actor, "", reason, nil)
}

func (s *JobService) DeadLetter(ctx context.Context, taskID string, actor string, messageID string, reason string) error {
	return s.MarkStatus(ctx, taskID, baldastate.JobStatusDeadLettered, actor, messageID, reason, nil)
}

func (s *JobService) ReserveDelivery(ctx context.Context, record baldastate.DeliveryRecord) (baldastate.DeliveryRecord, bool, error) {
	if s == nil {
		return baldastate.DeliveryRecord{}, false, nil
	}
	return s.store.ReserveDelivery(ctx, record)
}

func (s *JobService) MarkDeliverySent(ctx context.Context, deliveryKey string, providerMessageID string) error {
	if s == nil {
		return nil
	}
	return s.store.MarkDeliverySent(ctx, deliveryKey, providerMessageID)
}

func (s *JobService) MarkDeliverySending(ctx context.Context, deliveryKey string) error {
	if s == nil {
		return nil
	}
	return s.store.MarkDeliverySending(ctx, deliveryKey)
}

func (s *JobService) MarkDeliveryFailed(ctx context.Context, deliveryKey string, reason string) error {
	if s == nil {
		return nil
	}
	return s.store.MarkDeliveryFailed(ctx, deliveryKey, reason)
}

func (s *JobService) ReserveAgentStep(ctx context.Context, record baldastate.AgentStepRecord) (baldastate.AgentStepRecord, bool, error) {
	if s == nil {
		return baldastate.AgentStepRecord{}, false, nil
	}
	return s.store.ReserveAgentStep(ctx, record)
}

func (s *JobService) CompleteAgentStep(ctx context.Context, stepKey string, resultJSON string) error {
	if s == nil {
		return nil
	}
	return s.store.CompleteAgentStep(ctx, stepKey, resultJSON)
}

func (s *JobService) FailAgentStep(ctx context.Context, stepKey string, resultJSON string, reason string) error {
	if s == nil {
		return nil
	}
	return s.store.FailAgentStep(ctx, stepKey, resultJSON, reason)
}

func IsGoalJob(task baldastate.JobRecord) bool {
	owner := strings.TrimSpace(task.OwnerActor)
	assigned := strings.TrimSpace(task.AssignedActor)
	for _, prefix := range []string{"goalkeeper:", "goal:"} {
		if strings.HasPrefix(owner, prefix) || strings.HasPrefix(assigned, prefix) {
			return true
		}
	}
	return false
}

func (s *JobService) publishTaskEvent(ctx context.Context, event baldastate.JobEventRecord) error {
	if s == nil || s.bus == nil {
		return fmt.Errorf("event bus is required")
	}
	payload := strings.TrimSpace(event.PayloadJSON)
	if payload == "" {
		payload = "{}"
	}
	subject := baldaexecution.SubjectEventJobUpdated
	switch strings.TrimSpace(event.EventType) {
	case JobEventDeliverySent:
		subject = baldaexecution.SubjectEventDeliverySent
	case JobEventDeliveryFailed:
		subject = baldaexecution.SubjectEventDeliveryFailed
	case JobEventCreated:
		subject = baldaexecution.SubjectEventJobCreated
	case JobEventCompleted:
		subject = baldaexecution.SubjectEventJobCompleted
	}
	env := actorlayer.Envelope{
		ID:          event.ID,
		Namespace:   baldaexecution.NamespaceTelemetry,
		Kind:        "job_event",
		From:        actorlayer.SystemAddress("job-events"),
		To:          actorlayer.ActorAddress{Target: baldaexecution.ActorTypeJob, Key: event.JobID},
		PayloadJSON: payload,
		Meta: baldaexecution.WithJobIDMeta(map[string]string{
			"event_type": event.EventType,
			"actor":      event.Actor,
			"message_id": event.MessageID,
		}, event.JobID),
	}
	return s.bus.PublishEvent(ctx, subject, env)
}

func (s *JobService) publishEventRecord(ctx context.Context, event baldastate.JobEventRecord) error {
	if s == nil {
		return nil
	}
	return s.publishTaskEvent(ctx, event)
}

func (s *JobService) publishEventRecordBestEffort(ctx context.Context, event baldastate.JobEventRecord) {
	if err := s.publishEventRecord(ctx, event); err != nil {
		log.Ctx(ctx).Warn().
			Err(err).
			Str("job_id", event.JobID).
			Str("event_type", event.EventType).
			Str("event_id", event.ID).
			Msg("failed to publish job event")
	}
}

func marshalPayload(payload any) (string, error) {
	if payload == nil {
		return "", nil
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode job payload: %w", err)
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
