package jobs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/baldaworks/go-actorlayer"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	"github.com/google/uuid"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/actorcmd"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/rs/zerolog/log"
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
)

// ServiceStore is the job state needed by concrete job services.
type ServiceStore interface {
	baldastate.JobLifecycleStore
	baldastate.JobEventOutboxStore
	baldastate.DeliveryStore
	baldastate.AgentStepStore
}

func jobStatusEventType(status string) string {
	switch strings.TrimSpace(status) {
	case baldastate.JobStatusCreated:
		return JobEventCreated
	case baldastate.JobStatusQueued, baldastate.JobStatusWaitingForAgent, baldastate.JobStatusWaitingForUser:
		return JobEventAssigned
	case baldastate.JobStatusRunning:
		return JobEventStarted
	case baldastate.JobStatusValidating:
		return JobEventValidating
	case baldastate.JobStatusCompleted:
		return JobEventCompleted
	case baldastate.JobStatusFailed, baldastate.JobStatusDeadLettered:
		return JobEventFailed
	case baldastate.JobStatusCanceled:
		return JobEventCanceled
	default:
		return ""
	}
}

func isTerminalJobStatus(status string) bool {
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

func IsGoalJob(job baldastate.JobRecord) bool {
	owner := strings.TrimSpace(job.OwnerActor)
	assigned := strings.TrimSpace(job.AssignedActor)
	for _, prefix := range []string{"goalkeeper:", "goal:"} {
		if strings.HasPrefix(owner, prefix) || strings.HasPrefix(assigned, prefix) {
			return true
		}
	}
	return false
}

func jobEventRecord(jobID string, eventType string, actor string, messageID string, payload any) (baldastate.JobEventRecord, error) {
	data, err := marshalPayload(payload)
	if err != nil {
		return baldastate.JobEventRecord{}, err
	}
	eventID := ""
	if strings.TrimSpace(eventType) == JobEventAgentProgress {
		eventID = uuid.NewString()
	} else {
		parts := []string{
			strings.TrimSpace(jobID),
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
		eventID = "job:" + strings.TrimSpace(jobID) + ":event:" + eventTypePart + ":" + hex.EncodeToString(sum[:])[:16]
	}
	return baldastate.JobEventRecord{
		ID:        eventID,
		JobID:     strings.TrimSpace(jobID),
		EventType: strings.TrimSpace(eventType),
		Actor:     strings.TrimSpace(actor),
		MessageID: strings.TrimSpace(messageID),
		Payload:   data,
	}, nil
}

func jobEventEnvelope(event baldastate.JobEventRecord) (string, actorlayer.Envelope) {
	payload := strings.TrimSpace(event.Payload)
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
	return subject, actorlayer.Envelope{
		ID:        event.ID,
		Namespace: baldaexecution.NamespaceTelemetry,
		Kind:      "job_event",
		From:      actorlayer.SystemAddress("job-events"),
		To:        actorlayer.ActorAddress{Target: baldaexecution.ActorTypeJob, Key: event.JobID},
		Payload: actorlayer.Payload{
			Encoding: actorlayer.EncodingJSON,
			Data:     []byte(payload),
		},
		Meta: baldaexecution.WithJobIDMeta(map[string]string{
			"event_type": event.EventType,
			"actor":      event.Actor,
			"message_id": event.MessageID,
		}, event.JobID),
	}
}

func jobEventOutboxRecord(event baldastate.JobEventRecord) (baldastate.JobEventOutboxRecord, error) {
	subject, env := jobEventEnvelope(event)
	data, err := actorlayer.EncodeEnvelope(env)
	if err != nil {
		return baldastate.JobEventOutboxRecord{}, fmt.Errorf("encode job event envelope: %w", err)
	}
	return baldastate.JobEventOutboxRecord{
		ID:       strings.TrimSpace(event.ID),
		JobID:    strings.TrimSpace(event.JobID),
		Subject:  subject,
		Envelope: data,
	}, nil
}

func publishOutboxRecord(
	ctx context.Context,
	store eventOutboxStore,
	bus actortransport.EventPublisher,
	record baldastate.JobEventOutboxRecord,
) error {
	if store == nil {
		return fmt.Errorf("job event outbox store is required")
	}
	if bus == nil {
		err := fmt.Errorf("event bus is required")
		return errors.Join(err, store.MarkJobEventPublishFailed(ctx, record.ID, err.Error()))
	}
	var env actorlayer.Envelope
	decoded, err := actorlayer.DecodeEnvelope(record.Envelope)
	if err != nil {
		decodeErr := fmt.Errorf("decode job event outbox %q: %w", record.ID, err)
		return errors.Join(decodeErr, store.MarkJobEventPublishFailed(ctx, record.ID, decodeErr.Error()))
	}
	env = decoded
	if err := bus.PublishEvent(ctx, record.Subject, env); err != nil {
		return errors.Join(err, store.MarkJobEventPublishFailed(ctx, record.ID, err.Error()))
	}
	return store.MarkJobEventPublished(ctx, record.ID)
}

func publishOutboxBestEffort(
	ctx context.Context,
	store eventOutboxStore,
	bus actortransport.EventPublisher,
	record baldastate.JobEventOutboxRecord,
) {
	if store == nil {
		return
	}
	if err := publishOutboxRecord(ctx, store, bus, record); err != nil {
		log.Ctx(ctx).Warn().
			Err(err).
			Str("job_id", record.JobID).
			Str("event_id", record.ID).
			Msg("job event remains pending in outbox")
	}
}

func marshalPayload(payload any) (string, error) {
	if payload == nil {
		return "", nil
	}
	data, err := actorlayer.MarshalPayload(payload)
	if err != nil {
		return "", fmt.Errorf("encode job payload: %w", err)
	}
	return data.String(), nil
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
