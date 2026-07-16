package turncmd

import (
	"fmt"
	"strings"

	"github.com/baldaworks/go-actorlayer"
	"github.com/google/uuid"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/actorcmd"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
)

const (
	SourceTelegram = "telegram"
	SourceWebhook  = "webhook"
	SourceSchedule = "schedule"
	SourceAuto     = "auto"
)

type SessionTurnPayload struct {
	JobID           string                       `json:"job_id,omitempty"`
	Text            string                       `json:"text"`
	Locator         baldasession.SessionLocator  `json:"locator"`
	ReportTo        *baldasession.SessionLocator `json:"report_to,omitempty"`
	ParentJobID     string                       `json:"parent_job_id,omitempty"`
	UserID          string                       `json:"user_id,omitempty"`
	RequesterUserID string                       `json:"requester_user_id,omitempty"`
	AgentSessionID  string                       `json:"agent_session_id,omitempty"`
	ScheduledJobID  string                       `json:"scheduled_job_id,omitempty"`
	MessageID       int                          `json:"message_id,omitempty"`
	TopicID         int                          `json:"topic_id,omitempty"`
	DeliveryOptions deliveryfmt.Options          `json:"delivery_options,omitempty,omitzero"`
	ProgressPolicy  deliveryfmt.ProgressPolicy   `json:"progress_policy,omitempty"`
	Deliver         bool                         `json:"deliver"`
	Source          string                       `json:"source,omitempty"`
	DedupeKey       string                       `json:"dedupe_key,omitempty"`
	QuestionID      string                       `json:"question_id,omitempty"`
}

type jobEnvelopePayload struct {
	Kind         string               `json:"kind"`
	ScheduledJob *scheduledJobPayload `json:"scheduled_job,omitempty"`
	SessionTurn  *SessionTurnPayload  `json:"session_turn,omitempty"`
}

type scheduledJobPayload struct {
	JobID       string                       `json:"job_id"`
	Content     string                       `json:"content"`
	Locator     baldasession.SessionLocator  `json:"locator"`
	ReportTo    *baldasession.SessionLocator `json:"report_to,omitempty"`
	ParentJobID string                       `json:"parent_job_id,omitempty"`
	UserID      string                       `json:"user_id"`
	TopicID     int                          `json:"topic_id,omitempty"`
}

const (
	jobPayloadKindScheduledJob       = "scheduled_job"
	jobPayloadKindWebhookSessionTurn = "session_turn"
)

func SessionTurnEnvelope(payload SessionTurnPayload) (actorlayer.Envelope, error) {
	if strings.TrimSpace(payload.Locator.SessionID) == "" {
		return actorlayer.Envelope{}, fmt.Errorf("session id is required")
	}
	data, err := actorlayer.MarshalPayload(payload)
	if err != nil {
		return actorlayer.Envelope{}, fmt.Errorf("encode session turn payload: %w", err)
	}
	source := strings.TrimSpace(payload.Source)
	if source == "" {
		source = SourceTelegram
	}
	priority := 90
	namespace := baldaexecution.NamespaceHumanInbound
	kind := baldaexecution.KindMessage
	switch {
	case strings.EqualFold(source, SourceWebhook):
		priority = 80
		namespace = baldaexecution.NamespaceWebhookInbound
		kind = baldaexecution.KindWebhookEvent
	case strings.EqualFold(source, SourceSchedule):
		priority = 50
		namespace = baldaexecution.NamespaceScheduleInbound
	case strings.EqualFold(source, "agent"):
		namespace = baldaexecution.NamespaceGoalkeeperCommand
	}
	return actorlayer.Envelope{
		ID:        uuid.NewString(),
		Namespace: namespace,
		Kind:      kind,
		From:      actorlayer.ActorAddress{Target: source, Key: firstNonEmpty(payload.UserID, payload.Locator.AddressKey, "unknown")},
		To:        actorlayer.ActorAddress{Target: baldaexecution.ActorTypeSession, Key: payload.Locator.SessionID},
		Meta:      baldaexecution.WithSessionIDMeta(baldaexecution.WithJobIDMeta(nil, payload.JobID), payload.Locator.SessionID),
		Priority:  priority,
		DedupeKey: strings.TrimSpace(payload.DedupeKey),
		Payload:   data,
	}, nil
}

func WebhookJobEnvelope(payload SessionTurnPayload, routeName string, requestID string) (actorlayer.Envelope, string, error) {
	dedupeBase := strings.TrimSpace(payload.DedupeKey)
	dedupeBase = strings.TrimSuffix(dedupeBase, ":task")
	dedupeBase = strings.TrimSuffix(dedupeBase, ":session")
	if dedupeBase == "" {
		dedupeBase = strings.Join([]string{"webhook", strings.TrimSpace(routeName), strings.TrimSpace(requestID)}, ":")
	}
	trimmedRoute := strings.ToLower(strings.TrimSpace(routeName))
	var routePart strings.Builder
	lastDash := false
	for _, r := range trimmedRoute {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			routePart.WriteRune(r)
			lastDash = false
		case r == '-' || r == '_':
			routePart.WriteRune(r)
			lastDash = false
		default:
			if routePart.Len() > 0 && !lastDash {
				routePart.WriteByte('-')
				lastDash = true
			}
		}
		if routePart.Len() >= 48 {
			break
		}
	}
	part := strings.Trim(routePart.String(), "-_")
	if part == "" {
		part = "inbound"
	}
	jobID := "webhook-" + part + "-" + shortJobHash(dedupeBase)
	payload.JobID = jobID
	payload.DedupeKey = dedupeBase + ":session"
	data, err := actorlayer.MarshalPayload(jobEnvelopePayload{
		Kind:        jobPayloadKindWebhookSessionTurn,
		SessionTurn: &payload,
	})
	if err != nil {
		return actorlayer.Envelope{}, "", fmt.Errorf("encode webhook job payload: %w", err)
	}
	return actorlayer.Envelope{
		ID:        uuid.NewString(),
		Namespace: baldaexecution.NamespaceWebhookInbound,
		Kind:      baldaexecution.KindWebhookEvent,
		From:      actorlayer.ActorAddress{Target: "webhook", Key: firstNonEmpty(routeName, requestID, "inbound")},
		To:        actorlayer.ActorAddress{Target: baldaexecution.ActorTypeJob, Key: jobID},
		Meta:      baldaexecution.WithSessionIDMeta(baldaexecution.WithJobIDMeta(nil, jobID), payload.Locator.SessionID),
		Priority:  80,
		DedupeKey: dedupeBase + ":job",
		Payload:   data,
	}, jobID, nil
}

func ScheduledJobEnvelope(
	scheduledJobID string,
	content string,
	locator baldasession.SessionLocator,
	reportTo *baldasession.SessionLocator,
	userID string,
	topicID int,
	dispatchKey string,
) (actorlayer.Envelope, error) {
	payload := jobEnvelopePayload{
		Kind: jobPayloadKindScheduledJob,
		ScheduledJob: &scheduledJobPayload{
			JobID:    strings.TrimSpace(scheduledJobID),
			Content:  content,
			Locator:  locator,
			ReportTo: reportTo,
			UserID:   strings.TrimSpace(userID),
			TopicID:  topicID,
		},
	}
	data, err := actorlayer.MarshalPayload(payload)
	if err != nil {
		return actorlayer.Envelope{}, fmt.Errorf("encode scheduled job payload: %w", err)
	}
	jobID := "scheduled-" + strings.TrimSpace(scheduledJobID) + "-" + strings.TrimSpace(dispatchKey)
	return actorlayer.Envelope{
		ID:        uuid.NewString(),
		Namespace: baldaexecution.NamespaceScheduleInbound,
		Kind:      baldaexecution.KindScheduledJob,
		From:      actorlayer.ActorAddress{Target: "schedule", Key: strings.TrimSpace(scheduledJobID)},
		To:        actorlayer.ActorAddress{Target: baldaexecution.ActorTypeJob, Key: jobID},
		Meta:      baldaexecution.WithSessionIDMeta(baldaexecution.WithJobIDMeta(nil, jobID), locator.SessionID),
		DedupeKey: strings.TrimSpace(dispatchKey),
		Payload:   data,
	}, nil
}

func NormalizeSessionDeliveryOptions(payload SessionTurnPayload) deliveryfmt.Options {
	options := deliveryfmt.NormalizeOptions(payload.DeliveryOptions)
	if !options.ProgressPolicy.Typing && !options.ProgressPolicy.Thinking {
		options.ProgressPolicy = payload.ProgressPolicy
	}
	return deliveryfmt.NormalizeOptions(options)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
