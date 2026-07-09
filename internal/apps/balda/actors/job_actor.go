package actors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	baldajobs "github.com/normahq/balda/internal/apps/balda/jobs"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/pkg/actorlayer"
	actortransport "github.com/normahq/balda/pkg/actorlayer/transport"
	"go.uber.org/fx"
)

const (
	jobPayloadKindScheduledJob       = "scheduled_job"
	jobPayloadKindWebhookSessionTurn = "session_turn"
	jobPayloadKindDelivery           = "delivery"
)

type jobEnvelopePayload struct {
	Kind         string               `json:"kind"`
	ScheduledJob *scheduledJobPayload `json:"scheduled_job,omitempty"`
	// Retain the legacy wire field for durable webhook envelopes already in flight.
	SessionTurn *SessionTurnPayload `json:"session_turn,omitempty"`
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

type DeliveryPayload = deliverycmd.Payload

type DeliveryMode = deliverycmd.Mode
type DeliveryProgress = deliverycmd.Progress
type DeliveryProgressKind = deliverycmd.ProgressKind

const (
	DeliveryModeAgentReply DeliveryMode = deliverycmd.ModeAgentReply
	DeliveryModePlain      DeliveryMode = deliverycmd.ModePlain
	DeliveryModeMarkdown   DeliveryMode = deliverycmd.ModeMarkdown
	DeliveryModeDraftPlain DeliveryMode = deliverycmd.ModeDraftPlain
	DeliveryModeChatAction DeliveryMode = deliverycmd.ModeChatAction
	DeliveryModeProgress   DeliveryMode = deliverycmd.ModeProgress
)

const (
	DeliveryProgressThinking   DeliveryProgressKind = deliverycmd.ProgressThinking
	DeliveryProgressPlanUpdate DeliveryProgressKind = deliverycmd.ProgressPlanUpdate
)

type jobActorExecutor struct {
	tasks      *baldajobs.JobService
	dispatcher actortransport.Dispatcher
	sessions   *baldasession.Manager
}

type jobActorExecutorParams struct {
	fx.In

	JobService *baldajobs.JobService
	Dispatcher actortransport.Dispatcher
	Sessions   *baldasession.Manager `optional:"true"`
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
	data, err := json.Marshal(jobEnvelopePayload{
		Kind:        jobPayloadKindWebhookSessionTurn,
		SessionTurn: &payload,
	})
	if err != nil {
		return actorlayer.Envelope{}, "", fmt.Errorf("encode webhook job payload: %w", err)
	}
	return actorlayer.Envelope{
		ID:          uuid.NewString(),
		Namespace:   baldaexecution.NamespaceWebhookInbound,
		Kind:        baldaexecution.KindWebhookEvent,
		From:        actorlayer.ActorAddress{Target: "webhook", Key: firstNonEmpty(routeName, requestID, "inbound")},
		To:          actorlayer.ActorAddress{Target: baldaexecution.ActorTypeJob, Key: jobID},
		SessionID:   payload.Locator.SessionID,
		Meta:        baldaexecution.WithJobIDMeta(nil, jobID),
		Priority:    80,
		DedupeKey:   dedupeBase + ":job",
		PayloadJSON: string(data),
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
	data, err := json.Marshal(payload)
	if err != nil {
		return actorlayer.Envelope{}, fmt.Errorf("encode scheduled job payload: %w", err)
	}
	jobID := "scheduled-" + strings.TrimSpace(scheduledJobID) + "-" + strings.TrimSpace(dispatchKey)
	return actorlayer.Envelope{
		ID:          uuid.NewString(),
		Namespace:   baldaexecution.NamespaceScheduleInbound,
		Kind:        baldaexecution.KindScheduledJob,
		From:        actorlayer.ActorAddress{Target: "schedule", Key: strings.TrimSpace(scheduledJobID)},
		To:          actorlayer.ActorAddress{Target: baldaexecution.ActorTypeJob, Key: jobID},
		SessionID:   locator.SessionID,
		Meta:        baldaexecution.WithJobIDMeta(nil, jobID),
		DedupeKey:   strings.TrimSpace(dispatchKey),
		PayloadJSON: string(data),
	}, nil
}

func shortJobHash(value string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(value)))
	return hex.EncodeToString(sum[:])[:16]
}

func (e *jobActorExecutor) Address() string {
	return actorlayer.WildcardAddress(baldaexecution.ActorTypeJob)
}

func (e *jobActorExecutor) Handle(ctx context.Context, env actorlayer.Envelope) error {
	var payload jobEnvelopePayload
	if err := json.Unmarshal([]byte(env.PayloadJSON), &payload); err != nil {
		return actorlayer.PermanentError(fmt.Errorf("decode job payload: %w", err))
	}
	switch strings.TrimSpace(payload.Kind) {
	case "goal":
		return actorlayer.PolicyError(fmt.Errorf("goal jobs are handled by goal actor"))
	case jobPayloadKindScheduledJob:
		if payload.ScheduledJob == nil {
			return actorlayer.PolicyError(fmt.Errorf("scheduled job payload is required"))
		}
		return e.startScheduledJob(ctx, env, *payload.ScheduledJob)
	case jobPayloadKindWebhookSessionTurn:
		if payload.SessionTurn == nil {
			return actorlayer.PolicyError(fmt.Errorf("session turn job payload is required"))
		}
		if !strings.EqualFold(env.Namespace, baldaexecution.NamespaceWebhookInbound) {
			return actorlayer.PolicyError(fmt.Errorf("session turn jobs are reserved for durable webhook delivery"))
		}
		return e.dispatchWebhookSessionTurn(ctx, env, *payload.SessionTurn)
	default:
		return actorlayer.PolicyError(fmt.Errorf("unsupported job payload kind %q", payload.Kind))
	}
}

func (e *jobActorExecutor) dispatchWebhookSessionTurn(ctx context.Context, env actorlayer.Envelope, payload SessionTurnPayload) error {
	jobID := strings.TrimSpace(baldaexecution.EnvelopeJobID(env))
	if jobID == "" {
		return actorlayer.PolicyError(fmt.Errorf("job id is required"))
	}
	if payloadJobID := strings.TrimSpace(payload.JobID); payloadJobID != "" && payloadJobID != jobID {
		return actorlayer.PolicyError(fmt.Errorf("webhook session job id mismatch: envelope=%q payload=%q", jobID, payloadJobID))
	}
	if e.tasks != nil {
		if _, ok, err := e.tasks.Get(ctx, jobID); err != nil {
			return actorlayer.TransientError(err)
		} else if ok {
			return nil
		}
		created, err := e.tasks.Create(ctx, baldastate.JobRecord{
			ID:            jobID,
			SessionID:     strings.TrimSpace(payload.Locator.SessionID),
			ParentJobID:   strings.TrimSpace(payload.ParentJobID),
			Title:         webhookJobTitle(),
			Objective:     strings.TrimSpace(payload.Text),
			Status:        baldastate.JobStatusCreated,
			OwnerActor:    baldaexecution.ActorTypeJob + ":" + jobID,
			AssignedActor: baldaexecution.ActorTypeSession + ":" + payload.Locator.SessionID,
			Priority:      webhookJobPriority(),
			CreatedBy:     strings.TrimSpace(payload.UserID),
		}, "job.actor", payload)
		if err != nil {
			return actorlayer.TransientError(err)
		}
		if !created {
			return nil
		}
	}
	payload.JobID = jobID
	sessionEnv, err := SessionTurnEnvelope(payload)
	if err != nil {
		return actorlayer.PermanentError(err)
	}
	sessionEnv.CorrelationID = firstNonEmpty(env.CorrelationID, jobID)
	sessionEnv.CausationID = env.ID
	if strings.TrimSpace(sessionEnv.DedupeKey) != "" {
		sessionEnv.ID = sessionEnv.DedupeKey
	}
	if _, err := e.dispatcher.Dispatch(ctx, sessionEnv); err != nil {
		return actorlayer.TransientError(err)
	}
	if jobID != "" && e.tasks != nil {
		if err := e.tasks.MarkStatus(ctx, jobID, baldastate.JobStatusRunning, "job.actor", env.ID, "", nil); err != nil {
			return actorlayer.TransientError(err)
		}
	}
	return nil
}

func webhookJobTitle() string {
	return "Webhook job"
}

func webhookJobPriority() int {
	return 80
}

func (e *jobActorExecutor) startScheduledJob(ctx context.Context, env actorlayer.Envelope, payload scheduledJobPayload) error {
	jobID := strings.TrimSpace(baldaexecution.EnvelopeJobID(env))
	content := strings.TrimSpace(payload.Content)
	if jobID == "" {
		return actorlayer.PolicyError(fmt.Errorf("job id is required"))
	}
	if strings.TrimSpace(payload.JobID) == "" {
		return actorlayer.PolicyError(fmt.Errorf("scheduled job id is required"))
	}
	if content == "" {
		return actorlayer.PolicyError(fmt.Errorf("scheduled job content is required"))
	}
	if e.tasks != nil {
		if _, ok, err := e.tasks.Get(ctx, jobID); err != nil {
			return actorlayer.TransientError(err)
		} else if ok {
			return nil
		}
		created, err := e.tasks.Create(ctx, baldastate.JobRecord{
			ID:            jobID,
			SessionID:     strings.TrimSpace(payload.Locator.SessionID),
			ParentJobID:   strings.TrimSpace(payload.ParentJobID),
			Title:         "Scheduled job: " + strings.TrimSpace(payload.JobID),
			Objective:     content,
			Status:        baldastate.JobStatusCreated,
			OwnerActor:    baldaexecution.ActorTypeJob + ":" + jobID,
			AssignedActor: baldaexecution.ActorTypeSession + ":" + payload.Locator.SessionID,
			Priority:      50,
			CreatedBy:     strings.TrimSpace(payload.UserID),
		}, "job.actor", payload)
		if err != nil {
			return actorlayer.TransientError(err)
		}
		if !created {
			return nil
		}
	}
	sessionPayload := SessionTurnPayload{
		JobID:          jobID,
		Text:           content,
		Locator:        payload.Locator,
		ReportTo:       payload.ReportTo,
		ParentJobID:    strings.TrimSpace(payload.ParentJobID),
		UserID:         payload.UserID,
		ScheduledJobID: payload.JobID,
		TopicID:        payload.TopicID,
		DeliveryOptions: deliveryfmt.Options{
			Profile: deliveryfmt.Profile{Format: deliveryfmt.FormatAuto},
		},
		Deliver:   payload.ReportTo != nil,
		Source:    sessionTurnSourceSchedule,
		DedupeKey: firstNonEmpty(env.DedupeKey, jobID) + ":session",
	}
	sessionEnv, err := SessionTurnEnvelope(sessionPayload)
	if err != nil {
		return actorlayer.PermanentError(err)
	}
	sessionEnv.CorrelationID = firstNonEmpty(env.CorrelationID, jobID)
	sessionEnv.CausationID = env.ID
	if strings.TrimSpace(sessionEnv.DedupeKey) != "" {
		sessionEnv.ID = sessionEnv.DedupeKey
	}
	if _, err := e.dispatcher.Dispatch(ctx, sessionEnv); err != nil {
		return actorlayer.TransientError(err)
	}
	if e.tasks != nil {
		if err := e.tasks.MarkStatus(ctx, jobID, baldastate.JobStatusRunning, "job.actor", env.ID, "", nil); err != nil {
			return actorlayer.TransientError(err)
		}
	}
	return nil
}
