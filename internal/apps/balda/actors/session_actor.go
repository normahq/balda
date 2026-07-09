package actors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	baldachannel "github.com/normahq/balda/internal/apps/balda/channel"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	baldajobs "github.com/normahq/balda/internal/apps/balda/jobs"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/pkg/actorlayer"
	"go.uber.org/fx"
)

const (
	sessionTurnSourceTelegram = "telegram"
	sessionTurnSourceWebhook  = "webhook"
	sessionTurnSourceSchedule = "schedule"
)

type SessionTurnPayload struct {
	JobID           string                       `json:"job_id,omitempty"`
	Text            string                       `json:"text"`
	Locator         baldasession.SessionLocator  `json:"locator"`
	ReportTo        *baldasession.SessionLocator `json:"report_to,omitempty"`
	ParentJobID     string                       `json:"parent_job_id,omitempty"`
	UserID          string                       `json:"user_id,omitempty"`
	AgentSessionID  string                       `json:"agent_session_id,omitempty"`
	ScheduledJobID  string                       `json:"scheduled_job_id,omitempty"`
	MessageID       int                          `json:"message_id,omitempty"`
	TopicID         int                          `json:"topic_id,omitempty"`
	DeliveryOptions deliveryfmt.Options          `json:"delivery_options,omitempty,omitzero"`
	ProgressPolicy  baldachannel.ProgressPolicy  `json:"progress_policy,omitempty"`
	Deliver         bool                         `json:"deliver"`
	Source          string                       `json:"source,omitempty"`
	DedupeKey       string                       `json:"dedupe_key,omitempty"`
}

type SessionTurnRunner interface {
	RunSessionTurnPayload(ctx context.Context, payload SessionTurnPayload) error
}

type ScheduledTaskRecorder interface {
	MarkSuccess(ctx context.Context, taskID string) error
	RecordExecutionFailure(ctx context.Context, taskID string, cause error) error
}

func SessionTurnEnvelope(payload SessionTurnPayload) (actorlayer.Envelope, error) {
	if strings.TrimSpace(payload.Locator.SessionID) == "" {
		return actorlayer.Envelope{}, fmt.Errorf("session id is required")
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return actorlayer.Envelope{}, fmt.Errorf("encode session turn payload: %w", err)
	}
	source := strings.TrimSpace(payload.Source)
	if source == "" {
		source = sessionTurnSourceTelegram
	}
	priority := 90
	namespace := baldaexecution.NamespaceHumanInbound
	kind := baldaexecution.KindMessage
	switch {
	case strings.EqualFold(source, sessionTurnSourceWebhook):
		priority = 80
		namespace = baldaexecution.NamespaceWebhookInbound
		kind = baldaexecution.KindWebhookEvent
	case strings.EqualFold(source, sessionTurnSourceSchedule):
		priority = 50
		namespace = baldaexecution.NamespaceScheduleInbound
	case strings.EqualFold(source, "agent"):
		namespace = baldaexecution.NamespaceGoalkeeperCommand
	}
	return actorlayer.Envelope{
		ID:          uuid.NewString(),
		Namespace:   namespace,
		Kind:        kind,
		From:        actorlayer.ActorAddress{Target: source, Key: firstNonEmpty(payload.UserID, payload.Locator.AddressKey, "unknown")},
		To:          actorlayer.ActorAddress{Target: baldaexecution.ActorTypeSession, Key: payload.Locator.SessionID},
		SessionID:   payload.Locator.SessionID,
		Meta:        baldaexecution.WithJobIDMeta(nil, payload.JobID),
		Priority:    priority,
		DedupeKey:   strings.TrimSpace(payload.DedupeKey),
		PayloadJSON: string(data),
	}, nil
}

type sessionActorExecutor struct {
	turns     TurnQueue
	runner    SessionTurnRunner
	tasks     *baldajobs.JobService
	scheduler ScheduledTaskRecorder
}

type sessionActorExecutorParams struct {
	fx.In

	Turns     *TurnDispatcher
	Runner    SessionTurnRunner
	Tasks     *baldajobs.JobService `optional:"true"`
	Scheduler ScheduledTaskRecorder `optional:"true"`
}

func (e *sessionActorExecutor) Address() string {
	return actorlayer.WildcardAddress(baldaexecution.ActorTypeSession)
}

func (e *sessionActorExecutor) Handle(ctx context.Context, env actorlayer.Envelope) error {
	switch strings.TrimSpace(env.Namespace) {
	case baldaexecution.NamespaceHumanInbound, baldaexecution.NamespaceWebhookInbound, baldaexecution.NamespaceScheduleInbound, baldaexecution.NamespaceGoalkeeperCommand, baldaexecution.NamespaceJobControl:
		return e.enqueueTurn(ctx, env)
	default:
		return actorlayer.PolicyError(fmt.Errorf("unsupported session namespace %q", env.Namespace))
	}
}

func (e *sessionActorExecutor) enqueueTurn(ctx context.Context, env actorlayer.Envelope) error {
	var payload SessionTurnPayload
	if err := json.Unmarshal([]byte(env.PayloadJSON), &payload); err != nil {
		return actorlayer.PermanentError(fmt.Errorf("decode session turn payload: %w", err))
	}
	if strings.TrimSpace(payload.Locator.SessionID) == "" {
		payload.Locator.SessionID = strings.TrimSpace(env.To.Key)
	}
	envelopeJobID := strings.TrimSpace(baldaexecution.EnvelopeJobID(env))
	payloadJobID := strings.TrimSpace(payload.JobID)
	switch {
	case envelopeJobID != "" && payloadJobID == "":
		return actorlayer.PolicyError(fmt.Errorf("session payload job id is required when envelope job scope is set"))
	case envelopeJobID != "" && payloadJobID != envelopeJobID:
		return actorlayer.PolicyError(fmt.Errorf("session job id mismatch: envelope=%q payload=%q", envelopeJobID, payloadJobID))
	}
	if e.sessionTaskAlreadyDone(ctx, env, payload) {
		return nil
	}
	if e.turns == nil {
		return actorlayer.TransientError(fmt.Errorf("turn dispatcher is required"))
	}
	if env.Meta != nil && strings.TrimSpace(env.Meta["queue_mode"]) == baldaexecution.QueueModeInterrupt {
		_, _, err := e.turns.CancelSession(payload.Locator, true)
		if err != nil {
			return actorlayer.TransientError(fmt.Errorf("interrupt session turn: %w", err))
		}
	}
	if e.runner == nil {
		return actorlayer.TransientError(fmt.Errorf("session turn runner is required"))
	}

	done := make(chan error, 1)
	_, err := e.turns.Enqueue(TurnTask{
		SessionID: payload.Locator.SessionID,
		Context:   ctx,
		Run: func(runCtx context.Context) error {
			err := e.runner.RunSessionTurnPayload(runCtx, payload)
			done <- err
			return err
		},
	})
	if err != nil {
		if errors.Is(err, ErrTurnQueueFull) {
			return actorlayer.TransientError(err)
		}
		return actorlayer.TransientError(fmt.Errorf("enqueue session actor turn: %w", err))
	}

	select {
	case err := <-done:
		return e.settleSessionTurnResult(ctx, env, payload, err)
	case <-ctx.Done():
		return actorlayer.TransientError(ctx.Err())
	}
}

func (e *sessionActorExecutor) settleSessionTurnResult(ctx context.Context, env actorlayer.Envelope, payload SessionTurnPayload, runErr error) error {
	if recordErr := e.recordSessionTaskResult(ctx, env, payload, runErr); recordErr != nil {
		return actorlayer.TransientError(recordErr)
	}
	if errors.Is(runErr, context.Canceled) {
		return nil
	}
	if runErr == nil {
		return nil
	}
	// Contract: once task terminal failure is durably recorded, settle command without retry.
	if sessionTurnUsesJobLifecycle(env, payload) {
		return nil
	}
	return runErr
}

func (e *sessionActorExecutor) sessionTaskAlreadyDone(ctx context.Context, env actorlayer.Envelope, payload SessionTurnPayload) bool {
	if e == nil || e.tasks == nil || !sessionTurnUsesJobLifecycle(env, payload) {
		return false
	}
	task, ok, err := e.tasks.Get(ctx, strings.TrimSpace(payload.JobID))
	if err != nil || !ok {
		return false
	}
	return isTerminalTaskStatus(task.Status)
}

func (e *sessionActorExecutor) recordSessionTaskResult(ctx context.Context, env actorlayer.Envelope, payload SessionTurnPayload, runErr error) error {
	if e == nil {
		return nil
	}
	if e.scheduler != nil && strings.TrimSpace(payload.ScheduledJobID) != "" {
		if runErr == nil {
			if err := e.scheduler.MarkSuccess(ctx, payload.ScheduledJobID); err != nil {
				return fmt.Errorf("mark scheduled task %q success: %w", payload.ScheduledJobID, err)
			}
		} else {
			if err := e.scheduler.RecordExecutionFailure(ctx, payload.ScheduledJobID, runErr); err != nil {
				return fmt.Errorf("record scheduled task %q failure: %w", payload.ScheduledJobID, err)
			}
		}
	}
	if e.tasks == nil || !sessionTurnUsesJobLifecycle(env, payload) {
		return nil
	}
	if errors.Is(runErr, context.Canceled) {
		if err := e.tasks.MarkStatus(ctx, payload.JobID, baldastate.JobStatusCanceled, "session.actor", env.ID, runErr.Error(), map[string]any{
			"namespace": env.Namespace,
			"kind":      env.Kind,
		}); err != nil {
			return fmt.Errorf("mark session job %q canceled: %w", payload.JobID, err)
		}
		return nil
	}
	if runErr == nil {
		if err := e.tasks.MarkStatus(ctx, payload.JobID, baldastate.JobStatusCompleted, "session.actor", env.ID, "", map[string]any{
			"namespace": env.Namespace,
			"kind":      env.Kind,
		}); err != nil {
			return fmt.Errorf("mark session job %q completed: %w", payload.JobID, err)
		}
		return nil
	}
	if err := e.tasks.MarkStatus(ctx, payload.JobID, baldastate.JobStatusFailed, "session.actor", env.ID, runErr.Error(), map[string]any{
		"namespace": env.Namespace,
		"kind":      env.Kind,
	}); err != nil {
		return fmt.Errorf("mark session job %q failed: %w", payload.JobID, err)
	}
	return nil
}

func sessionTurnUsesJobLifecycle(env actorlayer.Envelope, payload SessionTurnPayload) bool {
	if strings.TrimSpace(payload.JobID) == "" {
		return false
	}
	if strings.TrimSpace(payload.ScheduledJobID) != "" {
		return true
	}
	switch {
	case strings.EqualFold(env.Namespace, baldaexecution.NamespaceWebhookInbound):
		return true
	case strings.EqualFold(env.Namespace, baldaexecution.NamespaceScheduleInbound):
		return true
	case strings.EqualFold(payload.Source, sessionTurnSourceWebhook):
		return true
	case strings.EqualFold(payload.Source, sessionTurnSourceSchedule):
		return true
	default:
		return false
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func NormalizeSessionDeliveryOptions(payload SessionTurnPayload) deliveryfmt.Options {
	options := deliveryfmt.NormalizeOptions(payload.DeliveryOptions)
	if !options.ProgressPolicy.Typing && !options.ProgressPolicy.Thinking {
		options.ProgressPolicy = payload.ProgressPolicy
	}
	return deliveryfmt.NormalizeOptions(options)
}
