package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	baldachannel "github.com/normahq/balda/internal/apps/balda/channel"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"go.uber.org/fx"
)

const (
	sessionTurnSourceTelegram = "telegram"
	sessionTurnSourceWebhook  = "webhook"
	sessionTurnSourceSchedule = "schedule"
)

type sessionTurnPayload struct {
	Text            string                       `json:"text"`
	Locator         baldasession.SessionLocator  `json:"locator"`
	ReportTo        *baldasession.SessionLocator `json:"report_to,omitempty"`
	UserID          string                       `json:"user_id,omitempty"`
	AgentSessionID  string                       `json:"agent_session_id,omitempty"`
	ScheduledTaskID string                       `json:"scheduled_task_id,omitempty"`
	MessageID       int                          `json:"message_id,omitempty"`
	TopicID         int                          `json:"topic_id,omitempty"`
	ProgressPolicy  baldachannel.ProgressPolicy  `json:"progress_policy,omitempty"`
	Deliver         bool                         `json:"deliver"`
	Source          string                       `json:"source,omitempty"`
	DedupeKey       string                       `json:"dedupe_key,omitempty"`
}

func (h *BaldaHandler) submitSessionTurn(ctx context.Context, payload sessionTurnPayload) (*swarm.CommandPublishResult, error) {
	return h.submitSessionTurnToSwarm(ctx, payload)
}

func (h *BaldaHandler) submitSessionTurnToSwarm(ctx context.Context, payload sessionTurnPayload) (*swarm.CommandPublishResult, error) {
	if h.swarmCoordinator == nil || !h.swarmCoordinator.RuntimeEnabled() {
		return nil, fmt.Errorf("jetstream swarm runtime is unavailable")
	}
	env, err := sessionTurnEnvelope(payload)
	if err != nil {
		return nil, err
	}
	return h.swarmCoordinator.Submit(ctx, env)
}

func sessionTurnEnvelope(payload sessionTurnPayload) (swarm.Envelope, error) {
	if strings.TrimSpace(payload.Locator.SessionID) == "" {
		return swarm.Envelope{}, fmt.Errorf("session id is required")
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return swarm.Envelope{}, fmt.Errorf("encode session turn payload: %w", err)
	}
	source := strings.TrimSpace(payload.Source)
	if source == "" {
		source = sessionTurnSourceTelegram
	}
	priority := 90
	if strings.EqualFold(source, sessionTurnSourceWebhook) {
		priority = 80
	} else if strings.EqualFold(source, sessionTurnSourceSchedule) {
		priority = 50
	}
	return swarm.Envelope{
		ID:          uuid.NewString(),
		Namespace:   sessionTurnNamespace(source),
		Kind:        sessionTurnKind(source),
		From:        swarm.ActorAddress{Target: source, Key: firstNonEmpty(payload.UserID, payload.Locator.AddressKey, "unknown")},
		To:          swarm.ActorAddress{Target: swarm.ActorTypeSession, Key: payload.Locator.SessionID},
		SessionID:   payload.Locator.SessionID,
		Priority:    priority,
		DedupeKey:   strings.TrimSpace(payload.DedupeKey),
		PayloadJSON: string(data),
	}, nil
}

func (h *BaldaHandler) runSessionTurnPayload(ctx context.Context, payload sessionTurnPayload) error {
	ts, err := h.sessionManager.GetSession(payload.Locator)
	if err != nil {
		userID := strings.TrimSpace(payload.UserID)
		if userID == "" {
			h.logger.Debug().
				Str("session_id", payload.Locator.SessionID).
				Str("address_key", payload.Locator.AddressKey).
				Msg("dropping queued turn for inactive session without restore user")
			return nil
		}
		ts, err = h.sessionManager.RestoreSession(ctx, baldasession.SessionContext{
			Locator: payload.Locator,
			UserID:  userID,
		})
		if err != nil {
			if !errors.Is(err, baldasession.ErrNoPersistedSession) {
				return fmt.Errorf("restore session for queued turn: %w", err)
			}
			ts, err = h.sessionManager.EnsureSession(ctx, baldasession.SessionContext{
				Locator: payload.Locator,
				UserID:  userID,
			}, ownerSessionLabel)
			if err != nil {
				return fmt.Errorf("create session for queued turn: %w", err)
			}
		}
	}
	userID := strings.TrimSpace(payload.UserID)
	if userID == "" {
		userID = ts.GetUserID()
	}
	agentSessionID := strings.TrimSpace(payload.AgentSessionID)
	if agentSessionID == "" {
		agentSessionID = ts.GetAgentSessionID()
	}
	deliveryLocator := payload.Locator
	if payload.ReportTo != nil {
		deliveryLocator = *payload.ReportTo
	}
	return h.runTurnTaskWithDelivery(
		ctx,
		payload.Text,
		ts.GetRunner(),
		userID,
		ts.GetSessionID(),
		agentSessionID,
		deliveryLocator,
		payload.MessageID,
		payload.TopicID,
		payload.ProgressPolicy,
		payload.Deliver,
	)
}

type sessionActorExecutor struct {
	handler   *BaldaHandler
	tasks     *swarm.TaskService
	scheduler *ScheduledTaskScheduler
}

type sessionActorExecutorParams struct {
	fx.In

	Handler   *BaldaHandler
	Tasks     *swarm.TaskService      `optional:"true"`
	Scheduler *ScheduledTaskScheduler `optional:"true"`
}

func newSessionActorExecutor(params sessionActorExecutorParams) swarm.Actor {
	return &sessionActorExecutor{handler: params.Handler, tasks: params.Tasks, scheduler: params.Scheduler}
}

func (e *sessionActorExecutor) Address() string {
	return swarm.WildcardAddress(swarm.ActorTypeSession)
}

func (e *sessionActorExecutor) Handle(ctx context.Context, env swarm.Envelope) error {
	switch strings.TrimSpace(env.Namespace) {
	case swarm.NamespaceHumanInbound, swarm.NamespaceWebhookInbound, swarm.NamespaceScheduleInbound, swarm.NamespaceAgentCommand, swarm.NamespaceTaskControl:
		return e.enqueueTurn(ctx, env)
	default:
		return swarm.PolicyError(fmt.Errorf("unsupported session namespace %q", env.Namespace))
	}
}

func (e *sessionActorExecutor) enqueueTurn(ctx context.Context, env swarm.Envelope) error {
	var payload sessionTurnPayload
	if err := json.Unmarshal([]byte(env.PayloadJSON), &payload); err != nil {
		return swarm.PermanentError(fmt.Errorf("decode session turn payload: %w", err))
	}
	if strings.TrimSpace(payload.Locator.SessionID) == "" {
		payload.Locator.SessionID = strings.TrimSpace(env.To.Key)
	}
	if e.sessionTaskAlreadyDone(ctx, env.TaskID) {
		return nil
	}
	if e.handler.turnDispatcher == nil {
		return swarm.TransientError(fmt.Errorf("turn dispatcher is required"))
	}
	if swarm.QueueModeOf(env) == swarm.QueueModeInterrupt {
		_, _, err := e.handler.turnDispatcher.CancelSession(payload.Locator, true)
		if err != nil {
			return swarm.TransientError(fmt.Errorf("interrupt session turn: %w", err))
		}
	}

	done := make(chan error, 1)
	_, err := e.handler.turnDispatcher.Enqueue(TurnTask{
		SessionID: payload.Locator.SessionID,
		Context:   ctx,
		Run: func(runCtx context.Context) error {
			err := e.handler.runSessionTurnPayload(runCtx, payload)
			done <- err
			return err
		},
	})
	if err != nil {
		if errors.Is(err, ErrTurnQueueFull) {
			return swarm.TransientError(err)
		}
		return swarm.TransientError(fmt.Errorf("enqueue session actor turn: %w", err))
	}

	select {
	case err := <-done:
		return e.settleSessionTurnResult(ctx, env, payload, err)
	case <-ctx.Done():
		return swarm.TransientError(ctx.Err())
	}
}

func (e *sessionActorExecutor) settleSessionTurnResult(ctx context.Context, env swarm.Envelope, payload sessionTurnPayload, runErr error) error {
	if recordErr := e.recordSessionTaskResult(ctx, env, payload, runErr); recordErr != nil {
		return swarm.TransientError(recordErr)
	}
	if runErr == nil {
		return nil
	}
	// Contract: once task terminal failure is durably recorded, settle command without retry.
	if strings.TrimSpace(env.TaskID) != "" {
		return nil
	}
	return runErr
}

func (e *sessionActorExecutor) sessionTaskAlreadyDone(ctx context.Context, taskID string) bool {
	if e == nil || e.tasks == nil || strings.TrimSpace(taskID) == "" {
		return false
	}
	task, ok, err := e.tasks.Get(ctx, taskID)
	if err != nil || !ok {
		return false
	}
	return isTerminalTaskStatus(task.Status)
}

func (e *sessionActorExecutor) recordSessionTaskResult(ctx context.Context, env swarm.Envelope, payload sessionTurnPayload, runErr error) error {
	if e == nil {
		return nil
	}
	if e.scheduler != nil && strings.TrimSpace(payload.ScheduledTaskID) != "" {
		if runErr == nil {
			if err := e.scheduler.markSuccess(ctx, payload.ScheduledTaskID); err != nil {
				return fmt.Errorf("mark scheduled task %q success: %w", payload.ScheduledTaskID, err)
			}
		} else {
			if err := e.scheduler.recordExecutionFailure(ctx, payload.ScheduledTaskID, runErr); err != nil {
				return fmt.Errorf("record scheduled task %q failure: %w", payload.ScheduledTaskID, err)
			}
		}
	}
	if e.tasks == nil || strings.TrimSpace(env.TaskID) == "" {
		return nil
	}
	if runErr == nil {
		if err := e.tasks.MarkStatus(ctx, env.TaskID, baldastate.SwarmTaskStatusCompleted, "session.actor", env.ID, "", map[string]any{
			"namespace": env.Namespace,
			"kind":      env.Kind,
		}); err != nil {
			return fmt.Errorf("mark session task %q completed: %w", env.TaskID, err)
		}
		return nil
	}
	if err := e.tasks.MarkStatus(ctx, env.TaskID, baldastate.SwarmTaskStatusFailed, "session.actor", env.ID, runErr.Error(), map[string]any{
		"namespace": env.Namespace,
		"kind":      env.Kind,
	}); err != nil {
		return fmt.Errorf("mark session task %q failed: %w", env.TaskID, err)
	}
	return nil
}

func sessionTurnNamespace(source string) string {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case sessionTurnSourceWebhook:
		return swarm.NamespaceWebhookInbound
	case sessionTurnSourceSchedule:
		return swarm.NamespaceScheduleInbound
	case "agent":
		return swarm.NamespaceAgentCommand
	default:
		return swarm.NamespaceHumanInbound
	}
}

func sessionTurnKind(source string) string {
	if strings.EqualFold(strings.TrimSpace(source), sessionTurnSourceWebhook) {
		return swarm.KindWebhookEvent
	}
	return swarm.KindMessage
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
