package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	baldachannel "github.com/normahq/balda/internal/apps/balda/channel"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"go.uber.org/fx"
)

type sessionTurnPayload struct {
	Text           string                      `json:"text"`
	Locator        baldasession.SessionLocator `json:"locator"`
	UserID         string                      `json:"user_id,omitempty"`
	AgentSessionID string                      `json:"agent_session_id,omitempty"`
	MessageID      int                         `json:"message_id,omitempty"`
	TopicID        int                         `json:"topic_id,omitempty"`
	ProgressPolicy baldachannel.ProgressPolicy `json:"progress_policy,omitempty"`
	Deliver        bool                        `json:"deliver"`
	Source         string                      `json:"source,omitempty"`
	DedupeKey      string                      `json:"dedupe_key,omitempty"`
}

func (h *BaldaHandler) submitSessionTurn(ctx context.Context, payload sessionTurnPayload) (int, error) {
	if strings.EqualFold(strings.TrimSpace(payload.Source), "webhook") {
		return h.submitWebhookSessionTurn(ctx, payload)
	}
	return h.submitGlobalSessionTurn(ctx, payload)
}

func (h *BaldaHandler) submitWebhookSessionTurn(ctx context.Context, payload sessionTurnPayload) (int, error) {
	if h.swarmCoordinator != nil && h.swarmCoordinator.WebhookShadowEnabled() {
		h.shadowSessionTurn(ctx, payload)
		position, err := h.enqueueSessionTurnDirect(payload)
		if err == nil {
			h.swarmCoordinator.RecordShadowDispatch()
		}
		return position, err
	}
	if h.swarmCoordinator != nil && h.swarmCoordinator.WebhookMailboxEnabled() {
		return h.submitSessionTurnToSwarm(ctx, payload)
	}
	return h.enqueueSessionTurnDirect(payload)
}

func (h *BaldaHandler) submitGlobalSessionTurn(ctx context.Context, payload sessionTurnPayload) (int, error) {
	if h.swarmCoordinator != nil && h.swarmCoordinator.ShadowEnabled() {
		h.shadowSessionTurn(ctx, payload)
		position, err := h.enqueueSessionTurnDirect(payload)
		if err == nil {
			h.swarmCoordinator.RecordShadowDispatch()
		}
		return position, err
	}
	if h.swarmCoordinator != nil && h.swarmCoordinator.Enabled() {
		return h.submitSessionTurnToSwarm(ctx, payload)
	}
	return h.enqueueSessionTurnDirect(payload)
}

func (h *BaldaHandler) submitSessionTurnToSwarm(ctx context.Context, payload sessionTurnPayload) (int, error) {
	env, err := sessionTurnEnvelope(payload)
	if err != nil {
		return 0, err
	}
	submitted, err := h.swarmCoordinator.Submit(ctx, env)
	if err != nil {
		if errors.Is(err, swarm.ErrQueueFull) {
			return 0, ErrTurnQueueFull
		}
		return 0, err
	}
	return submitted.QueuePosition, nil
}

func (h *BaldaHandler) shadowSessionTurn(ctx context.Context, payload sessionTurnPayload) {
	env, err := sessionTurnEnvelope(payload)
	if err != nil {
		h.logger.Warn().Err(err).Msg("failed to build swarm shadow session envelope")
		return
	}
	if _, err := h.swarmCoordinator.SubmitShadow(ctx, env); err != nil {
		h.logger.Warn().Err(err).Str("session_id", payload.Locator.SessionID).Msg("failed to persist swarm shadow session envelope")
	}
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
		source = "telegram"
	}
	return swarm.Envelope{
		Namespace:   sessionTurnNamespace(source),
		Kind:        sessionTurnKind(source),
		From:        swarm.ActorAddress{Target: source, Key: firstNonEmpty(payload.UserID, payload.Locator.AddressKey, "unknown")},
		To:          swarm.ActorAddress{Target: swarm.ActorTypeSession, Key: payload.Locator.SessionID},
		SessionID:   payload.Locator.SessionID,
		DedupeKey:   strings.TrimSpace(payload.DedupeKey),
		PayloadJSON: string(data),
	}, nil
}

func (h *BaldaHandler) enqueueSessionTurnDirect(payload sessionTurnPayload) (int, error) {
	if h.turnDispatcher == nil {
		return 0, fmt.Errorf("balda turn dispatcher is required")
	}
	return h.turnDispatcher.Enqueue(TurnTask{
		SessionID: payload.Locator.SessionID,
		Run: func(runCtx context.Context) error {
			return h.runSessionTurnPayload(runCtx, payload)
		},
	})
}

func (h *BaldaHandler) runSessionTurnPayload(ctx context.Context, payload sessionTurnPayload) error {
	ts, err := h.sessionManager.GetSession(payload.Locator)
	if err != nil {
		h.logger.Debug().
			Str("session_id", payload.Locator.SessionID).
			Str("address_key", payload.Locator.AddressKey).
			Msg("dropping queued turn for inactive session")
		return nil
	}
	userID := strings.TrimSpace(payload.UserID)
	if userID == "" {
		userID = ts.GetUserID()
	}
	agentSessionID := strings.TrimSpace(payload.AgentSessionID)
	if agentSessionID == "" {
		agentSessionID = ts.GetAgentSessionID()
	}
	return h.runTurnTaskWithDelivery(
		ctx,
		payload.Text,
		ts.GetRunner(),
		userID,
		ts.GetSessionID(),
		agentSessionID,
		payload.Locator,
		payload.MessageID,
		payload.TopicID,
		payload.ProgressPolicy,
		payload.Deliver,
	)
}

type sessionActorExecutor struct {
	handler *BaldaHandler
}

type sessionActorExecutorParams struct {
	fx.In

	Handler *BaldaHandler
}

func newSessionActorExecutor(params sessionActorExecutorParams) swarm.Actor {
	return &sessionActorExecutor{handler: params.Handler}
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
	if e.handler.turnDispatcher == nil {
		return e.handler.runSessionTurnPayload(ctx, payload)
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
		return err
	case <-ctx.Done():
		return swarm.TransientError(ctx.Err())
	}
}

func sessionTurnNamespace(source string) string {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case "webhook":
		return swarm.NamespaceWebhookInbound
	case "schedule":
		return swarm.NamespaceScheduleInbound
	case "agent":
		return swarm.NamespaceAgentCommand
	default:
		return swarm.NamespaceHumanInbound
	}
}

func sessionTurnKind(source string) string {
	if strings.EqualFold(strings.TrimSpace(source), "webhook") {
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
