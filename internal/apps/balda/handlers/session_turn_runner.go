package handlers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/normahq/balda/internal/apps/balda/actors"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	actortransport "github.com/normahq/balda/pkg/actorlayer/transport"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

// BaldaSessionTurnRunner executes queued agent turns independent of the inbound
// channel that produced them.
type BaldaSessionTurnRunner struct {
	sessionManager     *baldasession.Manager
	actorDispatcher    actortransport.Dispatcher
	taskService        *swarm.TaskService
	planUpdatesEnabled bool
	logger             zerolog.Logger
	now                func() time.Time
}

type sessionTurnRunnerParams struct {
	fx.In

	SessionManager     *baldasession.Manager
	ActorDispatcher    actortransport.Dispatcher
	TaskService        *swarm.TaskService `optional:"true"`
	PlanUpdatesEnabled bool               `name:"balda_telegram_plan_updates"`
	Logger             zerolog.Logger
}

func NewBaldaSessionTurnRunner(params sessionTurnRunnerParams) *BaldaSessionTurnRunner {
	return &BaldaSessionTurnRunner{
		sessionManager:     params.SessionManager,
		actorDispatcher:    params.ActorDispatcher,
		taskService:        params.TaskService,
		planUpdatesEnabled: params.PlanUpdatesEnabled,
		logger:             params.Logger.With().Str("component", "balda.session_turn_runner").Logger(),
	}
}

// RunSessionTurnPayload restores the target session and executes one provider turn.
func (r *BaldaSessionTurnRunner) RunSessionTurnPayload(ctx context.Context, payload actors.SessionTurnPayload) error {
	if r.sessionManager == nil {
		return fmt.Errorf("session turn: session manager is unavailable")
	}
	ts, err := r.sessionManager.GetSession(payload.Locator)
	if err != nil {
		userID := strings.TrimSpace(payload.UserID)
		ts, err = r.sessionManager.RestoreSession(ctx, baldasession.SessionContext{
			Locator: payload.Locator,
			UserID:  userID,
		})
		if err != nil {
			if !errors.Is(err, baldasession.ErrNoPersistedSession) {
				return fmt.Errorf("restore session for queued turn: %w", err)
			}
			if userID == "" {
				r.logger.Debug().
					Str("session_id", payload.Locator.SessionID).
					Str("channel_type", payload.Locator.ChannelType).
					Str("address_key", payload.Locator.AddressKey).
					Msg("dropping queued turn for unknown session without transport user")
				return nil
			}
			ts, err = r.sessionManager.EnsureSession(ctx, baldasession.SessionContext{
				Locator: payload.Locator,
				UserID:  userID,
			}, ownerSessionLabel)
			if err != nil {
				return fmt.Errorf("create session for queued turn: %w", err)
			}
		}
	}
	if ts == nil {
		return fmt.Errorf("session turn: session %s unavailable after restore", payload.Locator.SessionID)
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
	handler := &BaldaHandler{
		sessionManager:     r.sessionManager,
		actorDispatcher:    r.actorDispatcher,
		taskService:        r.taskService,
		planUpdatesEnabled: r.planUpdatesEnabled,
		logger:             r.logger,
		now:                r.now,
	}
	return handler.runTurnWithDelivery(
		ctx,
		payload.Text,
		ts.GetRunner(),
		userID,
		ts.GetSessionID(),
		payload.TaskID,
		agentSessionID,
		deliveryLocator,
		payload.MessageID,
		payload.ProgressPolicy,
		payload.Deliver,
	)
}
