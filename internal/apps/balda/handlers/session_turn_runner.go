package handlers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/normahq/balda/internal/apps/balda/actors"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	baldajobs "github.com/normahq/balda/internal/apps/balda/jobs"
	"github.com/normahq/balda/internal/apps/balda/memory"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/pkg/actorlayer"
	actortransport "github.com/normahq/balda/pkg/actorlayer/transport"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

// BaldaSessionTurnRunner executes queued agent turns independent of the inbound
// channel that produced them.
type BaldaSessionTurnRunner struct {
	sessionManager     *baldasession.Manager
	actorDispatcher    actortransport.Dispatcher
	jobService         *baldajobs.JobService
	memoryStore        *memory.Store
	planUpdatesEnabled bool
	logger             zerolog.Logger
	now                func() time.Time
}

type sessionTurnRunnerParams struct {
	fx.In

	SessionManager     *baldasession.Manager
	Dispatcher         actortransport.Dispatcher
	JobService         *baldajobs.JobService `optional:"true"`
	MemoryStore        *memory.Store
	PlanUpdatesEnabled bool `name:"balda_telegram_plan_updates"`
	Logger             zerolog.Logger
}

func NewBaldaSessionTurnRunner(params sessionTurnRunnerParams) *BaldaSessionTurnRunner {
	return &BaldaSessionTurnRunner{
		sessionManager:     params.SessionManager,
		actorDispatcher:    params.Dispatcher,
		jobService:         params.JobService,
		memoryStore:        params.MemoryStore,
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
	outboundFrom := actorlayer.ActorAddress{Target: baldaexecution.ActorTypeSession, Key: ts.GetSessionID()}
	handler := &BaldaHandler{
		sessionManager:     r.sessionManager,
		actorDispatcher:    r.actorDispatcher,
		jobService:         r.jobService,
		memoryStore:        r.memoryStore,
		planUpdatesEnabled: r.planUpdatesEnabled,
		logger:             r.logger,
		now:                r.now,
		outboundFrom:       outboundFrom,
	}
	handler.progressEmitter = newSessionProgressDispatcher(
		r.actorDispatcher,
		outboundFrom,
		deliveryLocator,
		payload.MessageID+1,
		payload.TopicID,
		actors.NormalizeSessionDeliveryOptions(payload).ProgressPolicy,
		r.logger,
	)
	runOpts, err := prepareMemoryRunOptions(ctx, r.memoryStore, ts)
	if err != nil {
		return err
	}
	return handler.runTurnWithDeliveryOptions(
		ctx,
		payload.Text,
		ts.GetRunner(),
		userID,
		ts.GetSessionID(),
		payload.JobID,
		agentSessionID,
		deliveryLocator,
		payload.MessageID,
		actors.NormalizeSessionDeliveryOptions(payload),
		payload.Deliver,
		runOpts...,
	)
}
