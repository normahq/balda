package handlers

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/normahq/balda/internal/apps/balda/actors"
	"github.com/normahq/balda/internal/apps/balda/actors/goalkeeper"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	actortransport "github.com/normahq/balda/pkg/actorlayer/transport"
)

func (h *BaldaHandler) submitSessionTurn(ctx context.Context, payload actors.SessionTurnPayload) (*actortransport.DispatchReceipt, error) {
	if h.actorDispatcher == nil {
		return nil, fmt.Errorf("runtime runtime is unavailable")
	}
	env, err := actors.SessionTurnEnvelope(payload)
	if err != nil {
		return nil, err
	}
	return h.actorDispatcher.Dispatch(ctx, env)
}

func (h *BaldaHandler) submitWebhookTask(ctx context.Context, payload actors.SessionTurnPayload, routeName string, requestID string) (*actortransport.DispatchReceipt, string, error) {
	if h.actorDispatcher == nil {
		return nil, "", fmt.Errorf("runtime runtime is unavailable")
	}
	env, jobID, err := actors.WebhookJobEnvelope(payload, routeName, requestID)
	if err != nil {
		return nil, "", err
	}
	result, err := h.actorDispatcher.Dispatch(ctx, env)
	if err != nil {
		return nil, "", err
	}
	return result, jobID, nil
}

func (h *BaldaHandler) RunSessionTurnPayload(ctx context.Context, payload actors.SessionTurnPayload) error {
	ts, err := h.sessionManager.GetSession(payload.Locator)
	if err != nil {
		userID := strings.TrimSpace(payload.UserID)
		ts, err = h.sessionManager.RestoreSession(ctx, baldasession.SessionContext{
			Locator: payload.Locator,
			UserID:  userID,
		})
		if err != nil {
			if !errors.Is(err, baldasession.ErrNoPersistedSession) {
				return fmt.Errorf("restore session for queued turn: %w", err)
			}
			if userID == "" {
				h.logger.Debug().
					Str("session_id", payload.Locator.SessionID).
					Str("address_key", payload.Locator.AddressKey).
					Msg("dropping queued turn for unknown session without transport user")
				return nil
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
	runOpts, err := prepareMemoryRunOptions(ctx, h.memoryStore, ts)
	if err != nil {
		return err
	}
	return h.runTurnJobWithDeliveryOptions(
		ctx,
		payload.Text,
		ts.GetRunner(),
		userID,
		ts.GetSessionID(),
		payload.JobID,
		agentSessionID,
		deliveryLocator,
		payload.MessageID,
		payload.TopicID,
		actors.NormalizeSessionDeliveryOptions(payload),
		payload.Deliver,
		runOpts...,
	)
}

func (h *CommandHandler) submitGoalTask(ctx context.Context, locator baldasession.SessionLocator, objective string, transportUserID string) (bool, error) {
	return h.submitGoalTaskWithProfile(ctx, locator, deliverycmd.Profile{}, objective, transportUserID)
}

func (h *CommandHandler) submitGoalTaskWithProfile(ctx context.Context, locator baldasession.SessionLocator, deliveryProfile deliverycmd.Profile, objective string, transportUserID string) (bool, error) {
	if h.jobService != nil {
		activeGoals, err := h.jobService.ListActiveGoalJobsBySession(ctx, locator.SessionID)
		if err != nil {
			return false, fmt.Errorf("list active goal jobs: %w", err)
		}
		if len(activeGoals) > 0 {
			return false, nil
		}
	}
	maxIterations := normalizeGoalMaxIterations(h.goalMaxIterations)
	env, err := goalkeeper.GoalTaskEnvelopeWithProfile(locator, deliveryProfile, objective, transportUserID, maxIterations)
	if err != nil {
		return false, err
	}
	if h.actorDispatcher == nil {
		return false, fmt.Errorf("runtime runtime is unavailable")
	}
	_, err = h.actorDispatcher.Dispatch(ctx, env)
	if err != nil {
		return false, err
	}
	return true, nil
}
