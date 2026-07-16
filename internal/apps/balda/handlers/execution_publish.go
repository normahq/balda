package handlers

import (
	"context"
	"fmt"

	actortransport "github.com/baldaworks/go-actorlayer/transport"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	"github.com/normahq/balda/internal/apps/balda/goalkeepercmd"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/turncmd"
)

func (h *BaldaHandler) submitSessionTurn(ctx context.Context, payload turncmd.SessionTurnPayload) (*actortransport.DispatchReceipt, error) {
	if h.actorDispatcher == nil {
		return nil, fmt.Errorf("runtime is unavailable")
	}
	env, err := turncmd.SessionTurnEnvelope(payload)
	if err != nil {
		return nil, err
	}
	return h.actorDispatcher.Dispatch(ctx, env)
}

func (h *BaldaHandler) submitWebhookTask(ctx context.Context, payload turncmd.SessionTurnPayload, routeName string, requestID string) (*actortransport.DispatchReceipt, string, error) {
	if h.actorDispatcher == nil {
		return nil, "", fmt.Errorf("runtime is unavailable")
	}
	env, jobID, err := turncmd.WebhookJobEnvelope(payload, routeName, requestID)
	if err != nil {
		return nil, "", err
	}
	result, err := h.actorDispatcher.Dispatch(ctx, env)
	if err != nil {
		return nil, "", err
	}
	return result, jobID, nil
}

func (h *CommandHandler) submitGoalJob(ctx context.Context, locator baldasession.SessionLocator, objective string, transportUserID string) (bool, error) {
	return h.submitGoalJobWithOptions(ctx, locator, deliveryfmt.Options{}, objective, transportUserID)
}

func (h *CommandHandler) submitGoalJobWithOptions(ctx context.Context, locator baldasession.SessionLocator, deliveryOptions deliveryfmt.Options, objective string, transportUserID string) (bool, error) {
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
	env, err := goalkeepercmd.JobEnvelopeWithOptions(locator, deliveryfmt.NormalizeOptions(deliveryOptions), objective, transportUserID, maxIterations)
	if err != nil {
		return false, err
	}
	if h.actorDispatcher == nil {
		return false, fmt.Errorf("runtime is unavailable")
	}
	_, err = h.actorDispatcher.Dispatch(ctx, env)
	if err != nil {
		return false, err
	}
	return true, nil
}
