package sessionturnapp

import (
	"context"
	"fmt"
	"strings"

	"github.com/baldaworks/go-actorlayer"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	"github.com/normahq/balda/internal/apps/balda/progress"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/rs/zerolog"
)

type SessionProgressEmitter interface {
	HandleNonTerminal(ctx context.Context, update SessionProgressUpdate) (SessionProgressResult, error)
}

type sessionProgressDispatcher struct {
	dispatcher actortransport.Dispatcher
	from       actorlayer.ActorAddress
	locator    baldasession.SessionLocator
	jobID      string
	topicID    int
	policy     deliveryfmt.ProgressPolicy
	logger     zerolog.Logger
	failHard   bool

	lastPlanText string
	deliverySeq  int
}

type SessionProgressUpdate struct {
	Plan                   progress.PlanSnapshot
	PlanProgressText       string
	HasPlanUpdate          bool
	ReasoningText          string
	HasThoughtUpdate       bool
	HasVisibleResponseText bool
	VisibleResponseText    string
}

type SessionProgressResult struct {
	SentProgress       bool
	DispatchedPlanText string
}

func NewSessionProgressDispatcher(
	dispatcher actortransport.Dispatcher,
	from actorlayer.ActorAddress,
	locator baldasession.SessionLocator,
	jobID string,
	topicID int,
	policy deliveryfmt.ProgressPolicy,
	failHard bool,
	logger zerolog.Logger,
) SessionProgressEmitter {
	return &sessionProgressDispatcher{
		dispatcher: dispatcher,
		from:       from,
		locator:    locator,
		jobID:      jobID,
		topicID:    topicID,
		policy:     policy,
		logger:     logger,
		failHard:   failHard,
	}
}

func (d *sessionProgressDispatcher) HandleNonTerminal(ctx context.Context, update SessionProgressUpdate) (SessionProgressResult, error) {
	if d == nil {
		return SessionProgressResult{}, nil
	}
	result := SessionProgressResult{}
	if update.HasPlanUpdate && update.PlanProgressText != "" && update.PlanProgressText != d.lastPlanText {
		visiblePlanUpdate := d.policy.PlanUpdates
		d.deliverySeq++
		dedupeSuffix := fmt.Sprintf("progress:plan:%03d", d.deliverySeq)
		if err := sendProgressPlanUpdate(ctx, d.dispatcher, d.jobID, d.from, d.locator, d.policy, visiblePlanUpdate, &update.Plan, update.PlanProgressText, dedupeSuffix); err != nil {
			if dispatchErr := d.handleDispatchError(err, "failed to dispatch plan progress delivery"); dispatchErr != nil {
				return result, dispatchErr
			}
		} else {
			d.lastPlanText = update.PlanProgressText
			result.DispatchedPlanText = strings.TrimSpace(update.PlanProgressText)
			result.SentProgress = true
		}
	}
	if update.HasThoughtUpdate {
		visibleThinking := d.policy.Thinking && strings.TrimSpace(update.ReasoningText) != "" && !update.HasVisibleResponseText
		d.logger.Debug().
			Str("session_id", d.locator.SessionID).
			Bool("policy_thinking", d.policy.Thinking).
			Bool("has_visible_response_text", update.HasVisibleResponseText).
			Int("reasoning_text_char_count", len(strings.TrimSpace(update.ReasoningText))).
			Bool("visible_thinking", visibleThinking).
			Msg("session progress thinking decision")
		d.deliverySeq++
		dedupeSuffix := fmt.Sprintf("progress:thinking:%03d", d.deliverySeq)
		if err := sendProgressThinking(ctx, d.dispatcher, d.jobID, d.from, d.locator, d.policy, visibleThinking, update.ReasoningText, d.deliverySeq, dedupeSuffix); err != nil {
			if dispatchErr := d.handleDispatchError(err, "failed to dispatch thinking progress delivery"); dispatchErr != nil {
				return result, dispatchErr
			}
		} else {
			result.SentProgress = true
		}
	}
	if d.locator.ChannelType == string(deliverycmd.ChannelTypeSlackAgent) && strings.TrimSpace(update.VisibleResponseText) != "" {
		d.deliverySeq++
		dedupeSuffix := fmt.Sprintf("progress:stream:%03d", d.deliverySeq)
		if err := sendProgressThinking(ctx, d.dispatcher, d.jobID, d.from, d.locator, d.policy, true, update.VisibleResponseText, d.deliverySeq, dedupeSuffix); err != nil {
			if dispatchErr := d.handleDispatchError(err, "failed to dispatch slack agent streaming progress delivery"); dispatchErr != nil {
				return result, dispatchErr
			}
		} else {
			result.SentProgress = true
		}
	}
	if result.SentProgress {
		return result, nil
	}
	d.deliverySeq++
	dedupeSuffix := fmt.Sprintf("progress:activity:%03d", d.deliverySeq)
	if err := sendProgressActivity(ctx, d.dispatcher, d.jobID, d.from, d.locator, d.policy, d.deliverySeq, dedupeSuffix); err != nil {
		if dispatchErr := d.handleDispatchError(err, "failed to dispatch activity progress delivery"); dispatchErr != nil {
			return result, dispatchErr
		}
	}
	return result, nil
}

func (d *sessionProgressDispatcher) handleDispatchError(err error, msg string) error {
	if d.failHard {
		return err
	}
	d.logger.Warn().Err(err).Int("topic_id", d.topicID).Msg(msg)
	return nil
}
