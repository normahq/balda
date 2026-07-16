package actors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/baldaworks/go-actorlayer"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/actorcmd"
	"github.com/normahq/balda/internal/apps/balda/appports"
	"github.com/normahq/balda/internal/apps/balda/automodecmd"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	"github.com/normahq/balda/internal/apps/balda/questioncmd"
	"github.com/normahq/balda/internal/apps/balda/questions"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/turncmd"
	"go.uber.org/fx"
)

const (
	sessionTurnSourceTelegram = turncmd.SourceTelegram
	sessionTurnSourceWebhook  = turncmd.SourceWebhook
	sessionTurnSourceSchedule = turncmd.SourceSchedule
)

type SessionTurnPayload = turncmd.SessionTurnPayload
type SessionTurnRunner = appports.SessionTurnRunner
type ScheduledJobRecorder = appports.ScheduledJobRecorder

type sessionJobLifecycle interface {
	Get(ctx context.Context, jobID string) (baldastate.JobRecord, bool, error)
	MarkStatus(ctx context.Context, jobID string, status string, actor string, messageID string, reason string, payload any) error
}

func SessionTurnEnvelope(payload SessionTurnPayload) (actorlayer.Envelope, error) {
	return turncmd.SessionTurnEnvelope(payload)
}

type sessionActorExecutor struct {
	turns      appports.TurnQueue
	runner     appports.SessionTurnRunner
	tasks      sessionJobLifecycle
	scheduler  appports.ScheduledJobRecorder
	dispatcher actortransport.Dispatcher
	questions  *questions.Service
	sessions   sessionRuntimeStateUpdater
}

type sessionRuntimeStateUpdater interface {
	GetSession(locator baldasession.SessionLocator) (*baldasession.TopicSession, error)
}

type sessionActorExecutorParams struct {
	fx.In

	Dispatcher actortransport.Dispatcher
	Turns      appports.TurnQueue
	Runner     appports.SessionTurnRunner
	Tasks      sessionJobLifecycle           `optional:"true"`
	Scheduler  appports.ScheduledJobRecorder `optional:"true"`
	Questions  *questions.Service            `optional:"true"`
	Sessions   *baldasession.Manager         `optional:"true"`
}

func (e *sessionActorExecutor) Address() string {
	return actorlayer.WildcardAddress(baldaexecution.ActorTypeSession)
}

func (e *sessionActorExecutor) Handle(ctx context.Context, env actorlayer.Envelope) error {
	switch strings.TrimSpace(env.Namespace) {
	case baldaexecution.NamespaceHumanInbound, baldaexecution.NamespaceWebhookInbound, baldaexecution.NamespaceScheduleInbound, baldaexecution.NamespaceGoalkeeperCommand, baldaexecution.NamespaceJobControl:
		return e.enqueueTurn(ctx, env)
	case baldaexecution.NamespaceAutoModeCommand:
		return e.updateAutoModeState(ctx, env)
	default:
		return actorlayer.PolicyError(fmt.Errorf("unsupported session namespace %q", env.Namespace))
	}
}

func (e *sessionActorExecutor) updateAutoModeState(ctx context.Context, env actorlayer.Envelope) error {
	if e == nil || e.sessions == nil {
		return actorlayer.TransientError(fmt.Errorf("session runtime state updater is required"))
	}
	var payload automodecmd.Payload
	if err := actorlayer.UnmarshalPayload(env.Payload, &payload); err != nil {
		return actorlayer.PermanentError(fmt.Errorf("decode auto mode payload: %w", err))
	}
	ts, err := e.sessions.GetSession(baldasession.SessionLocator(payload.Locator))
	if err != nil {
		return actorlayer.TransientError(fmt.Errorf("lookup session for auto mode update: %w", err))
	}
	if ts == nil {
		return actorlayer.TransientError(fmt.Errorf("session %q unavailable for auto mode update", payload.Locator.SessionID))
	}
	if err := ts.UpdateRuntimeState(ctx, payload.State); err != nil {
		return actorlayer.TransientError(fmt.Errorf("update auto mode runtime state: %w", err))
	}
	return nil
}

func (e *sessionActorExecutor) enqueueTurn(ctx context.Context, env actorlayer.Envelope) error {
	var payload SessionTurnPayload
	if err := actorlayer.UnmarshalPayload(env.Payload, &payload); err != nil {
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
	settlement := newSessionSettlementCoordinator(e.tasks, e.scheduler)
	if settlement.taskAlreadyDone(ctx, env, payload) {
		return nil
	}
	if handled, err := e.handleScheduledQuestionTimeout(ctx, env, payload); handled {
		return settlement.settle(ctx, env, payload, err)
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

	result, _, err := e.turns.Enqueue(ctx, appports.TurnTask{
		SessionID: payload.Locator.SessionID,
		Run: func(runCtx context.Context) error {
			return e.runner.RunSessionTurnPayload(runCtx, payload)
		},
	})
	if err != nil {
		if errors.Is(err, ErrTurnQueueFull) {
			return actorlayer.TransientError(err)
		}
		return actorlayer.TransientError(fmt.Errorf("enqueue session actor turn: %w", err))
	}

	select {
	case err := <-result:
		return settlement.settle(ctx, env, payload, err)
	case <-ctx.Done():
		return actorlayer.TransientError(ctx.Err())
	}
}

func (e *sessionActorExecutor) handleScheduledQuestionTimeout(ctx context.Context, env actorlayer.Envelope, payload SessionTurnPayload) (bool, error) {
	if e == nil || e.questions == nil || e.dispatcher == nil {
		return false, nil
	}
	if !strings.EqualFold(strings.TrimSpace(payload.Source), sessionTurnSourceSchedule) && !strings.EqualFold(strings.TrimSpace(env.Namespace), baldaexecution.NamespaceScheduleInbound) {
		return false, nil
	}
	questionID, ok := questioncmd.ParseTimeoutScheduledContent(payload.Text)
	if !ok {
		return false, nil
	}
	record, settled, err := e.questions.Timeout(ctx, questionID, time.Now().UTC())
	if err != nil || !settled {
		return true, err
	}
	var interaction questioncmd.InteractionContext
	if err := json.Unmarshal([]byte(record.InteractionJSON), &interaction); err != nil {
		return true, fmt.Errorf("decode timed out question interaction: %w", err)
	}
	var resume questioncmd.ResumeTarget
	if err := json.Unmarshal([]byte(record.ResumeJSON), &resume); err != nil {
		return true, fmt.Errorf("decode timed out question resume: %w", err)
	}
	timeoutEnv, err := questioncmd.TimedOutEnvelope(resume, interaction, record.QuestionID, record.AnsweredAt)
	if err != nil {
		return true, err
	}
	_, err = e.dispatcher.Dispatch(ctx, timeoutEnv)
	return true, err
}

func NormalizeSessionDeliveryOptions(payload SessionTurnPayload) deliveryfmt.Options {
	return turncmd.NormalizeSessionDeliveryOptions(payload)
}
