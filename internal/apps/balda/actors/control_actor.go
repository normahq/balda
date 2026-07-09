package actors

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	baldajobs "github.com/normahq/balda/internal/apps/balda/jobs"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/pkg/actorlayer"
	actortransport "github.com/normahq/balda/pkg/actorlayer/transport"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

const (
	jobControlActionCancel     = "cancel"
	jobControlActionCancelTurn = "cancel_turn"
	jobControlActionClearGoal  = "clear_goal"
)

type jobControlPayload struct {
	Action      string                      `json:"action"`
	JobID       string                      `json:"job_id,omitempty"`
	SessionID   string                      `json:"session_id,omitempty"`
	Locator     baldasession.SessionLocator `json:"locator"`
	Reason      string                      `json:"reason,omitempty"`
	RequestedBy string                      `json:"requested_by,omitempty"`
	Notify      bool                        `json:"notify,omitempty"`
}

type jobControlActor struct {
	turnDispatcher TurnQueue
	dispatcher     actortransport.Dispatcher
	tasks          *baldajobs.JobService
	taskRuns       *JobRunRegistry
	logger         zerolog.Logger
}

// SessionWorkCanceller synchronously stops queued, running, and job-backed
// work for a session without going through the async control actor path.
type SessionWorkCanceller struct {
	turnDispatcher TurnQueue
	tasks          *baldajobs.JobService
	taskRuns       *JobRunRegistry
	logger         zerolog.Logger
}

type jobControlActorParams struct {
	fx.In

	TurnDispatcher *TurnDispatcher
	Dispatcher     actortransport.Dispatcher
	JobService     *baldajobs.JobService
	JobRuns        *JobRunRegistry
	Logger         zerolog.Logger
}

type sessionWorkCancellerParams struct {
	fx.In

	TurnDispatcher *TurnDispatcher
	JobService     *baldajobs.JobService
	JobRuns        *JobRunRegistry
	Logger         zerolog.Logger
}

func NewSessionWorkCanceller(params sessionWorkCancellerParams) *SessionWorkCanceller {
	return &SessionWorkCanceller{
		turnDispatcher: params.TurnDispatcher,
		tasks:          params.JobService,
		taskRuns:       params.JobRuns,
		logger:         params.Logger.With().Str("component", "balda.session_work_canceller").Logger(),
	}
}

func (c *SessionWorkCanceller) CancelWork(ctx context.Context, locator baldasession.SessionLocator, actor string, reason string) error {
	if c == nil {
		return nil
	}
	sessionID := strings.TrimSpace(locator.SessionID)
	if sessionID == "" {
		return fmt.Errorf("session id is required")
	}
	if c.turnDispatcher != nil {
		if _, _, err := c.turnDispatcher.CancelSession(locator, true); err != nil {
			return fmt.Errorf("cancel session turn queue: %w", err)
		}
	}
	if c.tasks == nil {
		return nil
	}
	taskIDs, err := c.tasks.CancelBySession(ctx, sessionID, actor, reason)
	if err != nil {
		return fmt.Errorf("cancel session jobs: %w", err)
	}
	if c.taskRuns == nil {
		return nil
	}
	for _, taskID := range taskIDs {
		c.taskRuns.Cancel(taskID)
	}
	return nil
}

func (a *jobControlActor) Address() string {
	return "system:control"
}

func (a *jobControlActor) Handle(ctx context.Context, env actorlayer.Envelope) error {
	if strings.TrimSpace(env.Namespace) != baldaexecution.NamespaceJobControl {
		return actorlayer.PolicyError(fmt.Errorf("unsupported control namespace %q", env.Namespace))
	}
	var payload jobControlPayload
	if err := json.Unmarshal([]byte(env.PayloadJSON), &payload); err != nil {
		return actorlayer.PermanentError(fmt.Errorf("decode control payload: %w", err))
	}
	switch strings.TrimSpace(payload.Action) {
	case jobControlActionCancel:
		if strings.TrimSpace(payload.JobID) != "" {
			return a.cancelTask(ctx, env, payload)
		}
		return a.cancelSession(ctx, payload)
	case jobControlActionCancelTurn:
		return a.cancelSessionTurn(ctx, payload)
	case jobControlActionClearGoal:
		return a.clearGoal(ctx, payload)
	default:
		return actorlayer.PolicyError(fmt.Errorf("unsupported control action %q", payload.Action))
	}
}

func (a *jobControlActor) cancelTask(ctx context.Context, env actorlayer.Envelope, payload jobControlPayload) error {
	taskID := strings.TrimSpace(payload.JobID)
	if taskID == "" {
		return actorlayer.PolicyError(fmt.Errorf("job id is required"))
	}
	if a.tasks == nil {
		return actorlayer.TransientError(fmt.Errorf("job service is required"))
	}
	task, ok, err := a.tasks.Get(ctx, taskID)
	if err != nil {
		return actorlayer.TransientError(err)
	}
	if !ok {
		if payload.Notify {
			a.sendControlMessage(ctx, payload.Locator, fmt.Sprintf("Task %q not found.", taskID))
		}
		return nil
	}
	if isTerminalTaskStatus(task.Status) {
		if payload.Notify {
			a.sendControlMessage(ctx, payload.Locator, fmt.Sprintf("Task %s is already %s.", task.ID, task.Status))
		}
		return nil
	}
	runCanceled := false
	if a.taskRuns != nil {
		runCanceled = a.taskRuns.Cancel(task.ID)
	}
	if !runCanceled && a.turnDispatcher != nil && strings.TrimSpace(payload.Locator.SessionID) != "" {
		hadInFlight, dropped, err := a.turnDispatcher.CancelSession(payload.Locator, true)
		if err != nil {
			return actorlayer.TransientError(err)
		}
		runCanceled = hadInFlight || dropped > 0
	}
	reason := firstNonEmpty(payload.Reason, "task canceled by user")
	if err := a.tasks.CancelJob(ctx, task.ID, "command.task", reason); err != nil {
		return actorlayer.TransientError(err)
	}
	if payload.Notify {
		a.sendControlMessage(ctx, payload.Locator, fmt.Sprintf("Canceled task %s. Active run canceled: %t.", task.ID, runCanceled))
	}
	return nil
}

func (a *jobControlActor) cancelSession(ctx context.Context, payload jobControlPayload) error {
	if strings.TrimSpace(payload.Locator.SessionID) == "" {
		return actorlayer.PolicyError(fmt.Errorf("session id is required"))
	}
	hadInFlight := false
	dropped := 0
	if a.turnDispatcher != nil {
		var err error
		hadInFlight, dropped, err = a.turnDispatcher.CancelSession(payload.Locator, true)
		if err != nil {
			return actorlayer.TransientError(err)
		}
	}
	taskCanceled := 0
	if a.tasks != nil {
		taskIDs, err := a.tasks.CancelBySession(ctx, payload.Locator.SessionID, "command.cancel", firstNonEmpty(payload.Reason, "session canceled by user"))
		if err != nil {
			return actorlayer.TransientError(err)
		}
		for _, taskID := range taskIDs {
			if a.taskRuns != nil && a.taskRuns.Cancel(taskID) {
				taskCanceled++
			}
		}
	}
	if payload.Notify {
		response := "Canceled current turn."
		if !hadInFlight && dropped == 0 && taskCanceled == 0 {
			response = "No running or queued session work."
		} else if !hadInFlight {
			response = "No running turn to cancel."
		}
		if dropped > 0 {
			response += fmt.Sprintf("\nDropped %d queued session message(s).", dropped)
		}
		if taskCanceled > 0 {
			response += fmt.Sprintf("\nCanceled %d active task(s).", taskCanceled)
		}
		a.sendControlMessage(ctx, payload.Locator, response)
	}
	return nil
}

func (a *jobControlActor) cancelSessionTurn(ctx context.Context, payload jobControlPayload) error {
	if strings.TrimSpace(payload.Locator.SessionID) == "" {
		return actorlayer.PolicyError(fmt.Errorf("session id is required"))
	}
	hadInFlight := false
	dropped := 0
	if a.turnDispatcher != nil {
		var err error
		hadInFlight, dropped, err = a.turnDispatcher.CancelSession(payload.Locator, true)
		if err != nil {
			return actorlayer.TransientError(err)
		}
	}
	if payload.Notify {
		response := "Canceled current turn."
		if !hadInFlight && dropped == 0 {
			response = "No running or queued session work."
		} else if !hadInFlight {
			response = "No running turn to cancel."
		}
		if dropped > 0 {
			response += fmt.Sprintf("\nDropped %d queued session message(s).", dropped)
		}
		a.sendControlMessage(ctx, payload.Locator, response)
	}
	return nil
}

func (a *jobControlActor) clearGoal(ctx context.Context, payload jobControlPayload) error {
	if strings.TrimSpace(payload.Locator.SessionID) == "" {
		return actorlayer.PolicyError(fmt.Errorf("session id is required"))
	}
	if a.tasks == nil {
		return actorlayer.TransientError(fmt.Errorf("job service is required"))
	}
	tasks, err := a.tasks.ListActiveGoalJobsBySession(ctx, payload.Locator.SessionID)
	if err != nil {
		return actorlayer.TransientError(err)
	}
	cleared := 0
	for _, task := range tasks {
		if err := a.tasks.CancelJob(ctx, task.ID, "command.goal", firstNonEmpty(payload.Reason, "goal cleared by user")); err != nil {
			return actorlayer.TransientError(err)
		}
		if a.taskRuns != nil {
			a.taskRuns.Cancel(task.ID)
		}
		cleared++
	}
	if payload.Notify {
		switch cleared {
		case 0:
			a.sendControlMessage(ctx, payload.Locator, "No active goal run.")
		case 1:
			a.sendControlMessage(ctx, payload.Locator, "Cleared active goal run.")
		default:
			a.sendControlMessage(ctx, payload.Locator, fmt.Sprintf("Cleared %d active goal runs.", cleared))
		}
	}
	return nil
}

func (a *jobControlActor) sendControlMessage(ctx context.Context, locator baldasession.SessionLocator, text string) {
	if a == nil || a.dispatcher == nil || strings.TrimSpace(text) == "" {
		return
	}
	env, err := PlainDeliveryEnvelopeWithSettlement("", actorlayer.SystemAddress("control"), locator, deliverycmd.SettlementBypass, text, "")
	if err != nil {
		a.logger.Warn().Err(err).Str("session_id", locator.SessionID).Msg("failed to build control response")
		return
	}
	if _, err := a.dispatcher.Dispatch(ctx, env); err != nil {
		a.logger.Warn().Err(err).Str("session_id", locator.SessionID).Msg("failed to send control response")
	}
}

func ControlCancelEnvelope(locator baldasession.SessionLocator, taskID string, requestedBy string, reason string) (actorlayer.Envelope, error) {
	return ControlCancelEnvelopeWithNotify(locator, taskID, requestedBy, reason, true)
}

func ControlCancelEnvelopeWithNotify(locator baldasession.SessionLocator, taskID string, requestedBy string, reason string, notify bool) (actorlayer.Envelope, error) {
	return controlEnvelope(locator, jobControlActionCancel, taskID, requestedBy, reason, notify)
}

func ControlCancelTurnEnvelopeWithNotify(locator baldasession.SessionLocator, requestedBy string, reason string, notify bool) (actorlayer.Envelope, error) {
	return controlEnvelope(locator, jobControlActionCancelTurn, "", requestedBy, reason, notify)
}

func ControlClearGoalEnvelopeWithNotify(locator baldasession.SessionLocator, requestedBy string, reason string, notify bool) (actorlayer.Envelope, error) {
	return controlEnvelope(locator, jobControlActionClearGoal, "", requestedBy, reason, notify)
}

func controlEnvelope(locator baldasession.SessionLocator, action string, taskID string, requestedBy string, reason string, notify bool) (actorlayer.Envelope, error) {
	payload := jobControlPayload{
		Action:      strings.TrimSpace(action),
		JobID:       strings.TrimSpace(taskID),
		SessionID:   strings.TrimSpace(locator.SessionID),
		Locator:     locator,
		Reason:      strings.TrimSpace(reason),
		RequestedBy: strings.TrimSpace(requestedBy),
		Notify:      notify,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return actorlayer.Envelope{}, fmt.Errorf("encode control payload: %w", err)
	}
	id := uuid.NewString()
	return actorlayer.Envelope{
		ID:          id,
		Namespace:   baldaexecution.NamespaceJobControl,
		Kind:        baldaexecution.KindCancel,
		From:        actorlayer.ActorAddress{Target: "telegram", Key: firstNonEmpty(requestedBy, locator.AddressKey, "unknown")},
		To:          actorlayer.SystemAddress("control"),
		SessionID:   locator.SessionID,
		Meta:        baldaexecution.WithJobIDMeta(nil, taskID),
		Priority:    100,
		DedupeKey:   "control:" + strings.TrimSpace(action) + ":" + id,
		PayloadJSON: string(data),
	}, nil
}
