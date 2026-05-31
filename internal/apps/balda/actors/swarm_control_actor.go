package actors

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

const taskControlActionCancel = "cancel"

type taskControlPayload struct {
	Action      string                      `json:"action"`
	TaskID      string                      `json:"task_id,omitempty"`
	SessionID   string                      `json:"session_id,omitempty"`
	Locator     baldasession.SessionLocator `json:"locator"`
	Reason      string                      `json:"reason,omitempty"`
	RequestedBy string                      `json:"requested_by,omitempty"`
	Notify      bool                        `json:"notify,omitempty"`
}

type taskControlActor struct {
	turnDispatcher TurnQueue
	tasks          *swarm.TaskService
	taskRuns       *TaskRunRegistry
	channel        *baldatelegram.Adapter
	logger         zerolog.Logger
}

type taskControlActorParams struct {
	fx.In

	TurnDispatcher *TurnDispatcher
	TaskService    *swarm.TaskService
	TaskRuns       *TaskRunRegistry
	Channel        *baldatelegram.Adapter
	Logger         zerolog.Logger
}

func newTaskControlActor(params taskControlActorParams) swarm.Actor {
	return &taskControlActor{
		turnDispatcher: params.TurnDispatcher,
		tasks:          params.TaskService,
		taskRuns:       params.TaskRuns,
		channel:        params.Channel,
		logger:         params.Logger.With().Str("component", "balda.task_control_actor").Logger(),
	}
}

func (a *taskControlActor) Address() string {
	return "system:control"
}

func (a *taskControlActor) Handle(ctx context.Context, envelope any) error {
	env, err := swarm.AssertEnvelope(envelope)
	if err != nil {
		return err
	}
	if strings.TrimSpace(env.Namespace) != swarm.NamespaceTaskControl {
		return swarm.PolicyError(fmt.Errorf("unsupported control namespace %q", env.Namespace))
	}
	var payload taskControlPayload
	if err := json.Unmarshal([]byte(env.PayloadJSON), &payload); err != nil {
		return swarm.PermanentError(fmt.Errorf("decode control payload: %w", err))
	}
	if strings.TrimSpace(payload.Action) != taskControlActionCancel {
		return swarm.PolicyError(fmt.Errorf("unsupported control action %q", payload.Action))
	}
	if strings.TrimSpace(payload.TaskID) != "" {
		return a.cancelTask(ctx, env, payload)
	}
	return a.cancelSession(ctx, payload)
}

func (a *taskControlActor) cancelTask(ctx context.Context, env swarm.Envelope, payload taskControlPayload) error {
	taskID := firstNonEmpty(payload.TaskID, env.TaskID)
	if taskID == "" {
		return swarm.PolicyError(fmt.Errorf("task id is required"))
	}
	if a.tasks == nil {
		return swarm.TransientError(fmt.Errorf("task service is required"))
	}
	task, ok, err := a.tasks.Get(ctx, taskID)
	if err != nil {
		return swarm.TransientError(err)
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
			return swarm.TransientError(err)
		}
		runCanceled = hadInFlight || dropped > 0
	}
	reason := firstNonEmpty(payload.Reason, "task canceled by user")
	if err := a.tasks.CancelTask(ctx, task.ID, "command.task", reason); err != nil {
		return swarm.TransientError(err)
	}
	if payload.Notify {
		a.sendControlMessage(ctx, payload.Locator, fmt.Sprintf("Canceled task %s. Active run canceled: %t.", task.ID, runCanceled))
	}
	return nil
}

func (a *taskControlActor) cancelSession(ctx context.Context, payload taskControlPayload) error {
	if strings.TrimSpace(payload.Locator.SessionID) == "" {
		return swarm.PolicyError(fmt.Errorf("session id is required"))
	}
	hadInFlight := false
	dropped := 0
	if a.turnDispatcher != nil {
		var err error
		hadInFlight, dropped, err = a.turnDispatcher.CancelSession(payload.Locator, true)
		if err != nil {
			return swarm.TransientError(err)
		}
	}
	taskCanceled := 0
	if a.tasks != nil {
		taskIDs, err := a.tasks.CancelBySession(ctx, payload.Locator.SessionID, "command.cancel", firstNonEmpty(payload.Reason, "session canceled by user"))
		if err != nil {
			return swarm.TransientError(err)
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

func (a *taskControlActor) sendControlMessage(ctx context.Context, locator baldasession.SessionLocator, text string) {
	if a == nil || a.channel == nil || strings.TrimSpace(text) == "" {
		return
	}
	if err := a.channel.SendPlain(ctx, locator, text); err != nil {
		a.logger.Warn().Err(err).Str("session_id", locator.SessionID).Msg("failed to send control response")
	}
}

func ControlCancelEnvelope(locator baldasession.SessionLocator, taskID string, requestedBy string, reason string) (swarm.Envelope, error) {
	return ControlCancelEnvelopeWithNotify(locator, taskID, requestedBy, reason, true)
}

func ControlCancelEnvelopeWithNotify(locator baldasession.SessionLocator, taskID string, requestedBy string, reason string, notify bool) (swarm.Envelope, error) {
	payload := taskControlPayload{
		Action:      taskControlActionCancel,
		TaskID:      strings.TrimSpace(taskID),
		SessionID:   strings.TrimSpace(locator.SessionID),
		Locator:     locator,
		Reason:      strings.TrimSpace(reason),
		RequestedBy: strings.TrimSpace(requestedBy),
		Notify:      notify,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return swarm.Envelope{}, fmt.Errorf("encode control payload: %w", err)
	}
	id := uuid.NewString()
	return swarm.Envelope{
		ID:          id,
		Namespace:   swarm.NamespaceTaskControl,
		Kind:        swarm.KindCancel,
		From:        swarm.ActorAddress{Target: "telegram", Key: firstNonEmpty(requestedBy, locator.AddressKey, "unknown")},
		To:          swarm.SystemAddress("control"),
		SessionID:   locator.SessionID,
		TaskID:      strings.TrimSpace(taskID),
		Priority:    100,
		DedupeKey:   "control:cancel:" + id,
		PayloadJSON: string(data),
	}, nil
}
