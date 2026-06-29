package actors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"go.uber.org/fx"
)

const (
	taskPayloadKindScheduledTask = "scheduled_task"
	taskPayloadKindSessionTurn   = "session_turn"
	taskPayloadKindDelivery      = "delivery"
)

type taskEnvelopePayload struct {
	Kind          string                `json:"kind"`
	ScheduledTask *scheduledTaskPayload `json:"scheduled_task,omitempty"`
	SessionTurn   *SessionTurnPayload   `json:"session_turn,omitempty"`
}

type scheduledTaskPayload struct {
	TaskID       string                       `json:"task_id"`
	Content      string                       `json:"content"`
	Locator      baldasession.SessionLocator  `json:"locator"`
	ReportTo     *baldasession.SessionLocator `json:"report_to,omitempty"`
	ParentTaskID string                       `json:"parent_task_id,omitempty"`
	UserID       string                       `json:"user_id"`
	TopicID      int                          `json:"topic_id,omitempty"`
}

type DeliveryPayload = deliverycmd.Payload

type DeliveryMode = deliverycmd.Mode

const (
	DeliveryModeAgentReply DeliveryMode = deliverycmd.ModeAgentReply
	DeliveryModePlain      DeliveryMode = deliverycmd.ModePlain
	DeliveryModeMarkdown   DeliveryMode = deliverycmd.ModeMarkdown
	DeliveryModeDraftPlain DeliveryMode = deliverycmd.ModeDraftPlain
	DeliveryModeChatAction DeliveryMode = deliverycmd.ModeChatAction
)

type taskActorExecutor struct {
	tasks      *swarm.TaskService
	dispatcher swarm.ActorDispatcher
	sessions   *baldasession.Manager
}

type taskActorExecutorParams struct {
	fx.In

	TaskService *swarm.TaskService
	Dispatcher  swarm.ActorDispatcher
	Sessions    *baldasession.Manager `optional:"true"`
}

func WebhookTaskEnvelope(payload SessionTurnPayload, routeName string, requestID string) (swarm.Envelope, string, error) {
	dedupeBase := strings.TrimSpace(payload.DedupeKey)
	dedupeBase = strings.TrimSuffix(dedupeBase, ":task")
	dedupeBase = strings.TrimSuffix(dedupeBase, ":session")
	if dedupeBase == "" {
		dedupeBase = strings.Join([]string{"webhook", strings.TrimSpace(routeName), strings.TrimSpace(requestID)}, ":")
	}
	trimmedRoute := strings.ToLower(strings.TrimSpace(routeName))
	var routePart strings.Builder
	lastDash := false
	for _, r := range trimmedRoute {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			routePart.WriteRune(r)
			lastDash = false
		case r == '-' || r == '_':
			routePart.WriteRune(r)
			lastDash = false
		default:
			if routePart.Len() > 0 && !lastDash {
				routePart.WriteByte('-')
				lastDash = true
			}
		}
		if routePart.Len() >= 48 {
			break
		}
	}
	part := strings.Trim(routePart.String(), "-_")
	if part == "" {
		part = "inbound"
	}
	taskID := "webhook-" + part + "-" + shortTaskHash(dedupeBase)
	payload.DedupeKey = dedupeBase + ":session"
	data, err := json.Marshal(taskEnvelopePayload{
		Kind:        taskPayloadKindSessionTurn,
		SessionTurn: &payload,
	})
	if err != nil {
		return swarm.Envelope{}, "", fmt.Errorf("encode webhook task payload: %w", err)
	}
	return swarm.Envelope{
		ID:          uuid.NewString(),
		Namespace:   swarm.NamespaceWebhookInbound,
		Kind:        swarm.KindWebhookEvent,
		From:        swarm.ActorAddress{Target: "webhook", Key: firstNonEmpty(routeName, requestID, "inbound")},
		To:          swarm.ActorAddress{Target: swarm.ActorTypeTask, Key: taskID},
		SessionID:   payload.Locator.SessionID,
		TaskID:      taskID,
		Priority:    80,
		DedupeKey:   dedupeBase + ":task",
		PayloadJSON: string(data),
	}, taskID, nil
}

func PromptTurnTaskEnvelope(payload SessionTurnPayload) (swarm.Envelope, string, error) {
	dedupeBase := promptTurnDedupeBase(payload)
	if dedupeBase == "" {
		return swarm.Envelope{}, "", fmt.Errorf("prompt turn dedupe key is required")
	}
	taskID := "turn-" + shortTaskHash(dedupeBase)
	payload.DedupeKey = dedupeBase + ":session"
	data, err := json.Marshal(taskEnvelopePayload{
		Kind:        taskPayloadKindSessionTurn,
		SessionTurn: &payload,
	})
	if err != nil {
		return swarm.Envelope{}, "", fmt.Errorf("encode prompt turn task payload: %w", err)
	}
	return swarm.Envelope{
		ID:          uuid.NewString(),
		Namespace:   swarm.NamespaceHumanInbound,
		Kind:        swarm.KindMessage,
		From:        swarm.ActorAddress{Target: firstNonEmpty(payload.Source, sessionTurnSourceTelegram), Key: firstNonEmpty(payload.UserID, payload.Locator.AddressKey, "unknown")},
		To:          swarm.ActorAddress{Target: swarm.ActorTypeTask, Key: taskID},
		SessionID:   payload.Locator.SessionID,
		TaskID:      taskID,
		Priority:    90,
		DedupeKey:   dedupeBase + ":task",
		PayloadJSON: string(data),
	}, taskID, nil
}

func ScheduledTaskEnvelope(
	scheduledTaskID string,
	content string,
	locator baldasession.SessionLocator,
	reportTo *baldasession.SessionLocator,
	userID string,
	topicID int,
	dispatchKey string,
) (swarm.Envelope, error) {
	payload := taskEnvelopePayload{
		Kind: taskPayloadKindScheduledTask,
		ScheduledTask: &scheduledTaskPayload{
			TaskID:   strings.TrimSpace(scheduledTaskID),
			Content:  content,
			Locator:  locator,
			ReportTo: reportTo,
			UserID:   strings.TrimSpace(userID),
			TopicID:  topicID,
		},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return swarm.Envelope{}, fmt.Errorf("encode scheduled task task: %w", err)
	}
	taskID := "scheduled-" + strings.TrimSpace(scheduledTaskID) + "-" + strings.TrimSpace(dispatchKey)
	return swarm.Envelope{
		ID:          uuid.NewString(),
		Namespace:   swarm.NamespaceScheduleInbound,
		Kind:        swarm.KindScheduledTask,
		From:        swarm.ActorAddress{Target: "schedule", Key: strings.TrimSpace(scheduledTaskID)},
		To:          swarm.ActorAddress{Target: swarm.ActorTypeTask, Key: taskID},
		SessionID:   locator.SessionID,
		TaskID:      taskID,
		DedupeKey:   strings.TrimSpace(dispatchKey),
		PayloadJSON: string(data),
	}, nil
}

func shortTaskHash(value string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(value)))
	return hex.EncodeToString(sum[:])[:16]
}

func promptTurnDedupeBase(payload SessionTurnPayload) string {
	base := strings.TrimSpace(payload.DedupeKey)
	base = strings.TrimSuffix(base, ":task")
	base = strings.TrimSuffix(base, ":session")
	if base != "" {
		return base
	}
	parts := []string{
		firstNonEmpty(payload.Source, sessionTurnSourceTelegram),
		strings.TrimSpace(payload.Locator.SessionID),
		strconv.Itoa(payload.MessageID),
		strings.TrimSpace(payload.Text),
	}
	return shortTaskHash(strings.Join(parts, "\x00"))
}

func (e *taskActorExecutor) Address() string {
	return swarm.WildcardAddress(swarm.ActorTypeTask)
}

func (e *taskActorExecutor) Handle(ctx context.Context, envelope any) error {
	env, err := swarm.AssertEnvelope(envelope)
	if err != nil {
		return err
	}
	var payload taskEnvelopePayload
	if err := json.Unmarshal([]byte(env.PayloadJSON), &payload); err != nil {
		return swarm.PermanentError(fmt.Errorf("decode task payload: %w", err))
	}
	switch strings.TrimSpace(payload.Kind) {
	case "goal":
		return swarm.PolicyError(fmt.Errorf("goal tasks are handled by goal actor"))
	case taskPayloadKindScheduledTask:
		if payload.ScheduledTask == nil {
			return swarm.PolicyError(fmt.Errorf("scheduled task payload is required"))
		}
		return e.startScheduledTaskTask(ctx, env, *payload.ScheduledTask)
	case taskPayloadKindSessionTurn:
		if payload.SessionTurn == nil {
			return swarm.PolicyError(fmt.Errorf("session turn task payload is required"))
		}
		return e.dispatchSessionTurn(ctx, env, *payload.SessionTurn)
	default:
		return swarm.PolicyError(fmt.Errorf("unsupported task payload kind %q", payload.Kind))
	}
}

func (e *taskActorExecutor) dispatchSessionTurn(ctx context.Context, env swarm.Envelope, payload SessionTurnPayload) error {
	taskID := firstNonEmpty(env.TaskID, env.To.Key)
	if taskID != "" && e.tasks != nil {
		if _, ok, err := e.tasks.Get(ctx, taskID); err != nil {
			return swarm.TransientError(err)
		} else if ok {
			return nil
		}
		created, err := e.tasks.Create(ctx, baldastate.SwarmTaskRecord{
			ID:            taskID,
			SessionID:     strings.TrimSpace(payload.Locator.SessionID),
			ParentTaskID:  strings.TrimSpace(payload.ParentTaskID),
			Title:         sessionTurnTaskTitle(payload),
			Objective:     strings.TrimSpace(payload.Text),
			Status:        baldastate.SwarmTaskStatusCreated,
			OwnerActor:    swarm.ActorTypeTask + ":" + taskID,
			AssignedActor: swarm.ActorTypeSession + ":" + payload.Locator.SessionID,
			Priority:      sessionTurnTaskPriority(payload),
			CreatedBy:     strings.TrimSpace(payload.UserID),
		}, "task.actor", payload)
		if err != nil {
			return swarm.TransientError(err)
		}
		if !created {
			return nil
		}
	}
	payload.TaskID = taskID
	sessionEnv, err := SessionTurnEnvelope(payload)
	if err != nil {
		return swarm.PermanentError(err)
	}
	sessionEnv.TaskID = taskID
	sessionEnv.CorrelationID = firstNonEmpty(env.CorrelationID, taskID)
	sessionEnv.CausationID = env.ID
	if strings.TrimSpace(sessionEnv.DedupeKey) != "" {
		sessionEnv.ID = sessionEnv.DedupeKey
	}
	if _, err := e.dispatcher.Dispatch(ctx, sessionEnv); err != nil {
		return swarm.TransientError(err)
	}
	if taskID != "" && e.tasks != nil {
		if err := e.tasks.MarkStatus(ctx, taskID, baldastate.SwarmTaskStatusRunning, "task.actor", env.ID, "", nil); err != nil {
			return swarm.TransientError(err)
		}
	}
	return nil
}

func sessionTurnTaskTitle(payload SessionTurnPayload) string {
	switch strings.ToLower(strings.TrimSpace(payload.Source)) {
	case sessionTurnSourceWebhook:
		return "Webhook task"
	case sessionTurnSourceSchedule:
		return "Scheduled task"
	case "agent":
		return "Agent task"
	default:
		return "Prompt turn"
	}
}

func sessionTurnTaskPriority(payload SessionTurnPayload) int {
	switch strings.ToLower(strings.TrimSpace(payload.Source)) {
	case sessionTurnSourceWebhook:
		return 80
	case sessionTurnSourceSchedule:
		return 50
	default:
		return 90
	}
}

func (e *taskActorExecutor) startScheduledTaskTask(ctx context.Context, env swarm.Envelope, payload scheduledTaskPayload) error {
	taskID := firstNonEmpty(env.TaskID, env.To.Key)
	content := strings.TrimSpace(payload.Content)
	if taskID == "" {
		return swarm.PolicyError(fmt.Errorf("task id is required"))
	}
	if strings.TrimSpace(payload.TaskID) == "" {
		return swarm.PolicyError(fmt.Errorf("scheduled task id is required"))
	}
	if content == "" {
		return swarm.PolicyError(fmt.Errorf("scheduled task content is required"))
	}
	if e.tasks != nil {
		if _, ok, err := e.tasks.Get(ctx, taskID); err != nil {
			return swarm.TransientError(err)
		} else if ok {
			return nil
		}
		created, err := e.tasks.Create(ctx, baldastate.SwarmTaskRecord{
			ID:            taskID,
			SessionID:     strings.TrimSpace(payload.Locator.SessionID),
			ParentTaskID:  strings.TrimSpace(payload.ParentTaskID),
			Title:         "Scheduled task: " + strings.TrimSpace(payload.TaskID),
			Objective:     content,
			Status:        baldastate.SwarmTaskStatusCreated,
			OwnerActor:    swarm.ActorTypeTask + ":" + taskID,
			AssignedActor: swarm.ActorTypeSession + ":" + payload.Locator.SessionID,
			Priority:      50,
			CreatedBy:     strings.TrimSpace(payload.UserID),
		}, "task.actor", payload)
		if err != nil {
			return swarm.TransientError(err)
		}
		if !created {
			return nil
		}
	}
	sessionPayload := SessionTurnPayload{
		Text:            content,
		Locator:         payload.Locator,
		ReportTo:        payload.ReportTo,
		ParentTaskID:    strings.TrimSpace(payload.ParentTaskID),
		UserID:          payload.UserID,
		ScheduledTaskID: payload.TaskID,
		TopicID:         payload.TopicID,
		DeliveryOptions: deliveryfmt.Options{
			Profile: deliveryfmt.Profile{Format: deliveryfmt.FormatAuto},
		},
		Deliver:   payload.ReportTo != nil,
		Source:    sessionTurnSourceSchedule,
		DedupeKey: firstNonEmpty(env.DedupeKey, taskID) + ":session",
	}
	sessionEnv, err := SessionTurnEnvelope(sessionPayload)
	if err != nil {
		return swarm.PermanentError(err)
	}
	sessionEnv.TaskID = taskID
	sessionEnv.CorrelationID = firstNonEmpty(env.CorrelationID, taskID)
	sessionEnv.CausationID = env.ID
	if strings.TrimSpace(sessionEnv.DedupeKey) != "" {
		sessionEnv.ID = sessionEnv.DedupeKey
	}
	if _, err := e.dispatcher.Dispatch(ctx, sessionEnv); err != nil {
		return swarm.TransientError(err)
	}
	if e.tasks != nil {
		if err := e.tasks.MarkStatus(ctx, taskID, baldastate.SwarmTaskStatusRunning, "task.actor", env.ID, "", nil); err != nil {
			return swarm.TransientError(err)
		}
	}
	return nil
}
