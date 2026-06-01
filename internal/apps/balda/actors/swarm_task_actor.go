package actors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"go.uber.org/fx"
)

const (
	taskPayloadKindGoal          = "goal"
	taskPayloadKindScheduledTask = "scheduled_task"
	taskPayloadKindSessionTurn   = "session_turn"
	taskPayloadKindDelivery      = "delivery"

	taskResultSchemaVersionV1          = "task_result.v1"
	taskReviewableOutcomeSchemaVersion = "task_reviewable_outcome.v1"

	taskMemoryScopeCompleted   = "task.completed"
	taskMemoryOperationSummary = "task_summary"
	taskMemoryOperationFacts   = "fact_extract"
	taskMemoryOperationContext = "context_pack"
	taskMemoryActorKeyGlobal   = "global"
)

type taskEnvelopePayload struct {
	Kind          string                `json:"kind"`
	Goal          *goalTaskPayload      `json:"goal,omitempty"`
	ScheduledTask *scheduledTaskPayload `json:"scheduled_task,omitempty"`
	SessionTurn   *SessionTurnPayload   `json:"session_turn,omitempty"`
}

type goalTaskPayload struct {
	TaskID          string                      `json:"task_id,omitempty"`
	Locator         baldasession.SessionLocator `json:"locator"`
	Objective       string                      `json:"objective"`
	TransportUserID string                      `json:"transport_user_id"`
	MaxIterations   int                         `json:"max_iterations,omitempty"`
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

type taskDeliveryPayload struct {
	TaskID  string                      `json:"task_id"`
	Locator baldasession.SessionLocator `json:"locator"`
	Text    string                      `json:"text"`
}

type taskResultPayloadV1 struct {
	SchemaVersion     string                  `json:"schema_version"`
	GoalReached       bool                    `json:"goal_reached"`
	Iterations        int                     `json:"iterations"`
	PlannerOutput     string                  `json:"planner_output,omitempty"`
	ExecutorOutput    string                  `json:"executor_output,omitempty"`
	ReviewerOutput    string                  `json:"reviewer_output,omitempty"`
	ReviewerNotes     string                  `json:"reviewer_feedback,omitempty"`
	Artifacts         *taskArtifactResultV1   `json:"artifacts,omitempty"`
	Export            *taskExportResultV1     `json:"export,omitempty"`
	ReviewableOutcome taskReviewableOutcomeV1 `json:"reviewable_outcome"`
}

type taskArtifactResultV1 struct {
	WorkspaceDir string   `json:"workspace_dir,omitempty"`
	BranchName   string   `json:"branch_name,omitempty"`
	Commit       string   `json:"commit,omitempty"`
	ChangedFiles []string `json:"changed_files,omitempty"`
	GitError     string   `json:"git_error,omitempty"`
}

type taskExportResultV1 struct {
	Status        string `json:"status,omitempty"`
	CommitMessage string `json:"commit_message,omitempty"`
	BaseCommit    string `json:"base_commit,omitempty"`
	Error         string `json:"error,omitempty"`
}

type taskReviewableOutcomeV1 struct {
	SchemaVersion string `json:"schema_version"`
	WhatWasDone   string `json:"what_was_done,omitempty"`
	Validation    string `json:"validation_output,omitempty"`
	Verified      string `json:"what_was_verified,omitempty"`
	NotVerified   string `json:"what_was_not_verified,omitempty"`
	NextAction    string `json:"next_action,omitempty"`
}

type taskMemorySyncPayload struct {
	Operation string `json:"operation,omitempty"`
	Scope     string `json:"scope,omitempty"`
	TaskID    string `json:"task_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	Content   string `json:"content,omitempty"`
}

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
	case taskPayloadKindGoal:
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
			Title:         "Webhook task",
			Objective:     strings.TrimSpace(payload.Text),
			Status:        baldastate.SwarmTaskStatusCreated,
			OwnerActor:    swarm.ActorTypeTask + ":" + taskID,
			AssignedActor: swarm.ActorTypeSession + ":" + payload.Locator.SessionID,
			Priority:      80,
			CreatedBy:     strings.TrimSpace(payload.UserID),
		}, "task.actor", payload)
		if err != nil {
			return swarm.TransientError(err)
		}
		if !created {
			return nil
		}
	}
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
		Deliver:         payload.ReportTo != nil,
		Source:          sessionTurnSourceSchedule,
		DedupeKey:       firstNonEmpty(env.DedupeKey, taskID) + ":session",
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

func reviewerPassed(text string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(text)), "verdict: pass")
}
