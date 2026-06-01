package goalkeeper

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/normahq/balda/internal/apps/balda/progress"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/normahq/balda/internal/git"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
	adkagent "google.golang.org/adk/agent"
	adkrunner "google.golang.org/adk/runner"
	adksession "google.golang.org/adk/session"
	"google.golang.org/genai"
)

const (
	actorName                     = "goalkeeper.actor"
	ownerSessionLabel             = "balda"
	defaultGoalMaxIterations      = 25
	goalExportStatusFailed        = "export_failed"
	MetadataEventKey              = "norma.goal.event"
	MetadataStepKey               = "norma.goal.step"
	StepStarted                   = "step_started"
	StepCompleted                 = "step_completed"
	StepFailed                    = "step_failed"
	WorkerStep                    = "worker"
	ValidatorStep                 = "validator"
	taskPayloadKindGoal           = "goal"
	taskPayloadKindDelivery       = "delivery"
	taskResultSchemaVersionV1     = "task_result.v1"
	taskReviewableOutcomeSchemaV1 = "task_reviewable_outcome.v1"
	taskMemoryScopeCompleted      = "task.completed"
	taskMemoryOperationSummary    = "task_summary"
	taskMemoryOperationFacts      = "fact_extract"
	taskMemoryOperationContext    = "context_pack"
	taskMemoryActorKeyGlobal      = "global"
	progressKindPlan              = "plan"
	progressKindOutput            = "output"
	progressKindCompleted         = "completed"
)

var (
	secretBearerHeaderPattern = regexp.MustCompile(`(?i)(authorization\s*:\s*bearer\s+)([^\s]+)`)
	secretKeyValuePattern     = regexp.MustCompile(`(?i)\b(token|secret|password|api[_-]?key|access[_-]?key|private[_-]?key)\b(\s*[:=]\s*)([^\s,;]+)`)
	secretPEMPattern          = regexp.MustCompile(`(?s)-----BEGIN [^-]+-----.*?-----END [^-]+-----`)
	secretGitHubTokenPattern  = regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9_]{20,}\b`)
	secretTelegramToken       = regexp.MustCompile(`\b\d{6,10}:[A-Za-z0-9_-]{20,}\b`)
)

type GoalRuntimeBuilder interface {
	BuildGoalRuntime(ctx context.Context, cfg GoalRuntimeConfig) (GoalRuntime, error)
}

type GoalRuntimeConfig struct {
	SourceSessionID string
	TaskID          string
	UserID          string
	MaxIterations   uint
}

type GoalRunner interface {
	Run(ctx context.Context, userID string, sessionID string, userContent *genai.Content, cfg adkagent.RunConfig, opts ...adkrunner.RunOption) iter.Seq2[*adksession.Event, error]
}

type GoalRuntime interface {
	Runner() GoalRunner
	SessionID() string
	WorkspaceDir() string
	BranchName() string
	Close() error
	CleanupResources(ctx context.Context) error
	BuildCommitMessage(ctx context.Context, objective string, workerOutput string, validatorOutput string) (string, error)
	ExportWorkspace(ctx context.Context, commitMessage string) error
}

type TaskRuns interface {
	Register(taskID string, cancel context.CancelFunc) string
	Unregister(taskID string, runID string)
}

type ActorParams struct {
	fx.In

	TaskService        *swarm.TaskService
	Dispatcher         swarm.ActorDispatcher
	SessionManager     *baldasession.Manager
	RuntimeBuilder     GoalRuntimeBuilder
	TaskRuns           TaskRuns
	MaxIterations      int  `name:"balda_goal_max_iterations"`
	PlanUpdatesEnabled bool `name:"balda_telegram_plan_updates"`
	Logger             zerolog.Logger
}

type Actor struct {
	tasks              *swarm.TaskService
	dispatcher         swarm.ActorDispatcher
	sessions           *baldasession.Manager
	runtimeBuilder     GoalRuntimeBuilder
	taskRuns           TaskRuns
	maxIters           int
	planUpdatesEnabled bool
	logger             zerolog.Logger
}

type taskEnvelopePayload struct {
	Kind string           `json:"kind"`
	Goal *goalTaskPayload `json:"goal,omitempty"`
}

type goalTaskPayload struct {
	TaskID          string                      `json:"task_id,omitempty"`
	Locator         baldasession.SessionLocator `json:"locator"`
	Objective       string                      `json:"objective"`
	TransportUserID string                      `json:"transport_user_id"`
	MaxIterations   int                         `json:"max_iterations,omitempty"`
}

type taskDeliveryPayload struct {
	TaskID  string                      `json:"task_id"`
	Locator baldasession.SessionLocator `json:"locator"`
	Text    string                      `json:"text"`
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

type taskResultPayloadV1 struct {
	SchemaVersion     string                  `json:"schema_version"`
	GoalReached       bool                    `json:"goal_reached"`
	Iterations        int                     `json:"iterations"`
	ExecutorOutput    string                  `json:"executor_output,omitempty"`
	ReviewerOutput    string                  `json:"reviewer_output,omitempty"`
	ReviewerNotes     string                  `json:"reviewer_feedback,omitempty"`
	Artifacts         *taskArtifactResultV1   `json:"artifacts,omitempty"`
	Export            *taskExportResultV1     `json:"export,omitempty"`
	ReviewableOutcome taskReviewableOutcomeV1 `json:"reviewable_outcome"`
}

type taskArtifactSnapshot struct {
	WorkspaceDir string
	BranchName   string
	Commit       string
	ChangedFiles []string
	GitError     string
}

type taskMemorySyncPayload struct {
	Operation string `json:"operation,omitempty"`
	Scope     string `json:"scope,omitempty"`
	TaskID    string `json:"task_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	Content   string `json:"content,omitempty"`
}

type goalRunResult struct {
	payload         goalTaskPayload
	iterations      int
	workerOutput    string
	validatorOutput string
	finalText       string
}

type stepProgressState struct {
	lastVisibleText string
	lastPlanText    string
	deliveredOutput bool
}

func NewActor(params ActorParams) *Actor {
	return &Actor{
		tasks:              params.TaskService,
		dispatcher:         params.Dispatcher,
		sessions:           params.SessionManager,
		runtimeBuilder:     params.RuntimeBuilder,
		taskRuns:           params.TaskRuns,
		maxIters:           normalizeGoalMaxIterations(params.MaxIterations),
		planUpdatesEnabled: params.PlanUpdatesEnabled,
		logger:             params.Logger.With().Str("component", "balda.goalkeeper_actor").Logger(),
	}
}

func (a *Actor) Address() string {
	return swarm.WildcardAddress(swarm.ActorTypeGoalkeeper)
}

func (a *Actor) Handle(ctx context.Context, envelope any) error {
	env, err := swarm.AssertEnvelope(envelope)
	if err != nil {
		return err
	}
	if strings.TrimSpace(env.Namespace) != swarm.NamespaceGoalkeeperCommand {
		return swarm.PolicyError(fmt.Errorf("unsupported goal namespace %q", env.Namespace))
	}
	var payload taskEnvelopePayload
	if err := json.Unmarshal([]byte(env.PayloadJSON), &payload); err != nil {
		return swarm.PermanentError(fmt.Errorf("decode goal payload: %w", err))
	}
	if strings.TrimSpace(payload.Kind) != taskPayloadKindGoal || payload.Goal == nil {
		return swarm.PolicyError(fmt.Errorf("goal payload is required"))
	}
	return a.runGoal(ctx, env, *payload.Goal)
}

func GoalTaskEnvelope(
	locator baldasession.SessionLocator,
	objective string,
	transportUserID string,
	maxIterations int,
) (swarm.Envelope, error) {
	taskID := "goal-" + locator.SessionID + "-" + uuid.NewString()
	payload := taskEnvelopePayload{
		Kind: taskPayloadKindGoal,
		Goal: &goalTaskPayload{
			TaskID:          taskID,
			Locator:         locator,
			Objective:       strings.TrimSpace(objective),
			TransportUserID: strings.TrimSpace(transportUserID),
			MaxIterations:   normalizeGoalMaxIterations(maxIterations),
		},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return swarm.Envelope{}, fmt.Errorf("encode goal task payload: %w", err)
	}
	return swarm.Envelope{
		ID:          uuid.NewString(),
		Namespace:   swarm.NamespaceGoalkeeperCommand,
		Kind:        swarm.KindGoal,
		From:        swarm.ActorAddress{Target: "telegram", Key: firstNonEmpty(transportUserID, locator.AddressKey, "unknown")},
		To:          swarm.ActorAddress{Target: swarm.ActorTypeGoalkeeper, Key: taskID},
		SessionID:   locator.SessionID,
		TaskID:      taskID,
		Priority:    90,
		PayloadJSON: string(data),
	}, nil
}

func (a *Actor) runGoal(ctx context.Context, env swarm.Envelope, payload goalTaskPayload) error {
	taskID := firstNonEmpty(payload.TaskID, env.TaskID, env.To.Key)
	objective := strings.TrimSpace(payload.Objective)
	if taskID == "" {
		return swarm.PolicyError(fmt.Errorf("task id is required"))
	}
	if objective == "" {
		return swarm.PolicyError(fmt.Errorf("goal objective is required"))
	}
	if a.taskStatusIs(ctx, taskID, baldastate.SwarmTaskStatusCompleted, baldastate.SwarmTaskStatusFailed, baldastate.SwarmTaskStatusCanceled, baldastate.SwarmTaskStatusDeadLettered) {
		return nil
	}
	maxIterations := normalizeGoalMaxIterations(payload.MaxIterations)
	if maxIterations == defaultGoalMaxIterations && a.maxIters != defaultGoalMaxIterations {
		maxIterations = a.maxIters
	}
	payload.TaskID = taskID
	payload.Objective = objective
	payload.MaxIterations = maxIterations

	if err := a.ensureGoalTask(ctx, payload); err != nil {
		return err
	}
	ts, err := a.resolveSession(ctx, payload)
	if err != nil {
		return swarm.TransientError(err)
	}
	if err := a.tasks.MarkStatus(ctx, taskID, baldastate.SwarmTaskStatusRunning, actorName, env.ID, "", map[string]any{
		"objective": objective,
	}); err != nil {
		return swarm.TransientError(err)
	}
	if err := a.deliver(ctx, taskID, payload.Locator, fmt.Sprintf("Goal run started. Max iterations: %d.\n\nGoal: %s", maxIterations, objective), "started"); err != nil {
		return err
	}

	if a.runtimeBuilder == nil {
		return swarm.TransientError(fmt.Errorf("goal runtime builder is required"))
	}
	runtime, err := a.runtimeBuilder.BuildGoalRuntime(ctx, GoalRuntimeConfig{
		SourceSessionID: payload.Locator.SessionID,
		TaskID:          taskID,
		UserID:          ts.GetUserID(),
		MaxIterations:   uint(maxIterations),
	})
	if err != nil {
		return swarm.TransientError(err)
	}
	defer func() {
		if err := runtime.Close(); err != nil {
			a.logger.Warn().Err(err).Str("task_id", taskID).Msg("failed to close goal runtime")
		}
	}()

	runCtx, cancel := context.WithCancel(ctx)
	runID := ""
	if a.taskRuns != nil {
		runID = a.taskRuns.Register(taskID, cancel)
		defer a.taskRuns.Unregister(taskID, runID)
	}
	defer cancel()

	result, err := a.runWorkflow(runCtx, runtime, ts.GetUserID(), runtime.SessionID(), payload)
	artifacts := snapshotGoalRuntimeArtifacts(ctx, runtime)
	if err != nil {
		if errors.Is(runCtx.Err(), context.Canceled) {
			if cleanupErr := runtime.CleanupResources(ctx); cleanupErr != nil {
				a.logger.Warn().Err(cleanupErr).Str("task_id", taskID).Msg("failed to cleanup canceled goal runtime")
			}
			if setErr := a.tasks.SetResult(ctx, taskID, result.toTaskResult(false, artifacts, &taskExportResultV1{Status: "canceled"}), baldastate.SwarmTaskStatusCanceled, actorName, "goal run canceled"); setErr != nil {
				return swarm.TransientError(setErr)
			}
			return a.deliver(ctx, taskID, payload.Locator, "Goal run canceled.", "canceled")
		}
		reason := redactSecrets(err.Error())
		if cleanupErr := runtime.CleanupResources(ctx); cleanupErr != nil {
			a.logger.Warn().Err(cleanupErr).Str("task_id", taskID).Msg("failed to cleanup failed goal runtime")
		}
		if setErr := a.tasks.SetResult(ctx, taskID, result.toTaskResult(false, artifacts, &taskExportResultV1{Status: "failed", Error: reason}), baldastate.SwarmTaskStatusFailed, actorName, reason); setErr != nil {
			return swarm.TransientError(setErr)
		}
		return a.deliver(ctx, taskID, payload.Locator, "Goal run failed: "+reason, "failed")
	}
	if reviewerPassed(result.validatorOutput) {
		commitMessage, commitErr := runtime.BuildCommitMessage(ctx, payload.Objective, result.workerOutput, result.validatorOutput)
		if commitErr != nil {
			a.logger.Warn().Err(commitErr).Str("task_id", taskID).Msg("failed to generate goal export commit message; using fallback")
		}
		exportSummary := &taskExportResultV1{
			Status:        "pending",
			CommitMessage: commitMessage,
		}
		if exportErr := runtime.ExportWorkspace(ctx, commitMessage); exportErr != nil {
			exportSummary.Status = goalExportStatusFailed
			exportSummary.Error = redactSecrets(exportErr.Error())
			taskResult := result.toTaskResult(true, artifacts, exportSummary)
			if setErr := a.tasks.SetResult(ctx, taskID, taskResult, baldastate.SwarmTaskStatusFailed, actorName, exportSummary.Error); setErr != nil {
				return swarm.TransientError(setErr)
			}
			return a.deliver(ctx, taskID, payload.Locator, a.renderTaskOutcome(ctx, taskID, "Goal validation passed, but export failed."), "export-failed")
		}
		exportSummary.Status = "exported"
		taskResult := result.toTaskResult(true, artifacts, exportSummary)
		if err := a.tasks.SetResult(ctx, taskID, taskResult, baldastate.SwarmTaskStatusCompleted, actorName, ""); err != nil {
			return swarm.TransientError(err)
		}
		if err := a.enqueueTaskCompletionMemorySync(ctx, payload, taskResult); err != nil {
			return err
		}
		if cleanupErr := runtime.CleanupResources(ctx); cleanupErr != nil {
			a.logger.Warn().Err(cleanupErr).Str("task_id", taskID).Msg("failed to cleanup completed goal runtime")
		}
		return a.deliver(ctx, taskID, payload.Locator, a.renderTaskOutcome(ctx, taskID, "Goal run completed."), "completed")
	}
	if cleanupErr := runtime.CleanupResources(ctx); cleanupErr != nil {
		a.logger.Warn().Err(cleanupErr).Str("task_id", taskID).Msg("failed to cleanup max-iteration goal runtime")
	}
	taskResult := result.toTaskResult(false, artifacts, &taskExportResultV1{Status: "not_exported"})
	if err := a.tasks.SetResult(ctx, taskID, taskResult, baldastate.SwarmTaskStatusFailed, actorName, "max iterations reached"); err != nil {
		return swarm.TransientError(err)
	}
	return a.deliver(ctx, taskID, payload.Locator, a.renderTaskOutcome(ctx, taskID, "Goal run reached max iterations without passing validation."), "max-iterations")
}

func (a *Actor) ensureGoalTask(ctx context.Context, payload goalTaskPayload) error {
	if a.tasks == nil {
		return swarm.TransientError(fmt.Errorf("task service is required"))
	}
	title := strings.TrimSpace(payload.Objective)
	if title != "" {
		const maxTitleRunes = 80
		runes := []rune(title)
		if len(runes) > maxTitleRunes {
			title = strings.TrimSpace(string(runes[:maxTitleRunes])) + "..."
		}
		title = "Goal: " + title
	} else {
		title = "Goal"
	}
	record := baldastate.SwarmTaskRecord{
		ID:            strings.TrimSpace(payload.TaskID),
		SessionID:     strings.TrimSpace(payload.Locator.SessionID),
		Title:         title,
		Objective:     strings.TrimSpace(payload.Objective),
		Status:        baldastate.SwarmTaskStatusCreated,
		OwnerActor:    swarm.ActorTypeGoalkeeper + ":" + strings.TrimSpace(payload.TaskID),
		AssignedActor: swarm.ActorTypeGoalkeeper + ":" + strings.TrimSpace(payload.TaskID),
		Priority:      90,
		CreatedBy:     strings.TrimSpace(payload.TransportUserID),
	}
	if _, err := a.tasks.Create(ctx, record, actorName, payload); err != nil {
		return swarm.TransientError(err)
	}
	task, ok, err := a.tasks.Get(ctx, payload.TaskID)
	if err != nil {
		return swarm.TransientError(err)
	}
	if !ok {
		return swarm.TransientError(fmt.Errorf("goal task %q was not persisted", payload.TaskID))
	}
	switch strings.TrimSpace(task.Status) {
	case "", baldastate.SwarmTaskStatusCreated, baldastate.SwarmTaskStatusQueued:
		return a.tasks.MarkStatus(ctx, payload.TaskID, baldastate.SwarmTaskStatusQueued, actorName, "", "", nil)
	default:
		return nil
	}
}

func (a *Actor) resolveSession(ctx context.Context, payload goalTaskPayload) (*baldasession.TopicSession, error) {
	if a.sessions == nil {
		return nil, fmt.Errorf("session manager is required")
	}
	ts, err := a.sessions.GetSession(payload.Locator)
	if err == nil {
		return ts, nil
	}
	userID := strings.TrimSpace(payload.TransportUserID)
	if userID == "" {
		return nil, fmt.Errorf("restore user id is required")
	}
	sessionCtx := baldasession.SessionContext{Locator: payload.Locator, UserID: userID}
	ts, err = a.sessions.RestoreSession(ctx, sessionCtx)
	if err == nil {
		return ts, nil
	}
	if !errors.Is(err, baldasession.ErrNoPersistedSession) {
		return nil, fmt.Errorf("restore session for goal: %w", err)
	}
	ts, err = a.sessions.EnsureSession(ctx, sessionCtx, ownerSessionLabel)
	if err != nil {
		return nil, fmt.Errorf("create session for goal: %w", err)
	}
	return ts, nil
}

func (a *Actor) runWorkflow(
	ctx context.Context,
	runtime GoalRuntime,
	userID string,
	agentSessionID string,
	payload goalTaskPayload,
) (goalRunResult, error) {
	result := goalRunResult{payload: payload}
	if runtime == nil || runtime.Runner() == nil {
		return result, fmt.Errorf("goal runner is required")
	}
	userContent := genai.NewContentFromText("Goal:\n"+strings.TrimSpace(payload.Objective), genai.RoleUser)
	currentStep := ""
	sawTurnComplete := false
	deliverySeq := 0
	stepStates := map[string]*stepProgressState{
		WorkerStep:    {},
		ValidatorStep: {},
	}
	for ev, err := range runtime.Runner().Run(ctx, userID, agentSessionID, userContent, adkagent.RunConfig{}) {
		if err != nil {
			return result, fmt.Errorf("run goal workflow: %w", err)
		}
		if ev == nil {
			continue
		}
		iteration := result.iterations + 1
		if len(ev.CustomMetadata) != 0 {
			eventType, _ := ev.CustomMetadata[MetadataEventKey].(string)
			step, _ := ev.CustomMetadata[MetadataStepKey].(string)
			eventType = strings.TrimSpace(eventType)
			step = strings.TrimSpace(step)
			if eventType != "" && step != "" {
				switch eventType {
				case StepStarted:
					currentStep = step
					if err := a.recordStepStarted(ctx, payload, step, iteration); err != nil {
						return result, err
					}
				case StepCompleted:
					if err := a.recordStepCompleted(ctx, payload, step, iteration, stepStates[step], &deliverySeq); err != nil {
						return result, err
					}
					if step == ValidatorStep {
						result.iterations++
					}
					currentStep = ""
				case StepFailed:
					return result, fmt.Errorf("%s step failed", step)
				}
			}
		}
		if currentStep == "" {
			if ev.TurnComplete {
				sawTurnComplete = true
			}
			continue
		}
		state := stepStates[currentStep]
		if state == nil {
			state = &stepProgressState{}
			stepStates[currentStep] = state
		}
		if a.planUpdatesEnabled {
			if planText, ok := progress.PlanUpdateText(ev); ok && planText != "" && planText != state.lastPlanText {
				state.lastPlanText = planText
				message := fmt.Sprintf("Goal iteration %d/%d: %s plan update.\n\n%s", iteration, normalizeGoalMaxIterations(payload.MaxIterations), currentStep, planText)
				if err := a.recordStepProgress(ctx, payload, currentStep, iteration, progressKindPlan, message, &deliverySeq); err != nil {
					return result, err
				}
			}
		}
		text := visibleText(ev)
		if text != "" && text != state.lastVisibleText {
			state.lastVisibleText = text
			result.finalText = appendVisibleText(result.finalText, text)
			switch currentStep {
			case WorkerStep:
				result.workerOutput = appendVisibleText(result.workerOutput, text)
			case ValidatorStep:
				result.validatorOutput = appendVisibleText(result.validatorOutput, text)
			}
			message := fmt.Sprintf("Goal iteration %d/%d: %s update.\n\n%s", iteration, normalizeGoalMaxIterations(payload.MaxIterations), currentStep, text)
			if err := a.recordStepProgress(ctx, payload, currentStep, iteration, progressKindOutput, message, &deliverySeq); err != nil {
				return result, err
			}
			state.deliveredOutput = true
		}
		if ev.TurnComplete {
			sawTurnComplete = true
		}
	}
	if result.iterations == 0 {
		result.iterations = 1
	}
	if !sawTurnComplete {
		return result, fmt.Errorf("goal workflow ended without completion")
	}
	return result, nil
}

func (a *Actor) recordStepStarted(ctx context.Context, payload goalTaskPayload, step string, iteration int) error {
	status := baldastate.SwarmTaskStatusWaitingForAgent
	if step == ValidatorStep {
		status = baldastate.SwarmTaskStatusValidating
	}
	if err := a.tasks.MarkStatus(ctx, payload.TaskID, status, actorName, "", "", map[string]any{
		"step":      step,
		"iteration": iteration,
	}); err != nil {
		return swarm.TransientError(err)
	}
	if err := a.tasks.AppendEvent(ctx, payload.TaskID, swarm.TaskEventAgentStarted, actorName, "", map[string]any{
		"step":      step,
		"iteration": iteration,
	}); err != nil {
		return swarm.TransientError(err)
	}
	return a.deliver(ctx, payload.TaskID, payload.Locator, fmt.Sprintf("Goal iteration %d/%d: %s started.", iteration, normalizeGoalMaxIterations(payload.MaxIterations), step), "started:"+step+":"+strconv.Itoa(iteration))
}

func (a *Actor) recordStepCompleted(
	ctx context.Context,
	payload goalTaskPayload,
	step string,
	iteration int,
	state *stepProgressState,
	deliverySeq *int,
) error {
	if iteration <= 0 {
		iteration = 1
	}
	message := fmt.Sprintf("Goal iteration %d/%d: %s completed.", iteration, normalizeGoalMaxIterations(payload.MaxIterations), step)
	if state != nil && !state.deliveredOutput && state.lastVisibleText != "" {
		message += "\n\n" + state.lastVisibleText
	}
	if err := a.recordStepProgress(ctx, payload, step, iteration, progressKindCompleted, message, deliverySeq); err != nil {
		return err
	}
	if err := a.tasks.AppendEvent(ctx, payload.TaskID, swarm.TaskEventAgentResult, actorName, "", map[string]any{
		"step":      step,
		"iteration": iteration,
	}); err != nil {
		return swarm.TransientError(err)
	}
	return nil
}

func (a *Actor) recordStepProgress(
	ctx context.Context,
	payload goalTaskPayload,
	step string,
	iteration int,
	kind string,
	text string,
	deliverySeq *int,
) error {
	if deliverySeq != nil {
		(*deliverySeq)++
	}
	suffix := fmt.Sprintf("progress:%s:%s:%d:%03d", kind, step, iteration, valueOrZero(deliverySeq))
	if err := a.deliver(ctx, payload.TaskID, payload.Locator, text, suffix); err != nil {
		return err
	}
	if err := a.tasks.AppendEvent(ctx, payload.TaskID, swarm.TaskEventAgentProgress, actorName, "", map[string]any{
		"step":      step,
		"iteration": iteration,
		"kind":      kind,
		"text":      redactSecrets(strings.TrimSpace(text)),
	}); err != nil {
		return swarm.TransientError(err)
	}
	return nil
}

func (r goalRunResult) toTaskResult(goalReached bool, artifacts taskArtifactSnapshot, export *taskExportResultV1) taskResultPayloadV1 {
	workerOutput := redactSecrets(strings.TrimSpace(r.workerOutput))
	validatorOutput := redactSecrets(strings.TrimSpace(r.validatorOutput))
	finalText := redactSecrets(strings.TrimSpace(r.finalText))
	whatWasDone := firstNonEmpty(workerOutput, finalText, strings.TrimSpace(r.payload.Objective))
	validation := firstNonEmpty(validatorOutput, finalText)
	verified := "validator returned feedback"
	if reviewerPassed(validatorOutput) {
		verified = "validator returned pass"
	}
	nextAction := "Inspect events and decide whether to continue, cancel, or ask a human."
	if goalReached {
		nextAction = "Review the exported result and continue with follow-up work if needed."
		if export != nil && strings.TrimSpace(export.Status) == goalExportStatusFailed {
			nextAction = "Inspect the preserved goal workspace and retry export after resolving the base-branch issue."
		}
	} else if r.payload.MaxIterations > 0 && r.iterations >= r.payload.MaxIterations {
		nextAction = "Review failure evidence and rerun /goal or assign a narrower follow-up task."
	}
	artifactResult := &taskArtifactResultV1{
		WorkspaceDir: strings.TrimSpace(artifacts.WorkspaceDir),
		BranchName:   strings.TrimSpace(artifacts.BranchName),
		Commit:       strings.TrimSpace(artifacts.Commit),
		ChangedFiles: append([]string(nil), artifacts.ChangedFiles...),
		GitError:     strings.TrimSpace(artifacts.GitError),
	}
	return taskResultPayloadV1{
		SchemaVersion:  taskResultSchemaVersionV1,
		GoalReached:    goalReached,
		Iterations:     r.iterations,
		ExecutorOutput: workerOutput,
		ReviewerOutput: validatorOutput,
		ReviewerNotes:  validatorOutput,
		Artifacts:      artifactResult,
		Export:         export,
		ReviewableOutcome: taskReviewableOutcomeV1{
			SchemaVersion: taskReviewableOutcomeSchemaV1,
			WhatWasDone:   whatWasDone,
			Validation:    validation,
			Verified:      verified,
			NotVerified:   "manual review still required",
			NextAction:    nextAction,
		},
	}
}

func snapshotGoalRuntimeArtifacts(ctx context.Context, runtime GoalRuntime) taskArtifactSnapshot {
	if runtime == nil {
		return taskArtifactSnapshot{}
	}
	artifacts := taskArtifactSnapshot{
		WorkspaceDir: strings.TrimSpace(runtime.WorkspaceDir()),
		BranchName:   strings.TrimSpace(runtime.BranchName()),
	}
	if artifacts.WorkspaceDir == "" {
		return artifacts
	}
	if !git.Available(ctx, artifacts.WorkspaceDir) {
		artifacts.GitError = "workspace is not a git repository"
		return artifacts
	}
	status, err := git.GitRunCmdOutput(ctx, artifacts.WorkspaceDir, "git", "status", "--short")
	if err != nil {
		artifacts.GitError = err.Error()
	} else {
		for _, line := range strings.Split(strings.TrimSpace(status), "\n") {
			if trimmed := strings.TrimSpace(line); trimmed != "" {
				artifacts.ChangedFiles = append(artifacts.ChangedFiles, trimmed)
			}
		}
	}
	commit, err := git.GitRunCmdOutput(ctx, artifacts.WorkspaceDir, "git", "rev-parse", "--short", "HEAD")
	if err != nil {
		if artifacts.GitError == "" {
			artifacts.GitError = err.Error()
		}
	} else {
		artifacts.Commit = strings.TrimSpace(commit)
	}
	return artifacts
}

func (a *Actor) enqueueTaskCompletionMemorySync(ctx context.Context, payload goalTaskPayload, result taskResultPayloadV1) error {
	if a == nil || a.dispatcher == nil {
		return swarm.TransientError(fmt.Errorf("actor dispatcher is required"))
	}
	commands := []taskMemorySyncPayload{
		{
			Operation: taskMemoryOperationSummary,
			Scope:     taskMemoryScopeCompleted,
			TaskID:    strings.TrimSpace(payload.TaskID),
			SessionID: strings.TrimSpace(payload.Locator.SessionID),
			Content: strings.TrimSpace(strings.Join([]string{
				"Objective: " + strings.TrimSpace(payload.Objective),
				"What was done: " + strings.TrimSpace(result.ReviewableOutcome.WhatWasDone),
				"Validation: " + strings.TrimSpace(firstNonEmpty(result.ReviewableOutcome.Validation, result.ReviewerOutput)),
				"Next action: " + strings.TrimSpace(result.ReviewableOutcome.NextAction),
			}, "\n")),
		},
		{
			Operation: taskMemoryOperationFacts,
			Scope:     taskMemoryScopeCompleted,
			TaskID:    strings.TrimSpace(payload.TaskID),
			SessionID: strings.TrimSpace(payload.Locator.SessionID),
			Content: strings.TrimSpace(strings.Join([]string{
				"fact: " + strings.TrimSpace(result.ReviewableOutcome.WhatWasDone),
				"fact: " + strings.TrimSpace(result.ReviewableOutcome.Verified),
				"fact: " + strings.TrimSpace(result.ReviewableOutcome.NextAction),
			}, "\n")),
		},
		{
			Operation: taskMemoryOperationContext,
			Scope:     taskMemoryScopeCompleted,
			TaskID:    strings.TrimSpace(payload.TaskID),
			SessionID: strings.TrimSpace(payload.Locator.SessionID),
			Content: strings.TrimSpace(strings.Join([]string{
				"task_id=" + strings.TrimSpace(payload.TaskID),
				"session_id=" + strings.TrimSpace(payload.Locator.SessionID),
				"iteration=" + strconv.Itoa(result.Iterations),
				"goal_reached=true",
			}, "\n")),
		},
	}
	for _, command := range commands {
		if strings.TrimSpace(command.Content) == "" {
			continue
		}
		commandJSON, err := json.Marshal(command)
		if err != nil {
			return swarm.PermanentError(fmt.Errorf("encode memory sync command: %w", err))
		}
		dedupeKey := strings.TrimSpace(payload.TaskID) + ":memory:" + command.Operation + ":" + strconv.Itoa(result.Iterations)
		env := swarm.Envelope{
			ID:            dedupeKey,
			Namespace:     swarm.NamespaceMemorySync,
			Kind:          command.Operation,
			From:          swarm.ActorAddress{Target: swarm.ActorTypeGoalkeeper, Key: strings.TrimSpace(payload.TaskID)},
			To:            swarm.ActorAddress{Target: swarm.ActorTypeMemory, Key: taskMemoryActorKeyGlobal},
			SessionID:     strings.TrimSpace(payload.Locator.SessionID),
			TaskID:        strings.TrimSpace(payload.TaskID),
			CorrelationID: strings.TrimSpace(payload.TaskID),
			Priority:      60,
			DedupeKey:     dedupeKey,
			PayloadJSON:   string(commandJSON),
		}
		if _, err := a.dispatcher.Dispatch(ctx, env); err != nil {
			return swarm.TransientError(err)
		}
	}
	return nil
}

func (a *Actor) taskStatusIs(ctx context.Context, taskID string, statuses ...string) bool {
	if a == nil || a.tasks == nil || strings.TrimSpace(taskID) == "" {
		return false
	}
	task, ok, err := a.tasks.Get(ctx, taskID)
	if err != nil || !ok {
		return false
	}
	for _, status := range statuses {
		if strings.TrimSpace(task.Status) == strings.TrimSpace(status) {
			return true
		}
	}
	return false
}

func (a *Actor) renderTaskOutcome(ctx context.Context, taskID string, fallback string) string {
	if a == nil || a.tasks == nil {
		return fallback
	}
	task, ok, err := a.tasks.Get(ctx, taskID)
	if err != nil || !ok {
		return fallback
	}
	return renderReviewableOutcome(task, taskArtifactSnapshot{})
}

func (a *Actor) deliver(
	ctx context.Context,
	taskID string,
	locator baldasession.SessionLocator,
	text string,
	dedupeSuffix string,
) error {
	if a.dispatcher == nil {
		return swarm.TransientError(fmt.Errorf("actor dispatcher is required"))
	}
	message := redactSecrets(strings.TrimSpace(text))
	if message == "" {
		return nil
	}
	payload := taskDeliveryPayload{
		TaskID:  strings.TrimSpace(taskID),
		Locator: locator,
		Text:    message,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return swarm.PermanentError(fmt.Errorf("encode task delivery payload: %w", err))
	}
	dedupeKey := strings.TrimSpace(taskID)
	if dedupeKey == "" {
		dedupeKey = "delivery:" + shortTaskHash(strings.Join([]string{
			strings.TrimSpace(locator.SessionID),
			strings.TrimSpace(locator.AddressKey),
			message,
		}, "|"))
	}
	if suffix := strings.TrimSpace(dedupeSuffix); suffix != "" {
		dedupeKey += ":delivery:" + suffix
	}
	env := swarm.Envelope{
		ID:            dedupeKey,
		Namespace:     swarm.NamespaceAgentResult,
		Kind:          taskPayloadKindDelivery,
		From:          swarm.ActorAddress{Target: swarm.ActorTypeGoalkeeper, Key: taskID},
		To:            swarm.ActorAddress{Target: swarm.ActorTypeDelivery, Key: firstNonEmpty(locator.AddressKey, locator.SessionID, "telegram")},
		SessionID:     locator.SessionID,
		TaskID:        taskID,
		CorrelationID: taskID,
		Priority:      70,
		DedupeKey:     dedupeKey,
		PayloadJSON:   string(data),
	}
	if _, err := a.dispatcher.Dispatch(ctx, env); err != nil {
		return swarm.TransientError(err)
	}
	return nil
}

func normalizeGoalMaxIterations(v int) int {
	if v <= 0 {
		return defaultGoalMaxIterations
	}
	return v
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func appendVisibleText(existing string, next string) string {
	existing = strings.TrimSpace(existing)
	next = strings.TrimSpace(next)
	if existing == "" {
		return next
	}
	if next == "" {
		return existing
	}
	return existing + "\n\n" + next
}

func visibleText(ev *adksession.Event) string {
	if ev == nil || ev.Content == nil {
		return ""
	}
	content := ev.Content
	var parts []string
	for _, part := range content.Parts {
		if part != nil && !part.Thought && strings.TrimSpace(part.Text) != "" {
			parts = append(parts, strings.TrimSpace(part.Text))
		}
	}
	return strings.TrimSpace(strings.Join(parts, "\n\n"))
}

func reviewerPassed(text string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(text)), "verdict: pass")
}

func redactSecrets(raw string) string {
	text := strings.TrimSpace(raw)
	if text == "" {
		return text
	}
	text = secretPEMPattern.ReplaceAllString(text, "[REDACTED_PEM]")
	text = secretBearerHeaderPattern.ReplaceAllString(text, "${1}[REDACTED]")
	text = secretKeyValuePattern.ReplaceAllString(text, "${1}${2}[REDACTED]")
	text = secretGitHubTokenPattern.ReplaceAllString(text, "[REDACTED_TOKEN]")
	text = secretTelegramToken.ReplaceAllString(text, "[REDACTED_TOKEN]")
	return text
}

func shortTaskHash(value string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(value)))
	return hex.EncodeToString(sum[:])[:12]
}

func renderReviewableOutcome(task baldastate.SwarmTaskRecord, artifacts taskArtifactSnapshot) string {
	_ = artifacts
	var result map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(task.ResultJSON)), &result); err != nil {
		result = nil
	}
	parsedOutcome := struct {
		WhatWasDone string
		Validation  string
		Verified    string
		NotVerified string
		NextAction  string
	}{}
	hasOutcome := false
	if len(result) != 0 {
		if outcomeMap, ok := result["reviewable_outcome"].(map[string]any); ok {
			parsedOutcome.WhatWasDone = redactSecrets(strings.TrimSpace(fmt.Sprint(outcomeMap["what_was_done"])))
			parsedOutcome.Validation = redactSecrets(strings.TrimSpace(fmt.Sprint(outcomeMap["validation_output"])))
			parsedOutcome.Verified = redactSecrets(strings.TrimSpace(fmt.Sprint(outcomeMap["what_was_verified"])))
			parsedOutcome.NotVerified = redactSecrets(strings.TrimSpace(fmt.Sprint(outcomeMap["what_was_not_verified"])))
			parsedOutcome.NextAction = redactSecrets(strings.TrimSpace(fmt.Sprint(outcomeMap["next_action"])))
			hasOutcome = parsedOutcome.WhatWasDone != "" ||
				parsedOutcome.Validation != "" ||
				parsedOutcome.Verified != "" ||
				parsedOutcome.NotVerified != "" ||
				parsedOutcome.NextAction != ""
		}
	}
	goalReached := false
	switch typed := result["goal_reached"].(type) {
	case bool:
		goalReached = typed
	case string:
		goalReached = strings.EqualFold(strings.TrimSpace(typed), "true")
	}
	resultText := func(key string) string {
		if len(result) == 0 {
			return ""
		}
		value, ok := result[key]
		if !ok || value == nil {
			return ""
		}
		return strings.TrimSpace(fmt.Sprint(value))
	}
	executorOutput := redactSecrets(firstNonEmpty(resultText("executor_output"), resultText("final_text")))
	reviewerOutput := redactSecrets(firstNonEmpty(resultText("reviewer_output"), resultText("reviewer_feedback")))
	whatWasDone := firstNonEmpty(executorOutput, task.Objective)
	if hasOutcome {
		whatWasDone = firstNonEmpty(parsedOutcome.WhatWasDone, whatWasDone)
	}
	if !goalReached && task.Status != baldastate.SwarmTaskStatusCompleted && resultText("final_text") != "" {
		whatWasDone = redactSecrets(resultText("final_text"))
	}
	validation := reviewerOutput
	if hasOutcome {
		validation = firstNonEmpty(parsedOutcome.Validation, validation)
	}
	verified := firstNonEmpty(parsedOutcome.Verified, "validator returned feedback")
	notVerified := firstNonEmpty(parsedOutcome.NotVerified, "manual review still required")
	nextAction := firstNonEmpty(parsedOutcome.NextAction, "Inspect events and decide whether to continue, cancel, or ask a human.")

	var parts []string
	if goalReached {
		parts = append(parts, "Result: Goal completed.")
	} else {
		parts = append(parts, "Result: Goal not completed.")
	}
	if whatWasDone != "" {
		parts = append(parts, "What was done:\n"+whatWasDone)
	}
	if validation != "" {
		parts = append(parts, "Validation:\n"+validation)
	}
	if verified != "" {
		parts = append(parts, "Verified: "+verified)
	}
	if notVerified != "" {
		parts = append(parts, "Not verified: "+notVerified)
	}
	if nextAction != "" {
		parts = append(parts, "Next action: "+nextAction)
	}
	return strings.TrimSpace(strings.Join(parts, "\n\n"))
}

func valueOrZero(value *int) int {
	if value == nil {
		return 0
	}
	return *value
}
