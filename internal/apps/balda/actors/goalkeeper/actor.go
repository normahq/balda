package goalkeeper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
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
	goalExportStatusExported      = "exported"
	goalExportStatusFailed        = "export_failed"
	goalExportStatusNotExported   = "not_exported"
	goalExportReasonDisabled      = "workspace_disabled"
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
	progressKindPlan              = "plan"
	progressKindOutput            = "output"
	progressKindCompleted         = "completed"
	defaultNotVerifiedText        = "manual review still required"
	defaultInspectNextAction      = "Inspect events and decide whether to continue, cancel, or ask a human."
	defaultExportedNextAction     = "Review the exported result and continue with follow-up work if needed."
	defaultNotExportedNextAction  = "Review the direct working directory changes and commit or follow up manually if needed."
)

var (
	secretBearerHeaderPattern = regexp.MustCompile(`(?i)(authorization\s*:\s*bearer\s+)([^\s]+)`)
	secretKeyValuePattern     = regexp.MustCompile(`(?i)\b(token|secret|password|api[_-]?key|access[_-]?key|private[_-]?key)\b(\s*[:=]\s*)([^\s,;]+)`)
	secretPEMPattern          = regexp.MustCompile(`(?s)-----BEGIN [^-]+-----.*?-----END [^-]+-----`)
	secretGitHubTokenPattern  = regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9_]{20,}\b`)
	secretTelegramToken       = regexp.MustCompile(`\b\d{6,10}:[A-Za-z0-9_-]{20,}\b`)
)

type GoalRunPreparer interface {
	PrepareGoalRun(ctx context.Context, cfg GoalRunConfig) (GoalRun, error)
}

type GoalRunConfig struct {
	SourceSessionID string
	TaskID          string
	UserID          string
	MaxIterations   uint
}

type GoalRunner interface {
	Run(ctx context.Context, userID string, sessionID string, userContent *genai.Content, cfg adkagent.RunConfig, opts ...adkrunner.RunOption) iter.Seq2[*adksession.Event, error]
}

type GoalRun interface {
	Runner() GoalRunner
	SessionID() string
	WorkspaceDir() string
	BranchName() string
	Close() error
	CleanupResources(ctx context.Context) error
	Finalize(ctx context.Context, objective string, workerOutput string, validatorOutput string) (GoalFinalizationResult, error)
}

type GoalFinalizationResult struct {
	Status        string
	CommitMessage string
	Reason        string
	Error         string
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
	GoalRunPreparer    GoalRunPreparer
	TaskRuns           TaskRuns
	MaxIterations      int  `name:"balda_goal_max_iterations"`
	PlanUpdatesEnabled bool `name:"balda_telegram_plan_updates"`
	Logger             zerolog.Logger
}

type Actor struct {
	tasks              *swarm.TaskService
	dispatcher         swarm.ActorDispatcher
	sessions           *baldasession.Manager
	goalRunPreparer    GoalRunPreparer
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
	DeliveryProfile deliverycmd.Profile         `json:"delivery_profile,omitempty,omitzero"`
	Objective       string                      `json:"objective"`
	TransportUserID string                      `json:"transport_user_id"`
	MaxIterations   int                         `json:"max_iterations,omitempty"`
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
	Reason        string `json:"reason,omitempty"`
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

type goalRunResult struct {
	payload               goalTaskPayload
	iterations            int
	workerOutput          string
	validatorOutput       string
	latestWorkerOutput    string
	latestValidatorOutput string
	finalText             string
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
		goalRunPreparer:    params.GoalRunPreparer,
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
	return GoalTaskEnvelopeWithProfile(locator, deliverycmd.Profile{}, objective, transportUserID, maxIterations)
}

func GoalTaskEnvelopeWithProfile(
	locator baldasession.SessionLocator,
	deliveryProfile deliverycmd.Profile,
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
			DeliveryProfile: normalizeGoalDeliveryProfile(deliveryProfile),
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
	skip, err := a.ensureNoOtherActiveGoal(ctx, taskID, payload)
	if err != nil {
		return err
	}
	if skip {
		return nil
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
	if err := a.deliver(ctx, taskID, payload, renderGoalStartedMessage(payload.DeliveryProfile, maxIterations, objective), "started"); err != nil {
		return err
	}

	if a.goalRunPreparer == nil {
		return swarm.TransientError(fmt.Errorf("goal run preparer is required"))
	}
	goalRun, err := a.goalRunPreparer.PrepareGoalRun(ctx, GoalRunConfig{
		SourceSessionID: payload.Locator.SessionID,
		TaskID:          taskID,
		UserID:          ts.GetUserID(),
		MaxIterations:   uint(maxIterations),
	})
	if err != nil {
		return swarm.TransientError(err)
	}
	defer func() {
		if err := goalRun.Close(); err != nil {
			a.logger.Warn().Err(err).Str("task_id", taskID).Msg("failed to close goal run")
		}
	}()

	runCtx, cancel := context.WithCancel(ctx)
	runID := ""
	if a.taskRuns != nil {
		runID = a.taskRuns.Register(taskID, cancel)
		defer a.taskRuns.Unregister(taskID, runID)
	}
	defer cancel()

	result, err := a.runWorkflow(runCtx, goalRun, ts.GetUserID(), goalRun.SessionID(), payload)
	artifacts := snapshotGoalRunArtifacts(ctx, goalRun)
	if err != nil {
		if errors.Is(runCtx.Err(), context.Canceled) {
			if cleanupErr := goalRun.CleanupResources(ctx); cleanupErr != nil {
				a.logger.Warn().Err(cleanupErr).Str("task_id", taskID).Msg("failed to cleanup canceled goal run")
			}
			if setErr := a.tasks.SetResult(ctx, taskID, result.toTaskResult(false, artifacts, &taskExportResultV1{Status: "canceled"}), baldastate.SwarmTaskStatusCanceled, actorName, "goal run canceled"); setErr != nil {
				return swarm.TransientError(setErr)
			}
			return a.deliver(ctx, taskID, payload, renderGoalStatusMessage(payload.DeliveryProfile, "Goal run canceled."), "canceled")
		}
		reason := redactSecrets(err.Error())
		if cleanupErr := goalRun.CleanupResources(ctx); cleanupErr != nil {
			a.logger.Warn().Err(cleanupErr).Str("task_id", taskID).Msg("failed to cleanup failed goal run")
		}
		if setErr := a.tasks.SetResult(ctx, taskID, result.toTaskResult(false, artifacts, &taskExportResultV1{Status: "failed", Error: reason}), baldastate.SwarmTaskStatusFailed, actorName, reason); setErr != nil {
			return swarm.TransientError(setErr)
		}
		return a.deliver(ctx, taskID, payload, renderGoalStatusMessage(payload.DeliveryProfile, "Goal run failed: "+reason), "failed")
	}
	if reviewerPassed(result.latestValidatorOutput) {
		finalization, exportErr := goalRun.Finalize(ctx, payload.Objective, result.latestWorkerOutput, result.latestValidatorOutput)
		exportSummary := finalization.toTaskExportResult()
		if exportErr != nil || strings.TrimSpace(exportSummary.Status) == goalExportStatusFailed {
			if exportSummary.Status == "" {
				exportSummary.Status = goalExportStatusFailed
			}
			if exportSummary.Error == "" && exportErr != nil {
				exportSummary.Error = redactSecrets(exportErr.Error())
			}
			taskResult := result.toTaskResult(true, artifacts, exportSummary)
			if setErr := a.tasks.SetResult(ctx, taskID, taskResult, baldastate.SwarmTaskStatusFailed, actorName, exportSummary.Error); setErr != nil {
				return swarm.TransientError(setErr)
			}
			return a.deliver(ctx, taskID, payload, a.renderTaskOutcome(ctx, taskID, payload.DeliveryProfile, "Goal validation passed, but export failed."), "export-failed")
		}
		taskResult := result.toTaskResult(true, artifacts, exportSummary)
		if err := a.tasks.SetResult(ctx, taskID, taskResult, baldastate.SwarmTaskStatusCompleted, actorName, ""); err != nil {
			return swarm.TransientError(err)
		}
		if cleanupErr := goalRun.CleanupResources(ctx); cleanupErr != nil {
			a.logger.Warn().Err(cleanupErr).Str("task_id", taskID).Msg("failed to cleanup completed goal run")
		}
		return a.deliver(ctx, taskID, payload, a.renderTaskOutcome(ctx, taskID, payload.DeliveryProfile, "Goal run completed."), "completed")
	}
	if cleanupErr := goalRun.CleanupResources(ctx); cleanupErr != nil {
		a.logger.Warn().Err(cleanupErr).Str("task_id", taskID).Msg("failed to cleanup max-iteration goal run")
	}
	taskResult := result.toTaskResult(false, artifacts, &taskExportResultV1{Status: goalExportStatusNotExported})
	if err := a.tasks.SetResult(ctx, taskID, taskResult, baldastate.SwarmTaskStatusFailed, actorName, "max iterations reached"); err != nil {
		return swarm.TransientError(err)
	}
	return a.deliver(ctx, taskID, payload, a.renderTaskOutcome(ctx, taskID, payload.DeliveryProfile, "Goal run reached max iterations without passing validation."), "max-iterations")
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

func (a *Actor) ensureNoOtherActiveGoal(ctx context.Context, taskID string, payload goalTaskPayload) (bool, error) {
	if a == nil || a.tasks == nil {
		return false, swarm.TransientError(fmt.Errorf("task service is required"))
	}
	activeGoals, err := a.tasks.ListActiveGoalTasksBySession(ctx, payload.Locator.SessionID)
	if err != nil {
		return false, swarm.TransientError(fmt.Errorf("list active goal tasks: %w", err))
	}
	for _, task := range activeGoals {
		if strings.TrimSpace(task.ID) == strings.TrimSpace(taskID) {
			continue
		}
		reason := "another goal run is already active for this session"
		if setErr := a.tasks.SetResult(ctx, taskID, goalRunResult{payload: payload}.toTaskResult(false, taskArtifactSnapshot{}, &taskExportResultV1{
			Status: "canceled",
			Error:  reason,
		}), baldastate.SwarmTaskStatusCanceled, actorName, reason); setErr != nil {
			return false, swarm.TransientError(setErr)
		}
		if err := a.deliver(ctx, taskID, payload, renderGoalStatusMessage(payload.DeliveryProfile, "A goal run is already active for this session."), "already-active"); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
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
	runtime GoalRun,
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
					resetLatestStepOutput(&result, step)
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
				message := renderGoalStepMessage(payload.DeliveryProfile, iteration, normalizeGoalMaxIterations(payload.MaxIterations), currentStep, "plan update", planText)
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
				result.latestWorkerOutput = appendVisibleText(result.latestWorkerOutput, text)
			case ValidatorStep:
				result.validatorOutput = appendVisibleText(result.validatorOutput, text)
				result.latestValidatorOutput = appendVisibleText(result.latestValidatorOutput, text)
			}
			message := renderGoalStepMessage(payload.DeliveryProfile, iteration, normalizeGoalMaxIterations(payload.MaxIterations), currentStep, "update", text)
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
	return a.deliver(ctx, payload.TaskID, payload, renderGoalStepMessage(payload.DeliveryProfile, iteration, normalizeGoalMaxIterations(payload.MaxIterations), step, "started", ""), "started:"+step+":"+strconv.Itoa(iteration))
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
	message := renderGoalStepMessage(payload.DeliveryProfile, iteration, normalizeGoalMaxIterations(payload.MaxIterations), step, "completed", "")
	if state != nil && !state.deliveredOutput && state.lastVisibleText != "" {
		message = renderGoalStepMessage(payload.DeliveryProfile, iteration, normalizeGoalMaxIterations(payload.MaxIterations), step, "completed", state.lastVisibleText)
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
	if err := a.deliver(ctx, payload.TaskID, payload, text, suffix); err != nil {
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
	latestWorkerOutput := redactSecrets(strings.TrimSpace(r.latestWorkerOutput))
	latestValidatorOutput := redactSecrets(strings.TrimSpace(r.latestValidatorOutput))
	finalText := redactSecrets(strings.TrimSpace(r.finalText))
	whatWasDone := firstNonEmpty(latestWorkerOutput, workerOutput, finalText, strings.TrimSpace(r.payload.Objective))
	validation := firstNonEmpty(latestValidatorOutput, validatorOutput, finalText)
	verified := "validator returned feedback"
	if reviewerPassed(latestValidatorOutput) {
		verified = "validator returned pass"
	}
	nextAction := defaultInspectNextAction
	if goalReached {
		nextAction = defaultExportedNextAction
		if export != nil {
			switch strings.TrimSpace(export.Status) {
			case goalExportStatusFailed:
				nextAction = "Inspect the preserved goal workspace and retry export after resolving the base-branch issue."
			case goalExportStatusNotExported:
				nextAction = "Review the direct working directory changes and commit or follow up manually if needed."
			}
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
			NotVerified:   defaultNotVerifiedText,
			NextAction:    nextAction,
		},
	}
}

func (r GoalFinalizationResult) toTaskExportResult() *taskExportResultV1 {
	status := strings.TrimSpace(r.Status)
	if status == "" {
		status = goalExportStatusNotExported
	}
	return &taskExportResultV1{
		Status:        status,
		CommitMessage: redactSecrets(strings.TrimSpace(r.CommitMessage)),
		Reason:        redactSecrets(strings.TrimSpace(r.Reason)),
		Error:         redactSecrets(strings.TrimSpace(r.Error)),
	}
}

func snapshotGoalRunArtifacts(ctx context.Context, runtime GoalRun) taskArtifactSnapshot {
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

func (a *Actor) renderTaskOutcome(ctx context.Context, taskID string, profile deliverycmd.Profile, fallback string) string {
	if a == nil || a.tasks == nil {
		return renderGoalStatusMessage(profile, fallback)
	}
	task, ok, err := a.tasks.Get(ctx, taskID)
	if err != nil || !ok {
		return renderGoalStatusMessage(profile, fallback)
	}
	return renderReviewableOutcomeWithProfile(profile, task, taskArtifactSnapshot{})
}

func (a *Actor) deliver(
	ctx context.Context,
	taskID string,
	payload goalTaskPayload,
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
	locator := normalizeGoalDeliveryLocator(payload.Locator)
	env, err := deliverycmd.AgentReplyEnvelopeWithProfile(
		strings.TrimSpace(taskID),
		swarm.ActorAddress{Target: swarm.ActorTypeGoalkeeper, Key: taskID},
		locator,
		normalizeGoalDeliveryProfile(payload.DeliveryProfile),
		message,
		dedupeSuffix,
	)
	if err != nil {
		return swarm.PermanentError(fmt.Errorf("build goal delivery envelope: %w", err))
	}
	if _, err := a.dispatcher.Dispatch(ctx, env); err != nil {
		return swarm.TransientError(err)
	}
	return nil
}

func normalizeGoalDeliveryLocator(locator baldasession.SessionLocator) baldasession.SessionLocator {
	if strings.TrimSpace(locator.ChannelType) == "" {
		locator.ChannelType = "telegram"
	}
	return locator
}

func normalizeGoalDeliveryProfile(profile deliverycmd.Profile) deliverycmd.Profile {
	return deliveryfmt.NormalizeProfile(profile)
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

func resetLatestStepOutput(result *goalRunResult, step string) {
	if result == nil {
		return
	}
	switch strings.TrimSpace(step) {
	case WorkerStep:
		result.latestWorkerOutput = ""
	case ValidatorStep:
		result.latestValidatorOutput = ""
	}
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

func renderReviewableOutcomeWithProfile(profile deliverycmd.Profile, task baldastate.SwarmTaskRecord, artifacts taskArtifactSnapshot) string {
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
	exportStatus := ""
	exportReason := ""
	exportError := ""
	if len(result) != 0 {
		if exportMap, ok := result["export"].(map[string]any); ok {
			exportStatus = redactSecrets(strings.TrimSpace(fmt.Sprint(exportMap["status"])))
			exportReason = redactSecrets(strings.TrimSpace(fmt.Sprint(exportMap["reason"])))
			exportError = redactSecrets(strings.TrimSpace(fmt.Sprint(exportMap["error"])))
		}
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
	routineSuccessfulOutcome := goalReached && exportStatusIsRoutineSuccess(exportStatus)
	verified := firstNonEmpty(parsedOutcome.Verified, "validator returned feedback")
	notVerified := firstNonEmpty(parsedOutcome.NotVerified, defaultNotVerifiedText)
	nextAction := firstNonEmpty(parsedOutcome.NextAction, defaultInspectNextAction)
	renderNotVerified := shouldRenderNotVerified(parsedOutcome.NotVerified)
	renderNextAction := shouldRenderNextAction(parsedOutcome.NextAction, goalReached, exportStatus)
	renderVerified := shouldRenderVerified(verified, routineSuccessfulOutcome)
	renderValidation := shouldRenderValidation(validation, goalReached)

	var parts []string
	if goalReached {
		parts = append(parts, goalOutcomeLine(profile, "Result", "Goal completed."))
	} else {
		parts = append(parts, goalOutcomeLine(profile, "Result", "Goal not completed."))
	}
	if goalReached && exportStatus != "" {
		switch exportStatus {
		case goalExportStatusExported:
			// Routine success; do not add export noise to chat output.
		case goalExportStatusNotExported:
			// Workspace-disabled/direct mode is expected; keep it out of final chat output.
		case goalExportStatusFailed:
			parts = append(parts, goalOutcomeLine(profile, "Export", "failed: "+goalSystemText(goalMessageStyleForProfile(profile), firstNonEmpty(exportError, exportReason, "unknown error"))))
		default:
			parts = append(parts, goalOutcomeLine(profile, "Export", goalSystemText(goalMessageStyleForProfile(profile), exportStatus)+"."))
		}
	}
	if whatWasDone != "" {
		if routineSuccessfulOutcome {
			parts = append(parts, strings.TrimSpace(whatWasDone))
		} else {
			parts = append(parts, goalOutcomeBlock(profile, "What was done", whatWasDone))
		}
	}
	if renderValidation && validation != "" {
		parts = append(parts, goalOutcomeBlock(profile, "Validation", validation))
	}
	if renderVerified && verified != "" {
		parts = append(parts, goalOutcomeLine(profile, "Verified", verified))
	}
	if renderNotVerified && notVerified != "" {
		parts = append(parts, goalOutcomeLine(profile, "Not verified", notVerified))
	}
	if renderNextAction && nextAction != "" {
		parts = append(parts, goalOutcomeLine(profile, "Next action", nextAction))
	}
	return strings.TrimSpace(strings.Join(parts, "\n\n"))
}

func shouldRenderNotVerified(value string) bool {
	trimmed := strings.TrimSpace(value)
	return trimmed != "" && !strings.EqualFold(trimmed, defaultNotVerifiedText)
}

func shouldRenderVerified(value string, routineSuccessfulOutcome bool) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return false
	}
	if !routineSuccessfulOutcome {
		return true
	}
	return !strings.EqualFold(trimmed, "validator returned pass")
}

func shouldRenderValidation(value string, goalReached bool) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return false
	}
	if !goalReached {
		return true
	}
	return !validationIsRoutinePass(trimmed)
}

func validationIsRoutinePass(value string) bool {
	lowered := strings.ToLower(strings.TrimSpace(value))
	if strings.Contains(lowered, "evidence:") || strings.Contains(lowered, "verdict: fail") || strings.Contains(lowered, "verdict fail") {
		return false
	}
	normalized := strings.ToLower(strings.TrimSpace(strings.ReplaceAll(value, ":", " ")))
	normalized = strings.Join(strings.Fields(normalized), " ")
	return strings.Contains(normalized, "verdict pass")
}

func shouldRenderNextAction(value string, goalReached bool, exportStatus string) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return false
	}
	if !goalReached {
		return true
	}
	if !strings.EqualFold(trimmed, defaultExportedNextAction) {
		if goalReached && strings.TrimSpace(exportStatus) == goalExportStatusNotExported && strings.EqualFold(trimmed, defaultNotExportedNextAction) {
			return false
		}
		return true
	}
	switch strings.TrimSpace(exportStatus) {
	case goalExportStatusFailed, goalExportStatusNotExported:
		return true
	default:
		return false
	}
}

func exportStatusIsRoutineSuccess(status string) bool {
	switch strings.TrimSpace(status) {
	case "", goalExportStatusExported, goalExportStatusNotExported:
		return true
	default:
		return false
	}
}

func valueOrZero(value *int) int {
	if value == nil {
		return 0
	}
	return *value
}
