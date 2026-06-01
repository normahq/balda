package actors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	baldaagent "github.com/normahq/balda/internal/apps/balda/agent"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/normahq/balda/internal/git"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
	adkagent "google.golang.org/adk/agent"
	"google.golang.org/genai"
)

const (
	goalActorName = "goal.actor"

	goalMetadataEventKey = "norma.goal.event"
	goalMetadataStepKey  = "norma.goal.step"

	goalStepStarted   = "step_started"
	goalStepCompleted = "step_completed"
	goalStepFailed    = "step_failed"
	goalWorkerStep    = "worker"
	goalValidatorStep = "validator"
)

type goalRuntimeBuilder interface {
	BuildGoalRuntime(ctx context.Context, cfg baldaagent.GoalRuntimeConfig) (*baldaagent.GoalRuntime, error)
}

type goalActor struct {
	tasks          *swarm.TaskService
	dispatcher     swarm.ActorDispatcher
	sessions       *baldasession.Manager
	runtimeBuilder goalRuntimeBuilder
	taskRuns       *TaskRunRegistry
	maxIters       int
	logger         zerolog.Logger
}

type goalActorParams struct {
	fx.In

	TaskService    *swarm.TaskService
	Dispatcher     swarm.ActorDispatcher
	SessionManager *baldasession.Manager
	RuntimeManager *baldaagent.RuntimeManager
	TaskRuns       *TaskRunRegistry
	MaxIters       int `name:"balda_goal_max_iterations"`
	Logger         zerolog.Logger
}

func (a *goalActor) Address() string {
	return swarm.WildcardAddress(swarm.ActorTypeGoal)
}

func (a *goalActor) Handle(ctx context.Context, envelope any) error {
	env, err := swarm.AssertEnvelope(envelope)
	if err != nil {
		return err
	}
	if strings.TrimSpace(env.Namespace) != swarm.NamespaceGoalCommand {
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
		Namespace:   swarm.NamespaceGoalCommand,
		Kind:        swarm.KindGoal,
		From:        swarm.ActorAddress{Target: "telegram", Key: firstNonEmpty(transportUserID, locator.AddressKey, "unknown")},
		To:          swarm.ActorAddress{Target: swarm.ActorTypeGoal, Key: taskID},
		SessionID:   locator.SessionID,
		TaskID:      taskID,
		Priority:    90,
		PayloadJSON: string(data),
	}, nil
}

func (a *goalActor) runGoal(ctx context.Context, env swarm.Envelope, payload goalTaskPayload) error {
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
	if err := a.tasks.MarkStatus(ctx, taskID, baldastate.SwarmTaskStatusRunning, goalActorName, env.ID, "", map[string]any{
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
	runtime, err := a.runtimeBuilder.BuildGoalRuntime(ctx, baldaagent.GoalRuntimeConfig{
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
	runID := a.taskRuns.Register(taskID, cancel)
	defer a.taskRuns.Unregister(taskID, runID)
	defer cancel()

	result, err := a.runWorkflow(runCtx, runtime, ts.GetUserID(), runtime.SessionID, payload)
	artifacts := snapshotGoalRuntimeArtifacts(ctx, runtime)
	if err != nil {
		if errors.Is(runCtx.Err(), context.Canceled) {
			if cleanupErr := runtime.CleanupResources(ctx); cleanupErr != nil {
				a.logger.Warn().Err(cleanupErr).Str("task_id", taskID).Msg("failed to cleanup canceled goal runtime")
			}
			if setErr := a.tasks.SetResult(ctx, taskID, result.toTaskResult(false, artifacts, &taskExportResultV1{Status: "canceled"}), baldastate.SwarmTaskStatusCanceled, goalActorName, "goal run canceled"); setErr != nil {
				return swarm.TransientError(setErr)
			}
			return a.deliver(ctx, taskID, payload.Locator, "Goal run canceled.", "canceled")
		}
		reason := redactSecrets(err.Error())
		if cleanupErr := runtime.CleanupResources(ctx); cleanupErr != nil {
			a.logger.Warn().Err(cleanupErr).Str("task_id", taskID).Msg("failed to cleanup failed goal runtime")
		}
		if setErr := a.tasks.SetResult(ctx, taskID, result.toTaskResult(false, artifacts, &taskExportResultV1{Status: "failed", Error: reason}), baldastate.SwarmTaskStatusFailed, goalActorName, reason); setErr != nil {
			return swarm.TransientError(setErr)
		}
		return a.deliver(ctx, taskID, payload.Locator, "Goal run failed: "+reason, "failed")
	}
	passed := reviewerPassed(result.validatorOutput)
	if passed {
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
			if setErr := a.tasks.SetResult(ctx, taskID, taskResult, baldastate.SwarmTaskStatusFailed, goalActorName, exportSummary.Error); setErr != nil {
				return swarm.TransientError(setErr)
			}
			return a.deliver(ctx, taskID, payload.Locator, a.renderTaskOutcome(ctx, taskID, "Goal validation passed, but export failed."), "export-failed")
		}
		exportSummary.Status = "exported"
		taskResult := result.toTaskResult(true, artifacts, exportSummary)
		if err := a.tasks.SetResult(ctx, taskID, taskResult, baldastate.SwarmTaskStatusCompleted, goalActorName, ""); err != nil {
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
	if err := a.tasks.SetResult(ctx, taskID, taskResult, baldastate.SwarmTaskStatusFailed, goalActorName, "max iterations reached"); err != nil {
		return swarm.TransientError(err)
	}
	return a.deliver(ctx, taskID, payload.Locator, a.renderTaskOutcome(ctx, taskID, "Goal run reached max iterations without passing validation."), "max-iterations")
}

func (a *goalActor) ensureGoalTask(ctx context.Context, payload goalTaskPayload) error {
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
		OwnerActor:    swarm.ActorTypeGoal + ":" + strings.TrimSpace(payload.TaskID),
		AssignedActor: swarm.ActorTypeGoal + ":" + strings.TrimSpace(payload.TaskID),
		Priority:      90,
		CreatedBy:     strings.TrimSpace(payload.TransportUserID),
	}
	if _, err := a.tasks.Create(ctx, record, goalActorName, payload); err != nil {
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
		return a.tasks.MarkStatus(ctx, payload.TaskID, baldastate.SwarmTaskStatusQueued, goalActorName, "", "", nil)
	default:
		return nil
	}
}

func (a *goalActor) resolveSession(ctx context.Context, payload goalTaskPayload) (*baldasession.TopicSession, error) {
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

type goalRunResult struct {
	payload         goalTaskPayload
	iterations      int
	workerOutput    string
	validatorOutput string
	finalText       string
}

func (a *goalActor) runWorkflow(
	ctx context.Context,
	runtime *baldaagent.GoalRuntime,
	userID string,
	agentSessionID string,
	payload goalTaskPayload,
) (goalRunResult, error) {
	result := goalRunResult{payload: payload}
	if runtime == nil || runtime.Runner == nil {
		return result, fmt.Errorf("goal runner is required")
	}
	userContent := genai.NewContentFromText("Goal:\n"+strings.TrimSpace(payload.Objective), genai.RoleUser)
	currentStep := ""
	sawTurnComplete := false
	for ev, err := range runtime.Runner.Run(ctx, userID, agentSessionID, userContent, adkagent.RunConfig{}) {
		if err != nil {
			return result, fmt.Errorf("run goal workflow: %w", err)
		}
		if ev == nil {
			continue
		}
		if len(ev.CustomMetadata) != 0 {
			eventType, _ := ev.CustomMetadata[goalMetadataEventKey].(string)
			step, _ := ev.CustomMetadata[goalMetadataStepKey].(string)
			eventType = strings.TrimSpace(eventType)
			step = strings.TrimSpace(step)
			if eventType != "" && step != "" {
				switch eventType {
				case goalStepStarted:
					currentStep = step
					if err := a.recordStepStarted(ctx, payload, step, result.iterations+1); err != nil {
						return result, err
					}
				case goalStepCompleted:
					if step == goalValidatorStep {
						result.iterations++
					}
					if err := a.recordStepCompleted(ctx, payload, step, result.iterations); err != nil {
						return result, err
					}
					currentStep = ""
				case goalStepFailed:
					return result, fmt.Errorf("%s step failed", step)
				}
			}
		}
		text := ""
		if ev.Content != nil {
			var parts []string
			for _, part := range ev.Content.Parts {
				if part != nil && !part.Thought && strings.TrimSpace(part.Text) != "" {
					parts = append(parts, part.Text)
				}
			}
			text = strings.TrimSpace(strings.Join(parts, "\n\n"))
		}
		if text != "" {
			result.finalText = appendVisibleText(result.finalText, text)
			switch currentStep {
			case goalWorkerStep:
				result.workerOutput = appendVisibleText(result.workerOutput, text)
			case goalValidatorStep:
				result.validatorOutput = appendVisibleText(result.validatorOutput, text)
			}
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

func (a *goalActor) recordStepStarted(ctx context.Context, payload goalTaskPayload, step string, iteration int) error {
	status := baldastate.SwarmTaskStatusWaitingForAgent
	if step == goalValidatorStep {
		status = baldastate.SwarmTaskStatusValidating
	}
	if err := a.tasks.MarkStatus(ctx, payload.TaskID, status, goalActorName, "", "", map[string]any{
		"step":      step,
		"iteration": iteration,
	}); err != nil {
		return swarm.TransientError(err)
	}
	if err := a.tasks.AppendEvent(ctx, payload.TaskID, swarm.TaskEventAgentStarted, goalActorName, "", map[string]any{
		"step":      step,
		"iteration": iteration,
	}); err != nil {
		return swarm.TransientError(err)
	}
	return a.deliver(ctx, payload.TaskID, payload.Locator, fmt.Sprintf("Goal iteration %d/%d: %s started.", iteration, normalizeGoalMaxIterations(payload.MaxIterations), step), "started:"+step+":"+strconv.Itoa(iteration))
}

func (a *goalActor) recordStepCompleted(ctx context.Context, payload goalTaskPayload, step string, iteration int) error {
	if iteration <= 0 {
		iteration = 1
	}
	if err := a.tasks.AppendEvent(ctx, payload.TaskID, swarm.TaskEventAgentResult, goalActorName, "", map[string]any{
		"step":      step,
		"iteration": iteration,
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
			SchemaVersion: taskReviewableOutcomeSchemaVersion,
			WhatWasDone:   whatWasDone,
			Validation:    validation,
			Verified:      verified,
			NotVerified:   "manual review still required",
			NextAction:    nextAction,
		},
	}
}

func snapshotGoalRuntimeArtifacts(ctx context.Context, runtime *baldaagent.GoalRuntime) taskArtifactSnapshot {
	if runtime == nil {
		return taskArtifactSnapshot{}
	}
	artifacts := taskArtifactSnapshot{
		WorkspaceDir: strings.TrimSpace(runtime.WorkspaceDir),
		BranchName:   strings.TrimSpace(runtime.BranchName),
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

func (a *goalActor) enqueueTaskCompletionMemorySync(ctx context.Context, payload goalTaskPayload, result taskResultPayloadV1) error {
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
			From:          swarm.ActorAddress{Target: swarm.ActorTypeGoal, Key: strings.TrimSpace(payload.TaskID)},
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

func (a *goalActor) taskStatusIs(ctx context.Context, taskID string, statuses ...string) bool {
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

func (a *goalActor) renderTaskOutcome(ctx context.Context, taskID string, fallback string) string {
	_ = ctx
	if a == nil || a.tasks == nil {
		return fallback
	}
	task, ok, err := a.tasks.Get(ctx, taskID)
	if err != nil || !ok {
		return fallback
	}
	artifacts := taskArtifactSnapshot{}
	return renderReviewableOutcome(task, artifacts)
}

func (a *goalActor) deliver(
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
		From:          swarm.ActorAddress{Target: swarm.ActorTypeGoal, Key: taskID},
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
