package handlers

import (
	"context"
	"fmt"
	"strings"
	"sync"

	baldaagent "github.com/normahq/balda/internal/apps/balda/agent"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
	adkagent "google.golang.org/adk/agent"
	"google.golang.org/adk/runner"
	adksession "google.golang.org/adk/session"
	"google.golang.org/genai"
)

const defaultGoalMaxIterations = 25

const (
	goalkeeperMetadataEventKey     = "norma.goalkeeper.event"
	goalkeeperMetadataStepKey      = "norma.goalkeeper.step"
	goalkeeperMetadataEscalatedKey = "escalated"

	goalkeeperStepEventStarted   = "step_started"
	goalkeeperStepEventCompleted = "step_completed"
	goalkeeperStepEventFailed    = "step_failed"

	goalkeeperWorkerStep    = "worker"
	goalkeeperValidatorStep = "validator"
	goalkeeperValidatorName = "GoalkeeperValidator"

	goalPhaseStarted  = "started"
	goalPhaseFinished = "finished"
)

type goalCommandRunner interface {
	Start(ctx context.Context, locator baldasession.SessionLocator, objective string, transportUserID string) (bool, error)
	StartTask(ctx context.Context, taskID string, locator baldasession.SessionLocator, objective string, transportUserID string) (bool, error)
	Cancel(locator baldasession.SessionLocator) bool
	MaxIterations() int
}

type goalkeeperRuntimeBuilder interface {
	BuildGoalkeeperRuntime(ctx context.Context, cfg baldaagent.GoalkeeperRuntimeConfig) (*baldaagent.GoalkeeperRuntime, error)
}

type goalRunnerParams struct {
	fx.In

	LC             fx.Lifecycle
	SessionManager *baldasession.Manager
	RuntimeManager *baldaagent.RuntimeManager
	Channel        *baldatelegram.Adapter
	TaskService    *swarm.TaskService
	Logger         zerolog.Logger
	MaxIterations  int `name:"balda_goal_max_iterations"`
}

// GoalRunner executes /goal loops per session with cancellation support.
type GoalRunner struct {
	sessionManager *baldasession.Manager
	runtimeManager goalkeeperRuntimeBuilder
	channel        *baldatelegram.Adapter
	tasks          *swarm.TaskService
	logger         zerolog.Logger
	maxIterations  int

	mu      sync.Mutex
	running map[string]context.CancelFunc
}

func NewGoalRunner(params goalRunnerParams) *GoalRunner {
	g := &GoalRunner{
		sessionManager: params.SessionManager,
		runtimeManager: params.RuntimeManager,
		channel:        params.Channel,
		tasks:          params.TaskService,
		logger:         params.Logger.With().Str("component", "balda.goal_runner").Logger(),
		maxIterations:  normalizeGoalMaxIterations(params.MaxIterations),
		running:        make(map[string]context.CancelFunc),
	}
	params.LC.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			g.stopAll()
			return nil
		},
	})
	return g
}

func normalizeGoalMaxIterations(v int) int {
	if v <= 0 {
		return defaultGoalMaxIterations
	}
	return v
}

func (g *GoalRunner) Start(
	ctx context.Context,
	locator baldasession.SessionLocator,
	objective string,
	transportUserID string,
) (bool, error) {
	return g.StartTask(ctx, "", locator, objective, transportUserID)
}

func (g *GoalRunner) MaxIterations() int {
	if g == nil {
		return defaultGoalMaxIterations
	}
	return normalizeGoalMaxIterations(g.maxIterations)
}

func (g *GoalRunner) StartTask(
	ctx context.Context,
	taskID string,
	locator baldasession.SessionLocator,
	objective string,
	transportUserID string,
) (bool, error) {
	sessionID := strings.TrimSpace(locator.SessionID)
	goal := strings.TrimSpace(objective)
	if sessionID == "" {
		return false, fmt.Errorf("session id is required")
	}
	if goal == "" {
		return false, fmt.Errorf("goal objective is required")
	}

	g.mu.Lock()
	if _, exists := g.running[sessionID]; exists {
		g.mu.Unlock()
		return false, nil
	}
	g.mu.Unlock()

	ts, err := g.resolveSession(ctx, locator, transportUserID)
	if err != nil {
		return false, err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	g.mu.Lock()
	if _, exists := g.running[sessionID]; exists {
		g.mu.Unlock()
		cancel()
		return false, nil
	}
	g.running[sessionID] = cancel
	g.mu.Unlock()

	go func() {
		defer g.removeRun(sessionID)
		g.runGoalLoop(runCtx, strings.TrimSpace(taskID), locator, ts, goal)
	}()

	return true, nil
}

func (g *GoalRunner) resolveSession(
	ctx context.Context,
	locator baldasession.SessionLocator,
	transportUserID string,
) (*baldasession.TopicSession, error) {
	ts, err := g.sessionManager.GetSession(locator)
	if err == nil {
		return ts, nil
	}

	return g.sessionManager.RestoreSession(ctx, baldasession.SessionContext{
		Locator: locator,
		UserID:  transportUserID,
	})
}

func (g *GoalRunner) runGoalLoop(
	ctx context.Context,
	taskID string,
	locator baldasession.SessionLocator,
	ts *baldasession.TopicSession,
	objective string,
) {
	if ts == nil {
		g.markTaskStatus(context.Background(), taskID, baldastate.SwarmTaskStatusFailed, "goal.runner", "session is unavailable", nil)
		g.sendGoalMessage(ctx, locator, "Goal run failed: session is unavailable.")
		return
	}

	maxIterations := g.maxIterations
	goalSessionID := strings.TrimSpace(ts.GetAgentSessionID())
	if goalSessionID == "" {
		g.markTaskStatus(context.Background(), taskID, baldastate.SwarmTaskStatusFailed, "goal.runner", "session is unavailable", nil)
		g.sendGoalMessage(ctx, locator, "Goal run failed: session is unavailable.")
		return
	}
	if g.runtimeManager == nil {
		g.markTaskStatus(context.Background(), taskID, baldastate.SwarmTaskStatusFailed, "goal.runner", "runtime is unavailable", nil)
		g.sendGoalMessage(ctx, locator, "Goal run failed: runtime is unavailable.")
		return
	}

	g.markTaskStatus(ctx, taskID, baldastate.SwarmTaskStatusRunning, "goal.runner", "", nil)
	goalRuntime, err := g.runtimeManager.BuildGoalkeeperRuntime(ctx, baldaagent.GoalkeeperRuntimeConfig{
		SessionID:     ts.GetSessionID(),
		BranchName:    ts.GetBranchName(),
		WorkspaceDir:  ts.GetWorkspaceDir(),
		MaxIterations: uint(maxIterations),
	})
	if err != nil {
		g.markTaskStatus(context.Background(), taskID, baldastate.SwarmTaskStatusFailed, "goal.runner", err.Error(), nil)
		g.sendGoalMessage(context.Background(), locator, fmt.Sprintf("Goal run failed: %v", err))
		return
	}
	defer func() {
		if err := goalRuntime.Close(); err != nil {
			g.logger.Warn().Err(err).Str("session_id", locator.SessionID).Msg("failed to close goalkeeper runtime")
		}
	}()

	g.sendGoalMessage(
		ctx,
		locator,
		fmt.Sprintf("Goal run started. Max iterations: %d.\n\nGoal: %s", maxIterations, objective),
	)

	result, err := runGoalkeeperWorkflow(
		ctx,
		goalRuntime.Runner,
		ts.GetUserID(),
		goalSessionID,
		formatGoalkeeperPrompt(objective),
		func(update goalPhaseUpdate) {
			g.recordGoalProgress(ctx, taskID, update)
			msg := formatGoalPhaseUpdate(update, maxIterations)
			if msg == "" {
				return
			}
			g.sendGoalMessage(ctx, locator, msg)
		},
	)
	if err != nil {
		if ctx.Err() != nil {
			g.markTaskStatus(context.Background(), taskID, baldastate.SwarmTaskStatusCanceled, "goal.runner", "goal run canceled", nil)
			g.sendGoalMessage(context.Background(), locator, "Goal run canceled.")
			return
		}
		g.markTaskStatus(context.Background(), taskID, baldastate.SwarmTaskStatusFailed, "goal.runner", err.Error(), nil)
		g.sendGoalMessage(context.Background(), locator, fmt.Sprintf("Goal run failed: %v", err))
		return
	}

	if result.GoalReached {
		g.setTaskResult(ctx, taskID, result, baldastate.SwarmTaskStatusCompleted, "")
		g.sendGoalMessage(ctx, locator, g.renderTaskOutcome(ctx, taskID, ts, "Goal run completed."))
		return
	}

	g.setTaskResult(ctx, taskID, result, baldastate.SwarmTaskStatusFailed, "max iterations reached")
	g.sendGoalMessage(ctx, locator, g.renderTaskOutcome(ctx, taskID, ts, "Goal run reached max iterations without passing validation."))
}

func (g *GoalRunner) markTaskStatus(ctx context.Context, taskID string, status string, actor string, reason string, payload any) {
	if strings.TrimSpace(taskID) == "" || g == nil || g.tasks == nil {
		return
	}
	if err := g.tasks.MarkStatus(ctx, taskID, status, actor, "", reason, payload); err != nil {
		g.logger.Warn().Err(err).Str("task_id", taskID).Str("status", status).Msg("failed to mark goal task status")
	}
}

func (g *GoalRunner) setTaskResult(ctx context.Context, taskID string, result goalRunResult, status string, reason string) {
	if strings.TrimSpace(taskID) == "" || g == nil || g.tasks == nil {
		return
	}
	payload := map[string]any{
		"final_text":   result.FinalText,
		"goal_reached": result.GoalReached,
		"iterations":   result.Iterations,
	}
	if err := g.tasks.SetResult(ctx, taskID, payload, status, "goal.runner", reason); err != nil {
		g.logger.Warn().Err(err).Str("task_id", taskID).Str("status", status).Msg("failed to set goal task result")
	}
}

func (g *GoalRunner) renderTaskOutcome(ctx context.Context, taskID string, ts *baldasession.TopicSession, fallback string) string {
	if strings.TrimSpace(taskID) == "" || g == nil || g.tasks == nil {
		return fallback
	}
	task, ok, err := g.tasks.Get(ctx, taskID)
	if err != nil || !ok {
		return fallback
	}
	artifacts := taskArtifactSnapshot{}
	if ts != nil {
		artifacts.WorkspaceDir = strings.TrimSpace(ts.GetWorkspaceDir())
		artifacts.BranchName = strings.TrimSpace(ts.GetBranchName())
	}
	return renderReviewableOutcome(task, enrichGitArtifacts(ctx, artifacts))
}

func (g *GoalRunner) recordGoalProgress(ctx context.Context, taskID string, update goalPhaseUpdate) {
	if strings.TrimSpace(taskID) == "" || g == nil || g.tasks == nil {
		return
	}
	status := ""
	if update.Kind == goalPhaseStarted {
		switch update.Step {
		case goalkeeperWorkerStep:
			status = baldastate.SwarmTaskStatusWaitingForAgent
		case goalkeeperValidatorStep:
			status = baldastate.SwarmTaskStatusValidating
		}
	}
	payload := map[string]any{
		"iteration":    update.Iteration,
		"step":         update.Step,
		"kind":         update.Kind,
		"text":         update.Text,
		"goal_reached": update.GoalReached,
		"failed":       update.Failed,
	}
	if update.Kind == goalPhaseStarted {
		if err := g.tasks.AppendEvent(ctx, taskID, swarm.TaskEventAgentStarted, "goal.runner", "", payload); err != nil {
			g.logger.Warn().Err(err).Str("task_id", taskID).Msg("failed to append goal task agent start")
		}
	}
	if status != "" {
		g.markTaskStatus(ctx, taskID, status, "goal.runner", "", payload)
		return
	}
	if err := g.tasks.AppendEvent(ctx, taskID, swarm.TaskEventAgentResult, "goal.runner", "", payload); err != nil {
		g.logger.Warn().Err(err).Str("task_id", taskID).Msg("failed to append goal task progress")
	}
}

func (g *GoalRunner) sendGoalMessage(ctx context.Context, locator baldasession.SessionLocator, text string) {
	if g == nil || g.channel == nil {
		return
	}
	_ = g.channel.SendAgentReply(ctx, locator, text)
}

type goalRunResult struct {
	FinalText   string
	GoalReached bool
	Iterations  int
}

type goalPhaseUpdate struct {
	Iteration   int
	Step        string
	Kind        string
	Text        string
	GoalReached bool
	Failed      bool
}

func formatGoalPhaseUpdate(update goalPhaseUpdate, maxIterations int) string {
	if update.Iteration <= 0 {
		return ""
	}
	step := strings.TrimSpace(update.Step)
	if step == "" {
		return ""
	}
	switch update.Kind {
	case goalPhaseStarted:
		return fmt.Sprintf("Goal iteration %d/%d: %s started.", update.Iteration, maxIterations, step)
	case goalPhaseFinished:
		prefix := fmt.Sprintf("Goal iteration %d/%d: %s finished.", update.Iteration, maxIterations, step)
		if update.Failed {
			prefix = fmt.Sprintf("Goal iteration %d/%d: %s failed.", update.Iteration, maxIterations, step)
		}
		if step == goalkeeperValidatorStep {
			status := "fail"
			if update.GoalReached {
				status = "pass"
			}
			prefix = fmt.Sprintf("Goal iteration %d/%d: validator finished (%s).", update.Iteration, maxIterations, status)
			if update.Failed {
				prefix = fmt.Sprintf("Goal iteration %d/%d: validator failed.", update.Iteration, maxIterations)
			}
		}
		details := strings.TrimSpace(update.Text)
		if details == "" {
			return prefix
		}
		return prefix + "\n\n" + details
	default:
		return ""
	}
}

func runGoalkeeperWorkflow(
	ctx context.Context,
	r *runner.Runner,
	userID string,
	goalSessionID string,
	prompt string,
	onProgress func(goalPhaseUpdate),
) (goalRunResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return goalRunResult{}, err
	}
	if r == nil {
		return goalRunResult{}, fmt.Errorf("runner is required")
	}
	userContent := genai.NewContentFromText(prompt, genai.RoleUser)

	result := goalRunResult{}
	currentIteration := 0
	currentStep := ""
	stepText := map[string]string{
		goalkeeperWorkerStep:    "",
		goalkeeperValidatorStep: "",
	}
	lastText := ""
	lastValidatorText := ""
	for ev, err := range r.Run(ctx, userID, goalSessionID, userContent, adkagent.RunConfig{}) {
		if err != nil {
			return result, fmt.Errorf("run goalkeeper workflow: %w", err)
		}
		if ev == nil {
			continue
		}

		if kind, step, ok := goalkeeperStepEvent(ev); ok {
			switch kind {
			case goalkeeperStepEventStarted:
				if step == goalkeeperWorkerStep {
					currentIteration++
					stepText[goalkeeperWorkerStep] = ""
					stepText[goalkeeperValidatorStep] = ""
				}
				if step == goalkeeperValidatorStep && currentIteration == 0 {
					currentIteration = 1
				}
				currentStep = step
				if onProgress != nil {
					onProgress(goalPhaseUpdate{
						Iteration: currentIteration,
						Step:      step,
						Kind:      goalPhaseStarted,
					})
				}
			case goalkeeperStepEventCompleted, goalkeeperStepEventFailed:
				stepResult := strings.TrimSpace(stepText[step])
				failed := kind == goalkeeperStepEventFailed
				if step == goalkeeperValidatorStep && metadataBool(ev, goalkeeperMetadataEscalatedKey) {
					result.GoalReached = true
				}
				if step == goalkeeperValidatorStep {
					result.Iterations = currentIteration
					if stepResult != "" {
						lastValidatorText = stepResult
					}
				}
				if onProgress != nil {
					onProgress(goalPhaseUpdate{
						Iteration:   currentIteration,
						Step:        step,
						Kind:        goalPhaseFinished,
						Text:        stepResult,
						GoalReached: step == goalkeeperValidatorStep && metadataBool(ev, goalkeeperMetadataEscalatedKey),
						Failed:      failed,
					})
				}
				if currentStep == step {
					currentStep = ""
				}
			}
			continue
		}

		text := visibleEventText(ev)
		if text == "" {
			continue
		}
		lastText = text
		if currentStep != "" {
			stepText[currentStep] = text
		}
		if isValidatorEvent(currentStep, ev) && ev.IsFinalResponse() {
			lastValidatorText = text
			if ev.Actions.Escalate {
				result.GoalReached = true
			}
		}
	}

	result.FinalText = lastValidatorText
	if result.FinalText == "" {
		result.FinalText = lastText
	}
	return result, nil
}

func runAgentTurn(
	ctx context.Context,
	r *runner.Runner,
	userID string,
	goalSessionID string,
	prompt string,
) (string, error) {
	return runAgentTurnWithProgress(ctx, r, userID, goalSessionID, prompt, nil)
}

func runAgentTurnWithProgress(
	ctx context.Context,
	r *runner.Runner,
	userID string,
	goalSessionID string,
	prompt string,
	onProgress func(string),
) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if r == nil {
		return "", fmt.Errorf("runner is required")
	}
	userContent := genai.NewContentFromText(prompt, genai.RoleUser)

	var out strings.Builder
	sawTurnComplete := false
	for ev, err := range r.Run(ctx, userID, goalSessionID, userContent, adkagent.RunConfig{}) {
		if err != nil {
			return "", fmt.Errorf("run agent turn: %w", err)
		}
		if ev == nil {
			continue
		}
		if ev.Content != nil {
			for _, part := range ev.Content.Parts {
				if part == nil || part.Thought || part.Text == "" {
					continue
				}
				out.WriteString(part.Text)
				if onProgress != nil && !ev.IsFinalResponse() {
					onProgress(part.Text)
				}
			}
		}
		if ev.TurnComplete {
			sawTurnComplete = true
		}
	}
	if !sawTurnComplete {
		return strings.TrimSpace(out.String()), fmt.Errorf("goal iteration ended without completion")
	}
	return strings.TrimSpace(out.String()), nil
}

func runGoalIteration(
	ctx context.Context,
	r *runner.Runner,
	userID string,
	goalSessionID string,
	prompt string,
) (string, error) {
	return runAgentTurn(ctx, r, userID, goalSessionID, prompt)
}

func formatGoalkeeperPrompt(goal string) string {
	return "Goal:\n" + strings.TrimSpace(goal)
}

func goalkeeperStepEvent(ev *adksession.Event) (kind string, step string, ok bool) {
	if ev == nil || ev.CustomMetadata == nil {
		return "", "", false
	}
	kind, _ = ev.CustomMetadata[goalkeeperMetadataEventKey].(string)
	step, _ = ev.CustomMetadata[goalkeeperMetadataStepKey].(string)
	if kind == "" || step == "" {
		return "", "", false
	}
	return kind, step, true
}

func metadataBool(ev *adksession.Event, key string) bool {
	if ev == nil || ev.CustomMetadata == nil {
		return false
	}
	value, _ := ev.CustomMetadata[key].(bool)
	return value
}

func isValidatorEvent(currentStep string, ev *adksession.Event) bool {
	if currentStep == goalkeeperValidatorStep {
		return true
	}
	return ev != nil && ev.Author == goalkeeperValidatorName
}

func visibleEventText(ev *adksession.Event) string {
	if ev == nil || ev.Content == nil {
		return ""
	}
	var parts []string
	for _, part := range ev.Content.Parts {
		if part != nil && !part.Thought && strings.TrimSpace(part.Text) != "" {
			parts = append(parts, strings.TrimSpace(part.Text))
		}
	}
	return strings.Join(parts, "\n\n")
}

func (g *GoalRunner) Cancel(locator baldasession.SessionLocator) bool {
	sessionID := strings.TrimSpace(locator.SessionID)
	if sessionID == "" {
		return false
	}

	g.mu.Lock()
	cancel := g.running[sessionID]
	g.mu.Unlock()
	if cancel == nil {
		return false
	}
	cancel()
	return true
}

func (g *GoalRunner) removeRun(sessionID string) {
	g.mu.Lock()
	delete(g.running, sessionID)
	g.mu.Unlock()
}

func (g *GoalRunner) stopAll() {
	g.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(g.running))
	for _, cancel := range g.running {
		cancels = append(cancels, cancel)
	}
	g.running = make(map[string]context.CancelFunc)
	g.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
}
