package agent

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"regexp"
	"strings"
	"sync"
	"time"

	adkagent "google.golang.org/adk/agent"
	adkrunner "google.golang.org/adk/runner"
	adksession "google.golang.org/adk/session"
	"google.golang.org/genai"
)

const (
	goalWorkerName           = "GoalWorker"
	goalValidatorName        = "GoalValidator"
	goalCommitterName        = "GoalCommitter"
	goalWorkerOutputStateKey = "goal_worker_output"

	goalkeeperRootAgentName = "GoalKeeper"

	goalMetadataEventKey              = "norma.goal.event"
	goalMetadataStepKey               = "norma.goal.step"
	goalMetadataAgentKey              = "norma.goal.agent"
	goalMetadataStepIDKey             = "norma.goal.step_id"
	goalMetadataEventCountKey         = "event_count"
	goalMetadataFinalResponseCountKey = "final_response_count"
	goalMetadataVisibleTextLenKey     = "visible_text_len"
	goalMetadataDurationMSKey         = "duration_ms"
	goalMetadataEscalatedKey          = "escalated"
	goalMetadataErrorKey              = "error"

	goalStepStarted   = "step_started"
	goalStepCompleted = "step_completed"
	goalStepFailed    = "step_failed"

	goalWorkerStep    = "worker"
	goalValidatorStep = "validator"
)

var conventionalCommitSubjectPattern = regexp.MustCompile(`^[a-z]+(?:\([^)]+\))?(?:!)?: .+`)

// GoalBuildConfig configures Balda's /goal worker-validator workflow.
type GoalBuildConfig struct {
	BaseAgent           adkagent.Agent
	ProviderID          string
	SessionID           string
	WorkerSessionID     string
	ValidatorSessionID  string
	BranchName          string
	WorkspaceDir        string
	MaxIterations       uint
	AppName             string
	SessionService      adksession.Service
	BundledMCPServerIDs []string
	ExtraMCPServerIDs   []string
}

// BuildGoalWorkflow builds Balda's /goal worker-validator workflow using
// Balda's configured provider for both child agents.
func (b *Builder) BuildGoalWorkflow(ctx context.Context, cfg GoalBuildConfig) (adkagent.Agent, error) {
	_ = ctx
	if b == nil {
		return nil, fmt.Errorf("agent builder is required")
	}
	providerID := strings.TrimSpace(cfg.ProviderID)
	if providerID == "" {
		return nil, fmt.Errorf("balda provider is not configured")
	}
	if strings.TrimSpace(cfg.SessionID) == "" {
		return nil, fmt.Errorf("session id is required")
	}
	if strings.TrimSpace(cfg.WorkerSessionID) == "" {
		return nil, fmt.Errorf("worker session id is required")
	}
	if strings.TrimSpace(cfg.ValidatorSessionID) == "" {
		return nil, fmt.Errorf("validator session id is required")
	}
	if strings.TrimSpace(cfg.WorkspaceDir) == "" {
		return nil, fmt.Errorf("workspace dir is required")
	}
	if cfg.MaxIterations == 0 {
		return nil, fmt.Errorf("max iterations must be greater than zero")
	}
	if cfg.BaseAgent == nil {
		return nil, fmt.Errorf("goal base agent is required")
	}
	if cfg.SessionService == nil {
		return nil, fmt.Errorf("goal session service is required")
	}
	appName := strings.TrimSpace(cfg.AppName)
	if appName == "" {
		appName = defaultRuntimeAppName
	}

	worker, err := wrapGoalPromptAgent(cfg.BaseAgent, goalPromptAgentConfig{
		Name:        goalWorkerName,
		Description: "Goal worker agent",
		BuildPrompt: func(ctx adkagent.InvocationContext) (string, error) {
			prompt := extractGoalPromptText(ctx.UserContent())
			if prompt == "" {
				return "", fmt.Errorf("goal prompt is empty")
			}
			return joinGoalPromptSections(goalWorkerInstruction(), prompt), nil
		},
	})
	if err != nil {
		return nil, err
	}

	validator, err := wrapGoalPromptAgent(cfg.BaseAgent, goalPromptAgentConfig{
		Name:        goalValidatorName,
		Description: "Goal validator agent",
		BuildPrompt: func(ctx adkagent.InvocationContext) (string, error) {
			prompt := extractGoalPromptText(ctx.UserContent())
			if prompt == "" {
				return "", fmt.Errorf("goal validation prompt is empty")
			}
			return joinGoalPromptSections(goalValidatorInstruction(), prompt), nil
		},
	})
	if err != nil {
		return nil, err
	}

	workerRunner, err := adkrunner.New(adkrunner.Config{
		AppName:        appName,
		Agent:          worker,
		SessionService: cfg.SessionService,
	})
	if err != nil {
		return nil, fmt.Errorf("creating goal worker runner: %w", err)
	}
	validatorRunner, err := adkrunner.New(adkrunner.Config{
		AppName:        appName,
		Agent:          validator,
		SessionService: cfg.SessionService,
	})
	if err != nil {
		return nil, fmt.Errorf("creating goal validator runner: %w", err)
	}

	workflow, err := newGoalKeeperAgent(goalKeeperConfig{
		Worker:             worker,
		Validator:          validator,
		WorkerRunner:       workerRunner,
		ValidatorRunner:    validatorRunner,
		WorkerSessionID:    cfg.WorkerSessionID,
		ValidatorSessionID: cfg.ValidatorSessionID,
		MaxIterations:      cfg.MaxIterations,
	})
	if err != nil {
		return nil, err
	}
	return &closableGoalWorkflow{Agent: workflow, base: workflow}, nil
}

type goalPromptBuilder func(adkagent.InvocationContext) (string, error)

type goalPromptAgentConfig struct {
	Name        string
	Description string
	OutputKey   string
	BuildPrompt goalPromptBuilder
}

func wrapGoalPromptAgent(base adkagent.Agent, cfg goalPromptAgentConfig) (adkagent.Agent, error) {
	if base == nil {
		return nil, fmt.Errorf("goal base agent is required")
	}
	if cfg.BuildPrompt == nil {
		return nil, fmt.Errorf("goal prompt builder is required")
	}
	outputKey := strings.TrimSpace(cfg.OutputKey)
	return adkagent.New(adkagent.Config{
		Name:        cfg.Name,
		Description: cfg.Description,
		Run: func(ctx adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
			return func(yield func(*adksession.Event, error) bool) {
				prompt, err := cfg.BuildPrompt(ctx)
				if err != nil {
					yield(nil, err)
					return
				}
				wrappedCtx := goalUserContentContext{
					InvocationContext: ctx,
					userContent:       genai.NewContentFromText(prompt, genai.RoleUser),
				}
				latestVisibleOutput := ""
				var bufferedEvent *adksession.Event
				for ev, err := range base.Run(wrappedCtx) {
					if text := visibleGoalEventText(ev); text != "" {
						latestVisibleOutput = strings.TrimSpace(text)
					}
					if err != nil {
						if bufferedEvent != nil {
							if !yield(bufferedEvent, nil) {
								return
							}
						}
						yield(ev, err)
						return
					}
					if ev == nil {
						continue
					}
					if ev.Partial {
						if !yield(ev, nil) {
							return
						}
						continue
					}
					if bufferedEvent != nil {
						if !yield(bufferedEvent, nil) {
							return
						}
					}
					bufferedEvent = ev
				}
				if bufferedEvent == nil {
					return
				}
				if outputKey != "" && latestVisibleOutput != "" {
					if bufferedEvent.Actions.StateDelta == nil {
						bufferedEvent.Actions.StateDelta = make(map[string]any)
					}
					bufferedEvent.Actions.StateDelta[outputKey] = latestVisibleOutput
					if ctx != nil && ctx.Session() != nil {
						if err := ctx.Session().State().Set(outputKey, latestVisibleOutput); err != nil {
							yield(nil, fmt.Errorf("set goal session output %q: %w", outputKey, err))
							return
						}
					}
				}
				yield(bufferedEvent, nil)
			}
		},
	})
}

type goalRunner interface {
	Run(ctx context.Context, userID string, sessionID string, userContent *genai.Content, cfg adkagent.RunConfig, opts ...adkrunner.RunOption) iter.Seq2[*adksession.Event, error]
}

type goalKeeperConfig struct {
	Worker             adkagent.Agent
	Validator          adkagent.Agent
	WorkerRunner       goalRunner
	ValidatorRunner    goalRunner
	WorkerSessionID    string
	ValidatorSessionID string
	MaxIterations      uint
}

func newGoalKeeperAgent(cfg goalKeeperConfig) (adkagent.Agent, error) {
	if cfg.Worker == nil {
		return nil, fmt.Errorf("goal worker agent is required")
	}
	if cfg.Validator == nil {
		return nil, fmt.Errorf("goal validator agent is required")
	}
	if cfg.WorkerRunner == nil {
		return nil, fmt.Errorf("goal worker runner is required")
	}
	if cfg.ValidatorRunner == nil {
		return nil, fmt.Errorf("goal validator runner is required")
	}
	if strings.TrimSpace(cfg.WorkerSessionID) == "" {
		return nil, fmt.Errorf("worker session id is required")
	}
	if strings.TrimSpace(cfg.ValidatorSessionID) == "" {
		return nil, fmt.Errorf("validator session id is required")
	}
	if cfg.MaxIterations == 0 {
		return nil, fmt.Errorf("max iterations must be greater than zero")
	}
	return adkagent.New(adkagent.Config{
		Name:        goalkeeperRootAgentName,
		Description: "Retries a goal worker and goal validator until validation passes one goal.",
		SubAgents:   []adkagent.Agent{cfg.Worker, cfg.Validator},
		Run: func(ctx adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
			return runGoalKeeperLoop(ctx, cfg)
		},
	})
}

func runGoalKeeperLoop(ctx adkagent.InvocationContext, cfg goalKeeperConfig) iter.Seq2[*adksession.Event, error] {
	return func(yield func(*adksession.Event, error) bool) {
		objective := extractGoalPromptText(ctx.UserContent())
		if objective == "" {
			yield(nil, fmt.Errorf("goal prompt is empty"))
			return
		}
		userID := ""
		if ctx != nil && ctx.Session() != nil {
			userID = strings.TrimSpace(ctx.Session().UserID())
		}
		if userID == "" {
			yield(nil, fmt.Errorf("goal user id is required"))
			return
		}

		var previousWorkerResult string
		var previousValidatorResult string
		for range cfg.MaxIterations {
			workerPrompt := buildGoalWorkerPrompt(objective, previousWorkerResult, previousValidatorResult)
			workerResult, err := runGoalStep(
				ctx,
				cfg.WorkerRunner,
				cfg.Worker,
				goalStepSpec{name: goalWorkerStep, id: 1},
				userID,
				cfg.WorkerSessionID,
				workerPrompt,
				yield,
			)
			if err != nil {
				return
			}
			previousWorkerResult = workerResult

			validatorPrompt := buildGoalValidationPromptFromText(objective, previousWorkerResult, previousValidatorResult)
			validatorResult, err := runGoalStep(
				ctx,
				cfg.ValidatorRunner,
				cfg.Validator,
				goalStepSpec{name: goalValidatorStep, id: 2},
				userID,
				cfg.ValidatorSessionID,
				validatorPrompt,
				yield,
			)
			if err != nil {
				return
			}
			previousValidatorResult = validatorResult
			if strings.HasPrefix(strings.TrimSpace(validatorResult), "verdict: pass") {
				return
			}
		}
	}
}

type goalStepSpec struct {
	name string
	id   int
}

type goalStepStats struct {
	eventCount         int
	finalResponseCount int
	visibleTextLen     int
	duration           time.Duration
	escalated          bool
	err                error
}

func (s *goalStepStats) record(ev *adksession.Event) {
	if ev == nil {
		return
	}
	s.eventCount++
	text := visibleGoalEventText(ev)
	s.visibleTextLen += len(text)
	if ev.IsFinalResponse() {
		s.finalResponseCount++
	}
	if ev.Actions.Escalate {
		s.escalated = true
	}
}

func runGoalStep(
	ctx context.Context,
	r goalRunner,
	agent adkagent.Agent,
	spec goalStepSpec,
	userID string,
	sessionID string,
	prompt string,
	yield func(*adksession.Event, error) bool,
) (string, error) {
	startedAt := time.Now()
	invocationID := ""
	if invCtx, ok := ctx.(adkagent.InvocationContext); ok {
		invocationID = invCtx.InvocationID()
	}
	if !yield(newGoalStepEvent(invocationID, agent, spec, goalStepStarted, goalStepStats{}), nil) {
		return "", fmt.Errorf("goal step delivery stopped")
	}

	stats := goalStepStats{}
	latestVisibleOutput := ""
	for ev, err := range r.Run(ctx, userID, sessionID, genai.NewContentFromText(prompt, genai.RoleUser), adkagent.RunConfig{}) {
		if err != nil {
			stats.err = err
			stats.duration = time.Since(startedAt)
			if !yield(newGoalStepEvent(invocationID, agent, spec, goalStepFailed, stats), nil) {
				return latestVisibleOutput, err
			}
			yield(ev, err)
			return latestVisibleOutput, err
		}
		if text := visibleGoalEventText(ev); text != "" {
			latestVisibleOutput = strings.TrimSpace(text)
		}
		stats.record(ev)
		if ev != nil {
			ev.Author = goalkeeperRootAgentName
		}
		if !yield(ev, nil) {
			return latestVisibleOutput, fmt.Errorf("goal step delivery stopped")
		}
	}
	stats.duration = time.Since(startedAt)
	if strings.HasPrefix(strings.TrimSpace(latestVisibleOutput), "verdict: pass") {
		stats.escalated = true
	}
	if !yield(newGoalStepEvent(invocationID, agent, spec, goalStepCompleted, stats), nil) {
		return latestVisibleOutput, fmt.Errorf("goal step delivery stopped")
	}
	return latestVisibleOutput, nil
}

func newGoalStepEvent(
	invocationID string,
	agent adkagent.Agent,
	spec goalStepSpec,
	eventType string,
	stats goalStepStats,
) *adksession.Event {
	ev := adksession.NewEvent(invocationID)
	ev.Author = goalkeeperRootAgentName
	ev.CustomMetadata = map[string]any{
		goalMetadataEventKey:  eventType,
		goalMetadataStepKey:   spec.name,
		goalMetadataStepIDKey: spec.id,
		goalMetadataAgentKey:  agent.Name(),
	}
	if eventType == goalStepCompleted || eventType == goalStepFailed {
		ev.CustomMetadata[goalMetadataEventCountKey] = stats.eventCount
		ev.CustomMetadata[goalMetadataFinalResponseCountKey] = stats.finalResponseCount
		ev.CustomMetadata[goalMetadataVisibleTextLenKey] = stats.visibleTextLen
		ev.CustomMetadata[goalMetadataDurationMSKey] = stats.duration.Milliseconds()
		ev.CustomMetadata[goalMetadataEscalatedKey] = stats.escalated
	}
	if stats.err != nil {
		ev.CustomMetadata[goalMetadataErrorKey] = stats.err.Error()
	}
	return ev
}

func goalWorkerInstruction() string {
	return strings.Join([]string{
		"You are the goal worker agent.",
		"You receive one user goal as plain text.",
		"Use the available goal and context.",
		"When previous worker or validator results are provided, use them as feedback and correct the next attempt.",
		"Do the requested work in the current working directory.",
		"Prefer direct execution over clarification when execution is possible.",
		"Ask clarifying questions only when execution is blocked by missing critical information.",
		"Return a concise plain-text summary of what changed and what evidence supports it.",
		"Run only lightweight sanity checks directly relevant to the work unless the goal asks for broader verification.",
	}, "\n")
}

func goalValidatorInstruction() string {
	return strings.Join([]string{
		"You are the goal validator agent.",
		"Validate the provided worker result against the original user goal using your isolated validator session.",
		"Inspect the current working directory as needed.",
		"Do not intentionally mutate files or continue the worker's implementation work.",
		"Start with exactly `verdict: pass` or `verdict: fail`.",
		"`verdict: pass` means the goal was reached.",
		"`verdict: fail` means the goal was not reached.",
		"Then provide brief evidence and a concise final summary.",
	}, "\n")
}

func buildGoalCommitAgent(base adkagent.Agent) (adkagent.Agent, error) {
	return wrapGoalPromptAgent(base, goalPromptAgentConfig{
		Name:        goalCommitterName,
		Description: "Goal export commit message generator",
		BuildPrompt: func(ctx adkagent.InvocationContext) (string, error) {
			return joinGoalPromptSections(goalCommitterInstruction(), extractGoalPromptText(ctx.UserContent())), nil
		},
	})
}

func goalCommitterInstruction() string {
	return strings.Join([]string{
		"You generate a Conventional Commit subject for goal export.",
		"Return exactly one line.",
		"Do not wrap the result in quotes, bullets, markdown, or code fences.",
		"Use a valid Conventional Commit subject like `feat: add retry logging` or `fix(goal): handle nil session`.",
		"Keep it concise and specific to the actual workspace changes.",
		"If the evidence is ambiguous, prefer `chore(goal): <summary>`.",
	}, "\n")
}

func normalizeGoalCommitMessage(objective, raw string) string {
	line := firstGoalCommitLine(raw)
	if conventionalCommitSubjectPattern.MatchString(line) {
		return line
	}
	return fallbackGoalCommitMessage(objective)
}

func fallbackGoalCommitMessage(objective string) string {
	summary := strings.Join(strings.Fields(strings.TrimSpace(objective)), " ")
	if summary == "" {
		summary = "apply goal workspace changes"
	}
	const maxLen = 72
	prefix := "chore(goal): "
	if len(summary) > maxLen-len(prefix) {
		summary = strings.TrimSpace(summary[:maxLen-len(prefix)])
	}
	return prefix + summary
}

func firstGoalCommitLine(raw string) string {
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		line = strings.TrimSpace(strings.Trim(line, "`"))
		if line != "" {
			return line
		}
	}
	return ""
}

func visibleGoalEventText(ev *adksession.Event) string {
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

func wrapGoalValidatorWithWorkerOutput(inner adkagent.Agent, workerOutputStateKey string, roleInstruction string) (adkagent.Agent, error) {
	key := strings.TrimSpace(workerOutputStateKey)
	if key == "" {
		return nil, fmt.Errorf("worker output state key is required")
	}
	base, err := wrapGoalPromptAgent(inner, goalPromptAgentConfig{
		Name:        inner.Name(),
		Description: inner.Description(),
		BuildPrompt: func(ctx adkagent.InvocationContext) (string, error) {
			prompt := buildGoalValidationPrompt(ctx, key)
			return joinGoalPromptSections(roleInstruction, prompt), nil
		},
	})
	if err != nil {
		return nil, err
	}
	return base, nil
}

type goalUserContentContext struct {
	adkagent.InvocationContext
	userContent *genai.Content
}

func (c goalUserContentContext) UserContent() *genai.Content {
	return c.userContent
}

type closableGoalWorkflow struct {
	adkagent.Agent
	base adkagent.Agent
	once sync.Once
	err  error
}

func (w *closableGoalWorkflow) Name() string {
	if w == nil || w.base == nil {
		return ""
	}
	return w.base.Name()
}

func (w *closableGoalWorkflow) Description() string {
	if w == nil || w.base == nil {
		return ""
	}
	return w.base.Description()
}

func (w *closableGoalWorkflow) SubAgents() []adkagent.Agent {
	if w == nil || w.base == nil {
		return nil
	}
	return w.base.SubAgents()
}

func (w *closableGoalWorkflow) Run(ctx adkagent.InvocationContext) iter.Seq2[*adksession.Event, error] {
	if w == nil || w.base == nil {
		return func(func(*adksession.Event, error) bool) {}
	}
	return w.base.Run(ctx)
}

func (w *closableGoalWorkflow) FindAgent(name string) adkagent.Agent {
	if w == nil || w.base == nil {
		return nil
	}
	return w.base.FindAgent(name)
}

func (w *closableGoalWorkflow) FindSubAgent(name string) adkagent.Agent {
	if w == nil || w.base == nil {
		return nil
	}
	return w.base.FindSubAgent(name)
}

func (w *closableGoalWorkflow) Close() error {
	if w == nil {
		return nil
	}
	w.once.Do(func() {
		errs := make([]error, 0, 1)
		if err := closeRuntimeAgent(w.base); err != nil {
			errs = append(errs, err)
		}
		w.err = errors.Join(errs...)
	})
	return w.err
}

func extractGoalPromptText(content *genai.Content) string {
	if content == nil {
		return ""
	}
	var parts []string
	for _, part := range content.Parts {
		if part == nil || part.Thought {
			continue
		}
		text := strings.TrimSpace(part.Text)
		if text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n\n")
}

func joinGoalPromptSections(parts ...string) string {
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			filtered = append(filtered, trimmed)
		}
	}
	return strings.Join(filtered, "\n\n")
}

func buildGoalValidationPrompt(ctx adkagent.InvocationContext, workerOutputStateKey string) string {
	goal := extractGoalPromptText(ctx.UserContent())
	if goal == "" {
		goal = "Goal:"
	}
	workerOutput := ""
	if ctx != nil && ctx.Session() != nil {
		value, err := ctx.Session().State().Get(workerOutputStateKey)
		if err == nil && value != nil {
			workerOutput = strings.TrimSpace(fmt.Sprintf("%v", value))
		}
	}
	if workerOutput == "" {
		workerOutput = "(none)"
	}
	return goal + "\n\nWorker result:\n" + workerOutput
}

func buildGoalWorkerPrompt(objective string, previousWorkerResult string, previousValidatorResult string) string {
	sections := []string{
		strings.TrimSpace(objective),
	}
	if trimmed := strings.TrimSpace(previousWorkerResult); trimmed != "" {
		sections = append(sections, "Previous worker result:\n"+trimmed)
	}
	if trimmed := strings.TrimSpace(previousValidatorResult); trimmed != "" {
		sections = append(sections, "Previous validator result:\n"+trimmed)
	}
	return joinGoalPromptSections(sections...)
}

func buildGoalValidationPromptFromText(objective string, workerResult string, previousValidatorResult string) string {
	workerResult = strings.TrimSpace(workerResult)
	if workerResult == "" {
		workerResult = "(none)"
	}
	sections := []string{
		strings.TrimSpace(objective),
		"Worker result:\n" + workerResult,
	}
	if trimmed := strings.TrimSpace(previousValidatorResult); trimmed != "" {
		sections = append(sections, "Previous validator result:\n"+trimmed)
	}
	return joinGoalPromptSections(sections...)
}
