package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

type taskAgentRuntimeBuilder interface {
	BuildTaskAgentRuntime(ctx context.Context, cfg baldaagent.TaskAgentRuntimeConfig) (*baldaagent.TaskAgentRuntime, error)
}

type taskAgentActor struct {
	sessions       *baldasession.Manager
	runtimeBuilder taskAgentRuntimeBuilder
	coordinator    *swarm.Coordinator
	agents         *swarm.AgentRegistry
	tasks          *swarm.TaskService
	taskRuns       *taskRunRegistry
	logger         zerolog.Logger
}

type taskAgentActorParams struct {
	fx.In

	SessionManager *baldasession.Manager
	RuntimeManager *baldaagent.RuntimeManager
	Coordinator    *swarm.Coordinator
	Agents         *swarm.AgentRegistry
	TaskService    *swarm.TaskService
	TaskRuns       *taskRunRegistry
	Logger         zerolog.Logger
}

func newTaskAgentActor(params taskAgentActorParams) swarm.Actor {
	return &taskAgentActor{
		sessions:       params.SessionManager,
		runtimeBuilder: params.RuntimeManager,
		coordinator:    params.Coordinator,
		agents:         params.Agents,
		tasks:          params.TaskService,
		taskRuns:       params.TaskRuns,
		logger:         params.Logger.With().Str("component", "balda.task_agent_actor").Logger(),
	}
}

func (a *taskAgentActor) Address() string {
	return swarm.WildcardAddress(swarm.ActorTypeAgent)
}

func (a *taskAgentActor) Handle(ctx context.Context, env swarm.Envelope) error {
	if strings.TrimSpace(env.Namespace) != swarm.NamespaceAgentCommand {
		return swarm.PolicyError(fmt.Errorf("unsupported agent namespace %q", env.Namespace))
	}
	var payload taskAgentCommandPayload
	if err := json.Unmarshal([]byte(env.PayloadJSON), &payload); err != nil {
		return swarm.PermanentError(fmt.Errorf("decode task agent command: %w", err))
	}
	payload.TaskID = firstNonEmpty(payload.TaskID, env.TaskID)
	payload.Role = normalizeTaskAgentRole(payload.Role)
	if payload.TaskID == "" {
		return swarm.PolicyError(fmt.Errorf("task id is required"))
	}
	if payload.Role == "" {
		return swarm.PolicyError(fmt.Errorf("task agent role is required"))
	}
	payload.AgentName = firstNonEmpty(payload.AgentName, env.To.Key, payload.Role)
	spec, ok := a.resolveAgentSpec(payload.AgentName)
	if !ok {
		return swarm.PolicyError(fmt.Errorf("task agent %q is not configured", payload.AgentName))
	}
	payload.AgentName = spec.Name
	if len(payload.RequestedTools) == 0 {
		payload.RequestedTools = spec.Tools
	}
	if payload.Iteration <= 0 {
		payload.Iteration = 1
	}
	if strings.TrimSpace(payload.Objective) == "" {
		return swarm.PolicyError(fmt.Errorf("task objective is required"))
	}

	stepKey := taskAgentStepKey(payload)
	payloadHash := hashTaskAgentCommandPayload(payload)
	if a.tasks != nil {
		record, created, err := a.tasks.ReserveAgentStep(ctx, baldastate.SwarmAgentStepRecord{
			ID:          uuid.NewString(),
			StepKey:     stepKey,
			TaskID:      payload.TaskID,
			AgentName:   payload.AgentName,
			Role:        payload.Role,
			Iteration:   payload.Iteration,
			PayloadHash: payloadHash,
			Status:      baldastate.SwarmAgentStepStatusRunning,
		})
		if err != nil {
			return swarm.TransientError(err)
		}
		if record.PayloadHash != "" && record.PayloadHash != payloadHash {
			return swarm.PermanentError(fmt.Errorf("agent step %q already reserved for different payload", stepKey))
		}
		if !created && agentStepHasStoredResult(record) {
			return a.publishStoredResult(ctx, env, payload, record.ResultJSON)
		}
	}

	ts, err := a.resolveSession(ctx, payload)
	if err != nil {
		return swarm.TransientError(err)
	}
	if a.runtimeBuilder == nil {
		return swarm.TransientError(fmt.Errorf("task agent runtime builder is required"))
	}
	runtime, err := a.runtimeBuilder.BuildTaskAgentRuntime(ctx, baldaagent.TaskAgentRuntimeConfig{
		SessionID:    ts.GetSessionID(),
		BranchName:   ts.GetBranchName(),
		WorkspaceDir: ts.GetWorkspaceDir(),
		Role:         payload.Role,
	})
	if err != nil {
		return swarm.TransientError(err)
	}
	defer func() {
		if err := runtime.Close(); err != nil {
			a.logger.Warn().Err(err).Str("task_id", payload.TaskID).Str("role", payload.Role).Msg("failed to close task agent runtime")
		}
	}()

	runCtx, cancel := context.WithCancel(ctx)
	a.taskRuns.register(payload.TaskID, cancel)
	defer a.taskRuns.unregister(payload.TaskID)
	defer cancel()

	prompt := formatTaskAgentPrompt(payload, spec)
	text, err := runAgentTurnWithProgress(runCtx, runtime.Runner, ts.GetUserID(), ts.GetAgentSessionID(), prompt, func(progress string) {
		a.recordProgress(ctx, payload, progress)
	})
	if err != nil {
		if errors.Is(runCtx.Err(), context.Canceled) {
			return a.publishResult(ctx, env, payload, stepKey, "", fmt.Errorf("goal run canceled"))
		}
		return a.publishResult(ctx, env, payload, stepKey, text, err)
	}
	return a.publishResult(ctx, env, payload, stepKey, text, nil)
}

func (a *taskAgentActor) recordProgress(ctx context.Context, payload taskAgentCommandPayload, text string) {
	if a == nil || a.tasks == nil {
		return
	}
	progress := strings.TrimSpace(text)
	if progress == "" {
		return
	}
	if err := a.tasks.AppendEvent(ctx, payload.TaskID, swarm.TaskEventAgentProgress, "agent:"+payload.AgentName, "", map[string]any{
		"role":       payload.Role,
		"agent_name": payload.AgentName,
		"iteration":  payload.Iteration,
		"text":       progress,
	}); err != nil {
		a.logger.Warn().Err(err).Str("task_id", payload.TaskID).Str("agent_name", payload.AgentName).Msg("failed to record task agent progress")
	}
}

func (a *taskAgentActor) resolveAgentSpec(name string) (swarm.AgentSpec, bool) {
	if a.agents == nil {
		return swarm.AgentSpec{Name: normalizeTaskAgentRole(name), Role: strings.TrimSpace(name)}, normalizeTaskAgentRole(name) != ""
	}
	return a.agents.Get(name)
}

func (a *taskAgentActor) resolveSession(ctx context.Context, payload taskAgentCommandPayload) (*baldasession.TopicSession, error) {
	if a.sessions == nil {
		return nil, fmt.Errorf("session manager is required")
	}
	ts, err := a.sessions.GetSession(payload.Locator)
	if err == nil {
		return ts, nil
	}
	userID := strings.TrimSpace(payload.TransportUserID)
	if userID == "" {
		return nil, fmt.Errorf("transport user id is required")
	}
	sessionCtx := baldasession.SessionContext{
		Locator: payload.Locator,
		UserID:  userID,
	}
	ts, err = a.sessions.RestoreSession(ctx, sessionCtx)
	if err == nil {
		return ts, nil
	}
	if !errors.Is(err, baldasession.ErrNoPersistedSession) {
		return nil, err
	}
	return a.sessions.EnsureSession(ctx, sessionCtx, ownerSessionLabel)
}

func (a *taskAgentActor) publishResult(
	ctx context.Context,
	cause swarm.Envelope,
	command taskAgentCommandPayload,
	stepKey string,
	text string,
	runErr error,
) error {
	data, err := marshalTaskAgentResult(command, text, runErr)
	if err != nil {
		return err
	}
	if a.tasks != nil {
		if runErr != nil {
			if err := a.tasks.FailAgentStep(ctx, stepKey, data, runErr.Error()); err != nil {
				return swarm.TransientError(err)
			}
		} else if err := a.tasks.CompleteAgentStep(ctx, stepKey, data); err != nil {
			return swarm.TransientError(err)
		}
	}
	return a.publishStoredResult(ctx, cause, command, data)
}

func (a *taskAgentActor) publishStoredResult(
	ctx context.Context,
	cause swarm.Envelope,
	command taskAgentCommandPayload,
	payloadJSON string,
) error {
	if a.coordinator == nil || !a.coordinator.RuntimeEnabled() {
		return swarm.TransientError(fmt.Errorf("swarm coordinator is required"))
	}
	env := swarm.Envelope{
		ID:            uuid.NewString(),
		Namespace:     swarm.NamespaceAgentResult,
		Kind:          swarm.KindGoal,
		From:          swarm.ActorAddress{Target: swarm.ActorTypeAgent, Key: firstNonEmpty(command.AgentName, command.Role)},
		To:            swarm.ActorAddress{Target: swarm.ActorTypeTask, Key: command.TaskID},
		SessionID:     command.Locator.SessionID,
		TaskID:        command.TaskID,
		CorrelationID: firstNonEmpty(cause.CorrelationID, command.TaskID),
		CausationID:   cause.ID,
		Priority:      75,
		DedupeKey:     taskAgentResultDedupeKey(command),
		PayloadJSON:   strings.TrimSpace(payloadJSON),
	}
	if _, err := a.coordinator.Submit(ctx, env); err != nil {
		return swarm.TransientError(err)
	}
	return nil
}

func marshalTaskAgentResult(command taskAgentCommandPayload, text string, runErr error) (string, error) {
	result := taskAgentResultPayload{
		TaskID:           command.TaskID,
		AgentName:        command.AgentName,
		Role:             command.Role,
		RequestedTools:   append([]string(nil), command.RequestedTools...),
		Iteration:        command.Iteration,
		Locator:          command.Locator,
		Objective:        command.Objective,
		Plan:             command.Plan,
		PlannerOutput:    command.PlannerOutput,
		TransportUserID:  command.TransportUserID,
		ExecutorOutput:   command.ExecutorOutput,
		ReviewerFeedback: command.ReviewerFeedback,
		Text:             strings.TrimSpace(text),
		MaxIterations:    command.MaxIterations,
	}
	if runErr != nil {
		result.Error = strings.TrimSpace(runErr.Error())
	}
	payload := taskEnvelopePayload{
		Kind:        taskPayloadKindAgentResult,
		AgentResult: &result,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", swarm.PermanentError(fmt.Errorf("encode task agent result: %w", err))
	}
	return string(data), nil
}

func taskAgentStepKey(command taskAgentCommandPayload) string {
	return command.TaskID + ":agent:" + firstNonEmpty(command.AgentName, command.Role) + ":" + command.Role + ":" + strconv.Itoa(command.Iteration)
}

func taskAgentResultDedupeKey(command taskAgentCommandPayload) string {
	return command.TaskID + ":result:" + firstNonEmpty(command.AgentName, command.Role) + ":" + command.Role + ":" + strconv.Itoa(command.Iteration)
}

func hashTaskAgentCommandPayload(command taskAgentCommandPayload) string {
	data, err := json.Marshal(command)
	if err != nil {
		data = []byte(taskAgentStepKey(command))
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func agentStepHasStoredResult(record baldastate.SwarmAgentStepRecord) bool {
	switch record.Status {
	case baldastate.SwarmAgentStepStatusSucceeded, baldastate.SwarmAgentStepStatusFailed:
		return strings.TrimSpace(record.ResultJSON) != ""
	default:
		return false
	}
}

func formatTaskAgentPrompt(payload taskAgentCommandPayload, spec swarm.AgentSpec) string {
	base := formatTaskAgentRoleWrapper(payload, spec)
	switch normalizeTaskAgentRole(payload.Role) {
	case taskAgentRoleReviewer:
		return joinPromptSections(base, formatTaskReviewerPrompt(payload))
	case taskAgentRolePlanner:
		return joinPromptSections(base, formatTaskPlannerPrompt(payload))
	default:
		return joinPromptSections(base, formatTaskExecutorPrompt(payload))
	}
}

func formatTaskAgentRoleWrapper(payload taskAgentCommandPayload, spec swarm.AgentSpec) string {
	var out strings.Builder
	out.WriteString("You are a Balda swarm logical agent.\n")
	out.WriteString("Agent: ")
	out.WriteString(firstNonEmpty(spec.Name, payload.AgentName, payload.Role))
	if role := strings.TrimSpace(spec.Role); role != "" {
		out.WriteString("\nRole: ")
		out.WriteString(role)
	}
	tools := spec.Tools
	if len(tools) == 0 {
		tools = payload.RequestedTools
	}
	if len(tools) > 0 {
		out.WriteString("\nAdvisory tools: ")
		out.WriteString(strings.Join(tools, ", "))
		out.WriteString("\nUse only tools that are actually available in the configured runtime.")
	}
	return out.String()
}

func formatTaskPlannerPrompt(payload taskAgentCommandPayload) string {
	var out strings.Builder
	out.WriteString("Task objective:\n")
	out.WriteString(strings.TrimSpace(payload.Objective))
	out.WriteString("\n\nIteration budget: ")
	out.WriteString(strconv.Itoa(normalizeGoalMaxIterations(payload.MaxIterations)))
	out.WriteString("\n\nCreate a concise execution plan for the executor. Include validation steps and any risks or assumptions. Do not make code changes in the planning step.")
	return out.String()
}

func formatTaskExecutorPrompt(payload taskAgentCommandPayload) string {
	var out strings.Builder
	out.WriteString("Task objective:\n")
	out.WriteString(strings.TrimSpace(payload.Objective))
	out.WriteString("\n\nIteration: ")
	out.WriteString(strconv.Itoa(payload.Iteration))
	out.WriteString("/")
	out.WriteString(strconv.Itoa(normalizeGoalMaxIterations(payload.MaxIterations)))
	if plan := strings.TrimSpace(payload.Plan); plan != "" {
		out.WriteString("\n\nCurrent plan:\n")
		out.WriteString(plan)
	}
	if plannerOutput := strings.TrimSpace(payload.PlannerOutput); plannerOutput != "" && plannerOutput != strings.TrimSpace(payload.Plan) {
		out.WriteString("\n\nPlanner output:\n")
		out.WriteString(plannerOutput)
	}
	if feedback := strings.TrimSpace(payload.ReviewerFeedback); feedback != "" {
		out.WriteString("\n\nReviewer feedback from previous iteration:\n")
		out.WriteString(feedback)
	}
	out.WriteString("\n\nDo the work now. Return a concise summary with changed files and verification evidence.")
	return out.String()
}

func formatTaskReviewerPrompt(payload taskAgentCommandPayload) string {
	var out strings.Builder
	out.WriteString("Task objective:\n")
	out.WriteString(strings.TrimSpace(payload.Objective))
	out.WriteString("\n\nIteration: ")
	out.WriteString(strconv.Itoa(payload.Iteration))
	out.WriteString("/")
	out.WriteString(strconv.Itoa(normalizeGoalMaxIterations(payload.MaxIterations)))
	if plan := strings.TrimSpace(payload.Plan); plan != "" {
		out.WriteString("\n\nCurrent plan:\n")
		out.WriteString(plan)
	}
	if plannerOutput := strings.TrimSpace(payload.PlannerOutput); plannerOutput != "" && plannerOutput != strings.TrimSpace(payload.Plan) {
		out.WriteString("\n\nPlanner output:\n")
		out.WriteString(plannerOutput)
	}
	out.WriteString("\n\nExecutor result:\n")
	if executorOutput := strings.TrimSpace(payload.ExecutorOutput); executorOutput != "" {
		out.WriteString(executorOutput)
	} else {
		out.WriteString("(none)")
	}
	out.WriteString("\n\nValidate the result. Start with exactly `verdict: pass` or `verdict: fail`, then provide evidence.")
	return out.String()
}

func joinPromptSections(parts ...string) string {
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return strings.Join(out, "\n\n")
}
