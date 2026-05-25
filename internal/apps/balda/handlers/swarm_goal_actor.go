package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
)

const (
	taskPayloadKindGoal         = "goal"
	taskPayloadKindScheduledJob = "scheduled_job"
	taskPayloadKindAgentResult  = "agent_result"
	taskPayloadKindDelivery     = "delivery"

	taskAgentRolePlanner  = swarm.AgentNamePlanner
	taskAgentRoleExecutor = swarm.AgentNameExecutor
	taskAgentRoleReviewer = swarm.AgentNameReviewer
	taskAgentRoleMemory   = swarm.AgentNameMemory
)

type taskEnvelopePayload struct {
	Kind         string                   `json:"kind"`
	Goal         *goalTaskPayload         `json:"goal,omitempty"`
	ScheduledJob *scheduledJobTaskPayload `json:"scheduled_job,omitempty"`
	AgentResult  *taskAgentResultPayload  `json:"agent_result,omitempty"`
}

type goalTaskPayload struct {
	TaskID          string                      `json:"task_id,omitempty"`
	Locator         baldasession.SessionLocator `json:"locator"`
	Objective       string                      `json:"objective"`
	TransportUserID string                      `json:"transport_user_id"`
	MaxIterations   int                         `json:"max_iterations,omitempty"`
}

type scheduledJobTaskPayload struct {
	JobID   string                      `json:"job_id"`
	Prompt  string                      `json:"prompt"`
	Locator baldasession.SessionLocator `json:"locator"`
	UserID  string                      `json:"user_id"`
	TopicID int                         `json:"topic_id,omitempty"`
}

type taskAgentCommandPayload struct {
	TaskID           string                      `json:"task_id"`
	AgentName        string                      `json:"agent_name,omitempty"`
	Role             string                      `json:"role"`
	RequestedTools   []string                    `json:"requested_tools,omitempty"`
	Iteration        int                         `json:"iteration"`
	Locator          baldasession.SessionLocator `json:"locator"`
	Objective        string                      `json:"objective"`
	Plan             string                      `json:"plan,omitempty"`
	PlannerOutput    string                      `json:"planner_output,omitempty"`
	TransportUserID  string                      `json:"transport_user_id"`
	ExecutorOutput   string                      `json:"executor_output,omitempty"`
	ReviewerFeedback string                      `json:"reviewer_feedback,omitempty"`
	MaxIterations    int                         `json:"max_iterations,omitempty"`
}

type taskAgentResultPayload struct {
	TaskID           string                      `json:"task_id"`
	AgentName        string                      `json:"agent_name,omitempty"`
	Role             string                      `json:"role"`
	RequestedTools   []string                    `json:"requested_tools,omitempty"`
	Iteration        int                         `json:"iteration"`
	Locator          baldasession.SessionLocator `json:"locator"`
	Objective        string                      `json:"objective"`
	Plan             string                      `json:"plan,omitempty"`
	PlannerOutput    string                      `json:"planner_output,omitempty"`
	TransportUserID  string                      `json:"transport_user_id"`
	ExecutorOutput   string                      `json:"executor_output,omitempty"`
	ReviewerFeedback string                      `json:"reviewer_feedback,omitempty"`
	Text             string                      `json:"text,omitempty"`
	Error            string                      `json:"error,omitempty"`
	MaxIterations    int                         `json:"max_iterations,omitempty"`
}

type taskDeliveryPayload struct {
	TaskID  string                      `json:"task_id"`
	Locator baldasession.SessionLocator `json:"locator"`
	Text    string                      `json:"text"`
}

type goalTaskPlan struct {
	Objective     string   `json:"objective"`
	MaxIterations int      `json:"max_iterations"`
	Steps         []string `json:"steps"`
}

func (h *CommandHandler) submitGoalTask(ctx context.Context, locator baldasession.SessionLocator, objective string, transportUserID string) (bool, error) {
	maxIterations := defaultGoalMaxIterations
	if h.goalRunner != nil {
		maxIterations = h.goalRunner.MaxIterations()
	}
	env, err := goalTaskEnvelope(locator, objective, transportUserID, maxIterations)
	if err != nil {
		return false, err
	}
	if err := h.createGoalTask(ctx, env, locator, objective, transportUserID); err != nil {
		return false, err
	}

	if h.swarmCoordinator != nil && h.swarmCoordinator.ShadowEnabled() {
		h.shadowGoalTask(ctx, env)
		started, err := h.goalRunner.StartTask(ctx, env.TaskID, locator, objective, transportUserID)
		if err == nil && started {
			h.swarmCoordinator.RecordShadowDispatch()
		}
		if err != nil {
			h.markGoalTaskFailed(ctx, env.TaskID, err)
		} else if !started {
			h.markGoalTaskCanceled(ctx, env.TaskID, "another goal run is already active for this session")
		}
		return started, err
	}
	if h.swarmCoordinator == nil || !h.swarmCoordinator.Enabled() {
		started, err := h.goalRunner.StartTask(ctx, env.TaskID, locator, objective, transportUserID)
		if err != nil {
			h.markGoalTaskFailed(ctx, env.TaskID, err)
		} else if !started {
			h.markGoalTaskCanceled(ctx, env.TaskID, "another goal run is already active for this session")
		}
		return started, err
	}
	_, err = h.swarmCoordinator.Submit(ctx, env)
	if err != nil {
		h.markGoalTaskFailed(ctx, env.TaskID, err)
		return false, err
	}
	return true, nil
}

func (h *CommandHandler) createGoalTask(
	ctx context.Context,
	env swarm.Envelope,
	locator baldasession.SessionLocator,
	objective string,
	transportUserID string,
) error {
	if h.tasks == nil {
		return nil
	}
	taskID := strings.TrimSpace(env.TaskID)
	if taskID == "" {
		return fmt.Errorf("goal task id is required")
	}
	payload := map[string]any{
		"objective":         strings.TrimSpace(objective),
		"session_id":        strings.TrimSpace(locator.SessionID),
		"transport_user_id": strings.TrimSpace(transportUserID),
	}
	if _, err := h.tasks.Create(ctx, baldastate.SwarmTaskRecord{
		ID:            taskID,
		SessionID:     strings.TrimSpace(locator.SessionID),
		Title:         goalTaskTitle(objective),
		Objective:     strings.TrimSpace(objective),
		Status:        baldastate.SwarmTaskStatusCreated,
		OwnerActor:    swarm.ActorTypeTask + ":" + taskID,
		AssignedActor: swarm.ActorTypeAgent + ":" + taskAgentRolePlanner,
		Priority:      90,
		CreatedBy:     strings.TrimSpace(transportUserID),
		CreatedFrom:   "goal",
	}, "command.goal", payload); err != nil {
		return err
	}
	return h.tasks.MarkStatus(ctx, taskID, baldastate.SwarmTaskStatusQueued, "command.goal", env.ID, "", payload)
}

func (h *CommandHandler) markGoalTaskFailed(ctx context.Context, taskID string, cause error) {
	if h.tasks == nil || cause == nil {
		return
	}
	if err := h.tasks.MarkStatus(ctx, taskID, baldastate.SwarmTaskStatusFailed, "command.goal", "", cause.Error(), nil); err != nil {
		log.Warn().Err(err).Str("task_id", taskID).Msg("failed to mark goal task failed")
	}
}

func (h *CommandHandler) markGoalTaskCanceled(ctx context.Context, taskID string, reason string) {
	if h.tasks == nil {
		return
	}
	if err := h.tasks.MarkStatus(ctx, taskID, baldastate.SwarmTaskStatusCanceled, "command.goal", "", reason, nil); err != nil {
		log.Warn().Err(err).Str("task_id", taskID).Msg("failed to mark goal task canceled")
	}
}

func (h *CommandHandler) shadowGoalTask(ctx context.Context, env swarm.Envelope) {
	if _, err := h.swarmCoordinator.SubmitShadow(ctx, env); err != nil {
		log.Warn().Err(err).Str("session_id", env.SessionID).Msg("failed to persist swarm shadow goal envelope")
	}
}

func goalTaskEnvelope(
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
		Namespace:   swarm.NamespaceAgentCommand,
		Kind:        swarm.KindGoal,
		From:        swarm.ActorAddress{Target: "telegram", Key: firstNonEmpty(transportUserID, locator.AddressKey, "unknown")},
		To:          swarm.ActorAddress{Target: swarm.ActorTypeTask, Key: taskID},
		SessionID:   locator.SessionID,
		TaskID:      taskID,
		Priority:    90,
		PayloadJSON: string(data),
	}, nil
}

func goalTaskTitle(objective string) string {
	const maxTitleRunes = 80
	title := strings.TrimSpace(objective)
	if title == "" {
		return "Goal"
	}
	runes := []rune(title)
	if len(runes) > maxTitleRunes {
		title = strings.TrimSpace(string(runes[:maxTitleRunes])) + "..."
	}
	return "Goal: " + title
}

type taskActorExecutor struct {
	tasks       *swarm.TaskService
	coordinator *swarm.Coordinator
	agents      *swarm.AgentAllocator
	sessions    *baldasession.Manager
	scheduler   *JobScheduler
	maxIters    int
}

type taskActorExecutorParams struct {
	fx.In

	TaskService *swarm.TaskService
	Coordinator *swarm.Coordinator
	Agents      *swarm.AgentAllocator `optional:"true"`
	Sessions    *baldasession.Manager `optional:"true"`
	Scheduler   *JobScheduler         `optional:"true"`
	MaxIters    int                   `name:"balda_goal_max_iterations"`
}

func newTaskActorExecutor(params taskActorExecutorParams) swarm.Actor {
	return &taskActorExecutor{
		tasks:       params.TaskService,
		coordinator: params.Coordinator,
		agents:      params.Agents,
		sessions:    params.Sessions,
		scheduler:   params.Scheduler,
		maxIters:    normalizeGoalMaxIterations(params.MaxIters),
	}
}

func (e *taskActorExecutor) Address() string {
	return swarm.WildcardAddress(swarm.ActorTypeTask)
}

func (e *taskActorExecutor) Handle(ctx context.Context, env swarm.Envelope) error {
	if strings.TrimSpace(env.Namespace) == swarm.NamespaceAgentResult {
		var payload taskEnvelopePayload
		if err := json.Unmarshal([]byte(env.PayloadJSON), &payload); err != nil {
			return swarm.PermanentError(fmt.Errorf("decode task result payload: %w", err))
		}
		if payload.AgentResult == nil {
			return swarm.PolicyError(fmt.Errorf("agent result task payload is required"))
		}
		return e.handleAgentResult(ctx, env, *payload.AgentResult)
	}

	var payload taskEnvelopePayload
	if err := json.Unmarshal([]byte(env.PayloadJSON), &payload); err != nil {
		return swarm.PermanentError(fmt.Errorf("decode task payload: %w", err))
	}
	switch strings.TrimSpace(payload.Kind) {
	case taskPayloadKindGoal:
		if payload.Goal == nil {
			return swarm.PolicyError(fmt.Errorf("goal task payload is required"))
		}
		return e.startGoalTask(ctx, env, *payload.Goal)
	case taskPayloadKindScheduledJob:
		if payload.ScheduledJob == nil {
			return swarm.PolicyError(fmt.Errorf("scheduled job task payload is required"))
		}
		if e.scheduler == nil {
			return swarm.TransientError(fmt.Errorf("job scheduler is required"))
		}
		return e.scheduler.executeScheduledJobTask(ctx, *payload.ScheduledJob)
	default:
		return swarm.PolicyError(fmt.Errorf("unsupported task payload kind %q", payload.Kind))
	}
}

func (e *taskActorExecutor) startGoalTask(ctx context.Context, env swarm.Envelope, payload goalTaskPayload) error {
	taskID := firstNonEmpty(payload.TaskID, env.TaskID, env.To.Key)
	objective := strings.TrimSpace(payload.Objective)
	if taskID == "" {
		return swarm.PolicyError(fmt.Errorf("task id is required"))
	}
	if objective == "" {
		return swarm.PolicyError(fmt.Errorf("goal objective is required"))
	}
	maxIterations := normalizeGoalMaxIterations(payload.MaxIterations)
	if maxIterations == defaultGoalMaxIterations && e.maxIters != defaultGoalMaxIterations {
		maxIterations = e.maxIters
	}
	if err := e.ensureGoalTask(ctx, taskID, payload, objective); err != nil {
		return err
	}
	if err := e.tasks.MarkStatus(ctx, taskID, baldastate.SwarmTaskStatusRunning, "task.actor", env.ID, "", map[string]any{
		"objective": objective,
	}); err != nil {
		return swarm.TransientError(err)
	}
	plan := goalTaskPlan{
		Objective:     objective,
		MaxIterations: maxIterations,
		Steps: []string{
			"Ask the planner agent for a focused execution plan.",
			"Execute the approved plan with the configured Balda provider.",
			"Validate the executor result with a reviewer agent.",
			"Repeat executor/reviewer iterations until validation passes or the iteration budget is exhausted.",
		},
	}
	if err := e.tasks.SetPlan(ctx, taskID, "task.actor", plan); err != nil {
		return swarm.TransientError(err)
	}
	if err := e.deliver(ctx, taskID, payload.Locator, fmt.Sprintf("Goal run started. Max iterations: %d.\n\nGoal: %s", maxIterations, objective), "started"); err != nil {
		return err
	}
	return e.dispatchAgent(ctx, taskAgentCommandPayload{
		TaskID:          taskID,
		Role:            taskAgentRolePlanner,
		Iteration:       1,
		Locator:         payload.Locator,
		Objective:       objective,
		TransportUserID: payload.TransportUserID,
		MaxIterations:   maxIterations,
	})
}

func (e *taskActorExecutor) ensureGoalTask(
	ctx context.Context,
	taskID string,
	payload goalTaskPayload,
	objective string,
) error {
	if e.tasks == nil {
		return swarm.TransientError(fmt.Errorf("task service is required"))
	}
	if _, ok, err := e.tasks.Get(ctx, taskID); err != nil {
		return swarm.TransientError(err)
	} else if ok {
		return nil
	}
	_, err := e.tasks.Create(ctx, baldastate.SwarmTaskRecord{
		ID:            taskID,
		SessionID:     strings.TrimSpace(payload.Locator.SessionID),
		Title:         goalTaskTitle(objective),
		Objective:     objective,
		Status:        baldastate.SwarmTaskStatusCreated,
		OwnerActor:    swarm.ActorTypeTask + ":" + taskID,
		AssignedActor: swarm.ActorTypeAgent + ":" + taskAgentRolePlanner,
		Priority:      90,
		CreatedBy:     strings.TrimSpace(payload.TransportUserID),
		CreatedFrom:   "goal",
	}, "task.actor", payload)
	if err != nil {
		return swarm.TransientError(err)
	}
	return e.tasks.MarkStatus(ctx, taskID, baldastate.SwarmTaskStatusQueued, "task.actor", "", "", nil)
}

func (e *taskActorExecutor) dispatchAgent(ctx context.Context, payload taskAgentCommandPayload) error {
	if e.coordinator == nil || !e.coordinator.RuntimeEnabled() {
		return swarm.TransientError(fmt.Errorf("swarm coordinator is required"))
	}
	role := normalizeTaskAgentRole(payload.Role)
	if role == "" {
		return swarm.PolicyError(fmt.Errorf("unsupported task agent role %q", payload.Role))
	}
	payload.Role = role
	if payload.Iteration <= 0 {
		payload.Iteration = 1
	}
	if len(payload.RequestedTools) == 0 {
		payload.RequestedTools = defaultTaskAgentTools(role)
	}
	agentName := role
	if e.agents != nil {
		spec, err := e.agents.Allocate(ctx, swarm.AgentAllocationRequest{
			Name:              payload.AgentName,
			Role:              role,
			Tools:             payload.RequestedTools,
			WorkspaceAffinity: strings.TrimSpace(payload.Locator.SessionID) != "",
		})
		if err != nil {
			return swarm.PolicyError(err)
		}
		agentName = spec.Name
	}
	payload.AgentName = agentName
	status := baldastate.SwarmTaskStatusWaitingForAgent
	stepName := "executor"
	switch role {
	case taskAgentRolePlanner:
		stepName = "planner"
	case taskAgentRoleReviewer:
		status = baldastate.SwarmTaskStatusValidating
		stepName = "validator"
	}
	if err := e.tasks.MarkStatus(ctx, payload.TaskID, status, "task.actor", "", "", map[string]any{
		"role":       role,
		"agent_name": agentName,
		"iteration":  payload.Iteration,
	}); err != nil {
		return swarm.TransientError(err)
	}
	if err := e.deliver(
		ctx,
		payload.TaskID,
		payload.Locator,
		fmt.Sprintf("Goal iteration %d/%d: %s started.", payload.Iteration, normalizeGoalMaxIterations(payload.MaxIterations), stepName),
		"started:"+role+":"+strconv.Itoa(payload.Iteration),
	); err != nil {
		return err
	}
	if err := e.tasks.AppendEvent(ctx, payload.TaskID, swarm.TaskEventAgentStarted, "task.actor", "", map[string]any{
		"role":            role,
		"agent_name":      agentName,
		"requested_tools": payload.RequestedTools,
		"iteration":       payload.Iteration,
	}); err != nil {
		return swarm.TransientError(err)
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return swarm.PermanentError(fmt.Errorf("encode task agent command: %w", err))
	}
	env := swarm.Envelope{
		ID:            uuid.NewString(),
		Namespace:     swarm.NamespaceAgentCommand,
		Kind:          swarm.KindGoal,
		From:          swarm.ActorAddress{Target: swarm.ActorTypeTask, Key: payload.TaskID},
		To:            swarm.ActorAddress{Target: swarm.ActorTypeAgent, Key: agentName},
		SessionID:     payload.Locator.SessionID,
		TaskID:        payload.TaskID,
		CorrelationID: payload.TaskID,
		Priority:      70,
		DedupeKey:     payload.TaskID + ":agent:" + agentName + ":" + role + ":" + strconv.Itoa(payload.Iteration),
		PayloadJSON:   string(data),
	}
	if _, err := e.coordinator.Submit(ctx, env); err != nil {
		return swarm.TransientError(err)
	}
	return nil
}

func (e *taskActorExecutor) handleAgentResult(ctx context.Context, env swarm.Envelope, payload taskAgentResultPayload) error {
	taskID := firstNonEmpty(payload.TaskID, env.TaskID, env.To.Key)
	if taskID == "" {
		return swarm.PolicyError(fmt.Errorf("task id is required"))
	}
	payload.TaskID = taskID
	role := normalizeTaskAgentRole(payload.Role)
	if role == "" {
		return swarm.PolicyError(fmt.Errorf("unsupported task agent role %q", payload.Role))
	}
	payload.Role = role
	if errText := strings.TrimSpace(payload.Error); errText != "" {
		status := baldastate.SwarmTaskStatusFailed
		if strings.Contains(strings.ToLower(errText), "cancel") {
			status = baldastate.SwarmTaskStatusCanceled
		}
		if err := e.tasks.SetResult(ctx, taskID, taskResultPayload(payload, false), status, "task.actor", errText); err != nil {
			return swarm.TransientError(err)
		}
		return e.deliver(ctx, taskID, payload.Locator, "Goal run failed: "+errText, "failed:"+role+":"+strconv.Itoa(payload.Iteration))
	}

	if err := e.tasks.AppendEvent(ctx, taskID, swarm.TaskEventAgentResult, "task.actor", env.ID, map[string]any{
		"role":       role,
		"agent_name": strings.TrimSpace(payload.AgentName),
		"iteration":  payload.Iteration,
		"text":       strings.TrimSpace(payload.Text),
	}); err != nil {
		return swarm.TransientError(err)
	}

	switch role {
	case taskAgentRolePlanner:
		return e.handlePlannerResult(ctx, payload)
	case taskAgentRoleExecutor:
		return e.handleExecutorResult(ctx, payload)
	case taskAgentRoleReviewer:
		return e.handleReviewerResult(ctx, payload)
	default:
		return swarm.PolicyError(fmt.Errorf("unsupported task agent role %q", payload.Role))
	}
}

func (e *taskActorExecutor) handlePlannerResult(ctx context.Context, payload taskAgentResultPayload) error {
	text := strings.TrimSpace(payload.Text)
	if text == "" {
		text = "(planner returned no visible output)"
	}
	payload.Plan = text
	payload.PlannerOutput = text
	if err := e.tasks.SetPlan(ctx, payload.TaskID, "task.actor", map[string]any{
		"objective":      strings.TrimSpace(payload.Objective),
		"max_iterations": normalizeGoalMaxIterations(payload.MaxIterations),
		"planner_output": text,
	}); err != nil {
		return swarm.TransientError(err)
	}
	if err := e.deliver(
		ctx,
		payload.TaskID,
		payload.Locator,
		fmt.Sprintf("Goal iteration %d/%d: planner finished.\n\n%s", payload.Iteration, normalizeGoalMaxIterations(payload.MaxIterations), text),
		"finished:planner:"+strconv.Itoa(payload.Iteration),
	); err != nil {
		return err
	}
	return e.dispatchAgent(ctx, taskAgentCommandPayload{
		TaskID:          payload.TaskID,
		Role:            taskAgentRoleExecutor,
		Iteration:       payload.Iteration,
		Locator:         payload.Locator,
		Objective:       payload.Objective,
		Plan:            text,
		PlannerOutput:   text,
		TransportUserID: payload.TransportUserID,
		MaxIterations:   payload.MaxIterations,
	})
}

func (e *taskActorExecutor) handleExecutorResult(ctx context.Context, payload taskAgentResultPayload) error {
	text := strings.TrimSpace(payload.Text)
	if text == "" {
		text = "(executor returned no visible output)"
	}
	payload.ExecutorOutput = text
	if err := e.deliver(
		ctx,
		payload.TaskID,
		payload.Locator,
		fmt.Sprintf("Goal iteration %d/%d: executor finished.\n\n%s", payload.Iteration, normalizeGoalMaxIterations(payload.MaxIterations), text),
		"finished:executor:"+strconv.Itoa(payload.Iteration),
	); err != nil {
		return err
	}
	return e.dispatchAgent(ctx, taskAgentCommandPayload{
		TaskID:           payload.TaskID,
		Role:             taskAgentRoleReviewer,
		Iteration:        payload.Iteration,
		Locator:          payload.Locator,
		Objective:        payload.Objective,
		Plan:             payload.Plan,
		PlannerOutput:    payload.PlannerOutput,
		TransportUserID:  payload.TransportUserID,
		ExecutorOutput:   text,
		ReviewerFeedback: payload.ReviewerFeedback,
		MaxIterations:    payload.MaxIterations,
	})
}

func (e *taskActorExecutor) handleReviewerResult(ctx context.Context, payload taskAgentResultPayload) error {
	text := strings.TrimSpace(payload.Text)
	passed := reviewerPassed(text)
	statusText := "fail"
	if passed {
		statusText = "pass"
	}
	if err := e.deliver(
		ctx,
		payload.TaskID,
		payload.Locator,
		fmt.Sprintf("Goal iteration %d/%d: validator finished (%s).\n\n%s", payload.Iteration, normalizeGoalMaxIterations(payload.MaxIterations), statusText, text),
		"finished:reviewer:"+strconv.Itoa(payload.Iteration),
	); err != nil {
		return err
	}
	if passed {
		if err := e.tasks.SetResult(ctx, payload.TaskID, taskResultPayload(payload, true), baldastate.SwarmTaskStatusCompleted, "task.actor", ""); err != nil {
			return swarm.TransientError(err)
		}
		return e.deliver(ctx, payload.TaskID, payload.Locator, e.renderTaskOutcome(ctx, payload.TaskID, "Goal run completed."), "completed")
	}

	maxIterations := normalizeGoalMaxIterations(payload.MaxIterations)
	if payload.Iteration >= maxIterations {
		if err := e.tasks.SetResult(ctx, payload.TaskID, taskResultPayload(payload, false), baldastate.SwarmTaskStatusFailed, "task.actor", "max iterations reached"); err != nil {
			return swarm.TransientError(err)
		}
		return e.deliver(ctx, payload.TaskID, payload.Locator, e.renderTaskOutcome(ctx, payload.TaskID, "Goal run reached max iterations without passing validation."), "max-iterations")
	}

	return e.dispatchAgent(ctx, taskAgentCommandPayload{
		TaskID:           payload.TaskID,
		Role:             taskAgentRoleExecutor,
		Iteration:        payload.Iteration + 1,
		Locator:          payload.Locator,
		Objective:        payload.Objective,
		Plan:             payload.Plan,
		PlannerOutput:    payload.PlannerOutput,
		TransportUserID:  payload.TransportUserID,
		ReviewerFeedback: text,
		MaxIterations:    maxIterations,
	})
}

func (e *taskActorExecutor) renderTaskOutcome(ctx context.Context, taskID string, fallback string) string {
	if e == nil || e.tasks == nil {
		return fallback
	}
	task, ok, err := e.tasks.Get(ctx, taskID)
	if err != nil || !ok {
		return fallback
	}
	return renderReviewableOutcome(task, taskArtifactsFromSessionProvider(ctx, e.sessions, task))
}

func (e *taskActorExecutor) deliver(
	ctx context.Context,
	taskID string,
	locator baldasession.SessionLocator,
	text string,
	dedupeSuffix string,
) error {
	if e.coordinator == nil || !e.coordinator.RuntimeEnabled() {
		return swarm.TransientError(fmt.Errorf("swarm coordinator is required"))
	}
	message := strings.TrimSpace(text)
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
	if dedupeKey != "" && strings.TrimSpace(dedupeSuffix) != "" {
		dedupeKey += ":delivery:" + strings.TrimSpace(dedupeSuffix)
	}
	env := swarm.Envelope{
		ID:            uuid.NewString(),
		Namespace:     swarm.NamespaceAgentResult,
		Kind:          taskPayloadKindDelivery,
		From:          swarm.ActorAddress{Target: swarm.ActorTypeTask, Key: taskID},
		To:            swarm.ActorAddress{Target: swarm.ActorTypeDelivery, Key: firstNonEmpty(locator.AddressKey, locator.SessionID, "telegram")},
		SessionID:     locator.SessionID,
		TaskID:        taskID,
		CorrelationID: taskID,
		Priority:      70,
		DedupeKey:     dedupeKey,
		PayloadJSON:   string(data),
	}
	if _, err := e.coordinator.Submit(ctx, env); err != nil {
		return swarm.TransientError(err)
	}
	return nil
}

func normalizeTaskAgentRole(role string) string {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "planner":
		return taskAgentRolePlanner
	case "executor", "worker":
		return taskAgentRoleExecutor
	case "reviewer", "validator":
		return taskAgentRoleReviewer
	case "memory":
		return taskAgentRoleMemory
	default:
		return ""
	}
}

func defaultTaskAgentTools(role string) []string {
	switch normalizeTaskAgentRole(role) {
	case taskAgentRoleExecutor:
		return []string{swarm.AgentToolWorkspace, swarm.AgentToolShell, swarm.AgentToolMCP}
	case taskAgentRoleReviewer:
		return []string{swarm.AgentToolWorkspace, swarm.AgentToolShell}
	case taskAgentRoleMemory:
		return []string{swarm.AgentToolMemory}
	default:
		return nil
	}
}

func reviewerPassed(text string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(text)), "verdict: pass")
}

func taskResultPayload(payload taskAgentResultPayload, goalReached bool) map[string]any {
	return map[string]any{
		"goal_reached":      goalReached,
		"iterations":        payload.Iteration,
		"planner_output":    strings.TrimSpace(payload.PlannerOutput),
		"executor_output":   strings.TrimSpace(payload.ExecutorOutput),
		"reviewer_output":   strings.TrimSpace(payload.Text),
		"reviewer_feedback": strings.TrimSpace(payload.ReviewerFeedback),
	}
}
