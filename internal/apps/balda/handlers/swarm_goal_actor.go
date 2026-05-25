package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"go.uber.org/fx"
)

const (
	taskPayloadKindGoal         = "goal"
	taskPayloadKindScheduledJob = "scheduled_job"
	taskPayloadKindSessionTurn  = "session_turn"
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
	SessionTurn  *sessionTurnPayload      `json:"session_turn,omitempty"`
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
	if h.swarmCoordinator == nil || !h.swarmCoordinator.Enabled() {
		return false, fmt.Errorf("jetstream swarm runtime is unavailable")
	}
	_, err = h.swarmCoordinator.Submit(ctx, env)
	if err != nil {
		return false, err
	}
	return true, nil
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

func (h *BaldaHandler) submitWebhookTask(ctx context.Context, payload sessionTurnPayload, routeName string, requestID string) (*swarm.CommandPublishResult, string, error) {
	if h.swarmCoordinator == nil || !h.swarmCoordinator.RuntimeEnabled() {
		return nil, "", fmt.Errorf("jetstream swarm runtime is unavailable")
	}
	dedupeBase := webhookDedupeBase(routeName, requestID, payload.DedupeKey)
	taskID := webhookTaskID(routeName, dedupeBase)
	payload.DedupeKey = dedupeBase + ":session"
	data, err := json.Marshal(taskEnvelopePayload{
		Kind:        taskPayloadKindSessionTurn,
		SessionTurn: &payload,
	})
	if err != nil {
		return nil, "", fmt.Errorf("encode webhook task payload: %w", err)
	}
	env := swarm.Envelope{
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
	}
	result, err := h.swarmCoordinator.Submit(ctx, env)
	if err != nil {
		return nil, "", err
	}
	return result, taskID, nil
}

func webhookDedupeBase(routeName string, requestID string, raw string) string {
	base := strings.TrimSpace(raw)
	base = strings.TrimSuffix(base, ":task")
	base = strings.TrimSuffix(base, ":session")
	if base != "" {
		return base
	}
	return strings.Join([]string{"webhook", strings.TrimSpace(routeName), strings.TrimSpace(requestID)}, ":")
}

func webhookTaskID(routeName string, dedupeBase string) string {
	return "webhook-" + safeTaskIDPart(routeName) + "-" + shortTaskHash(dedupeBase)
}

func safeTaskIDPart(raw string) string {
	trimmed := strings.ToLower(strings.TrimSpace(raw))
	var out strings.Builder
	lastDash := false
	for _, r := range trimmed {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			out.WriteRune(r)
			lastDash = false
		case r == '-' || r == '_':
			out.WriteRune(r)
			lastDash = false
		default:
			if out.Len() > 0 && !lastDash {
				out.WriteByte('-')
				lastDash = true
			}
		}
		if out.Len() >= 48 {
			break
		}
	}
	part := strings.Trim(out.String(), "-_")
	if part == "" {
		return "inbound"
	}
	return part
}

func shortTaskHash(value string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(value)))
	return hex.EncodeToString(sum[:])[:16]
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
		return e.startScheduledJobTask(ctx, env, *payload.ScheduledJob)
	case taskPayloadKindSessionTurn:
		if payload.SessionTurn == nil {
			return swarm.PolicyError(fmt.Errorf("session turn task payload is required"))
		}
		return e.dispatchSessionTurn(ctx, env, *payload.SessionTurn)
	default:
		return swarm.PolicyError(fmt.Errorf("unsupported task payload kind %q", payload.Kind))
	}
}

func (e *taskActorExecutor) dispatchSessionTurn(ctx context.Context, env swarm.Envelope, payload sessionTurnPayload) error {
	taskID := firstNonEmpty(env.TaskID, env.To.Key)
	if taskID != "" && e.tasks != nil {
		if task, ok, err := e.tasks.Get(ctx, taskID); err != nil {
			return swarm.TransientError(err)
		} else if ok && isTerminalTaskStatus(task.Status) {
			return nil
		}
		_, err := e.tasks.Create(ctx, baldastate.SwarmTaskRecord{
			ID:            taskID,
			SessionID:     strings.TrimSpace(payload.Locator.SessionID),
			Title:         "Webhook task",
			Objective:     strings.TrimSpace(payload.Text),
			Status:        baldastate.SwarmTaskStatusCreated,
			OwnerActor:    swarm.ActorTypeTask + ":" + taskID,
			AssignedActor: swarm.ActorTypeSession + ":" + payload.Locator.SessionID,
			Priority:      80,
			CreatedBy:     strings.TrimSpace(payload.UserID),
			CreatedFrom:   strings.TrimSpace(payload.Source),
		}, "task.actor", payload)
		if err != nil {
			return swarm.TransientError(err)
		}
	}
	sessionEnv, err := sessionTurnEnvelope(payload)
	if err != nil {
		return swarm.PermanentError(err)
	}
	sessionEnv.TaskID = taskID
	sessionEnv.CorrelationID = firstNonEmpty(env.CorrelationID, taskID)
	sessionEnv.CausationID = env.ID
	if strings.TrimSpace(sessionEnv.DedupeKey) != "" {
		sessionEnv.ID = sessionEnv.DedupeKey
	}
	if _, err := e.coordinator.Submit(ctx, sessionEnv); err != nil {
		return swarm.TransientError(err)
	}
	if taskID != "" && e.tasks != nil {
		if err := e.tasks.MarkStatus(ctx, taskID, baldastate.SwarmTaskStatusRunning, "task.actor", env.ID, "", nil); err != nil {
			return swarm.TransientError(err)
		}
	}
	return nil
}

func (e *taskActorExecutor) startScheduledJobTask(ctx context.Context, env swarm.Envelope, payload scheduledJobTaskPayload) error {
	taskID := firstNonEmpty(env.TaskID, env.To.Key)
	prompt := strings.TrimSpace(payload.Prompt)
	if taskID == "" {
		return swarm.PolicyError(fmt.Errorf("task id is required"))
	}
	if strings.TrimSpace(payload.JobID) == "" {
		return swarm.PolicyError(fmt.Errorf("scheduled job id is required"))
	}
	if prompt == "" {
		return swarm.PolicyError(fmt.Errorf("scheduled job prompt is required"))
	}
	if e.tasks != nil {
		if task, ok, err := e.tasks.Get(ctx, taskID); err != nil {
			return swarm.TransientError(err)
		} else if ok && isTerminalTaskStatus(task.Status) {
			return nil
		}
		_, err := e.tasks.Create(ctx, baldastate.SwarmTaskRecord{
			ID:            taskID,
			SessionID:     strings.TrimSpace(payload.Locator.SessionID),
			Title:         "Scheduled job: " + strings.TrimSpace(payload.JobID),
			Objective:     prompt,
			Status:        baldastate.SwarmTaskStatusCreated,
			OwnerActor:    swarm.ActorTypeTask + ":" + taskID,
			AssignedActor: swarm.ActorTypeSession + ":" + payload.Locator.SessionID,
			Priority:      50,
			CreatedBy:     strings.TrimSpace(payload.UserID),
			CreatedFrom:   sessionTurnSourceSchedule,
		}, "task.actor", payload)
		if err != nil {
			return swarm.TransientError(err)
		}
	}
	sessionPayload := sessionTurnPayload{
		Text:           prompt,
		Locator:        payload.Locator,
		UserID:         payload.UserID,
		ScheduledJobID: payload.JobID,
		TopicID:        payload.TopicID,
		Deliver:        false,
		Source:         sessionTurnSourceSchedule,
		DedupeKey:      firstNonEmpty(env.DedupeKey, taskID) + ":session",
	}
	sessionEnv, err := sessionTurnEnvelope(sessionPayload)
	if err != nil {
		return swarm.PermanentError(err)
	}
	sessionEnv.TaskID = taskID
	sessionEnv.CorrelationID = firstNonEmpty(env.CorrelationID, taskID)
	sessionEnv.CausationID = env.ID
	if strings.TrimSpace(sessionEnv.DedupeKey) != "" {
		sessionEnv.ID = sessionEnv.DedupeKey
	}
	if _, err := e.coordinator.Submit(ctx, sessionEnv); err != nil {
		return swarm.TransientError(err)
	}
	if e.tasks != nil {
		if err := e.tasks.MarkStatus(ctx, taskID, baldastate.SwarmTaskStatusRunning, "task.actor", env.ID, "", nil); err != nil {
			return swarm.TransientError(err)
		}
	}
	return nil
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
	if e.taskStatusIs(ctx, taskID, baldastate.SwarmTaskStatusCompleted, baldastate.SwarmTaskStatusFailed, baldastate.SwarmTaskStatusCanceled, baldastate.SwarmTaskStatusDeadLettered) {
		return nil
	}
	maxIterations := normalizeGoalMaxIterations(payload.MaxIterations)
	if maxIterations == defaultGoalMaxIterations && e.maxIters != defaultGoalMaxIterations {
		maxIterations = e.maxIters
	}
	if err := e.ensureGoalTask(ctx, taskID, payload, objective); err != nil {
		return err
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
	dispatch, err := e.prepareAgentDispatch(ctx, taskAgentCommandPayload{
		TaskID:          taskID,
		Role:            taskAgentRolePlanner,
		Iteration:       1,
		Locator:         payload.Locator,
		Objective:       objective,
		TransportUserID: payload.TransportUserID,
		MaxIterations:   maxIterations,
	})
	if err != nil {
		return err
	}
	if err := e.submitAgentDispatch(ctx, dispatch); err != nil {
		return err
	}
	if err := e.tasks.MarkStatus(ctx, taskID, baldastate.SwarmTaskStatusRunning, "task.actor", env.ID, "", map[string]any{
		"objective": objective,
	}); err != nil {
		return swarm.TransientError(err)
	}
	if err := e.deliver(ctx, taskID, payload.Locator, fmt.Sprintf("Goal run started. Max iterations: %d.\n\nGoal: %s", maxIterations, objective), "started"); err != nil {
		return err
	}
	return e.recordAgentDispatch(ctx, dispatch)
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

type taskAgentDispatch struct {
	Payload   taskAgentCommandPayload
	Role      string
	AgentName string
	Status    string
	StepName  string
	Envelope  swarm.Envelope
}

func (e *taskActorExecutor) dispatchAgent(ctx context.Context, payload taskAgentCommandPayload) error {
	dispatch, err := e.prepareAgentDispatch(ctx, payload)
	if err != nil {
		return err
	}
	if err := e.submitAgentDispatch(ctx, dispatch); err != nil {
		return err
	}
	return e.recordAgentDispatch(ctx, dispatch)
}

func (e *taskActorExecutor) prepareAgentDispatch(ctx context.Context, payload taskAgentCommandPayload) (taskAgentDispatch, error) {
	role := normalizeTaskAgentRole(payload.Role)
	if role == "" {
		return taskAgentDispatch{}, swarm.PolicyError(fmt.Errorf("unsupported task agent role %q", payload.Role))
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
			return taskAgentDispatch{}, swarm.PolicyError(err)
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
	data, err := json.Marshal(payload)
	if err != nil {
		return taskAgentDispatch{}, swarm.PermanentError(fmt.Errorf("encode task agent command: %w", err))
	}
	dedupeKey := payload.TaskID + ":agent:" + agentName + ":" + role + ":" + strconv.Itoa(payload.Iteration)
	env := swarm.Envelope{
		ID:            dedupeKey,
		Namespace:     swarm.NamespaceAgentCommand,
		Kind:          swarm.KindGoal,
		From:          swarm.ActorAddress{Target: swarm.ActorTypeTask, Key: payload.TaskID},
		To:            swarm.ActorAddress{Target: swarm.ActorTypeAgent, Key: agentName},
		SessionID:     payload.Locator.SessionID,
		TaskID:        payload.TaskID,
		CorrelationID: payload.TaskID,
		Priority:      70,
		DedupeKey:     dedupeKey,
		PayloadJSON:   string(data),
	}
	return taskAgentDispatch{
		Payload:   payload,
		Role:      role,
		AgentName: agentName,
		Status:    status,
		StepName:  stepName,
		Envelope:  env,
	}, nil
}

func (e *taskActorExecutor) submitAgentDispatch(ctx context.Context, dispatch taskAgentDispatch) error {
	if e.coordinator == nil || !e.coordinator.RuntimeEnabled() {
		return swarm.TransientError(fmt.Errorf("swarm coordinator is required"))
	}
	if _, err := e.coordinator.Submit(ctx, dispatch.Envelope); err != nil {
		return swarm.TransientError(err)
	}
	return nil
}

func (e *taskActorExecutor) recordAgentDispatch(ctx context.Context, dispatch taskAgentDispatch) error {
	payload := dispatch.Payload
	if err := e.tasks.MarkStatus(ctx, payload.TaskID, dispatch.Status, "task.actor", "", "", map[string]any{
		"role":       dispatch.Role,
		"agent_name": dispatch.AgentName,
		"iteration":  payload.Iteration,
	}); err != nil {
		return swarm.TransientError(err)
	}
	if err := e.deliver(
		ctx,
		payload.TaskID,
		payload.Locator,
		fmt.Sprintf("Goal iteration %d/%d: %s started.", payload.Iteration, normalizeGoalMaxIterations(payload.MaxIterations), dispatch.StepName),
		"started:"+dispatch.Role+":"+strconv.Itoa(payload.Iteration),
	); err != nil {
		return err
	}
	if err := e.tasks.AppendEvent(ctx, payload.TaskID, swarm.TaskEventAgentStarted, "task.actor", dispatch.Envelope.DedupeKey, map[string]any{
		"role":            dispatch.Role,
		"agent_name":      dispatch.AgentName,
		"requested_tools": payload.RequestedTools,
		"iteration":       payload.Iteration,
	}); err != nil {
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
	if e.taskStatusIs(ctx, taskID, baldastate.SwarmTaskStatusCanceled, baldastate.SwarmTaskStatusDeadLettered) {
		return nil
	}
	if role != taskAgentRoleReviewer && e.taskStatusIs(ctx, taskID, baldastate.SwarmTaskStatusCompleted, baldastate.SwarmTaskStatusFailed) {
		return nil
	}
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

func (e *taskActorExecutor) taskStatusIs(ctx context.Context, taskID string, statuses ...string) bool {
	if e == nil || e.tasks == nil || strings.TrimSpace(taskID) == "" {
		return false
	}
	task, ok, err := e.tasks.Get(ctx, taskID)
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
	envelopeID := dedupeKey
	if strings.TrimSpace(envelopeID) == "" {
		envelopeID = uuid.NewString()
	}
	env := swarm.Envelope{
		ID:            envelopeID,
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
