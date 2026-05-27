package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/normahq/balda/internal/git"
	"github.com/rs/zerolog/log"
)

const (
	maxTaskEventLines       = 80
	maxTaskListItems        = 20
	maxTaskPayloadSummary   = 180
	maxTaskOutcomeTextRunes = 1200
	statusCommandArg        = "status"
)

type taskSessionInfoProvider interface {
	GetSessionInfo(ctx context.Context, sessionID string) (baldasession.TopicSessionInfo, error)
}

type taskArtifactSnapshot struct {
	WorkspaceDir string
	BranchName   string
	Commit       string
	ChangedFiles []string
	GitError     string
}

func (h *CommandHandler) onTasksCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	if !h.canUseSessionCommand(ctx, commandCtx.UserID) {
		return h.channel.SendPlain(ctx, commandCtx.Locator, "Only the bot owner or collaborators can use this command.")
	}
	if strings.TrimSpace(commandCtx.Args) != "" {
		return h.channel.SendPlain(ctx, commandCtx.Locator, "Usage: /tasks")
	}
	if h.tasks == nil {
		return h.channel.SendPlain(ctx, commandCtx.Locator, "Task visibility is unavailable right now.")
	}
	tasks, err := h.tasks.ListActiveBySession(ctx, commandCtx.Locator.SessionID)
	if err != nil {
		log.Warn().Err(err).Str("session_id", commandCtx.Locator.SessionID).Msg("failed to list active tasks")
		return h.channel.SendPlain(ctx, commandCtx.Locator, fmt.Sprintf("Failed to list tasks: %v", err))
	}
	return h.channel.SendAgentReply(ctx, commandCtx.Locator, formatTaskList(commandCtx.Locator.SessionID, tasks))
}

func (h *CommandHandler) onTaskCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	if !h.canUseSessionCommand(ctx, commandCtx.UserID) {
		return h.channel.SendPlain(ctx, commandCtx.Locator, "Only the bot owner or collaborators can use this command.")
	}
	fields := strings.Fields(commandCtx.Args)
	if len(fields) == 0 || len(fields) > 2 {
		return h.channel.SendPlain(ctx, commandCtx.Locator, "Usage: /task <id> [cancel|events]")
	}
	if h.tasks == nil {
		return h.channel.SendPlain(ctx, commandCtx.Locator, "Task visibility is unavailable right now.")
	}
	taskID := strings.TrimSpace(fields[0])
	task, ok, err := h.tasks.Get(ctx, taskID)
	if err != nil {
		log.Warn().Err(err).Str("task_id", taskID).Msg("failed to read task")
		return h.channel.SendPlain(ctx, commandCtx.Locator, fmt.Sprintf("Failed to read task: %v", err))
	}
	if !ok {
		return h.channel.SendPlain(ctx, commandCtx.Locator, fmt.Sprintf("Task %q not found.", taskID))
	}
	if len(fields) == 2 {
		switch strings.ToLower(fields[1]) {
		case "cancel":
			return h.cancelTaskCommand(ctx, commandCtx, task)
		case "events":
			return h.showTaskEventsCommand(ctx, commandCtx, task)
		default:
			return h.channel.SendPlain(ctx, commandCtx.Locator, "Usage: /task <id> [cancel|events]")
		}
	}

	events, err := h.tasks.ListEvents(ctx, task.ID)
	if err != nil {
		log.Warn().Err(err).Str("task_id", task.ID).Msg("failed to read task events")
		return h.channel.SendPlain(ctx, commandCtx.Locator, fmt.Sprintf("Failed to read task events: %v", err))
	}
	artifacts := h.taskArtifacts(ctx, task)
	return h.channel.SendAgentReply(ctx, commandCtx.Locator, formatTaskDetail(task, events, artifacts))
}

func (h *CommandHandler) cancelTaskCommand(ctx context.Context, commandCtx baldatelegram.CommandContext, task baldastate.SwarmTaskRecord) error {
	if isTerminalTaskStatus(task.Status) {
		return h.channel.SendPlain(ctx, commandCtx.Locator, fmt.Sprintf("Task %s is already %s.", task.ID, task.Status))
	}
	if h.swarmCoordinator == nil || !h.swarmCoordinator.RuntimeEnabled() {
		return h.channel.SendPlain(ctx, commandCtx.Locator, "Cancel is unavailable right now. Please try again.")
	}
	env, err := controlCancelEnvelope(commandCtx.Locator, task.ID, baldatelegram.UserID(commandCtx.UserID), "task canceled by user")
	if err != nil {
		return h.channel.SendPlain(ctx, commandCtx.Locator, fmt.Sprintf("Failed to build cancel request: %v", err))
	}
	if _, err := h.swarmCoordinator.Submit(ctx, env); err != nil {
		log.Warn().Err(err).Str("task_id", task.ID).Msg("failed to publish task cancel command")
		return h.channel.SendPlain(ctx, commandCtx.Locator, fmt.Sprintf("Failed to request task cancel: %v", err))
	}
	return h.channel.SendPlain(ctx, commandCtx.Locator, fmt.Sprintf("Cancel requested for task %s.", task.ID))
}

func (h *CommandHandler) showTaskEventsCommand(ctx context.Context, commandCtx baldatelegram.CommandContext, task baldastate.SwarmTaskRecord) error {
	events, err := h.tasks.ListEvents(ctx, task.ID)
	if err != nil {
		log.Warn().Err(err).Str("task_id", task.ID).Msg("failed to read task events")
		return h.channel.SendPlain(ctx, commandCtx.Locator, fmt.Sprintf("Failed to read task events: %v", err))
	}
	return h.sendPlainChunks(ctx, commandCtx.Locator, formatTaskEventStream(task.ID, events))
}

func (h *CommandHandler) onSwarmCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	if !h.canUseSessionCommand(ctx, commandCtx.UserID) {
		return h.channel.SendPlain(ctx, commandCtx.Locator, "Only the bot owner or collaborators can use this command.")
	}
	if strings.TrimSpace(commandCtx.Args) != statusCommandArg {
		return h.channel.SendPlain(ctx, commandCtx.Locator, "Usage: /swarm status")
	}
	text, err := h.formatSwarmStatus(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("failed to build swarm status")
		return h.channel.SendPlain(ctx, commandCtx.Locator, fmt.Sprintf("Failed to read swarm status: %v", err))
	}
	return h.channel.SendAgentReply(ctx, commandCtx.Locator, text)
}

func (h *CommandHandler) onMailboxCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	return h.onQueueStatusCommand(ctx, commandCtx, "mailbox")
}

func (h *CommandHandler) onQueueCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	return h.onQueueStatusCommand(ctx, commandCtx, "queue")
}

func (h *CommandHandler) onQueueStatusCommand(ctx context.Context, commandCtx baldatelegram.CommandContext, alias string) error {
	if !h.canUseSessionCommand(ctx, commandCtx.UserID) {
		return h.channel.SendPlain(ctx, commandCtx.Locator, "Only the bot owner or collaborators can use this command.")
	}
	if strings.TrimSpace(commandCtx.Args) != statusCommandArg {
		if alias == "queue" {
			return h.channel.SendPlain(ctx, commandCtx.Locator, "Usage: /queue status")
		}
		return h.channel.SendPlain(ctx, commandCtx.Locator, "Usage: /mailbox status")
	}
	status, err := h.formatSwarmStatus(ctx)
	if err != nil {
		return h.channel.SendPlain(ctx, commandCtx.Locator, fmt.Sprintf("Failed to read JetStream status: %v", err))
	}
	return h.channel.SendAgentReply(ctx, commandCtx.Locator, status)
}

func (h *CommandHandler) onDLQCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	if !h.canUseSessionCommand(ctx, commandCtx.UserID) {
		return h.channel.SendPlain(ctx, commandCtx.Locator, "Only the bot owner or collaborators can use this command.")
	}
	if strings.TrimSpace(commandCtx.Args) != "" {
		return h.channel.SendPlain(ctx, commandCtx.Locator, "Usage: /dlq")
	}
	if h.commandBus == nil {
		return h.channel.SendPlain(ctx, commandCtx.Locator, "DLQ visibility is unavailable right now.")
	}
	status, err := h.commandBus.Status(ctx)
	if err != nil {
		return h.channel.SendPlain(ctx, commandCtx.Locator, fmt.Sprintf("Failed to read DLQ status: %v", err))
	}
	var out strings.Builder
	out.WriteString("DLQ status")
	name := strings.TrimSpace(status.DLQ.Name)
	if name == "" {
		name = swarm.DefaultDLQStream
	}
	out.WriteString("\n- stream: ")
	out.WriteString(name)
	out.WriteString("\n- messages: ")
	fmt.Fprintf(&out, "%d", status.DLQ.Messages)
	out.WriteString("\n- bytes: ")
	fmt.Fprintf(&out, "%d", status.DLQ.Bytes)
	out.WriteString("\n- first_seq: ")
	fmt.Fprintf(&out, "%d", status.DLQ.FirstSeq)
	out.WriteString("\n- last_seq: ")
	fmt.Fprintf(&out, "%d", status.DLQ.LastSeq)
	return h.channel.SendAgentReply(ctx, commandCtx.Locator, out.String())
}

func (h *CommandHandler) onProjectionCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	if !h.canUseSessionCommand(ctx, commandCtx.UserID) {
		return h.channel.SendPlain(ctx, commandCtx.Locator, "Only the bot owner or collaborators can use this command.")
	}
	if strings.TrimSpace(commandCtx.Args) != statusCommandArg {
		return h.channel.SendPlain(ctx, commandCtx.Locator, "Usage: /projection status")
	}
	if h.commandBus == nil {
		return h.channel.SendPlain(ctx, commandCtx.Locator, "Projection visibility is unavailable right now.")
	}
	status, err := h.commandBus.Status(ctx)
	if err != nil {
		return h.channel.SendPlain(ctx, commandCtx.Locator, fmt.Sprintf("Failed to read projection status: %v", err))
	}
	var out strings.Builder
	out.WriteString("Projection status")
	if len(status.ProjectionLag) == 0 {
		out.WriteString("\n- none")
		return h.channel.SendAgentReply(ctx, commandCtx.Locator, out.String())
	}
	keys := make([]string, 0, len(status.ProjectionLag))
	for name := range status.ProjectionLag {
		keys = append(keys, name)
	}
	sort.Strings(keys)
	for _, name := range keys {
		out.WriteString("\n- ")
		out.WriteString(name)
		out.WriteString("_lag: ")
		fmt.Fprintf(&out, "%d", status.ProjectionLag[name])
	}
	return h.channel.SendAgentReply(ctx, commandCtx.Locator, out.String())
}

func (h *CommandHandler) onActorsCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	if !h.canUseSessionCommand(ctx, commandCtx.UserID) {
		return h.channel.SendPlain(ctx, commandCtx.Locator, "Only the bot owner or collaborators can use this command.")
	}
	if strings.TrimSpace(commandCtx.Args) != statusCommandArg {
		return h.channel.SendPlain(ctx, commandCtx.Locator, "Usage: /actors status")
	}
	var out strings.Builder
	out.WriteString("Actors status")
	if h.agentRegistry == nil || len(h.agentRegistry.All()) == 0 {
		out.WriteString("\n- none configured")
		return h.channel.SendAgentReply(ctx, commandCtx.Locator, out.String())
	}
	for _, agent := range h.agentRegistry.All() {
		out.WriteString("\n- ")
		out.WriteString(agent.Name)
		out.WriteString(": ")
		out.WriteString(agent.Role)
		if len(agent.Tools) > 0 {
			out.WriteString(" [")
			out.WriteString(strings.Join(agent.Tools, ", "))
			out.WriteString("]")
		}
		out.WriteString(" {shell_policy=")
		out.WriteString(agent.ShellExecutionPolicy())
		out.WriteString("}")
	}
	return h.channel.SendAgentReply(ctx, commandCtx.Locator, out.String())
}

func (h *CommandHandler) formatSwarmStatus(ctx context.Context) (string, error) {
	var out strings.Builder
	out.WriteString("Swarm status\n")
	out.WriteString("- enabled: ")
	fmt.Fprintf(&out, "%t", h.swarmConfig.Enabled)
	out.WriteString("\n- runtime enabled: ")
	fmt.Fprintf(&out, "%t", h.swarmCoordinator != nil && h.swarmCoordinator.RuntimeEnabled())
	out.WriteString("\n\nCommand bus")
	if h.commandBus != nil {
		status, err := h.commandBus.Status(ctx)
		if err != nil {
			return "", err
		}
		out.WriteString("\n- command_bus: ")
		out.WriteString(firstNonEmpty(status.CommandBus, "unknown"))
		if contract := strings.TrimSpace(status.DisabledMode); contract != "" {
			out.WriteString("\n- disabled_mode_contract: ")
			out.WriteString(contract)
		}
		out.WriteString("\n- command_event_publishing_mode: ")
		out.WriteString(swarm.CommandLifecycleEventPublishingMode)
		out.WriteString("\n- embedded_nats: ")
		fmt.Fprintf(&out, "%t", status.Embedded)
		out.WriteString("\n- running: ")
		fmt.Fprintf(&out, "%t", status.Running)
		out.WriteString("\n- jetstream: ")
		fmt.Fprintf(&out, "%t", status.JetStream)
		if status.ClientURL != "" {
			out.WriteString("\n- client_url: ")
			out.WriteString(status.ClientURL)
		}
		writeStreamStatus(&out, "\n\nBALDA_COMMANDS", status.Commands)
		writeConsumerStatus(&out, "\n\nBALDA_WORKER_COMMANDS", status.Worker)
		writeStreamStatus(&out, "\n\nBALDA_EVENTS", status.Events)
		writeStreamStatus(&out, "\n\nBALDA_DLQ", status.DLQ)
		if len(status.ProjectionLag) > 0 {
			out.WriteString("\n\nProjectors")
			keys := make([]string, 0, len(status.ProjectionLag))
			for name := range status.ProjectionLag {
				keys = append(keys, name)
			}
			sort.Strings(keys)
			for _, name := range keys {
				out.WriteString("\n- ")
				out.WriteString(name)
				out.WriteString("_lag: ")
				fmt.Fprintf(&out, "%d", status.ProjectionLag[name])
			}
		}
		out.WriteString("\n\nMetrics")
		out.WriteString("\n- commands_backlog: ")
		fmt.Fprintf(&out, "%d", status.Worker.NumPending)
		out.WriteString("\n- commands_redelivered_total: ")
		fmt.Fprintf(&out, "%d", status.Worker.NumRedelivered)
		out.WriteString("\n- dlq_messages_total: ")
		fmt.Fprintf(&out, "%d", status.DLQ.Messages)
		out.WriteString("\n- projection_lag_total: ")
		fmt.Fprintf(&out, "%d", sumProjectionLag(status.ProjectionLag))
	} else {
		out.WriteString("\n- unavailable")
	}

	out.WriteString("\n\nAgents")
	if h.agentRegistry == nil || len(h.agentRegistry.All()) == 0 {
		out.WriteString("\n- none configured")
	} else {
		for _, agent := range h.agentRegistry.All() {
			out.WriteString("\n- ")
			out.WriteString(agent.Name)
			out.WriteString(": ")
			out.WriteString(agent.Role)
			if len(agent.Tools) > 0 {
				out.WriteString(" [")
				out.WriteString(strings.Join(agent.Tools, ", "))
				out.WriteString("]")
			}
			out.WriteString(" {shell_policy=")
			out.WriteString(agent.ShellExecutionPolicy())
			out.WriteString("}")
		}
	}

	out.WriteString("\n\nTasks")
	if h.tasks == nil {
		out.WriteString("\n- unavailable")
	} else {
		out.WriteString("\n- state_source_of_truth: ")
		out.WriteString(h.tasks.StateSourceOfTruth())
		out.WriteString("\n- event_publishing_mode: ")
		out.WriteString(h.tasks.EventPublishingMode())
		counts, err := h.tasks.StatusCounts(ctx)
		if err != nil {
			return "", err
		}
		if len(counts) == 0 {
			out.WriteString("\n- none")
		} else {
			for _, count := range counts {
				out.WriteString("\n- ")
				out.WriteString(count.Status)
				out.WriteString(": ")
				fmt.Fprintf(&out, "%d", count.Count)
			}
		}
	}

	return out.String(), nil
}

func writeStreamStatus(out *strings.Builder, title string, status swarm.StreamStatus) {
	out.WriteString(title)
	out.WriteString("\n- messages: ")
	fmt.Fprintf(out, "%d", status.Messages)
	out.WriteString("\n- bytes: ")
	fmt.Fprintf(out, "%d", status.Bytes)
	out.WriteString("\n- first_seq: ")
	fmt.Fprintf(out, "%d", status.FirstSeq)
	out.WriteString("\n- last_seq: ")
	fmt.Fprintf(out, "%d", status.LastSeq)
}

func writeConsumerStatus(out *strings.Builder, title string, status swarm.ConsumerStatus) {
	out.WriteString(title)
	out.WriteString("\n- num_pending: ")
	fmt.Fprintf(out, "%d", status.NumPending)
	out.WriteString("\n- num_ack_pending: ")
	fmt.Fprintf(out, "%d", status.NumAckPending)
	out.WriteString("\n- num_redelivered: ")
	fmt.Fprintf(out, "%d", status.NumRedelivered)
}

func sumProjectionLag(lag map[string]uint64) uint64 {
	var total uint64
	for _, value := range lag {
		total += value
	}
	return total
}

func (h *CommandHandler) taskArtifacts(ctx context.Context, task baldastate.SwarmTaskRecord) taskArtifactSnapshot {
	return taskArtifactsFromSessionProvider(ctx, h.sessionManager, task)
}

func taskArtifactsFromSessionProvider(ctx context.Context, provider any, task baldastate.SwarmTaskRecord) taskArtifactSnapshot {
	info, ok := taskSessionInfo(ctx, provider, task.SessionID)
	artifacts := taskArtifactSnapshot{}
	if ok {
		artifacts.WorkspaceDir = strings.TrimSpace(info.WorkspaceDir)
		artifacts.BranchName = strings.TrimSpace(info.BranchName)
	}
	if artifacts.BranchName == "" {
		artifacts.BranchName = strings.TrimSpace(task.AssignedActor)
	}
	return enrichGitArtifacts(ctx, artifacts)
}

func enrichGitArtifacts(ctx context.Context, artifacts taskArtifactSnapshot) taskArtifactSnapshot {
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

func taskSessionInfo(ctx context.Context, rawProvider any, sessionID string) (baldasession.TopicSessionInfo, bool) {
	provider, ok := rawProvider.(taskSessionInfoProvider)
	if !ok || strings.TrimSpace(sessionID) == "" {
		return baldasession.TopicSessionInfo{}, false
	}
	info, err := provider.GetSessionInfo(ctx, sessionID)
	if err != nil {
		return baldasession.TopicSessionInfo{}, false
	}
	return info, true
}

func formatTaskList(sessionID string, tasks []baldastate.SwarmTaskRecord) string {
	if len(tasks) == 0 {
		return "No active tasks for this session."
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].CreatedAt.Before(tasks[j].CreatedAt) })
	if len(tasks) > maxTaskListItems {
		tasks = tasks[:maxTaskListItems]
	}
	var out strings.Builder
	out.WriteString("Active tasks for session ")
	out.WriteString(strings.TrimSpace(sessionID))
	out.WriteString(":")
	for _, task := range tasks {
		out.WriteString("\n- ")
		out.WriteString(task.ID)
		out.WriteString(" [")
		out.WriteString(task.Status)
		out.WriteString("] ")
		out.WriteString(firstNonEmpty(task.Title, task.Objective))
	}
	return out.String()
}

func formatTaskDetail(task baldastate.SwarmTaskRecord, events []baldastate.SwarmTaskEventRecord, artifacts taskArtifactSnapshot) string {
	var out strings.Builder
	out.WriteString("Task ")
	out.WriteString(task.ID)
	out.WriteString("\n")
	out.WriteString("Status: ")
	out.WriteString(task.Status)
	out.WriteString("\nObjective: ")
	out.WriteString(task.Objective)
	if task.SessionID != "" {
		out.WriteString("\nSession: ")
		out.WriteString(task.SessionID)
	}
	if task.CreatedFrom != "" {
		out.WriteString("\nSource: ")
		out.WriteString(task.CreatedFrom)
	}
	out.WriteString("\nCreated: ")
	out.WriteString(formatTaskTime(task.CreatedAt))
	out.WriteString("\nUpdated: ")
	out.WriteString(formatTaskTime(task.UpdatedAt))
	if task.Error != "" {
		out.WriteString("\nError: ")
		out.WriteString(task.Error)
	}
	if isTerminalTaskStatus(task.Status) && strings.TrimSpace(task.ResultJSON) != "" {
		out.WriteString("\n\n")
		out.WriteString(renderReviewableOutcome(task, artifacts))
	}
	if len(events) > 0 {
		out.WriteString("\n\nLatest events")
		start := 0
		if len(events) > 5 {
			start = len(events) - 5
		}
		for _, event := range events[start:] {
			out.WriteString("\n- ")
			out.WriteString(formatTaskEventLine(event))
		}
	}
	return out.String()
}

func renderReviewableOutcome(task baldastate.SwarmTaskRecord, artifacts taskArtifactSnapshot) string {
	result := parseTaskResult(task.ResultJSON)
	goalReached := boolFromResult(result, "goal_reached")
	executorOutput := firstNonEmpty(stringFromResult(result, "executor_output"), stringFromResult(result, "final_text"))
	reviewerOutput := firstNonEmpty(stringFromResult(result, "reviewer_output"), stringFromResult(result, "reviewer_feedback"))
	whatWasDone := firstNonEmpty(executorOutput, task.Objective)
	if !goalReached && task.Status != baldastate.SwarmTaskStatusCompleted && stringFromResult(result, "final_text") != "" {
		whatWasDone = stringFromResult(result, "final_text")
	}

	var out strings.Builder
	out.WriteString("Result\n")
	out.WriteString("- What was done: ")
	out.WriteString(limitRunes(oneLine(whatWasDone), maxTaskOutcomeTextRunes))
	out.WriteString("\n\nArtifacts")
	out.WriteString("\n- Files changed: ")
	if len(artifacts.ChangedFiles) == 0 {
		out.WriteString("none detected")
	} else {
		out.WriteString(strings.Join(artifacts.ChangedFiles, "; "))
	}
	out.WriteString("\n- Branch: ")
	out.WriteString(firstNonEmpty(artifacts.BranchName, "not available"))
	out.WriteString("\n- Commit: ")
	out.WriteString(firstNonEmpty(artifacts.Commit, "not available"))
	out.WriteString("\n- Workspace export: ")
	if len(artifacts.ChangedFiles) > 0 {
		out.WriteString("pending review/export")
	} else {
		out.WriteString("no pending workspace changes detected")
	}
	out.WriteString("\n- Validation output: ")
	out.WriteString(limitRunes(oneLine(firstNonEmpty(reviewerOutput, "not available")), maxTaskOutcomeTextRunes))

	out.WriteString("\n\nConfidence")
	out.WriteString("\n- What was verified: ")
	switch {
	case goalReached || strings.HasPrefix(strings.ToLower(strings.TrimSpace(reviewerOutput)), "verdict: pass"):
		out.WriteString("reviewer returned pass")
	case reviewerOutput != "":
		out.WriteString("reviewer returned feedback")
	default:
		out.WriteString("no explicit validation captured")
	}
	out.WriteString("\n- What was not verified: ")
	switch {
	case artifacts.GitError != "":
		out.WriteString(artifacts.GitError)
	case reviewerOutput == "":
		out.WriteString("validation output was not captured")
	default:
		out.WriteString("manual review still required")
	}

	out.WriteString("\n\nNext action\n- ")
	switch {
	case task.Status == baldastate.SwarmTaskStatusCompleted && len(artifacts.ChangedFiles) > 0:
		out.WriteString("Review workspace changes and run `balda.workspace.export` when ready.")
	case task.Status == baldastate.SwarmTaskStatusCompleted:
		out.WriteString("Review the result and close or continue with a follow-up task.")
	case task.Status == baldastate.SwarmTaskStatusFailed:
		out.WriteString("Review failure evidence and rerun `/goal` or assign a narrower follow-up task.")
	case task.Status == baldastate.SwarmTaskStatusCanceled:
		out.WriteString("Start a new task when ready.")
	default:
		out.WriteString("Inspect events and decide whether to continue, cancel, or ask a human.")
	}
	return out.String()
}

func formatTaskEventStream(taskID string, events []baldastate.SwarmTaskEventRecord) string {
	if len(events) == 0 {
		return fmt.Sprintf("No events for task %s.", taskID)
	}
	if len(events) > maxTaskEventLines {
		events = events[len(events)-maxTaskEventLines:]
	}
	var out strings.Builder
	out.WriteString("Events for task ")
	out.WriteString(taskID)
	out.WriteString(":")
	for _, event := range events {
		out.WriteString("\n- ")
		out.WriteString(formatTaskEventLine(event))
	}
	return out.String()
}

func formatTaskEventLine(event baldastate.SwarmTaskEventRecord) string {
	parts := []string{formatTaskTime(event.CreatedAt), event.EventType}
	if event.Actor != "" {
		parts = append(parts, "actor="+event.Actor)
	}
	if summary := summarizeTaskPayload(event.PayloadJSON); summary != "" {
		parts = append(parts, summary)
	}
	return strings.Join(parts, " | ")
}

func summarizeTaskPayload(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return limitRunes(oneLine(trimmed), maxTaskPayloadSummary)
	}
	keys := []string{"status", "reason", "role", "agent_name", "iteration", "text"}
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		if value, ok := payload[key]; ok && fmt.Sprint(value) != "" {
			parts = append(parts, key+"="+limitRunes(oneLine(fmt.Sprint(value)), maxTaskPayloadSummary))
		}
	}
	if len(parts) == 0 {
		return limitRunes(oneLine(trimmed), maxTaskPayloadSummary)
	}
	return strings.Join(parts, ", ")
}

func parseTaskResult(raw string) map[string]any {
	var out map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &out); err != nil {
		return nil
	}
	return out
}

func stringFromResult(result map[string]any, key string) string {
	if len(result) == 0 {
		return ""
	}
	value, ok := result[key]
	if !ok || value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func boolFromResult(result map[string]any, key string) bool {
	value, ok := result[key]
	if !ok {
		return false
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true")
	default:
		return false
	}
}

func isTerminalTaskStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case baldastate.SwarmTaskStatusCompleted,
		baldastate.SwarmTaskStatusFailed,
		baldastate.SwarmTaskStatusCanceled,
		baldastate.SwarmTaskStatusDeadLettered:
		return true
	default:
		return false
	}
}

func formatTaskTime(t time.Time) string {
	if t.IsZero() {
		return "n/a"
	}
	return t.UTC().Format(time.RFC3339)
}

func oneLine(raw string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(raw)), " ")
}

func limitRunes(raw string, limit int) string {
	if limit <= 0 {
		return raw
	}
	runes := []rune(strings.TrimSpace(raw))
	if len(runes) <= limit {
		return string(runes)
	}
	return string(runes[:limit]) + "..."
}
