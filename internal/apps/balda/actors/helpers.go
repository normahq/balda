package actors

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	baldastate "github.com/normahq/balda/internal/apps/balda/state"
)

const (
	ownerSessionLabel        = "balda"
	defaultGoalMaxIterations = 25
	maxTaskOutcomeTextRunes  = 1200
	goalExportStatusFailed   = "export_failed"
)

var (
	secretBearerHeaderPattern = regexp.MustCompile(`(?i)(authorization\s*:\s*bearer\s+)([^\s]+)`)
	secretKeyValuePattern     = regexp.MustCompile(`(?i)\b(token|secret|password|api[_-]?key|access[_-]?key|private[_-]?key)\b(\s*[:=]\s*)([^\s,;]+)`)
	secretPEMPattern          = regexp.MustCompile(`(?s)-----BEGIN [^-]+-----.*?-----END [^-]+-----`)
	secretGitHubTokenPattern  = regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9_]{20,}\b`)
	secretTelegramToken       = regexp.MustCompile(`\b\d{6,10}:[A-Za-z0-9_-]{20,}\b`)
)

func normalizeGoalMaxIterations(v int) int {
	if v <= 0 {
		return defaultGoalMaxIterations
	}
	return v
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

type taskArtifactSnapshot struct {
	WorkspaceDir string
	BranchName   string
	Commit       string
	ChangedFiles []string
	GitError     string
}

func renderReviewableOutcome(task baldastate.SwarmTaskRecord, artifacts taskArtifactSnapshot) string {
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
	executorOutput := firstNonEmpty(resultText("executor_output"), resultText("final_text"))
	reviewerOutput := firstNonEmpty(resultText("reviewer_output"), resultText("reviewer_feedback"))
	executorOutput = redactSecrets(executorOutput)
	reviewerOutput = redactSecrets(reviewerOutput)
	whatWasDone := firstNonEmpty(executorOutput, task.Objective)
	if hasOutcome {
		whatWasDone = firstNonEmpty(parsedOutcome.WhatWasDone, whatWasDone)
	}
	if !goalReached && task.Status != baldastate.SwarmTaskStatusCompleted && resultText("final_text") != "" {
		whatWasDone = redactSecrets(resultText("final_text"))
	}
	if len(result) != 0 {
		if artifactMap, ok := result["artifacts"].(map[string]any); ok {
			artifacts.WorkspaceDir = firstNonEmpty(strings.TrimSpace(fmt.Sprint(artifactMap["workspace_dir"])), artifacts.WorkspaceDir)
			artifacts.BranchName = firstNonEmpty(strings.TrimSpace(fmt.Sprint(artifactMap["branch_name"])), artifacts.BranchName)
			artifacts.Commit = firstNonEmpty(strings.TrimSpace(fmt.Sprint(artifactMap["commit"])), artifacts.Commit)
			if artifacts.GitError == "" {
				artifacts.GitError = redactSecrets(strings.TrimSpace(fmt.Sprint(artifactMap["git_error"])))
			}
			if changed, ok := artifactMap["changed_files"].([]any); ok && len(artifacts.ChangedFiles) == 0 {
				for _, item := range changed {
					if trimmed := strings.TrimSpace(fmt.Sprint(item)); trimmed != "" {
						artifacts.ChangedFiles = append(artifacts.ChangedFiles, trimmed)
					}
				}
			}
		}
	}
	exportStatus := ""
	exportMessage := ""
	exportError := ""
	if len(result) != 0 {
		if exportMap, ok := result["export"].(map[string]any); ok {
			exportStatus = strings.TrimSpace(fmt.Sprint(exportMap["status"]))
			exportMessage = strings.TrimSpace(fmt.Sprint(exportMap["commit_message"]))
			exportError = redactSecrets(strings.TrimSpace(fmt.Sprint(exportMap["error"])))
		}
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
	switch exportStatus {
	case "exported":
		out.WriteString("exported to base branch")
	case goalExportStatusFailed:
		out.WriteString("failed; goal workspace preserved for recovery")
	case "canceled", "failed", "not_exported":
		out.WriteString("not exported")
	case "pending":
		out.WriteString("pending")
	default:
		if len(artifacts.ChangedFiles) > 0 {
			out.WriteString("pending review/export")
		} else {
			out.WriteString("no pending workspace changes detected")
		}
	}
	if exportMessage != "" {
		out.WriteString(" (")
		out.WriteString(oneLine(exportMessage))
		out.WriteString(")")
	}
	if exportError != "" {
		out.WriteString("; ")
		out.WriteString(oneLine(exportError))
	}
	out.WriteString("\n- Validation output: ")
	validationOutput := firstNonEmpty(reviewerOutput, "not available")
	if hasOutcome {
		validationOutput = firstNonEmpty(parsedOutcome.Validation, validationOutput)
	}
	out.WriteString(limitRunes(oneLine(validationOutput), maxTaskOutcomeTextRunes))
	if artifacts.GitError != "" {
		out.WriteString("\n- Workspace note: ")
		out.WriteString(limitRunes(oneLine(artifacts.GitError), maxTaskOutcomeTextRunes))
	}

	out.WriteString("\n\nConfidence")
	out.WriteString("\n- What was verified: ")
	verified := "not available"
	if hasOutcome {
		verified = firstNonEmpty(parsedOutcome.Verified, verified)
	}
	out.WriteString(limitRunes(oneLine(verified), maxTaskOutcomeTextRunes))
	out.WriteString("\n- What was not verified: ")
	notVerified := "not available"
	if hasOutcome {
		notVerified = firstNonEmpty(parsedOutcome.NotVerified, notVerified)
	}
	out.WriteString(limitRunes(oneLine(notVerified), maxTaskOutcomeTextRunes))
	out.WriteString("\n- Next action: ")
	nextAction := "review result"
	if hasOutcome {
		nextAction = firstNonEmpty(parsedOutcome.NextAction, nextAction)
	}
	out.WriteString(limitRunes(oneLine(nextAction), maxTaskOutcomeTextRunes))
	return out.String()
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
