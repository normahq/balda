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
	executorOutput := firstNonEmpty(stringFromResult(result, "executor_output"), stringFromResult(result, "final_text"))
	reviewerOutput := firstNonEmpty(stringFromResult(result, "reviewer_output"), stringFromResult(result, "reviewer_feedback"))
	executorOutput = redactSecrets(executorOutput)
	reviewerOutput = redactSecrets(reviewerOutput)
	whatWasDone := firstNonEmpty(executorOutput, task.Objective)
	if hasOutcome {
		whatWasDone = firstNonEmpty(parsedOutcome.WhatWasDone, whatWasDone)
	}
	if !goalReached && task.Status != baldastate.SwarmTaskStatusCompleted && stringFromResult(result, "final_text") != "" {
		whatWasDone = redactSecrets(stringFromResult(result, "final_text"))
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
	validationOutput := firstNonEmpty(reviewerOutput, "not available")
	if hasOutcome {
		validationOutput = firstNonEmpty(parsedOutcome.Validation, validationOutput)
	}
	out.WriteString(limitRunes(oneLine(validationOutput), maxTaskOutcomeTextRunes))

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
