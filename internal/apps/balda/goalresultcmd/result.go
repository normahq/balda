package goalresultcmd

import (
	"encoding/json"
	"strings"
)

const (
	StatusDone          = "done"
	StatusNeedUserInput = "need_user_input"
)

type WorkerResult struct {
	Status   string               `json:"status"`
	Summary  string               `json:"summary,omitempty"`
	Question string               `json:"question,omitempty"`
	Reason   string               `json:"reason,omitempty"`
	Options  []WorkerResultOption `json:"options,omitempty"`
}

type WorkerResultOption struct {
	ID    string `json:"id,omitempty"`
	Label string `json:"label"`
}

func ParseWorkerResult(text string) (WorkerResult, bool) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" || !strings.HasPrefix(trimmed, "{") {
		return WorkerResult{}, false
	}
	var result WorkerResult
	if err := json.Unmarshal([]byte(trimmed), &result); err != nil {
		return WorkerResult{}, false
	}
	result.Status = strings.TrimSpace(strings.ToLower(result.Status))
	result.Summary = strings.TrimSpace(result.Summary)
	result.Question = strings.TrimSpace(result.Question)
	result.Reason = strings.TrimSpace(result.Reason)
	result.Options = normalizeWorkerResultOptions(result.Options)
	switch result.Status {
	case StatusDone:
		return result, result.Summary != ""
	case StatusNeedUserInput:
		return result, result.Question != ""
	default:
		return WorkerResult{}, false
	}
}

func normalizeWorkerResultOptions(options []WorkerResultOption) []WorkerResultOption {
	if len(options) == 0 {
		return nil
	}
	normalized := make([]WorkerResultOption, 0, len(options))
	for _, option := range options {
		label := strings.TrimSpace(option.Label)
		if label == "" {
			continue
		}
		id := strings.TrimSpace(option.ID)
		if id == "" {
			id = strings.ToLower(strings.ReplaceAll(label, " ", "_"))
		}
		id = strings.Trim(id, "_")
		if id == "" {
			id = "option"
		}
		normalized = append(normalized, WorkerResultOption{ID: id, Label: label})
	}
	if len(normalized) == 0 {
		return nil
	}
	return normalized
}
