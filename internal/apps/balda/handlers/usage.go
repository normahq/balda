package handlers

import (
	"context"
	"fmt"
	"strings"

	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"google.golang.org/genai"
)

const usageStateKey = "balda:last_usage"

type usageSessionReader interface {
	RuntimeStateValue(ctx context.Context, locator baldasession.SessionLocator, key string) (any, bool, error)
}

type usageSnapshot struct {
	PromptTokenCount        int32  `json:"prompt_token_count,omitempty"`
	CachedContentTokenCount int32  `json:"cached_content_token_count,omitempty"`
	ResponseTokenCount      int32  `json:"response_token_count,omitempty"`
	ToolUsePromptTokenCount int32  `json:"tool_use_prompt_token_count,omitempty"`
	ThoughtsTokenCount      int32  `json:"thoughts_token_count,omitempty"`
	TotalTokenCount         int32  `json:"total_token_count,omitempty"`
	TrafficType             string `json:"traffic_type,omitempty"`
}

func usageSnapshotFromMetadata(meta *genai.GenerateContentResponseUsageMetadata) (usageSnapshot, bool) {
	if meta == nil {
		return usageSnapshot{}, false
	}
	snapshot := usageSnapshot{
		PromptTokenCount:        meta.PromptTokenCount,
		CachedContentTokenCount: meta.CachedContentTokenCount,
		ResponseTokenCount:      meta.CandidatesTokenCount,
		ToolUsePromptTokenCount: meta.ToolUsePromptTokenCount,
		ThoughtsTokenCount:      meta.ThoughtsTokenCount,
		TotalTokenCount:         meta.TotalTokenCount,
		TrafficType:             strings.TrimSpace(string(meta.TrafficType)),
	}
	if snapshot == (usageSnapshot{}) {
		return usageSnapshot{}, false
	}
	return snapshot, true
}

func loadUsageSnapshot(ctx context.Context, sessions usageSessionReader, locator baldasession.SessionLocator) (usageSnapshot, bool, error) {
	if sessions == nil {
		return usageSnapshot{}, false, nil
	}
	value, ok, err := sessions.RuntimeStateValue(ctx, locator, usageStateKey)
	if err != nil || !ok {
		return usageSnapshot{}, false, err
	}
	snapshot, ok := value.(map[string]any)
	if !ok {
		return usageSnapshot{}, false, nil
	}
	return usageSnapshotFromMap(snapshot)
}

func usageSnapshotFromMap(raw map[string]any) (usageSnapshot, bool, error) {
	if len(raw) == 0 {
		return usageSnapshot{}, false, nil
	}
	snapshot := usageSnapshot{
		PromptTokenCount:        int32FromAny(raw["prompt_token_count"]),
		CachedContentTokenCount: int32FromAny(raw["cached_content_token_count"]),
		ResponseTokenCount:      int32FromAny(raw["response_token_count"]),
		ToolUsePromptTokenCount: int32FromAny(raw["tool_use_prompt_token_count"]),
		ThoughtsTokenCount:      int32FromAny(raw["thoughts_token_count"]),
		TotalTokenCount:         int32FromAny(raw["total_token_count"]),
		TrafficType:             strings.TrimSpace(stringFromAny(raw["traffic_type"])),
	}
	if snapshot == (usageSnapshot{}) {
		return usageSnapshot{}, false, nil
	}
	return snapshot, true, nil
}

func renderUsageSnapshot(snapshot usageSnapshot) string {
	lines := []string{
		"Last provider usage:",
		fmt.Sprintf("Prompt tokens: %d", snapshot.PromptTokenCount),
		fmt.Sprintf("Cached prompt tokens: %d", snapshot.CachedContentTokenCount),
		fmt.Sprintf("Response tokens: %d", snapshot.ResponseTokenCount),
		fmt.Sprintf("Tool-use prompt tokens: %d", snapshot.ToolUsePromptTokenCount),
		fmt.Sprintf("Reasoning tokens: %d", snapshot.ThoughtsTokenCount),
		fmt.Sprintf("Total tokens: %d", snapshot.TotalTokenCount),
	}
	if snapshot.TrafficType != "" {
		lines = append(lines, fmt.Sprintf("Traffic type: %s", snapshot.TrafficType))
	}
	lines = append(lines, "Limits: provider did not include quota/remaining limits in the last response.")
	return strings.Join(lines, "\n")
}

func int32FromAny(value any) int32 {
	switch v := value.(type) {
	case int:
		return int32(v)
	case int32:
		return v
	case int64:
		return int32(v)
	case float64:
		return int32(v)
	case float32:
		return int32(v)
	default:
		return 0
	}
}

func stringFromAny(value any) string {
	switch v := value.(type) {
	case string:
		return v
	default:
		return ""
	}
}
