package handlers

import (
	"testing"

	"google.golang.org/genai"
)

func TestUsageSnapshotFromMetadata(t *testing.T) {
	snapshot, ok := usageSnapshotFromMetadata(&genai.GenerateContentResponseUsageMetadata{
		PromptTokenCount:        10,
		CachedContentTokenCount: 2,
		CandidatesTokenCount:    5,
		ToolUsePromptTokenCount: 1,
		ThoughtsTokenCount:      3,
		TotalTokenCount:         18,
		TrafficType:             genai.TrafficTypeOnDemand,
	})
	if !ok {
		t.Fatal("usageSnapshotFromMetadata() ok = false, want true")
	}
	if snapshot.TotalTokenCount != 18 {
		t.Fatalf("TotalTokenCount = %d, want 18", snapshot.TotalTokenCount)
	}
	if snapshot.TrafficType != "ON_DEMAND" {
		t.Fatalf("TrafficType = %q, want ON_DEMAND", snapshot.TrafficType)
	}
}

func TestUsageSnapshotFromMap(t *testing.T) {
	snapshot, ok, err := usageSnapshotFromMap(map[string]any{
		"prompt_token_count": float64(10),
		"total_token_count":  float64(15),
		"traffic_type":       "ON_DEMAND",
	})
	if err != nil {
		t.Fatalf("usageSnapshotFromMap() error = %v", err)
	}
	if !ok {
		t.Fatal("usageSnapshotFromMap() ok = false, want true")
	}
	if snapshot.PromptTokenCount != 10 || snapshot.TotalTokenCount != 15 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
}
