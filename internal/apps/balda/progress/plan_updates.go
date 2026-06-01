package progress

import (
	"fmt"
	"strings"

	adksession "google.golang.org/adk/session"
)

const (
	ACPPlanMetadataKey = "acp_plan"
	ACPUpdateKindKey   = "acp_update_kind"
	ACPUpdateKindPlan  = "plan"
	acpPlanEntriesKey  = "entries"
)

func PlanUpdateText(ev *adksession.Event) (string, bool) {
	if ev == nil {
		return "", false
	}
	var snapshot map[string]any
	if len(ev.CustomMetadata) != 0 {
		if rawKind, ok := ev.CustomMetadata[ACPUpdateKindKey]; ok {
			var kind string
			switch v := rawKind.(type) {
			case string:
				kind = v
			case fmt.Stringer:
				kind = v.String()
			default:
				kind = fmt.Sprintf("%v", rawKind)
			}
			if kind = strings.TrimSpace(kind); kind != "" && kind != ACPUpdateKindPlan {
				return "", false
			}
		}
		if candidate, ok := ev.CustomMetadata[ACPPlanMetadataKey].(map[string]any); ok {
			snapshot = candidate
		}
	}
	if snapshot == nil && len(ev.Actions.StateDelta) != 0 {
		if candidate, ok := ev.Actions.StateDelta[ACPPlanMetadataKey].(map[string]any); ok {
			snapshot = candidate
		}
	}
	if snapshot == nil {
		return "", false
	}
	rawEntries, ok := snapshot[acpPlanEntriesKey]
	if !ok {
		return "", false
	}
	var entries []map[string]any
	switch typed := rawEntries.(type) {
	case []map[string]any:
		if len(typed) == 0 {
			return "", false
		}
		entries = typed
	case []any:
		entries = make([]map[string]any, 0, len(typed))
		for _, rawEntry := range typed {
			entry, ok := rawEntry.(map[string]any)
			if !ok {
				return "", false
			}
			entries = append(entries, entry)
		}
		if len(entries) == 0 {
			return "", false
		}
	default:
		return "", false
	}

	lines := make([]string, 0, len(entries)+1)
	lines = append(lines, "Plan update")
	for _, entry := range entries {
		var content string
		switch v := entry["content"].(type) {
		case string:
			content = v
		case fmt.Stringer:
			content = v.String()
		default:
			content = fmt.Sprintf("%v", entry["content"])
		}
		content = strings.TrimSpace(content)
		if content == "" {
			content = "(no description)"
		}
		var status string
		switch v := entry["status"].(type) {
		case string:
			status = v
		case fmt.Stringer:
			status = v.String()
		default:
			status = fmt.Sprintf("%v", entry["status"])
		}
		status = strings.TrimSpace(status)
		if status == "" {
			status = "unknown"
		}
		status = strings.ReplaceAll(status, "_", " ")
		lines = append(lines, fmt.Sprintf("- [%s] %s", status, content))
	}
	return strings.Join(lines, "\n"), true
}
