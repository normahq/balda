package progress

import (
	"strings"

	adksession "google.golang.org/adk/v2/session"
)

// VisibleText returns non-thought text parts from an ADK event.
func VisibleText(ev *adksession.Event) string {
	if ev == nil || ev.Content == nil {
		return ""
	}
	var parts []string
	for _, part := range ev.Content.Parts {
		if part != nil && !part.Thought && strings.TrimSpace(part.Text) != "" {
			parts = append(parts, strings.TrimSpace(part.Text))
		}
	}
	return strings.TrimSpace(strings.Join(parts, "\n\n"))
}
