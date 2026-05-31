package swarm

import "strings"

func actorLaneKey(env Envelope) string {
	namespace := strings.TrimSpace(env.Namespace)
	taskID := strings.TrimSpace(env.TaskID)
	if taskID != "" {
		switch namespace {
		case NamespaceTaskControl,
			NamespaceGoalCommand,
			NamespaceHumanInbound,
			NamespaceWebhookInbound,
			NamespaceScheduleInbound:
			return "task:" + taskID
		case NamespaceAgentResult:
			if strings.EqualFold(strings.TrimSpace(env.To.Target), ActorTypeDelivery) {
				if address := strings.TrimSpace(env.To.Key); address != "" {
					return "delivery:" + address
				}
			}
			return "task:" + taskID
		}
	}
	switch namespace {
	case NamespaceGoalCommand:
		if key := strings.TrimSpace(env.To.Key); key != "" {
			return "goal:" + key
		}
	case NamespaceHumanInbound, NamespaceWebhookInbound, NamespaceScheduleInbound:
		if sessionID := strings.TrimSpace(env.SessionID); sessionID != "" {
			return "session:" + sessionID
		}
	}
	if to, err := env.To.String(); err == nil {
		return to
	}
	return strings.TrimSpace(env.ID)
}
