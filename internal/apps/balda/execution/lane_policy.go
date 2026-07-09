package execution

import (
	"fmt"
	"strings"

	"github.com/normahq/balda/pkg/actorlayer"
)

func executionAddressOf(env actorlayer.Envelope) (string, error) {
	to := env.To
	to.Target = canonicalActorTarget(to.Target)
	addr, err := to.String()
	if err != nil {
		return "", actorlayer.DecodeError(err)
	}
	if strings.TrimSpace(addr) == "" {
		return "", actorlayer.DecodeError(fmt.Errorf("empty actor address"))
	}
	return addr, nil
}

func actorLaneKeyFromEnvelope(env actorlayer.Envelope) string {
	namespace := canonicalNamespace(env.Namespace)
	jobID := EnvelopeJobID(env)
	if jobID != "" {
		switch namespace {
		case NamespaceJobControl,
			NamespaceGoalkeeperCommand,
			NamespaceHumanInbound,
			NamespaceWebhookInbound,
			NamespaceScheduleInbound:
			return "job:" + jobID
		case NamespaceAgentResult:
			if strings.EqualFold(strings.TrimSpace(env.To.Target), ActorTypeDelivery) {
				if address := strings.TrimSpace(env.To.Key); address != "" {
					return "delivery:" + address
				}
			}
			return "job:" + jobID
		}
	}
	switch namespace {
	case NamespaceGoalkeeperCommand:
		if key := strings.TrimSpace(env.To.Key); key != "" {
			return "goalkeeper:" + key
		}
	case NamespaceHumanInbound, NamespaceWebhookInbound, NamespaceScheduleInbound:
		if sessionID := strings.TrimSpace(env.SessionID); sessionID != "" {
			return "session:" + sessionID
		}
	}
	if to, err := executionAddressOf(env); err == nil {
		return to
	}
	return strings.TrimSpace(env.ID)
}

func canonicalActorTarget(target string) string {
	trimmed := strings.ToLower(strings.TrimSpace(target))
	if trimmed == ActorTypeJob {
		return ActorTypeJob
	}
	return trimmed
}

func canonicalNamespace(namespace string) string {
	trimmed := strings.TrimSpace(namespace)
	if trimmed == NamespaceJobControl {
		return NamespaceJobControl
	}
	return trimmed
}
