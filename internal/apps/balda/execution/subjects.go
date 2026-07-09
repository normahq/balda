package execution

import (
	"strconv"
	"strings"

	"github.com/normahq/balda/pkg/actorlayer"
)

const (
	SubjectCommandSession  = "balda.v1.cmd.session"
	SubjectCommandJob      = "balda.v1.cmd.job"
	SubjectCommandGoal     = "balda.v1.cmd.goal"
	SubjectCommandDelivery = "balda.v1.cmd.delivery"
	SubjectCommandMemory   = "balda.v1.cmd.memory"
	SubjectCommandControl  = "balda.v1.cmd.control"
	SubjectCommandAll      = "balda.v1.cmd.>"

	SubjectEventCommandAccepted     = "balda.v1.evt.command.accepted"
	SubjectEventCommandRunning      = "balda.v1.evt.command.running"
	SubjectEventCommandInProgress   = "balda.v1.evt.command.in_progress"
	SubjectEventCommandAcked        = "balda.v1.evt.command.acked"
	SubjectEventCommandRetrying     = "balda.v1.evt.command.retrying"
	SubjectEventCommandDeadLettered = "balda.v1.evt.command.deadlettered"
	SubjectEventCommandNoop         = "balda.v1.evt.command.noop"
	SubjectEventCommandDecodeFailed = "balda.v1.evt.command.decode_failed"
	SubjectEventJobCreated          = "balda.v1.evt.job.created"
	SubjectEventJobUpdated          = "balda.v1.evt.job.updated"
	SubjectEventJobCompleted        = "balda.v1.evt.job.completed"
	SubjectEventDeliverySent        = "balda.v1.evt.delivery.sent"
	SubjectEventDeliveryFailed      = "balda.v1.evt.delivery.failed"
	SubjectEventMemoryUpdated       = "balda.v1.evt.memory.updated"
	SubjectEventAll                 = "balda.v1.evt.>"

	SubjectDLQCommand = "balda.v1.dlq.command"
	SubjectDLQAll     = "balda.v1.dlq.>"
)

const (
	HeaderEnvelopeID    = "Balda-Envelope-ID"
	HeaderSessionID     = "Balda-Session-ID"
	HeaderCorrelationID = "Balda-Correlation-ID"
	HeaderCausationID   = "Balda-Causation-ID"
	HeaderDedupeKey     = "Balda-Dedupe-Key"
	HeaderNamespace     = "Balda-Namespace"
	HeaderActorKey      = "Balda-Actor-Key"
	HeaderPriority      = "Balda-Priority"
)

func SubjectForEnvelope(env actorlayer.Envelope) string {
	namespace := canonicalNamespace(env.Namespace)
	target := canonicalActorTarget(env.To.Target)
	if namespace == NamespaceJobControl {
		return SubjectCommandControl
	}
	switch target {
	case ActorTypeSession:
		return SubjectCommandSession
	case ActorTypeJob:
		return SubjectCommandJob
	case ActorTypeGoalkeeper:
		return SubjectCommandGoal
	case ActorTypeDelivery:
		return SubjectCommandDelivery
	case ActorTypeMemory:
		return SubjectCommandMemory
	default:
		switch namespace {
		case NamespaceGoalkeeperCommand:
			return SubjectCommandGoal
		case NamespaceJobControl:
			return SubjectCommandControl
		case NamespaceMemoryCommand:
			return SubjectCommandMemory
		case NamespaceWebhookInbound, NamespaceScheduleInbound:
			return SubjectCommandJob
		case NamespaceHumanInbound:
			return SubjectCommandSession
		default:
			return SubjectCommandJob
		}
	}
}

func EnvelopeHeaders(env actorlayer.Envelope) map[string]string {
	out := make(map[string]string, 8)
	addHeader(out, HeaderEnvelopeID, env.ID)
	addHeader(out, HeaderSessionID, env.SessionID)
	addHeader(out, HeaderCorrelationID, env.CorrelationID)
	addHeader(out, HeaderCausationID, env.CausationID)
	addHeader(out, HeaderDedupeKey, env.DedupeKey)
	addHeader(out, HeaderNamespace, env.Namespace)
	addHeader(out, HeaderActorKey, env.To.Key)
	addHeader(out, HeaderPriority, strconv.Itoa(env.Priority))
	return out
}

func addHeader(out map[string]string, key string, value string) {
	if trimmed := strings.TrimSpace(value); trimmed != "" {
		out[key] = trimmed
	}
}
