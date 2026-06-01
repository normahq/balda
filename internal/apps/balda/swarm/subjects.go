package swarm

import (
	"strconv"
	"strings"
)

const (
	SubjectCommandSession    = "balda.v1.cmd.session"
	SubjectCommandTask       = "balda.v1.cmd.task"
	SubjectCommandGoalkeeper = "balda.v1.cmd.goalkeeper"
	SubjectCommandGoal       = SubjectCommandGoalkeeper
	SubjectCommandDelivery   = "balda.v1.cmd.delivery"
	SubjectCommandMemory     = "balda.v1.cmd.memory"
	SubjectCommandControl    = "balda.v1.cmd.control"
	SubjectCommandAll        = "balda.v1.cmd.>"

	SubjectEventCommandAccepted     = "balda.v1.evt.command.accepted"
	SubjectEventCommandRunning      = "balda.v1.evt.command.running"
	SubjectEventCommandInProgress   = "balda.v1.evt.command.in_progress"
	SubjectEventCommandAcked        = "balda.v1.evt.command.acked"
	SubjectEventCommandRetrying     = "balda.v1.evt.command.retrying"
	SubjectEventCommandDeadLettered = "balda.v1.evt.command.deadlettered"
	SubjectEventCommandNoop         = "balda.v1.evt.command.noop"
	SubjectEventCommandDecodeFailed = "balda.v1.evt.command.decode_failed"
	SubjectEventTaskCreated         = "balda.v1.evt.task.created"
	SubjectEventTaskUpdated         = "balda.v1.evt.task.updated"
	SubjectEventTaskCompleted       = "balda.v1.evt.task.completed"
	SubjectEventDeliverySent        = "balda.v1.evt.delivery.sent"
	SubjectEventDeliveryFailed      = "balda.v1.evt.delivery.failed"
	SubjectEventAll                 = "balda.v1.evt.>"

	SubjectDLQCommand = "balda.v1.dlq.command"
	SubjectDLQAll     = "balda.v1.dlq.>"
)

const (
	HeaderEnvelopeID    = "Balda-Envelope-ID"
	HeaderSessionID     = "Balda-Session-ID"
	HeaderTaskID        = "Balda-Task-ID"
	HeaderCorrelationID = "Balda-Correlation-ID"
	HeaderCausationID   = "Balda-Causation-ID"
	HeaderDedupeKey     = "Balda-Dedupe-Key"
	HeaderNamespace     = "Balda-Namespace"
	HeaderActorKey      = "Balda-Actor-Key"
	HeaderPriority      = "Balda-Priority"
)

func SubjectForEnvelope(env Envelope) string {
	if strings.TrimSpace(env.Namespace) == NamespaceTaskControl {
		return SubjectCommandControl
	}
	switch strings.ToLower(strings.TrimSpace(env.To.Target)) {
	case ActorTypeSession:
		return SubjectCommandSession
	case ActorTypeTask:
		return SubjectCommandTask
	case ActorTypeGoalkeeper:
		return SubjectCommandGoal
	case ActorTypeDelivery:
		return SubjectCommandDelivery
	case ActorTypeMemory:
		return SubjectCommandMemory
	default:
		switch strings.TrimSpace(env.Namespace) {
		case NamespaceGoalkeeperCommand:
			return SubjectCommandGoal
		case NamespaceMemorySync:
			return SubjectCommandMemory
		case NamespaceTaskControl:
			return SubjectCommandControl
		case NamespaceWebhookInbound, NamespaceScheduleInbound:
			return SubjectCommandTask
		case NamespaceHumanInbound:
			return SubjectCommandSession
		default:
			return SubjectCommandTask
		}
	}
}

func EnvelopeHeaders(env Envelope) map[string]string {
	out := make(map[string]string, 8)
	addHeader(out, HeaderEnvelopeID, env.ID)
	addHeader(out, HeaderSessionID, env.SessionID)
	addHeader(out, HeaderTaskID, env.TaskID)
	addHeader(out, HeaderCorrelationID, env.CorrelationID)
	addHeader(out, HeaderCausationID, env.CausationID)
	addHeader(out, HeaderDedupeKey, env.DedupeKey)
	addHeader(out, HeaderNamespace, env.Namespace)
	addHeader(out, HeaderActorKey, env.To.Key)
	addHeader(out, HeaderPriority, strconv.Itoa(env.Priority))
	return out
}

func DedupeKeyOrID(env Envelope) string {
	if trimmed := strings.TrimSpace(env.DedupeKey); trimmed != "" {
		return trimmed
	}
	return strings.TrimSpace(env.ID)
}

func addHeader(out map[string]string, key string, value string) {
	if trimmed := strings.TrimSpace(value); trimmed != "" {
		out[key] = trimmed
	}
}
