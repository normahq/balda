package swarm

import (
	"strconv"
	"strings"
)

const (
	SubjectCommandSession  = "balda.v1.cmd.session"
	SubjectCommandTask     = "balda.v1.cmd.task"
	SubjectCommandAgent    = "balda.v1.cmd.agent"
	SubjectCommandMemory   = "balda.v1.cmd.memory"
	SubjectCommandDelivery = "balda.v1.cmd.delivery"
	SubjectCommandAll      = "balda.v1.cmd.>"

	SubjectEventIngressTelegram = "balda.v1.evt.ingress.telegram"
	SubjectEventIngressWebhook  = "balda.v1.evt.ingress.webhook"
	SubjectEventIngressSchedule = "balda.v1.evt.ingress.schedule"
	SubjectEventTask            = "balda.v1.evt.task"
	SubjectEventAgent           = "balda.v1.evt.agent"
	SubjectEventMemory          = "balda.v1.evt.memory"
	SubjectEventDelivery        = "balda.v1.evt.delivery"
	SubjectEventAll             = "balda.v1.evt.>"

	SubjectControlCancel = "balda.v1.ctrl.cancel"
	SubjectControlPause  = "balda.v1.ctrl.pause"
	SubjectControlResume = "balda.v1.ctrl.resume"
	SubjectControlRetry  = "balda.v1.ctrl.retry"
	SubjectControlAll    = "balda.v1.ctrl.>"

	SubjectWakeupMailbox = "balda.v1.wakeup.mailbox"
	SubjectDLQ           = "balda.v1.dlq"
)

const (
	HeaderEnvelopeID    = "Balda-Envelope-ID"
	HeaderSessionID     = "Balda-Session-ID"
	HeaderTaskID        = "Balda-Task-ID"
	HeaderActor         = "Balda-Actor"
	HeaderMailbox       = "Balda-Mailbox"
	HeaderCorrelationID = "Balda-Correlation-ID"
	HeaderCausationID   = "Balda-Causation-ID"
	HeaderDedupeKey     = "Balda-Dedupe-Key"
	HeaderPriority      = "Balda-Priority"
	HeaderNamespace     = "Balda-Namespace"
)

func SubjectForEnvelope(env Envelope) string {
	if strings.TrimSpace(env.Namespace) == NamespaceTaskControl {
		return controlSubjectForKind(env.Kind)
	}
	switch strings.ToLower(strings.TrimSpace(env.To.Target)) {
	case ActorTypeSession:
		return SubjectCommandSession
	case ActorTypeTask:
		return SubjectCommandTask
	case ActorTypeAgent:
		return SubjectCommandAgent
	case ActorTypeMemory:
		return SubjectCommandMemory
	case ActorTypeDelivery:
		return SubjectCommandDelivery
	default:
		return subjectForNamespace(env.Namespace)
	}
}

func EventSubjectForEnvelope(env Envelope) string {
	switch strings.TrimSpace(env.Namespace) {
	case NamespaceHumanInbound:
		return SubjectEventIngressTelegram
	case NamespaceWebhookInbound:
		return SubjectEventIngressWebhook
	case NamespaceScheduleInbound:
		return SubjectEventIngressSchedule
	case NamespaceAgentCommand, NamespaceAgentResult:
		return SubjectEventAgent
	case NamespaceMemorySync:
		return SubjectEventMemory
	case NamespaceTaskControl:
		return SubjectEventTask
	default:
		return SubjectEventTask
	}
}

func EnvelopeHeaders(env Envelope) map[string]string {
	out := make(map[string]string, 10)
	addHeader(out, HeaderEnvelopeID, env.ID)
	addHeader(out, HeaderSessionID, env.SessionID)
	addHeader(out, HeaderTaskID, env.TaskID)
	if actor, err := env.To.String(); err == nil {
		addHeader(out, HeaderActor, actor)
		addHeader(out, HeaderMailbox, actor)
	}
	addHeader(out, HeaderCorrelationID, env.CorrelationID)
	addHeader(out, HeaderCausationID, env.CausationID)
	addHeader(out, HeaderDedupeKey, env.DedupeKey)
	addHeader(out, HeaderPriority, strconv.Itoa(env.Priority))
	addHeader(out, HeaderNamespace, env.Namespace)
	return out
}

func DedupeKeyOrID(env Envelope) string {
	if trimmed := strings.TrimSpace(env.DedupeKey); trimmed != "" {
		return trimmed
	}
	return strings.TrimSpace(env.ID)
}

func subjectForNamespace(namespace string) string {
	switch strings.TrimSpace(namespace) {
	case NamespaceAgentCommand:
		return SubjectCommandAgent
	case NamespaceMemorySync:
		return SubjectCommandMemory
	case NamespaceTaskControl:
		return SubjectControlCancel
	case NamespaceWebhookInbound, NamespaceScheduleInbound:
		return SubjectCommandTask
	case NamespaceHumanInbound:
		return SubjectCommandSession
	default:
		return SubjectCommandTask
	}
}

func controlSubjectForKind(kind string) string {
	switch strings.TrimSpace(kind) {
	case "pause":
		return SubjectControlPause
	case "resume":
		return SubjectControlResume
	case "retry":
		return SubjectControlRetry
	default:
		return SubjectControlCancel
	}
}

func addHeader(out map[string]string, key string, value string) {
	if trimmed := strings.TrimSpace(value); trimmed != "" {
		out[key] = trimmed
	}
}
