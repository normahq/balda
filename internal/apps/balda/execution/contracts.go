// Package runtime contains Balda's actor runtime host and transport-facing contracts.
package execution

const (
	ActorTypeSystem     = "system"
	ActorTypeSession    = "session"
	ActorTypeJob        = "job"
	ActorTypeGoalkeeper = "goalkeeper"
	ActorTypeGoal       = ActorTypeGoalkeeper
	ActorTypeDelivery   = "delivery"
	ActorTypeMemory     = "memory"

	NamespaceHumanInbound      = "human.inbound"
	NamespaceWebhookInbound    = "webhook.inbound"
	NamespaceScheduleInbound   = "schedule.inbound"
	NamespaceAgentResult       = "agent.result"
	NamespaceGoalkeeperCommand = "goalkeeper.command"
	NamespaceGoalCommand       = NamespaceGoalkeeperCommand
	NamespaceJobControl        = "job.control"
	NamespaceMemoryCommand     = "memory.command"
	NamespaceTelemetry         = "telemetry"

	KindMessage        = "message"
	KindWebhookEvent   = "webhook_event"
	KindScheduledJob   = "scheduled_job"
	KindGoal           = "goal"
	KindCancel         = "cancel"
	KindMemoryRemember = "memory_remember"
)
