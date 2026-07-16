// Package actorcmd defines Balda's stable actor command and event wire taxonomy.
package actorcmd

const (
	ActorTypeSystem     = "system"
	ActorTypeSession    = "session"
	ActorTypeJob        = "job"
	ActorTypeGoalkeeper = "goalkeeper"
	ActorTypeGoal       = ActorTypeGoalkeeper
	ActorTypeDelivery   = "delivery"
	ActorTypeMemory     = "memory"
	ActorTypeQuestion   = "question"
	ActorTypePermission = "permission"

	NamespaceHumanInbound      = "human.inbound"
	NamespaceWebhookInbound    = "webhook.inbound"
	NamespaceScheduleInbound   = "schedule.inbound"
	NamespaceAgentResult       = "agent.result"
	NamespaceGoalkeeperCommand = "goalkeeper.command"
	NamespaceGoalCommand       = NamespaceGoalkeeperCommand
	NamespaceJobControl        = "job.control"
	NamespaceMemoryCommand     = "memory.command"
	NamespaceQuestionCommand   = "question.command"
	NamespacePermissionCommand = "permission.command"
	NamespaceAutoModeCommand   = "auto_mode.command"
	NamespaceTelemetry         = "telemetry"

	KindMessage          = "message"
	KindWebhookEvent     = "webhook_event"
	KindScheduledJob     = "scheduled_job"
	KindGoal             = "goal"
	KindCancel           = "cancel"
	KindMemoryRemember   = "memory_remember"
	KindQuestionAnswered = "question_answered"
	KindQuestionTimedOut = "question_timed_out"
	KindQuestionFailed   = "question_failed"

	QueueModeInterrupt = "interrupt"
)
