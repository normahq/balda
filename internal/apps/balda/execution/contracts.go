package execution

import "github.com/normahq/balda/internal/apps/balda/actorcmd"

const (
	ActorTypeSystem     = actorcmd.ActorTypeSystem
	ActorTypeSession    = actorcmd.ActorTypeSession
	ActorTypeJob        = actorcmd.ActorTypeJob
	ActorTypeGoalkeeper = actorcmd.ActorTypeGoalkeeper
	ActorTypeGoal       = actorcmd.ActorTypeGoal
	ActorTypeDelivery   = actorcmd.ActorTypeDelivery
	ActorTypeMemory     = actorcmd.ActorTypeMemory

	NamespaceHumanInbound      = actorcmd.NamespaceHumanInbound
	NamespaceWebhookInbound    = actorcmd.NamespaceWebhookInbound
	NamespaceScheduleInbound   = actorcmd.NamespaceScheduleInbound
	NamespaceAgentResult       = actorcmd.NamespaceAgentResult
	NamespaceGoalkeeperCommand = actorcmd.NamespaceGoalkeeperCommand
	NamespaceGoalCommand       = actorcmd.NamespaceGoalCommand
	NamespaceJobControl        = actorcmd.NamespaceJobControl
	NamespaceMemoryCommand     = actorcmd.NamespaceMemoryCommand
	NamespaceAutoModeCommand   = actorcmd.NamespaceAutoModeCommand
	NamespaceTelemetry         = actorcmd.NamespaceTelemetry

	KindMessage        = actorcmd.KindMessage
	KindWebhookEvent   = actorcmd.KindWebhookEvent
	KindScheduledJob   = actorcmd.KindScheduledJob
	KindGoal           = actorcmd.KindGoal
	KindCancel         = actorcmd.KindCancel
	KindMemoryRemember = actorcmd.KindMemoryRemember
)
