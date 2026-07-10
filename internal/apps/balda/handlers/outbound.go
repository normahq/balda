package handlers

import (
	"context"
	"fmt"

	"github.com/normahq/balda/internal/apps/balda/actors"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	"github.com/normahq/balda/internal/apps/balda/progress"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/pkg/actorlayer"
	actortransport "github.com/normahq/balda/pkg/actorlayer/transport"
)

var (
	baldaHandlerActorAddress   = actorlayer.ActorAddress{Target: "handler", Key: "balda"}
	commandHandlerActorAddress = actorlayer.ActorAddress{Target: "handler", Key: "command"}
	userHandlerActorAddress    = actorlayer.ActorAddress{Target: "handler", Key: "user"}
	startHandlerActorAddress   = actorlayer.ActorAddress{Target: "handler", Key: "start"}
)

func dispatchOutbound(ctx context.Context, dispatcher actortransport.Dispatcher, env actorlayer.Envelope) error {
	if dispatcher == nil {
		return fmt.Errorf("runtime is unavailable")
	}
	_, err := dispatcher.Dispatch(ctx, env)
	return err
}

func sendPlain(ctx context.Context, dispatcher actortransport.Dispatcher, from actorlayer.ActorAddress, locator baldasession.SessionLocator, text string) error {
	env, err := actors.PlainDeliveryEnvelopeWithSettlement("", from, locator, deliverycmd.SettlementBypass, text, "")
	if err != nil {
		return err
	}
	return dispatchOutbound(ctx, dispatcher, env)
}

func sendMarkdown(ctx context.Context, dispatcher actortransport.Dispatcher, from actorlayer.ActorAddress, locator baldasession.SessionLocator, text string) error {
	env, err := actors.MarkdownDeliveryEnvelopeWithSettlement("", from, locator, deliverycmd.SettlementBypass, text, "")
	if err != nil {
		return err
	}
	return dispatchOutbound(ctx, dispatcher, env)
}

func sendAgentReply(ctx context.Context, dispatcher actortransport.Dispatcher, from actorlayer.ActorAddress, locator baldasession.SessionLocator, text string) error {
	return sendAgentReplyWithProfile(ctx, dispatcher, from, locator, deliveryfmt.Profile{}, text)
}

func sendAgentReplyWithProfile(ctx context.Context, dispatcher actortransport.Dispatcher, from actorlayer.ActorAddress, locator baldasession.SessionLocator, profile deliveryfmt.Profile, text string) error {
	env, err := actors.AgentReplyDeliveryEnvelopeWithProfileAndSettlement("", from, locator, profile, deliverycmd.SettlementBypass, text, "")
	if err != nil {
		return err
	}
	return dispatchOutbound(ctx, dispatcher, env)
}

func sendProgressActivity(ctx context.Context, dispatcher actortransport.Dispatcher, jobID string, from actorlayer.ActorAddress, locator baldasession.SessionLocator, policy deliveryfmt.ProgressPolicy, sequence int, dedupeSuffix string) error {
	env, err := actors.ProgressActivityDeliveryEnvelope(jobID, from, locator, policy, sequence, dedupeSuffix)
	if err != nil {
		return err
	}
	return dispatchOutbound(ctx, dispatcher, env)
}

func sendProgressThinking(ctx context.Context, dispatcher actortransport.Dispatcher, jobID string, from actorlayer.ActorAddress, locator baldasession.SessionLocator, policy deliveryfmt.ProgressPolicy, visible bool, text string, sequence int, dedupeSuffix string) error {
	env, err := actors.ProgressThinkingDeliveryEnvelope(jobID, from, locator, policy, visible, text, sequence, dedupeSuffix)
	if err != nil {
		return err
	}
	return dispatchOutbound(ctx, dispatcher, env)
}

func sendProgressPlanUpdate(ctx context.Context, dispatcher actortransport.Dispatcher, jobID string, from actorlayer.ActorAddress, locator baldasession.SessionLocator, policy deliveryfmt.ProgressPolicy, visible bool, plan *progress.PlanSnapshot, text string, dedupeSuffix string) error {
	env, err := actors.ProgressPlanUpdateDeliveryEnvelope(jobID, from, locator, policy, visible, plan, text, dedupeSuffix)
	if err != nil {
		return err
	}
	return dispatchOutbound(ctx, dispatcher, env)
}
