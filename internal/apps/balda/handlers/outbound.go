package handlers

import (
	"context"
	"fmt"

	"github.com/normahq/balda/internal/apps/balda/actors"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
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
		return fmt.Errorf("swarm runtime is unavailable")
	}
	_, err := dispatcher.Dispatch(ctx, env)
	return err
}

func sendPlain(ctx context.Context, dispatcher actortransport.Dispatcher, from actorlayer.ActorAddress, locator baldasession.SessionLocator, text string) error {
	env, err := actors.PlainDeliveryEnvelope("", from, locator, text, "")
	if err != nil {
		return err
	}
	return dispatchOutbound(ctx, dispatcher, env)
}

func sendMarkdown(ctx context.Context, dispatcher actortransport.Dispatcher, from actorlayer.ActorAddress, locator baldasession.SessionLocator, text string) error {
	env, err := actors.MarkdownDeliveryEnvelope("", from, locator, text, "")
	if err != nil {
		return err
	}
	return dispatchOutbound(ctx, dispatcher, env)
}

func sendAgentReply(ctx context.Context, dispatcher actortransport.Dispatcher, from actorlayer.ActorAddress, locator baldasession.SessionLocator, text string) error {
	return sendAgentReplyWithProfile(ctx, dispatcher, from, locator, deliveryfmt.Profile{}, text)
}

func sendAgentReplyWithProfile(ctx context.Context, dispatcher actortransport.Dispatcher, from actorlayer.ActorAddress, locator baldasession.SessionLocator, profile deliveryfmt.Profile, text string) error {
	env, err := actors.AgentReplyDeliveryEnvelopeWithProfile("", from, locator, profile, text, "")
	if err != nil {
		return err
	}
	return dispatchOutbound(ctx, dispatcher, env)
}

func sendDraftPlain(ctx context.Context, dispatcher actortransport.Dispatcher, from actorlayer.ActorAddress, locator baldasession.SessionLocator, draftID int, text string) error {
	env, err := actors.DraftPlainDeliveryEnvelope("", from, locator, draftID, text)
	if err != nil {
		return err
	}
	return dispatchOutbound(ctx, dispatcher, env)
}

func sendTyping(ctx context.Context, dispatcher actortransport.Dispatcher, from actorlayer.ActorAddress, locator baldasession.SessionLocator) error {
	env, err := actors.ChatActionDeliveryEnvelope("", from, locator, "typing")
	if err != nil {
		return err
	}
	return dispatchOutbound(ctx, dispatcher, env)
}
