package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

type taskDeliveryActor struct {
	channel *baldatelegram.Adapter
	tasks   *swarm.TaskService
	logger  zerolog.Logger
}

type taskDeliveryActorParams struct {
	fx.In

	Channel     *baldatelegram.Adapter
	TaskService *swarm.TaskService
	Logger      zerolog.Logger
}

func newTaskDeliveryActor(params taskDeliveryActorParams) swarm.Actor {
	return &taskDeliveryActor{
		channel: params.Channel,
		tasks:   params.TaskService,
		logger:  params.Logger.With().Str("component", "balda.task_delivery_actor").Logger(),
	}
}

func (a *taskDeliveryActor) Address() string {
	return swarm.WildcardAddress(swarm.ActorTypeDelivery)
}

func (a *taskDeliveryActor) Handle(ctx context.Context, env swarm.Envelope) error {
	if strings.TrimSpace(env.Kind) != taskPayloadKindDelivery {
		return swarm.PolicyError(fmt.Errorf("unsupported delivery kind %q", env.Kind))
	}
	var payload taskDeliveryPayload
	if err := json.Unmarshal([]byte(env.PayloadJSON), &payload); err != nil {
		return swarm.PermanentError(fmt.Errorf("decode task delivery payload: %w", err))
	}
	text := strings.TrimSpace(payload.Text)
	if text == "" {
		return nil
	}
	if a.channel == nil {
		return swarm.TransientError(fmt.Errorf("telegram channel adapter is required"))
	}
	if err := a.channel.SendAgentReply(ctx, payload.Locator, text); err != nil {
		return swarm.TransientError(err)
	}
	if a.tasks != nil && strings.TrimSpace(payload.TaskID) != "" {
		if err := a.tasks.AppendEvent(ctx, payload.TaskID, swarm.TaskEventDeliverySent, "delivery.actor", env.ID, map[string]any{
			"text": text,
		}); err != nil {
			a.logger.Warn().Err(err).Str("task_id", payload.TaskID).Msg("failed to record task delivery event")
		}
	}
	return nil
}
