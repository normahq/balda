package handlers

import (
	"github.com/normahq/balda/internal/apps/balda/agent"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	"github.com/normahq/balda/internal/apps/balda/messenger"
	"github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/normahq/balda/internal/apps/balda/tgbotkit"
	"github.com/rs/zerolog"
	"github.com/tgbotkit/client"
	"go.uber.org/fx"
)

// Module provides handlers for the balda bot.
var Module = fx.Module("balda_handlers",
	fx.Provide(
		agent.NewBuilder,
		agent.NewRuntimeManager,
		session.NewManager,
		fx.Annotate(
			func(
				tgClient client.ClientWithResponsesInterface,
				logger zerolog.Logger,
				formattingMode string,
			) *messenger.Messenger {
				m := messenger.NewMessenger(tgClient, logger)
				m.SetAgentReplyFormattingMode(formattingMode)
				return m
			},
			fx.ParamTags(``, ``, `name:"balda_telegram_formatting_mode"`),
		),
		baldatelegram.NewAdapter,
		NewTurnDispatcher,
		newTaskRunRegistry,
		NewScheduledTaskScheduler,
		NewInboundWebhookReceiver,
		NewStartHandler,
		NewBaldaHandler,
		NewCommandHandler,
		NewUserHandler,
		fx.Annotate(
			newSessionActorExecutor,
			fx.As(new(swarm.Actor)),
			fx.ResultTags(`group:"balda_swarm_actors"`),
		),
		fx.Annotate(
			newTaskActorExecutor,
			fx.As(new(swarm.Actor)),
			fx.ResultTags(`group:"balda_swarm_actors"`),
		),
		fx.Annotate(
			newTaskAgentActor,
			fx.As(new(swarm.Actor)),
			fx.ResultTags(`group:"balda_swarm_actors"`),
		),
		fx.Annotate(
			newTaskDeliveryActor,
			fx.As(new(swarm.Actor)),
			fx.ResultTags(`group:"balda_swarm_actors"`),
		),
		fx.Annotate(
			newTaskControlActor,
			fx.As(new(swarm.Actor)),
			fx.ResultTags(`group:"balda_swarm_actors"`),
		),
		fx.Annotate(
			registerStartHandler,
			fx.As(new(tgbotkit.Handler)),
			fx.ResultTags(`group:"bot_handlers"`),
		),
		fx.Annotate(
			registerBaldaHandler,
			fx.As(new(tgbotkit.Handler)),
			fx.ResultTags(`group:"bot_handlers"`),
		),
		fx.Annotate(
			registerCommandHandler,
			fx.As(new(tgbotkit.Handler)),
			fx.ResultTags(`group:"bot_handlers"`),
		),
		fx.Annotate(
			registerUserHandler,
			fx.As(new(tgbotkit.Handler)),
			fx.ResultTags(`group:"bot_handlers"`),
		),
	),
	fx.Invoke(
		WireHandlers,
		func(*ScheduledTaskScheduler) {},
		func(*InboundWebhookReceiver) {},
	),
)

// WireHandlers connects the start handler to the balda handler.
func WireHandlers(start *StartHandler, balda *BaldaHandler) {
	start.SetBaldaHandler(balda)
}

func registerStartHandler(h *StartHandler) tgbotkit.Handler {
	return h
}

func registerBaldaHandler(h *BaldaHandler) tgbotkit.Handler {
	return h
}

func registerCommandHandler(h *CommandHandler) tgbotkit.Handler {
	return h
}

func registerUserHandler(h *userHandler) tgbotkit.Handler {
	return h
}
