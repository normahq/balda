package handlers

import (
	"context"

	"github.com/normahq/balda/internal/apps/balda/actors"
	"github.com/normahq/balda/internal/apps/balda/agent"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	"github.com/normahq/balda/internal/apps/balda/messenger"
	"github.com/normahq/balda/internal/apps/balda/session"
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
		newScheduledTaskScheduler,
		newInboundWebhookReceiver,
		func(params startHandlerParams) *StartHandler {
			return &StartHandler{
				ownerStore:        params.OwnerStore,
				inviteStore:       params.InviteStore,
				collaboratorStore: params.CollaboratorStore,
				messenger:         params.Messenger,
				authToken:         params.AuthToken,
			}
		},
		newBaldaHandler,
		provideSessionTurnRunner,
		provideScheduledTaskRecorder,
		func(params commandHandlerParams) *CommandHandler {
			return &CommandHandler{
				ownerStore:        params.OwnerStore,
				collaboratorStore: params.CollaboratorStore,
				channel:           params.Channel,
				sessionManager:    params.SessionManager,
				actorDispatcher:   params.ActorDispatcher,
				goalMaxIterations: normalizeGoalMaxIterations(params.MaxIterations),
				userHandler:       params.UserHandler,
			}
		},
		func(params userHandlerParams) *userHandler {
			return &userHandler{
				ownerStore:        params.OwnerStore,
				inviteStore:       params.InviteStore,
				collaboratorStore: params.CollaboratorStore,
				channel:           params.Channel,
				tgClient:          params.TGClient,
			}
		},
		fx.Annotate(
			func(h *StartHandler) tgbotkit.Handler { return h },
			fx.As(new(tgbotkit.Handler)),
			fx.ResultTags(`group:"bot_handlers"`),
		),
		fx.Annotate(
			func(h *BaldaHandler) tgbotkit.Handler { return h },
			fx.As(new(tgbotkit.Handler)),
			fx.ResultTags(`group:"bot_handlers"`),
		),
		fx.Annotate(
			func(h *CommandHandler) tgbotkit.Handler { return h },
			fx.As(new(tgbotkit.Handler)),
			fx.ResultTags(`group:"bot_handlers"`),
		),
	),
	fx.Invoke(
		func(start *StartHandler, balda *BaldaHandler) {
			start.setBaldaHandler(balda)
		},
		func(*ScheduledTaskScheduler) {},
		func(*InboundWebhookReceiver) {},
	),
)

type sessionTurnRunnerAdapter struct {
	handler *BaldaHandler
}

func provideSessionTurnRunner(h *BaldaHandler) actors.SessionTurnRunner {
	return sessionTurnRunnerAdapter{handler: h}
}

func (a sessionTurnRunnerAdapter) RunSessionTurnPayload(ctx context.Context, payload actors.SessionTurnPayload) error {
	return a.handler.runSessionTurnPayload(ctx, payload)
}

type scheduledTaskRecorderAdapter struct {
	scheduler *ScheduledTaskScheduler
}

func provideScheduledTaskRecorder(s *ScheduledTaskScheduler) actors.ScheduledTaskRecorder {
	return scheduledTaskRecorderAdapter{scheduler: s}
}

func (a scheduledTaskRecorderAdapter) MarkSuccess(ctx context.Context, taskID string) error {
	return a.scheduler.markSuccess(ctx, taskID)
}

func (a scheduledTaskRecorderAdapter) RecordExecutionFailure(ctx context.Context, taskID string, cause error) error {
	return a.scheduler.recordExecutionFailure(ctx, taskID, cause)
}
