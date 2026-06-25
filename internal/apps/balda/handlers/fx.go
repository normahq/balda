package handlers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/normahq/balda/internal/apps/balda/actors"
	"github.com/normahq/balda/internal/apps/balda/agent"
	baldachannel "github.com/normahq/balda/internal/apps/balda/channel"
	baldaslack "github.com/normahq/balda/internal/apps/balda/channel/slack"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	baldazulip "github.com/normahq/balda/internal/apps/balda/channel/zulip"
	"github.com/normahq/balda/internal/apps/balda/messenger"
	"github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
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
		baldazulip.NewAdapter,
		baldaslack.NewAdapter,
		NewBaldaSessionTurnRunner,
		func(tg *baldatelegram.Adapter, zu *baldazulip.Adapter, sl *baldaslack.Adapter) *baldachannel.Router {
			return baldachannel.NewRouter(map[string]baldachannel.ChannelAdapter{
				baldastate.ChannelTypeTelegram: tg,
				baldastate.ChannelTypeZulip:    zu,
				baldastate.ChannelTypeSlack:    sl,
			})
		},
		NewZulipBaldaHandler,
		NewSlackHandler,
		func(params scheduledTaskSchedulerParams) (*ScheduledTaskScheduler, error) {
			if params.StateProvider == nil {
				return nil, fmt.Errorf("balda state provider is required")
			}
			if params.Dispatcher == nil {
				return nil, fmt.Errorf("balda actor dispatcher is required for scheduler")
			}
			config, err := normalizeScheduledTaskSchedulerConfig(params.Config)
			if err != nil {
				return nil, err
			}
			if len(config.Tasks) > 0 && params.OwnerStore == nil {
				return nil, fmt.Errorf("balda owner store is required for scheduler tasks")
			}

			scheduler := &ScheduledTaskScheduler{
				taskStore:    params.StateProvider.ScheduledTasks(),
				dispatcher:   params.Dispatcher,
				owner:        params.OwnerStore,
				logger:       params.Logger.With().Str("component", "balda.scheduled_task_scheduler").Logger(),
				config:       config,
				pollInterval: defaultSchedulerPollInterval,
				dueBatchSize: defaultSchedulerDueBatchSize,
				now:          time.Now,
			}

			params.LC.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					if err := scheduler.reconcileConfiguredTasks(ctx); err != nil {
						return err
					}
					scheduler.start()
					return nil
				},
				OnStop: func(ctx context.Context) error {
					return scheduler.stop(ctx)
				},
			})

			return scheduler, nil
		},
		func(params inboundWebhookParams) (*InboundWebhookReceiver, error) {
			normalized, err := normalizeInboundWebhookConfig(params.Config)
			if err != nil {
				return nil, err
			}

			receiver := &InboundWebhookReceiver{
				enabled:    normalized.Enabled,
				listenAddr: normalized.ListenAddr,
				routes:     normalized.Routes,
				balda:      params.Balda,
				owner:      params.OwnerStore,
				logger:     params.Logger.With().Str("component", "balda.inbound_webhook").Logger(),
			}

			if !receiver.enabled {
				return receiver, nil
			}
			if receiver.balda == nil {
				return nil, fmt.Errorf("balda handler is required for inbound webhooks")
			}
			if receiver.owner == nil {
				return nil, fmt.Errorf("balda owner store is required for inbound webhooks")
			}

			params.LC.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					return receiver.start(ctx)
				},
				OnStop: func(ctx context.Context) error {
					return receiver.stop(ctx)
				},
			})

			return receiver, nil
		},
		func(params startHandlerParams) *StartHandler {
			return &StartHandler{
				ownerStore:        params.OwnerStore,
				inviteStore:       params.InviteStore,
				collaboratorStore: params.CollaboratorStore,
				actorDispatcher:   params.ActorDispatcher,
				authToken:         params.AuthToken,
			}
		},
		func(deps baldaHandlerDeps) (*BaldaHandler, error) {
			h := &BaldaHandler{
				ownerStore:         deps.OwnerStore,
				collaboratorStore:  deps.CollaboratorStore,
				channel:            deps.Channel,
				sessionManager:     deps.SessionManager,
				turnDispatcher:     deps.TurnDispatcher,
				actorDispatcher:    deps.ActorDispatcher,
				taskService:        deps.TaskService,
				messenger:          deps.Messenger,
				tgClient:           deps.TGClient,
				authToken:          strings.TrimSpace(deps.AuthToken),
				baldaProviderName:  strings.TrimSpace(deps.BaldaProviderID),
				planUpdatesEnabled: deps.PlanUpdatesEnabled,
				telegramEnabled:    deps.TelegramEnabled,
				telegramConfigured: true,
				logger:             deps.Logger.With().Str("component", "balda.handler").Logger(),
			}

			deps.LC.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					return h.onStart(ctx)
				},
			})

			return h, nil
		},
		fx.Annotate(
			func(r *BaldaSessionTurnRunner) actors.SessionTurnRunner {
				return r
			},
		),
		fx.Annotate(
			func(s *ScheduledTaskScheduler) actors.ScheduledTaskRecorder { return s },
		),
		func(params commandHandlerParams) *CommandHandler {
			return &CommandHandler{
				ownerStore:        params.OwnerStore,
				collaboratorStore: params.CollaboratorStore,
				channel:           params.Channel,
				sessionManager:    params.SessionManager,
				workCanceller:     params.WorkCanceller,
				actorDispatcher:   params.ActorDispatcher,
				taskService:       params.TaskService,
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
				actorDispatcher:   params.ActorDispatcher,
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
			start.baldaHandler = balda
		},
		func(*ScheduledTaskScheduler) {},
		func(*InboundWebhookReceiver) {},
		func(*ZulipBaldaHandler) {},
		func(*SlackHandler) {},
	),
)
