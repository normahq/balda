package actors

import (
	"github.com/normahq/balda/internal/apps/balda/actors/goalkeeper"
	baldaagent "github.com/normahq/balda/internal/apps/balda/agent"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

var Module = fx.Module("balda_actors",
	fx.Provide(
		NewTurnDispatcher,
		NewTaskRunRegistry,
		fx.Annotate(
			func(params sessionActorExecutorParams) swarm.Actor {
				return &sessionActorExecutor{turns: params.Turns, runner: params.Runner, tasks: params.Tasks, scheduler: params.Scheduler}
			},
			fx.As(new(swarm.Actor)),
			fx.ResultTags(`group:"balda_swarm_actors"`),
		),
		fx.Annotate(
			func(params taskActorExecutorParams) swarm.Actor {
				return &taskActorExecutor{
					tasks:      params.TaskService,
					dispatcher: params.Dispatcher,
					sessions:   params.Sessions,
				}
			},
			fx.As(new(swarm.Actor)),
			fx.ResultTags(`group:"balda_swarm_actors"`),
		),
		fx.Annotate(
			func(params struct {
				fx.In

				TaskService        *swarm.TaskService
				Dispatcher         swarm.ActorDispatcher
				SessionManager     *baldasession.Manager
				RuntimeManager     *baldaagent.RuntimeManager
				TaskRuns           *TaskRunRegistry
				MaxIterations      int  `name:"balda_goal_max_iterations"`
				PlanUpdatesEnabled bool `name:"balda_telegram_plan_updates"`
				Logger             zerolog.Logger
			}) swarm.Actor {
				return goalkeeper.NewActor(goalkeeper.ActorParams{
					TaskService:        params.TaskService,
					Dispatcher:         params.Dispatcher,
					SessionManager:     params.SessionManager,
					RuntimeBuilder:     goalRuntimeBuilderAdapter{manager: params.RuntimeManager},
					TaskRuns:           params.TaskRuns,
					MaxIterations:      params.MaxIterations,
					PlanUpdatesEnabled: params.PlanUpdatesEnabled,
					Logger:             params.Logger,
				})
			},
			fx.As(new(swarm.Actor)),
			fx.ResultTags(`group:"balda_swarm_actors"`),
		),
		fx.Annotate(
			func(params taskDeliveryActorParams) swarm.Actor {
				return &taskDeliveryActor{
					channel: params.Channel,
					tasks:   params.TaskService,
					logger:  params.Logger.With().Str("component", "balda.task_delivery_actor").Logger(),
				}
			},
			fx.As(new(swarm.Actor)),
			fx.ResultTags(`group:"balda_swarm_actors"`),
		),
		fx.Annotate(
			func(params taskControlActorParams) swarm.Actor {
				return &taskControlActor{
					turnDispatcher: params.TurnDispatcher,
					tasks:          params.TaskService,
					taskRuns:       params.TaskRuns,
					channel:        params.Channel,
					logger:         params.Logger.With().Str("component", "balda.task_control_actor").Logger(),
				}
			},
			fx.As(new(swarm.Actor)),
			fx.ResultTags(`group:"balda_swarm_actors"`),
		),
	),
)
