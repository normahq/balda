package actors

import (
	"github.com/normahq/balda/internal/apps/balda/swarm"
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
			func(params goalActorParams) swarm.Actor {
				return &goalActor{
					tasks:          params.TaskService,
					dispatcher:     params.Dispatcher,
					sessions:       params.SessionManager,
					runtimeBuilder: params.RuntimeManager,
					taskRuns:       params.TaskRuns,
					maxIters:       normalizeGoalMaxIterations(params.MaxIters),
					logger:         params.Logger.With().Str("component", "balda.goal_actor").Logger(),
				}
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
