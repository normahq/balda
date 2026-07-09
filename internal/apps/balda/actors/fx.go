package actors

import (
	"github.com/normahq/balda/internal/apps/balda/actors/goalkeeper"
	baldaagent "github.com/normahq/balda/internal/apps/balda/agent"
	baldajobs "github.com/normahq/balda/internal/apps/balda/jobs"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/pkg/actorlayer/dispatch"
	actortransport "github.com/normahq/balda/pkg/actorlayer/transport"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

var Module = fx.Module("balda_actors",
	fx.Provide(
		NewTurnDispatcher,
		NewJobRunRegistry,
		NewSessionWorkCanceller,
		fx.Annotate(
			func(params sessionActorExecutorParams) dispatch.Actor {
				return &sessionActorExecutor{turns: params.Turns, runner: params.Runner, tasks: params.Tasks, scheduler: params.Scheduler}
			},
			fx.As(new(dispatch.Actor)),
			fx.ResultTags(`group:"balda_product_actors"`),
		),
		fx.Annotate(
			func(params jobActorExecutorParams) dispatch.Actor {
				return &jobActorExecutor{
					tasks:      params.JobService,
					dispatcher: params.Dispatcher,
					sessions:   params.Sessions,
				}
			},
			fx.As(new(dispatch.Actor)),
			fx.ResultTags(`group:"balda_product_actors"`),
		),
		fx.Annotate(
			func(params struct {
				fx.In

				JobService     *baldajobs.JobService
				Dispatcher     actortransport.Dispatcher
				SessionManager *baldasession.Manager
				RuntimeManager *baldaagent.RuntimeManager
				JobRuns        *JobRunRegistry
				MaxIterations  int `name:"balda_goal_max_iterations"`
				Logger         zerolog.Logger
			}) dispatch.Actor {
				return goalkeeper.NewActor(goalkeeper.ActorParams{
					JobService:      params.JobService,
					Dispatcher:      params.Dispatcher,
					SessionManager:  params.SessionManager,
					GoalRunPreparer: goalRunPreparerAdapter{manager: params.RuntimeManager},
					JobRuns:         params.JobRuns,
					MaxIterations:   params.MaxIterations,
					Logger:          params.Logger,
				})
			},
			fx.As(new(dispatch.Actor)),
			fx.ResultTags(`group:"balda_product_actors"`),
		),
		fx.Annotate(
			func(params memoryActorExecutorParams) dispatch.Actor {
				return &memoryActorExecutor{
					store:  params.Store,
					events: params.Events,
				}
			},
			fx.As(new(dispatch.Actor)),
			fx.ResultTags(`group:"balda_product_actors"`),
		),
		fx.Annotate(
			func(params jobDeliveryActorParams) dispatch.Actor {
				return &jobDeliveryActor{
					channel: params.Channel,
					tasks:   params.JobService,
					logger:  params.Logger.With().Str("component", "balda.job_delivery_actor").Logger(),
				}
			},
			fx.As(new(dispatch.Actor)),
			fx.ResultTags(`group:"balda_product_actors"`),
		),
		fx.Annotate(
			func(params jobControlActorParams) dispatch.Actor {
				return &jobControlActor{
					turnDispatcher: params.TurnDispatcher,
					dispatcher:     params.Dispatcher,
					tasks:          params.JobService,
					taskRuns:       params.JobRuns,
					logger:         params.Logger.With().Str("component", "balda.job_control_actor").Logger(),
				}
			},
			fx.As(new(dispatch.Actor)),
			fx.ResultTags(`group:"balda_product_actors"`),
		),
	),
)
