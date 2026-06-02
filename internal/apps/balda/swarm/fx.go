package swarm

import "go.uber.org/fx"

var Module = fx.Module("balda_swarm",
	fx.Provide(
		NewTaskService,
		NewEventProjector,
		NewRuntime,
	),
	fx.Invoke(func(*EventProjector) {}),
	fx.Invoke(func(*Runtime) {}),
)
