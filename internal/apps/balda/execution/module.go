package execution

import "go.uber.org/fx"

var Module = fx.Module("balda_runtime",
	fx.Provide(
		NewActorHost,
	),
	fx.Invoke(func(*ActorHost) {}),
)
