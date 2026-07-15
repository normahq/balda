package questions

import (
	"fmt"

	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

var Module = fx.Module("balda_questions",
	fx.Provide(
		func(params struct {
			fx.In

			Store     baldastate.QuestionStore
			Scheduled baldastate.ScheduledJobStore
			Controls  ControlPublisher `optional:"true"`
			Logger    zerolog.Logger
		}) (*Service, error) {
			if params.Store == nil {
				return nil, fmt.Errorf("question store is required")
			}
			service := New(params.Store, params.Scheduled, params.Logger.With().Str("component", "balda.questions").Logger())
			service.SetControlPublisher(params.Controls)
			return service, nil
		},
		NewDeliveryBindingProjector,
	),
)
