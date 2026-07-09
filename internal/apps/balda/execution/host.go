package execution

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/normahq/balda/pkg/actorlayer"
	"github.com/normahq/balda/pkg/actorlayer/dispatch"
	actorengine "github.com/normahq/balda/pkg/actorlayer/engine"
	actortransport "github.com/normahq/balda/pkg/actorlayer/transport"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

type DeadLetterRecorder interface {
	DeadLetter(ctx context.Context, jobID string, actor string, messageID string, reason string) error
}

type ActorHost struct {
	source        actorengine.Source
	events        actortransport.EventPublisher
	jobs          DeadLetterRecorder
	engine        *actorengine.DispatchRuntime
	logger        zerolog.Logger
	heartbeatTick time.Duration

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type executionParams struct {
	fx.In

	LC     fx.Lifecycle
	Source actorengine.Source
	Events actortransport.EventPublisher `optional:"true"`
	Jobs   DeadLetterRecorder            `optional:"true"`
	Logger zerolog.Logger
	Actors []dispatch.Actor `group:"balda_product_actors"`
}

func NewActorHost(params executionParams) (*ActorHost, error) {
	if params.Source == nil {
		return nil, fmt.Errorf("actor delivery source is required")
	}
	registry := dispatch.NewMemoryRegistry()
	for _, actor := range params.Actors {
		if err := registry.Register(actor); err != nil {
			return nil, err
		}
	}
	r := &ActorHost{
		source:        params.Source,
		events:        params.Events,
		jobs:          params.Jobs,
		logger:        params.Logger.With().Str("component", "balda.execution.host").Logger(),
		heartbeatTick: heartbeatInterval,
	}
	engine, err := actorengine.NewDispatchRuntime(actorengine.RuntimeConfig{
		Registry:  registry,
		AddressOf: executionAddressOf,
		LaneKey:   actorLaneKeyFromEnvelope,
		Sink:      r,
		Retry: actorengine.RetryPolicy{
			IsRetryable:    actorlayer.IsRetryableError,
			Backoff:        actorlayer.RetryDelay,
			RetryExhausted: retryExhaustedDelivery,
		},
	})
	if err != nil {
		return nil, err
	}
	r.engine = engine
	params.LC.Append(fx.Hook{
		OnStart: r.Start,
		OnStop:  r.Stop,
	})
	return r, nil
}

func (r *ActorHost) Start(context.Context) error {
	if r == nil {
		return nil
	}
	if r.cancel != nil {
		return nil
	}
	runCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	source := executionSource{source: r.source, prepareFn: r.prepareDelivery}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		if err := r.engine.Run(runCtx, source); err != nil && !errors.Is(err, context.Canceled) {
			r.logger.Error().Err(err).Msg("actor delivery source stopped")
		}
	}()
	return nil
}

func (r *ActorHost) Stop(ctx context.Context) error {
	if r.cancel == nil {
		return nil
	}
	r.cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.wg.Wait()
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type executionSource struct {
	source    actorengine.Source
	prepareFn func(context.Context, actorengine.Delivery) (context.Context, func(), actorengine.Delivery)
}

func (s executionSource) Run(ctx context.Context, handler actorengine.Handler) error {
	if s.source == nil {
		return fmt.Errorf("actor delivery source is required")
	}
	if handler == nil {
		return fmt.Errorf("runtime handler is required")
	}
	if s.prepareFn == nil {
		return fmt.Errorf("execution command delivery factory is required")
	}
	return s.source.Run(ctx, func(ctx context.Context, delivery actorengine.Delivery) error {
		if delivery == nil {
			return fmt.Errorf("actor delivery is required")
		}
		executionCtx, stop, prepared := s.prepareFn(ctx, delivery)
		defer stop()
		return handler(executionCtx, prepared)
	})
}
