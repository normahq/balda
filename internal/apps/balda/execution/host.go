package execution

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/baldaworks/go-actorlayer"
	"github.com/baldaworks/go-actorlayer/dispatch"
	actorengine "github.com/baldaworks/go-actorlayer/engine"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
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

	cancel  context.CancelFunc
	wg      sync.WaitGroup
	stateMu sync.Mutex
	runErr  error
}

type executionParams struct {
	fx.In

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
	return r, nil
}

func (r *ActorHost) Start(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	if r.cancel != nil {
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	source := executionSource{source: r.source, prepareFn: r.prepareDelivery}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		if err := r.engine.Run(runCtx, source); err != nil && !errors.Is(err, context.Canceled) {
			r.stateMu.Lock()
			r.runErr = err
			r.stateMu.Unlock()
			r.logger.Error().Err(err).Msg("actor delivery source stopped")
		}
	}()
	return nil
}

func (r *ActorHost) Stop(ctx context.Context) error {
	r.stateMu.Lock()
	cancel := r.cancel
	r.stateMu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
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

// Ready reports whether the actor runtime is started and healthy.
func (r *ActorHost) Ready() error {
	if r == nil {
		return errors.New("actor host is nil")
	}
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	if r.runErr != nil {
		return fmt.Errorf("actor host stopped: %w", r.runErr)
	}
	if r.cancel == nil {
		return errors.New("actor host is not started")
	}
	return nil
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
