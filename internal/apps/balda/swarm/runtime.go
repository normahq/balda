package swarm

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	dispatch "github.com/normahq/norma/actorlayer/dispatch"
	actorengine "github.com/normahq/norma/actorlayer/engine"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

const heartbeatInterval = 30 * time.Second

const (
	retryBaseDelay = time.Second
	retryMaxDelay  = time.Minute
)

type Actor interface {
	Address() string
	Handle(ctx context.Context, env Envelope) error
}

type dispatchActor struct {
	actor   Actor
	address string
}

func (a dispatchActor) Address() string { return a.address }
func (a dispatchActor) Handle(ctx context.Context, envelope any) error {
	typed, ok := envelope.(Envelope)
	if !ok {
		return DecodeError(fmt.Errorf("unexpected actor envelope type %T", envelope))
	}
	return a.actor.Handle(ctx, typed)
}

func registerActors(actors []Actor) (dispatch.Registry, error) {
	registry := dispatch.NewMemoryRegistry()
	for _, actor := range actors {
		if actor == nil {
			continue
		}
		if err := registry.Register(dispatchActor{actor: actor, address: actor.Address()}); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

type Runtime struct {
	bus     RuntimeBus
	tasks   *TaskService
	engine  *actorengine.DispatchRuntime
	logger  zerolog.Logger
	enabled bool
	// heartbeatTick controls the in-progress ack cadence for long-running commands.
	// Zero falls back to the package default.
	heartbeatTick time.Duration

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// RuntimeLaneStatus summarizes currently active actor lanes.
type RuntimeLaneStatus struct {
	Active int
	Keys   []string
}

type runtimeParams struct {
	fx.In

	LC     fx.Lifecycle
	Bus    RuntimeBus
	Config Config
	Tasks  *TaskService
	Logger zerolog.Logger
	Actors []Actor `group:"balda_swarm_actors"`
}

func NewRuntime(params runtimeParams) (*Runtime, error) {
	if params.Bus == nil {
		return nil, fmt.Errorf("jetstream command bus is required")
	}
	registry, err := registerActors(params.Actors)
	if err != nil {
		return nil, err
	}
	r := &Runtime{
		bus:           params.Bus,
		tasks:         params.Tasks,
		logger:        params.Logger.With().Str("component", "balda.swarm.runtime").Logger(),
		enabled:       params.Config.Enabled,
		heartbeatTick: heartbeatInterval,
	}
	engine, err := actorengine.NewDispatchRuntime(actorengine.RuntimeConfig{
		Registry:  registry,
		AddressOf: runtimeAddressOf,
		LaneKey:   actorLaneKeyFromEnvelope,
		Retry: actorengine.RetryPolicy{
			IsRetryable: isRetryableRuntimeError,
			Backoff:     nextRetryDelay,
			RetryExhausted: func(delivery actorengine.Delivery) bool {
				wrapped, ok := delivery.(*runtimeDelivery)
				if !ok {
					return false
				}
				return retryExhaustedCommand(wrapped.cmd)
			},
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

func (r *Runtime) Start(context.Context) error {
	if r == nil || !r.enabled {
		return nil
	}
	if r.cancel != nil {
		return nil
	}
	runCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		if err := r.bus.RunCommandConsumer(runCtx, r.HandleCommand); err != nil && !errors.Is(err, context.Canceled) {
			r.logger.Error().Err(err).Msg("jetstream command consumer stopped")
		}
	}()
	return nil
}

func (r *Runtime) Stop(ctx context.Context) error {
	if r.cancel == nil {
		return nil
	}
	r.cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.wg.Wait()
	}()
	var stopErr error
	select {
	case <-done:
		stopErr = nil
	case <-ctx.Done():
		stopErr = ctx.Err()
	}
	return stopErr
}

func (r *Runtime) HandleCommand(ctx context.Context, cmd CommandMessage) error {
	env := cmd.Envelope()
	heartbeatCtx, stop := r.startHeartbeat(ctx, cmd, env)
	defer stop()
	if r.engine == nil {
		return nil
	}
	delivery := &runtimeDelivery{
		cmd: cmd,
		onDeadLetter: func(reason string) {
			r.deadletterTask(ctx, env, reason)
		},
	}
	return r.engine.Handle(heartbeatCtx, delivery)
}

func (r *Runtime) LaneStatus() RuntimeLaneStatus {
	if r == nil || r.engine == nil {
		return RuntimeLaneStatus{}
	}
	status := r.engine.LaneStatus()
	return RuntimeLaneStatus{
		Active: status.Active,
		Keys:   status.Keys,
	}
}

func (r *Runtime) startHeartbeat(ctx context.Context, cmd CommandMessage, env Envelope) (context.Context, func()) {
	child, cancel := context.WithCancel(ctx)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(r.heartbeatTickInterval())
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := cmd.InProgress(child); err != nil {
					r.logger.Warn().Err(err).Str("envelope_id", env.ID).Msg("failed to send jetstream in-progress ack")
				}
				_ = r.bus.PublishEvent(child, SubjectEventCommandInProgress, commandNoopEvent(env))
			case <-child.Done():
				return
			}
		}
	}()
	return child, cancel
}

func (r *Runtime) heartbeatTickInterval() time.Duration {
	if r == nil || r.heartbeatTick <= 0 {
		return heartbeatInterval
	}
	return r.heartbeatTick
}

func (r *Runtime) deadletterTask(ctx context.Context, env Envelope, reason string) {
	if r == nil || r.tasks == nil {
		return
	}
	taskID := strings.TrimSpace(env.TaskID)
	if taskID == "" {
		return
	}
	if err := r.tasks.DeadLetter(ctx, taskID, "swarm.runtime", env.ID, reason); err != nil {
		r.logger.Warn().Err(err).Str("task_id", taskID).Msg("failed to mark swarm task deadlettered")
	}
}

func isRetryableRuntimeError(err error) bool {
	if err != nil {
		if errors.Is(err, actorengine.ErrActorNotFound) {
			return false
		}
	}
	switch ClassifyError(err) {
	case ErrorKindDuplicate, ErrorKindAuth, ErrorKindPolicy, ErrorKindPermanent, ErrorKindDecode, ErrorKindCanceled:
		return false
	default:
		return true
	}
}

func retryExhaustedCommand(cmd CommandMessage) bool {
	if cmd == nil {
		return false
	}
	maxDeliveries := cmd.MaxDeliveries()
	return maxDeliveries > 0 && cmd.DeliveryAttempt() >= maxDeliveries
}

func nextRetryDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	delay := retryBaseDelay
	for range attempt {
		delay *= 2
		if delay >= retryMaxDelay {
			delay = retryMaxDelay
			break
		}
	}
	jitterCap := max(delay/4, time.Millisecond)
	jitter := time.Duration(rand.Int64N(int64(jitterCap)))
	return delay + jitter
}

func commandNoopEvent(env Envelope) Envelope {
	out := env
	out.ID = strings.TrimSpace(env.ID) + ":event:in_progress"
	out.Namespace = NamespaceTelemetry
	out.Kind = "command_event"
	out.DedupeKey = out.ID
	if strings.TrimSpace(out.PayloadJSON) == "" {
		out.PayloadJSON = `{"ok":true}`
	}
	return out
}
