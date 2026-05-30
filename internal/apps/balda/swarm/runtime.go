package swarm

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	dispatch "github.com/normahq/norma/pkg/actorlayer/dispatch"
	actorengine "github.com/normahq/norma/pkg/actorlayer/engine"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

const heartbeatInterval = 30 * time.Second

type Actor = dispatch.Actor

// runtimeActorRegistry is the local in-memory actor registry abstraction used by the runtime.
type runtimeActorRegistry interface {
	Register(actor Actor) error
	Resolve(address string) (Actor, bool)
}

// runtimeRuntime is the local runtime execution boundary used by Balda.
type runtimeRuntime interface {
	Run(runtimeCtx context.Context, source actorengine.Source, route runtimeRoute) error
	Handle(handleCtx context.Context, delivery actorengine.Delivery, route runtimeRoute) error
	EmitInProgress(eventCtx context.Context, delivery actorengine.Delivery)
	LaneStatus() actorengine.LaneStatus
}

// runtimeRoute is the execution callback contract used by the in-memory runtime.
type runtimeRoute = actorengine.Handler

func registerActors(actors []Actor) (runtimeActorRegistry, error) {
	registry := dispatch.NewMemoryRegistry()
	for _, actor := range actors {
		if err := registry.Register(actor); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

type Runtime struct {
	bus     RuntimeBus
	actors  runtimeActorRegistry
	tasks   *TaskService
	engine  runtimeRuntime
	logger  zerolog.Logger
	enabled bool
	// heartbeatTick controls the in-progress ack cadence for long-running commands.
	// Zero falls back to the package default.
	heartbeatTick time.Duration

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// RuntimeLaneStatus summarizes currently active actor lanes.
type RuntimeLaneStatus = actorengine.LaneStatus

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
		actors:        registry,
		tasks:         params.Tasks,
		logger:        params.Logger.With().Str("component", "balda.swarm.runtime").Logger(),
		enabled:       params.Config.Enabled,
		heartbeatTick: heartbeatInterval,
	}
	engine, err := actorengine.New(actorengine.Config{
		Resolver: runtimeResolver{},
		Sink:     r,
		Retry: actorengine.RetryPolicy{
			IsRetryable: IsRetryableError,
			Backoff:     RetryDelay,
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
	source := runtimeSource{
		bus: r.bus,
		prepareFn: func(ctx context.Context, cmd CommandMessage) (context.Context, func(), actorengine.Delivery) {
			return r.prepareCommandDelivery(ctx, cmd)
		},
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		if err := r.engine.Run(runCtx, source, r.route); err != nil && !errors.Is(err, context.Canceled) {
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

func (r *Runtime) handleCommand(ctx context.Context, cmd CommandMessage) error {
	executionCtx, stop, delivery := r.prepareCommandDelivery(ctx, cmd)
	defer stop()
	if r.engine == nil {
		return nil
	}
	return r.engine.Handle(executionCtx, delivery, r.route)
}

func (r *Runtime) prepareCommandDelivery(ctx context.Context, cmd CommandMessage) (context.Context, func(), actorengine.Delivery) {
	if r == nil || cmd == nil {
		return ctx, func() {}, &runtimeDelivery{cmd: cmd}
	}
	env := cmd.Envelope()
	delivery := &runtimeDelivery{
		cmd: cmd,
		onDeadLetter: func(reason string) {
			r.deadletterTask(ctx, env, reason)
		},
	}
	heartbeatCtx, stop := r.startHeartbeat(ctx, cmd, env, delivery)
	return heartbeatCtx, stop, delivery
}

func (r *Runtime) LaneStatus() RuntimeLaneStatus {
	if r == nil || r.engine == nil {
		return RuntimeLaneStatus{}
	}
	return r.engine.LaneStatus()
}

func (r *Runtime) route(ctx context.Context, delivery actorengine.Delivery) error {
	if r == nil {
		return nil
	}
	address, err := runtimeAddressOf(delivery.Envelope())
	if err != nil {
		return err
	}
	if r.actors == nil {
		return &actorengine.ResolveError{Address: address}
	}
	actor, found := r.actors.Resolve(address)
	if !found {
		return &actorengine.ResolveError{Address: address}
	}
	return actor.Handle(ctx, delivery.Envelope())
}

func (r *Runtime) startHeartbeat(ctx context.Context, cmd CommandMessage, env Envelope, delivery actorengine.Delivery) (context.Context, func()) {
	if r == nil || r.engine == nil || cmd == nil || delivery == nil {
		return ctx, func() {}
	}
	heartbeatCtx := withEnvelopeContext(ctx, env)
	child, cancel := context.WithCancel(heartbeatCtx)
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
				r.engine.EmitInProgress(child, delivery)
			case <-child.Done():
				return
			}
		}
	}()
	return child, cancel
}

func (r *Runtime) Publish(ctx context.Context, event actorengine.Event) {
	if r == nil || event.Type != actorengine.EventInProgress {
		return
	}
	env, ok := envelopeFromContext(ctx)
	if !ok {
		return
	}
	if err := r.bus.PublishEvent(ctx, SubjectEventCommandInProgress, commandNoopEvent(env)); err != nil {
		r.logger.Warn().Err(err).Str("envelope_id", env.ID).Msg("failed to publish command in-progress event")
	}
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

func retryExhaustedCommand(cmd CommandMessage) bool {
	if cmd == nil {
		return false
	}
	maxDeliveries := cmd.MaxDeliveries()
	return RetryExhausted(cmd.DeliveryAttempt(), maxDeliveries)
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

type runtimeDelivery struct {
	cmd          CommandMessage
	onDeadLetter func(reason string)
}

type envelopeContextKey struct{}

func envelopeFromContext(ctx context.Context) (Envelope, bool) {
	if ctx == nil {
		return Envelope{}, false
	}
	env, ok := ctx.Value(envelopeContextKey{}).(Envelope)
	if !ok {
		return Envelope{}, false
	}
	return env, true
}

func withEnvelopeContext(ctx context.Context, env Envelope) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, envelopeContextKey{}, env)
}

type runtimeSource struct {
	bus       RuntimeBus
	prepareFn func(context.Context, CommandMessage) (context.Context, func(), actorengine.Delivery)
}

type runtimeResolver struct{}

func (r runtimeResolver) LaneKey(delivery actorengine.Delivery) string {
	return actorLaneKeyFromEnvelope(delivery.Envelope())
}

func (s runtimeSource) Run(ctx context.Context, handler actorengine.Handler) error {
	if s.bus == nil {
		return fmt.Errorf("jetstream command bus is required")
	}
	if handler == nil {
		return fmt.Errorf("runtime handler is required")
	}
	if s.prepareFn == nil {
		return fmt.Errorf("runtime command delivery factory is required")
	}
	return s.bus.RunCommandConsumer(ctx, func(ctx context.Context, cmd CommandMessage) error {
		if cmd == nil {
			return fmt.Errorf("command is required")
		}
		executionCtx, stop, delivery := s.prepareFn(ctx, cmd)
		defer stop()
		return handler(executionCtx, delivery)
	})
}

func assertEnvelope(envelope any) (Envelope, error) {
	env, ok := envelope.(Envelope)
	if !ok {
		return Envelope{}, DecodeError(fmt.Errorf("unexpected actor envelope type %T", envelope))
	}
	return env, nil
}

func AssertEnvelope(envelope any) (Envelope, error) {
	return assertEnvelope(envelope)
}

func runtimeAddressOf(envelope any) (string, error) {
	env, err := assertEnvelope(envelope)
	if err != nil {
		return "", err
	}
	to, err := env.To.String()
	if err != nil {
		return "", DecodeError(err)
	}
	if strings.TrimSpace(to) == "" {
		return "", DecodeError(fmt.Errorf("empty actor address"))
	}
	return to, nil
}

func actorLaneKeyFromEnvelope(envelope any) string {
	env, ok := envelope.(Envelope)
	if !ok {
		return "unknown"
	}
	return actorLaneKey(env)
}

func (d *runtimeDelivery) Envelope() any { return d.cmd.Envelope() }
func (d *runtimeDelivery) Attempt() int  { return d.cmd.DeliveryAttempt() }
func (d *runtimeDelivery) MaxAttempts() int {
	return d.cmd.MaxDeliveries()
}
func (d *runtimeDelivery) InProgress(ctx context.Context) error {
	return d.cmd.InProgress(ctx)
}
func (d *runtimeDelivery) Ack(ctx context.Context) error {
	return d.cmd.Ack(ctx)
}
func (d *runtimeDelivery) Retry(ctx context.Context, delay time.Duration, reason string) error {
	return d.cmd.Retry(ctx, delay, reason)
}
func (d *runtimeDelivery) DeadLetter(ctx context.Context, reason string) error {
	if d.onDeadLetter != nil {
		d.onDeadLetter(reason)
	}
	return d.cmd.DeadLetter(ctx, reason)
}
