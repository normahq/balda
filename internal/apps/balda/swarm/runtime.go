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

type Runtime struct {
	source actorengine.Source
	events EventPublisher
	tasks  *TaskService
	engine *actorengine.DispatchRuntime
	logger zerolog.Logger
	// heartbeatTick controls the in-progress ack cadence for long-running commands.
	// Zero falls back to the package default.
	heartbeatTick time.Duration

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type runtimeParams struct {
	fx.In

	LC     fx.Lifecycle
	Source actorengine.Source
	Events EventPublisher `optional:"true"`
	Tasks  *TaskService
	Logger zerolog.Logger
	Actors []Actor `group:"balda_swarm_actors"`
}

func NewRuntime(params runtimeParams) (*Runtime, error) {
	if params.Source == nil {
		return nil, fmt.Errorf("actor delivery source is required")
	}
	registry := dispatch.NewMemoryRegistry()
	for _, actor := range params.Actors {
		if err := registry.Register(actor); err != nil {
			return nil, err
		}
	}
	r := &Runtime{
		source:        params.Source,
		events:        params.Events,
		tasks:         params.Tasks,
		logger:        params.Logger.With().Str("component", "balda.swarm.runtime").Logger(),
		heartbeatTick: heartbeatInterval,
	}
	engine, err := actorengine.NewDispatchRuntime(actorengine.RuntimeConfig{
		Registry:  registry,
		AddressOf: runtimeAddressOf,
		LaneKey:   actorLaneKeyFromEnvelope,
		Sink:      r,
		Retry: actorengine.RetryPolicy{
			IsRetryable:    IsRetryableError,
			Backoff:        RetryDelay,
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

func (r *Runtime) Start(context.Context) error {
	if r == nil {
		return nil
	}
	if r.cancel != nil {
		return nil
	}
	runCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	source := runtimeSource{source: r.source, prepareFn: r.prepareDelivery}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		if err := r.engine.Run(runCtx, source); err != nil && !errors.Is(err, context.Canceled) {
			r.logger.Error().Err(err).Msg("actor delivery source stopped")
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

func (r *Runtime) prepareDelivery(ctx context.Context, delivery actorengine.Delivery) (context.Context, func(), actorengine.Delivery) {
	if r == nil || delivery == nil {
		return ctx, func() {}, delivery
	}
	env, _ := delivery.Envelope().(Envelope)
	wrapped := &runtimeDelivery{
		delivery: delivery,
		onDeadLetter: func(reason string) {
			r.deadletterTask(ctx, env, reason)
		},
	}
	heartbeatCtx, stop := r.startHeartbeat(ctx, env, wrapped)
	return heartbeatCtx, stop, wrapped
}

func (r *Runtime) startHeartbeat(ctx context.Context, env Envelope, delivery actorengine.Delivery) (context.Context, func()) {
	if r == nil || r.engine == nil || delivery == nil {
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
				if err := delivery.InProgress(child); err != nil {
					r.logger.Warn().Err(err).Str("envelope_id", env.ID).Msg("failed to send actor delivery in-progress")
				}
				r.Publish(child, actorengine.Event{Type: actorengine.EventInProgress, Attempt: delivery.Attempt(), MaxAttempts: delivery.MaxAttempts()})
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
	if ctx == nil {
		return
	}
	env, ok := ctx.Value(envelopeContextKey{}).(Envelope)
	if !ok {
		return
	}
	if r.events == nil {
		return
	}
	inProgressEnv := env
	inProgressEnv.ID = strings.TrimSpace(env.ID) + ":event:in_progress"
	inProgressEnv.Namespace = NamespaceTelemetry
	inProgressEnv.Kind = "command_event"
	inProgressEnv.DedupeKey = inProgressEnv.ID
	if strings.TrimSpace(inProgressEnv.PayloadJSON) == "" {
		inProgressEnv.PayloadJSON = `{"ok":true}`
	}
	if err := r.events.PublishEvent(ctx, SubjectEventCommandInProgress, inProgressEnv); err != nil {
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

func retryExhaustedDelivery(delivery actorengine.Delivery) bool {
	if delivery == nil {
		return false
	}
	return RetryExhausted(delivery.Attempt(), delivery.MaxAttempts())
}

type runtimeDelivery struct {
	delivery     actorengine.Delivery
	onDeadLetter func(reason string)
}

type envelopeContextKey struct{}

func withEnvelopeContext(ctx context.Context, env Envelope) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, envelopeContextKey{}, env)
}

type runtimeSource struct {
	source    actorengine.Source
	prepareFn func(context.Context, actorengine.Delivery) (context.Context, func(), actorengine.Delivery)
}

func (s runtimeSource) Run(ctx context.Context, handler actorengine.Handler) error {
	if s.source == nil {
		return fmt.Errorf("actor delivery source is required")
	}
	if handler == nil {
		return fmt.Errorf("runtime handler is required")
	}
	if s.prepareFn == nil {
		return fmt.Errorf("runtime command delivery factory is required")
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
	namespace := strings.TrimSpace(env.Namespace)
	taskID := strings.TrimSpace(env.TaskID)
	if taskID != "" {
		switch namespace {
		case NamespaceTaskControl,
			NamespaceGoalkeeperCommand,
			NamespaceHumanInbound,
			NamespaceWebhookInbound,
			NamespaceScheduleInbound:
			return "task:" + taskID
		case NamespaceAgentResult:
			if strings.EqualFold(strings.TrimSpace(env.To.Target), ActorTypeDelivery) {
				if address := strings.TrimSpace(env.To.Key); address != "" {
					return "delivery:" + address
				}
			}
			return "task:" + taskID
		}
	}
	switch namespace {
	case NamespaceGoalkeeperCommand:
		if key := strings.TrimSpace(env.To.Key); key != "" {
			return "goalkeeper:" + key
		}
	case NamespaceHumanInbound, NamespaceWebhookInbound, NamespaceScheduleInbound:
		if sessionID := strings.TrimSpace(env.SessionID); sessionID != "" {
			return "session:" + sessionID
		}
	}
	if to, err := env.To.String(); err == nil {
		return to
	}
	return strings.TrimSpace(env.ID)
}

func (d *runtimeDelivery) Envelope() any { return d.delivery.Envelope() }
func (d *runtimeDelivery) Attempt() int  { return d.delivery.Attempt() }
func (d *runtimeDelivery) MaxAttempts() int {
	return d.delivery.MaxAttempts()
}
func (d *runtimeDelivery) InProgress(ctx context.Context) error {
	return d.delivery.InProgress(ctx)
}
func (d *runtimeDelivery) Ack(ctx context.Context) error {
	return d.delivery.Ack(ctx)
}
func (d *runtimeDelivery) Retry(ctx context.Context, delay time.Duration, reason string) error {
	return d.delivery.Retry(ctx, delay, reason)
}
func (d *runtimeDelivery) DeadLetter(ctx context.Context, reason string) error {
	if d.onDeadLetter != nil {
		d.onDeadLetter(reason)
	}
	return d.delivery.DeadLetter(ctx, reason)
}
