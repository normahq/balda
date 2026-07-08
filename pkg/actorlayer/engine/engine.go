package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/normahq/balda/pkg/actorlayer"
	"github.com/normahq/balda/pkg/actorlayer/dispatch"
)

type Delivery interface {
	Envelope() Envelope
	Attempt() int
	MaxAttempts() int
	InProgress(ctx context.Context) error
	Ack(ctx context.Context) error
	Retry(ctx context.Context, delay time.Duration, reason string) error
	DeadLetter(ctx context.Context, reason string) error
}

type Handler func(ctx context.Context, delivery Delivery) error

type Source interface {
	Run(ctx context.Context, handler Handler) error
}

type Resolver interface {
	LaneKey(delivery Delivery) string
}

type EventType string

const (
	EventRunning      EventType = "running"
	EventInProgress   EventType = "in_progress"
	EventAcked        EventType = "acked"
	EventRetrying     EventType = "retrying"
	EventDeadLettered EventType = "deadlettered"
)

type Event struct {
	Type        EventType
	EnvelopeID  string
	Namespace   string
	Kind        string
	LaneKey     string
	From        ActorAddress
	To          ActorAddress
	Reason      string
	RetryDelay  time.Duration
	Attempt     int
	MaxAttempts int
}

type EventSink interface {
	Publish(ctx context.Context, event Event)
}

type RetryPolicy struct {
	IsRetryable    func(error) bool
	Backoff        func(attempt int) time.Duration
	RetryExhausted func(delivery Delivery) bool
}

type Config struct {
	Resolver    Resolver
	Retry       RetryPolicy
	Sink        EventSink
	LaneIdleTTL time.Duration
}

type LaneStatus struct {
	Active int
	Keys   []string
}

const unknownLaneKey = "unknown"

type AddressResolver func(envelope Envelope) (string, error)
type LaneKeyResolver func(envelope Envelope) string

type RuntimeConfig struct {
	Registry    dispatch.Registry
	AddressOf   AddressResolver
	LaneKey     LaneKeyResolver
	Retry       RetryPolicy
	Sink        EventSink
	LaneIdleTTL time.Duration
}

type DispatchRuntime struct {
	runtime   *Runtime
	registry  dispatch.Registry
	addressOf AddressResolver
}

func NewDispatchRuntime(cfg RuntimeConfig) (*DispatchRuntime, error) {
	if cfg.Registry == nil {
		return nil, fmt.Errorf("runtime registry is required")
	}
	if cfg.AddressOf == nil {
		return nil, fmt.Errorf("runtime address resolver is required")
	}
	runtime, err := New(Config{
		Resolver:    dispatchRuntimeResolver{addressOf: cfg.AddressOf, laneKey: cfg.LaneKey},
		Retry:       cfg.Retry,
		Sink:        cfg.Sink,
		LaneIdleTTL: cfg.LaneIdleTTL,
	})
	if err != nil {
		return nil, err
	}
	return &DispatchRuntime{runtime: runtime, registry: cfg.Registry, addressOf: cfg.AddressOf}, nil
}

func (r *DispatchRuntime) Handle(ctx context.Context, delivery Delivery) error {
	if delivery == nil {
		return nil
	}
	if r == nil || r.runtime == nil {
		return fmt.Errorf("runtime engine is required")
	}
	return r.runtime.Handle(ctx, delivery, r.handleDelivery)
}

func (r *DispatchRuntime) Run(ctx context.Context, source Source) error {
	if r == nil || r.runtime == nil {
		return fmt.Errorf("runtime engine is required")
	}
	if source == nil {
		return fmt.Errorf("engine source is required")
	}
	return source.Run(ctx, func(ctx context.Context, delivery Delivery) error {
		return r.runtime.Handle(ctx, delivery, r.handleDelivery)
	})
}

func (r *DispatchRuntime) LaneStatus() LaneStatus {
	if r == nil || r.runtime == nil {
		return LaneStatus{}
	}
	return r.runtime.LaneStatus()
}

func (r *DispatchRuntime) handleDelivery(ctx context.Context, delivery Delivery) error {
	envelope := delivery.Envelope()
	address, err := r.addressOf(envelope)
	if err != nil {
		return err
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return fmt.Errorf("empty actor address")
	}
	actor, found := r.registry.Resolve(strings.ToLower(address))
	if !found {
		return &ResolveError{Address: address}
	}
	return actor.Handle(ctx, envelope)
}

func validateDeliveryEnvelope(delivery Delivery) error {
	if delivery == nil {
		return nil
	}
	if err := delivery.Envelope().Validate(); err != nil {
		return actorlayer.DecodeError(err)
	}
	return nil
}

type dispatchRuntimeResolver struct {
	addressOf AddressResolver
	laneKey   LaneKeyResolver
}

func (r dispatchRuntimeResolver) LaneKey(delivery Delivery) string {
	if delivery == nil {
		return unknownLaneKey
	}
	envelope := delivery.Envelope()
	if r.laneKey != nil {
		if key := strings.TrimSpace(r.laneKey(envelope)); key != "" {
			return key
		}
	}
	if r.addressOf == nil {
		return unknownLaneKey
	}
	address, err := r.addressOf(envelope)
	if err != nil {
		return unknownLaneKey
	}
	if key := strings.TrimSpace(strings.ToLower(address)); key != "" {
		return key
	}
	return unknownLaneKey
}

type Runtime struct {
	cfg Config

	mu    sync.Mutex
	lanes map[string]*lane
}

type lane struct {
	mu       sync.Mutex
	active   int
	lastUsed time.Time
}

type noopSink struct{}

func (noopSink) Publish(context.Context, Event) {}

func New(cfg Config) (*Runtime, error) {
	if cfg.Resolver == nil {
		return nil, fmt.Errorf("engine resolver is required")
	}
	if cfg.Sink == nil {
		cfg.Sink = noopSink{}
	}
	if cfg.Retry.IsRetryable == nil {
		cfg.Retry.IsRetryable = func(error) bool { return true }
	}
	if cfg.Retry.Backoff == nil {
		cfg.Retry.Backoff = func(_ int) time.Duration { return time.Second }
	}
	if cfg.Retry.RetryExhausted == nil {
		cfg.Retry.RetryExhausted = func(d Delivery) bool {
			if d == nil {
				return false
			}
			maxAttempts := d.MaxAttempts()
			return maxAttempts > 0 && d.Attempt() >= maxAttempts
		}
	}
	if cfg.LaneIdleTTL <= 0 {
		cfg.LaneIdleTTL = time.Hour
	}
	return &Runtime{cfg: cfg, lanes: make(map[string]*lane)}, nil
}

func (r *Runtime) Run(ctx context.Context, src Source, handler Handler) error {
	if r == nil {
		return fmt.Errorf("runtime engine is required")
	}
	if src == nil {
		return fmt.Errorf("engine source is required")
	}
	if handler == nil {
		return fmt.Errorf("engine handler is required")
	}
	return src.Run(ctx, func(ctx context.Context, delivery Delivery) error {
		return r.Handle(ctx, delivery, handler)
	})
}

func (r *Runtime) Handle(ctx context.Context, delivery Delivery, handler Handler) error {
	if delivery == nil {
		return nil
	}
	if r == nil {
		return fmt.Errorf("runtime engine is required")
	}
	if handler == nil {
		return fmt.Errorf("engine handler is required")
	}
	if err := validateDeliveryEnvelope(delivery); err != nil {
		return err
	}
	laneKey := r.cfg.Resolver.LaneKey(delivery)
	l := r.acquireLane(laneKey)
	defer r.releaseLane(laneKey, l)
	l.mu.Lock()
	defer l.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	r.emit(ctx, eventForDelivery(EventRunning, delivery, laneKey))
	err := handler(ctx, delivery)
	if err == nil {
		if err := delivery.Ack(ctx); err != nil {
			return err
		}
		r.emit(ctx, eventForDelivery(EventAcked, delivery, laneKey))
		return nil
	}
	if !r.cfg.Retry.IsRetryable(err) {
		reason := err.Error()
		if err := delivery.DeadLetter(ctx, reason); err != nil {
			return err
		}
		event := eventForDelivery(EventDeadLettered, delivery, laneKey)
		event.Reason = reason
		r.emit(ctx, event)
		return nil
	}
	if r.cfg.Retry.RetryExhausted(delivery) {
		reason := "retry exhausted: " + err.Error()
		if err := delivery.DeadLetter(ctx, reason); err != nil {
			return err
		}
		event := eventForDelivery(EventDeadLettered, delivery, laneKey)
		event.Reason = reason
		r.emit(ctx, event)
		return nil
	}
	delay := r.cfg.Retry.Backoff(max(delivery.Attempt()-1, 0))
	if settleErr := delivery.Retry(ctx, delay, err.Error()); settleErr != nil {
		return settleErr
	}
	event := eventForDelivery(EventRetrying, delivery, laneKey)
	event.Reason = err.Error()
	event.RetryDelay = delay
	r.emit(ctx, event)
	return nil
}

func (r *Runtime) LaneStatus() LaneStatus {
	if r == nil {
		return LaneStatus{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	r.pruneLanesLocked(now)
	keys := make([]string, 0, len(r.lanes))
	for key, lane := range r.lanes {
		if lane != nil && lane.active > 0 {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return LaneStatus{Active: len(keys), Keys: keys}
}

func (r *Runtime) EmitInProgress(ctx context.Context, delivery Delivery) {
	if delivery == nil {
		return
	}
	if err := validateDeliveryEnvelope(delivery); err != nil {
		return
	}
	laneKey := ""
	if r != nil && r.cfg.Resolver != nil {
		laneKey = r.cfg.Resolver.LaneKey(delivery)
	}
	r.emit(ctx, eventForDelivery(EventInProgress, delivery, laneKey))
}

func (r *Runtime) emit(ctx context.Context, event Event) {
	if r == nil || r.cfg.Sink == nil {
		return
	}
	r.cfg.Sink.Publish(ctx, event)
}

func (r *Runtime) acquireLane(key string) *lane {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		trimmed = unknownLaneKey
	}
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLanesLocked(now)
	l := r.lanes[trimmed]
	if l == nil {
		l = &lane{}
		r.lanes[trimmed] = l
	}
	l.active++
	l.lastUsed = now
	return l
}

func (r *Runtime) releaseLane(key string, l *lane) {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		trimmed = unknownLaneKey
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if l.active > 0 {
		l.active--
	}
	l.lastUsed = time.Now()
	r.lanes[trimmed] = l
}

func (r *Runtime) pruneLanesLocked(now time.Time) {
	cutoff := now.Add(-r.cfg.LaneIdleTTL)
	for key, l := range r.lanes {
		if l.active == 0 && !l.lastUsed.IsZero() && l.lastUsed.Before(cutoff) {
			delete(r.lanes, key)
		}
	}
}

func eventForDelivery(eventType EventType, delivery Delivery, laneKey string) Event {
	env := delivery.Envelope()
	return Event{
		Type:        eventType,
		EnvelopeID:  strings.TrimSpace(env.ID),
		Namespace:   strings.TrimSpace(env.Namespace),
		Kind:        strings.TrimSpace(env.Kind),
		LaneKey:     normalizeLaneKey(laneKey),
		From:        env.From,
		To:          env.To,
		Attempt:     delivery.Attempt(),
		MaxAttempts: delivery.MaxAttempts(),
	}
}

func normalizeLaneKey(key string) string {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		return unknownLaneKey
	}
	return trimmed
}
