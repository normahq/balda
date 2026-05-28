package swarm

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

const actorLaneIdleTTL = time.Hour

const (
	EngineEventRunning      = "running"
	EngineEventInProgress   = "in_progress"
	EngineEventAcked        = "acked"
	EngineEventRetrying     = "retrying"
	EngineEventDeadLettered = "deadlettered"
	EngineEventDecodeFailed = "decode_failed"
)

type EngineLifecycleEvent struct {
	Status      string
	Envelope    Envelope
	Reason      string
	RetryDelay  time.Duration
	Attempt     int
	MaxAttempts int
}

type EngineEventSink interface {
	PublishLifecycleEvent(ctx context.Context, event EngineLifecycleEvent)
}

type NoopEngineEventSink struct{}

func (NoopEngineEventSink) PublishLifecycleEvent(context.Context, EngineLifecycleEvent) {}

type EngineResolver interface {
	Resolve(address string) (Actor, bool)
	LaneKey(env Envelope) string
}

type runtimeEngineConfig struct {
	Resolver       EngineResolver
	EventSink      EngineEventSink
	DeadLetterTask func(ctx context.Context, env Envelope, reason string)
	IsRetryable    func(err error) bool
	ComputeBackoff func(attempt int) time.Duration
	RetryExhausted func(delivery CommandMessage) bool
}

type runtimeEngine struct {
	cfg runtimeEngineConfig

	mu    sync.Mutex
	lanes map[string]*actorLane
}

type actorLane struct {
	mu       sync.Mutex
	active   int
	lastUsed time.Time
}

func newRuntimeEngine(cfg runtimeEngineConfig) (*runtimeEngine, error) {
	if cfg.Resolver == nil {
		return nil, fmt.Errorf("runtime engine resolver is required")
	}
	if cfg.EventSink == nil {
		cfg.EventSink = NoopEngineEventSink{}
	}
	if cfg.DeadLetterTask == nil {
		cfg.DeadLetterTask = func(context.Context, Envelope, string) {}
	}
	if cfg.IsRetryable == nil {
		cfg.IsRetryable = func(error) bool { return true }
	}
	if cfg.ComputeBackoff == nil {
		cfg.ComputeBackoff = RetryDelay
	}
	if cfg.RetryExhausted == nil {
		cfg.RetryExhausted = retryExhaustedCommand
	}
	return &runtimeEngine{
		cfg:   cfg,
		lanes: make(map[string]*actorLane),
	}, nil
}

func (e *runtimeEngine) HandleDelivery(ctx context.Context, delivery CommandMessage) error {
	if delivery == nil {
		return nil
	}
	env := delivery.Envelope()
	e.emit(ctx, EngineLifecycleEvent{
		Status:      EngineEventRunning,
		Envelope:    env,
		Attempt:     delivery.DeliveryAttempt(),
		MaxAttempts: delivery.MaxDeliveries(),
	})
	key := e.cfg.Resolver.LaneKey(env)
	lane := e.acquire(key)
	defer e.release(key, lane)
	lane.mu.Lock()
	defer lane.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	to, err := env.To.String()
	if err != nil {
		reason := err.Error()
		e.cfg.DeadLetterTask(ctx, env, reason)
		e.emit(ctx, EngineLifecycleEvent{Status: EngineEventDeadLettered, Envelope: env, Reason: reason})
		return delivery.DeadLetter(ctx, reason)
	}
	actor, ok := e.cfg.Resolver.Resolve(to)
	if !ok {
		reason := "actor not found: " + to
		e.cfg.DeadLetterTask(ctx, env, reason)
		e.emit(ctx, EngineLifecycleEvent{Status: EngineEventDeadLettered, Envelope: env, Reason: reason})
		return delivery.DeadLetter(ctx, reason)
	}
	if err := actor.Handle(ctx, env); err != nil {
		if !e.cfg.IsRetryable(err) {
			reason := err.Error()
			e.cfg.DeadLetterTask(ctx, env, reason)
			e.emit(ctx, EngineLifecycleEvent{Status: EngineEventDeadLettered, Envelope: env, Reason: reason})
			return delivery.DeadLetter(ctx, reason)
		}
		if e.cfg.RetryExhausted(delivery) {
			reason := "retry exhausted: " + err.Error()
			e.cfg.DeadLetterTask(ctx, env, reason)
			e.emit(ctx, EngineLifecycleEvent{Status: EngineEventDeadLettered, Envelope: env, Reason: reason})
			return delivery.DeadLetter(ctx, reason)
		}
		delay := e.cfg.ComputeBackoff(max(delivery.DeliveryAttempt()-1, 0))
		e.emit(ctx, EngineLifecycleEvent{
			Status:      EngineEventRetrying,
			Envelope:    env,
			Reason:      err.Error(),
			RetryDelay:  delay,
			Attempt:     delivery.DeliveryAttempt(),
			MaxAttempts: delivery.MaxDeliveries(),
		})
		return delivery.Retry(ctx, delay, err.Error())
	}
	e.emit(ctx, EngineLifecycleEvent{
		Status:      EngineEventAcked,
		Envelope:    env,
		Attempt:     delivery.DeliveryAttempt(),
		MaxAttempts: delivery.MaxDeliveries(),
	})
	return delivery.Ack(ctx)
}

func (e *runtimeEngine) emit(ctx context.Context, event EngineLifecycleEvent) {
	if e == nil || e.cfg.EventSink == nil {
		return
	}
	e.cfg.EventSink.PublishLifecycleEvent(ctx, event)
}

func (e *runtimeEngine) acquire(key string) *actorLane {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		trimmed = "unknown"
	}
	now := time.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pruneLocked(now)
	lane := e.lanes[trimmed]
	if lane == nil {
		lane = &actorLane{}
		e.lanes[trimmed] = lane
	}
	lane.active++
	lane.lastUsed = now
	return lane
}

func (e *runtimeEngine) release(key string, lane *actorLane) {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		trimmed = "unknown"
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if lane.active > 0 {
		lane.active--
	}
	lane.lastUsed = time.Now()
	e.lanes[trimmed] = lane
}

func (e *runtimeEngine) pruneLocked(now time.Time) {
	cutoff := now.Add(-actorLaneIdleTTL)
	for key, lane := range e.lanes {
		if lane.active == 0 && !lane.lastUsed.IsZero() && lane.lastUsed.Before(cutoff) {
			delete(e.lanes, key)
		}
	}
}

func actorLaneKey(env Envelope) string {
	namespace := strings.TrimSpace(env.Namespace)
	taskID := strings.TrimSpace(env.TaskID)
	if taskID != "" {
		switch namespace {
		case NamespaceTaskControl,
			NamespaceAgentCommand,
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
	case NamespaceAgentCommand:
		if key := strings.TrimSpace(env.To.Key); key != "" {
			return "agent:" + key
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
