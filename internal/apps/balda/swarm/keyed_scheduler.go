package swarm

import (
	"context"
	"strings"
	"sync"
	"time"
)

const actorLaneIdleTTL = time.Hour

type ActorHandler func(ctx context.Context, env Envelope) error

type KeyedActorScheduler struct {
	mu    sync.Mutex
	lanes map[string]*ActorLane
}

type ActorLane struct {
	mu       sync.Mutex
	active   int
	lastUsed time.Time
}

func NewKeyedActorScheduler() *KeyedActorScheduler {
	return &KeyedActorScheduler{lanes: make(map[string]*ActorLane)}
}

func (s *KeyedActorScheduler) Dispatch(ctx context.Context, env Envelope, handler ActorHandler) error {
	if s == nil || handler == nil {
		return nil
	}
	key := actorKey(env)
	lane := s.acquire(key)
	defer s.release(key, lane)
	lane.mu.Lock()
	defer lane.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return handler(ctx, env)
	}
}

func (s *KeyedActorScheduler) acquire(key string) *ActorLane {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		trimmed = "unknown"
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(now)
	lane := s.lanes[trimmed]
	if lane == nil {
		lane = &ActorLane{}
		s.lanes[trimmed] = lane
	}
	lane.active++
	lane.lastUsed = now
	return lane
}

func (s *KeyedActorScheduler) release(key string, lane *ActorLane) {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		trimmed = "unknown"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if lane.active > 0 {
		lane.active--
	}
	lane.lastUsed = time.Now()
	s.lanes[trimmed] = lane
}

func (s *KeyedActorScheduler) pruneLocked(now time.Time) {
	cutoff := now.Add(-actorLaneIdleTTL)
	for key, lane := range s.lanes {
		if lane.active == 0 && !lane.lastUsed.IsZero() && lane.lastUsed.Before(cutoff) {
			delete(s.lanes, key)
		}
	}
}

func actorKey(env Envelope) string {
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
