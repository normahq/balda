package swarm

import (
	"context"
	"strings"
	"sync"
)

type ActorHandler func(ctx context.Context, env Envelope) error

type KeyedActorScheduler struct {
	mu    sync.Mutex
	lanes map[string]*ActorLane
}

type ActorLane struct {
	mu sync.Mutex
}

func NewKeyedActorScheduler() *KeyedActorScheduler {
	return &KeyedActorScheduler{lanes: make(map[string]*ActorLane)}
}

func (s *KeyedActorScheduler) Dispatch(ctx context.Context, env Envelope, handler ActorHandler) error {
	if s == nil || handler == nil {
		return nil
	}
	lane := s.lane(actorKey(env))
	lane.mu.Lock()
	defer lane.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return handler(ctx, env)
	}
}

func (s *KeyedActorScheduler) lane(key string) *ActorLane {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		trimmed = "unknown"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	lane := s.lanes[trimmed]
	if lane == nil {
		lane = &ActorLane{}
		s.lanes[trimmed] = lane
	}
	return lane
}

func actorKey(env Envelope) string {
	switch strings.TrimSpace(env.Namespace) {
	case NamespaceTaskControl:
		if taskID := strings.TrimSpace(env.TaskID); taskID != "" {
			return "task:" + taskID
		}
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
