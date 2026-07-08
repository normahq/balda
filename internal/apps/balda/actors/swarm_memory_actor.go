package actors

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/normahq/balda/internal/apps/balda/memory"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/normahq/balda/pkg/actorlayer"
	actortransport "github.com/normahq/balda/pkg/actorlayer/transport"
	"go.uber.org/fx"
)

// MemoryRememberPayload carries a fact that should be appended to memory.
type MemoryRememberPayload struct {
	Fact            string `json:"fact"`
	SourceSessionID string `json:"source_session_id,omitempty"`
}

type memoryActorExecutor struct {
	store  *memory.Store
	events actortransport.EventPublisher
}

type memoryActorExecutorParams struct {
	fx.In

	Store  *memory.Store
	Events actortransport.EventPublisher `optional:"true"`
}

// MemoryRememberEnvelope builds a command envelope for appending memory.
func MemoryRememberEnvelope(payload MemoryRememberPayload) (actorlayer.Envelope, error) {
	if strings.TrimSpace(payload.Fact) == "" {
		return actorlayer.Envelope{}, fmt.Errorf("fact is required")
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return actorlayer.Envelope{}, fmt.Errorf("encode memory remember payload: %w", err)
	}
	return actorlayer.Envelope{
		ID:          uuid.NewString(),
		Namespace:   swarm.NamespaceMemoryCommand,
		Kind:        swarm.KindMemoryRemember,
		From:        actorlayer.SystemAddress("memory"),
		To:          actorlayer.ActorAddress{Target: swarm.ActorTypeMemory, Key: "global"},
		SessionID:   strings.TrimSpace(payload.SourceSessionID),
		Priority:    70,
		DedupeKey:   uuid.NewString(),
		PayloadJSON: string(data),
	}, nil
}

func (e *memoryActorExecutor) Address() string {
	return actorlayer.WildcardAddress(swarm.ActorTypeMemory)
}

func (e *memoryActorExecutor) Handle(ctx context.Context, env actorlayer.Envelope) error {
	if strings.TrimSpace(env.Namespace) != swarm.NamespaceMemoryCommand {
		return actorlayer.PolicyError(fmt.Errorf("unsupported memory namespace %q", env.Namespace))
	}
	switch strings.TrimSpace(env.Kind) {
	case swarm.KindMemoryRemember:
		return e.remember(ctx, env)
	default:
		return actorlayer.PolicyError(fmt.Errorf("unsupported memory kind %q", env.Kind))
	}
}

func (e *memoryActorExecutor) remember(ctx context.Context, env actorlayer.Envelope) error {
	if e.store == nil {
		return actorlayer.TransientError(fmt.Errorf("memory store is required"))
	}
	var payload MemoryRememberPayload
	if err := json.Unmarshal([]byte(env.PayloadJSON), &payload); err != nil {
		return actorlayer.PermanentError(fmt.Errorf("decode memory remember payload: %w", err))
	}
	if strings.TrimSpace(payload.Fact) == "" {
		return actorlayer.PermanentError(fmt.Errorf("fact is required"))
	}
	snapshot, err := e.store.Remember(ctx, payload.Fact)
	if err != nil {
		return actorlayer.TransientError(fmt.Errorf("remember fact: %w", err))
	}
	e.publishUpdated(ctx, env, snapshot)
	return nil
}

func (e *memoryActorExecutor) publishUpdated(ctx context.Context, env actorlayer.Envelope, snapshot memory.Snapshot) {
	if e == nil || e.events == nil {
		return
	}
	payload := map[string]any{
		"version": snapshot.Version,
		"found":   snapshot.Found,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	eventEnv := env
	eventEnv.ID = strings.TrimSpace(env.ID) + ":event:memory_updated"
	eventEnv.Namespace = swarm.NamespaceTelemetry
	eventEnv.Kind = "memory_updated"
	eventEnv.DedupeKey = eventEnv.ID
	eventEnv.PayloadJSON = string(data)
	if err := e.events.PublishEvent(ctx, swarm.SubjectEventMemoryUpdated, eventEnv); err != nil {
		return
	}
}
