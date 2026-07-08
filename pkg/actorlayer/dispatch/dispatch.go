package dispatch

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/normahq/balda/pkg/actorlayer"
)

// Actor is an addressable message handler.
type Actor interface {
	Address() string
	Handle(ctx context.Context, envelope actorlayer.Envelope) error
}

// Registry resolves actors by normalized address.
type Registry interface {
	Register(actor Actor) error
	Resolve(address string) (Actor, bool)
}

// MemoryRegistry is an in-memory address registry with wildcard fallback.
type MemoryRegistry struct {
	mu     sync.RWMutex
	actors map[string]Actor
}

func NewMemoryRegistry() *MemoryRegistry {
	return &MemoryRegistry{actors: make(map[string]Actor)}
}

func (r *MemoryRegistry) Register(actor Actor) error {
	if actor == nil {
		return nil
	}
	address := normalizeAddress(actor.Address())
	if address == "" {
		return fmt.Errorf("actor address is required")
	}
	r.mu.Lock()
	if r.actors == nil {
		r.actors = make(map[string]Actor)
	}
	r.actors[address] = actor
	r.mu.Unlock()
	return nil
}

func (r *MemoryRegistry) Resolve(address string) (Actor, bool) {
	normalized := normalizeAddress(address)
	if normalized == "" {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if actor, ok := r.actors[normalized]; ok {
		return actor, true
	}
	idx := strings.Index(normalized, ":")
	if idx <= 0 {
		return nil, false
	}
	actor, ok := r.actors[normalized[:idx]+":*"]
	return actor, ok
}

func normalizeAddress(address string) string {
	return strings.ToLower(strings.TrimSpace(address))
}
