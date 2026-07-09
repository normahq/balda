package actors

import (
	"context"
	"strings"
	"sync"
	"testing"

	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	"github.com/normahq/balda/internal/apps/balda/memory"
)

func TestMemoryRememberEnvelopeRoutesToMemorySubject(t *testing.T) {
	t.Parallel()

	env, err := MemoryRememberEnvelope(MemoryRememberPayload{Fact: "remember this", SourceSessionID: "tg-1-2"})
	if err != nil {
		t.Fatalf("MemoryRememberEnvelope() error = %v", err)
	}
	if got := baldaexecution.SubjectForEnvelope(env); got != baldaexecution.SubjectCommandMemory {
		t.Fatalf("SubjectForEnvelope() = %q, want %q", got, baldaexecution.SubjectCommandMemory)
	}
	if env.To.Target != baldaexecution.ActorTypeMemory {
		t.Fatalf("env.To.Target = %q, want %q", env.To.Target, baldaexecution.ActorTypeMemory)
	}
}

func TestMemoryActorRememberWritesMemoryAndPublishesVersionEvent(t *testing.T) {
	t.Parallel()

	store := memory.NewStore(newActorMemoryKV(), "", true)
	bus := &recordingHandlerCommandBus{}
	exec := &memoryActorExecutor{store: store, events: bus}
	env, err := MemoryRememberEnvelope(MemoryRememberPayload{Fact: "remember actor fact"})
	if err != nil {
		t.Fatalf("MemoryRememberEnvelope() error = %v", err)
	}

	if err := exec.Handle(context.Background(), env); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	got, err := store.ReadMemory(context.Background())
	if err != nil {
		t.Fatalf("ReadMemory() error = %v", err)
	}
	if got != "remember actor fact" {
		t.Fatalf("ReadMemory() = %q, want remember actor fact", got)
	}
	if len(bus.eventSubjects) != 1 {
		t.Fatalf("published events = %d, want 1", len(bus.eventSubjects))
	}
	if bus.eventSubjects[0] != baldaexecution.SubjectEventMemoryUpdated {
		t.Fatalf("event subject = %q, want %q", bus.eventSubjects[0], baldaexecution.SubjectEventMemoryUpdated)
	}
	if strings.Contains(bus.eventEnvs[0].PayloadJSON, "remember actor fact") {
		t.Fatalf("memory updated event leaked fact payload: %s", bus.eventEnvs[0].PayloadJSON)
	}
}

type actorMemoryKV struct {
	mu     sync.Mutex
	values map[string]any
}

func newActorMemoryKV() *actorMemoryKV {
	return &actorMemoryKV{values: make(map[string]any)}
}

func (s *actorMemoryKV) GetJSON(_ context.Context, key string) (any, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.values[strings.TrimSpace(key)]
	return value, ok, nil
}

func (s *actorMemoryKV) SetJSON(_ context.Context, key string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[strings.TrimSpace(key)] = value
	return nil
}
