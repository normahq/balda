package handlers

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/normahq/balda/internal/apps/balda/memory"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	adksession "google.golang.org/adk/v2/session"
)

type memoryRefreshKV struct {
	mu     sync.Mutex
	values map[string]any
}

func newMemoryRefreshKV() *memoryRefreshKV {
	return &memoryRefreshKV{values: make(map[string]any)}
}

func (s *memoryRefreshKV) GetJSON(_ context.Context, key string) (any, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.values[strings.TrimSpace(key)]
	return value, ok, nil
}

func (s *memoryRefreshKV) SetJSON(_ context.Context, key string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[strings.TrimSpace(key)] = value
	return nil
}

func TestPrepareMemoryRunOptionsSkipsCurrentSessionVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memory.NewStore(newMemoryRefreshKV(), "", true)
	snapshot, err := store.Remember(ctx, "memory fact")
	if err != nil {
		t.Fatalf("Remember() error = %v", err)
	}

	sessionSvc := adksession.InMemoryService()
	created, err := sessionSvc.Create(ctx, &adksession.CreateRequest{
		AppName:   "balda-test",
		UserID:    "tg-101",
		SessionID: "agent-session-1",
		State: map[string]any{
			memory.MemoryVersionStateKey: "0",
		},
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	ts := &baldasession.TopicSession{}
	setUnexportedField(t, ts, "sessionID", "transport-session-1")
	setUnexportedField(t, ts, "agentSessionID", "agent-session-1")
	setUnexportedField(t, ts, "sessionSvc", sessionSvc)
	setUnexportedField(t, ts, "sess", created.Session)

	first, err := prepareMemoryRunOptions(ctx, store, ts)
	if err != nil {
		t.Fatalf("prepareMemoryRunOptions(first) error = %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("prepareMemoryRunOptions(first) options = %d, want 1", len(first))
	}

	current, err := sessionSvc.Get(ctx, &adksession.GetRequest{
		AppName:   "balda-test",
		UserID:    "tg-101",
		SessionID: "agent-session-1",
	})
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	event := adksession.NewEvent(context.Background(), "invocation-1")
	event.Actions.StateDelta = map[string]any{
		memory.MemoryVersionStateKey: memory.VersionStateValue(snapshot.Version),
	}
	if err := sessionSvc.AppendEvent(ctx, current.Session, event); err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}

	second, err := prepareMemoryRunOptions(ctx, store, ts)
	if err != nil {
		t.Fatalf("prepareMemoryRunOptions(second) error = %v", err)
	}
	if len(second) != 0 {
		t.Fatalf("prepareMemoryRunOptions(second) options = %d, want 0", len(second))
	}
}
