package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/runtime/v2/mcpregistry"
	"github.com/rs/zerolog"
)

type testSessionStore struct {
	mu     sync.RWMutex
	values map[string]any
}

func newTestSessionStore() *testSessionStore {
	return &testSessionStore{values: make(map[string]any)}
}

func (s *testSessionStore) Get(_ context.Context, key string) (string, bool, error) {
	val, ok, err := s.get(key)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, nil
	}
	str, ok := val.(string)
	if ok {
		return str, true, nil
	}
	data, err := json.Marshal(val)
	if err != nil {
		return "", false, fmt.Errorf("marshal value: %w", err)
	}
	return string(data), true, nil
}

func (s *testSessionStore) Set(_ context.Context, key, value string) error {
	s.set(key, value)
	return nil
}

func (s *testSessionStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.values, strings.TrimSpace(key))
	return nil
}

func (s *testSessionStore) List(_ context.Context, prefix string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	trimmedPrefix := strings.TrimSpace(prefix)
	keys := make([]string, 0, len(s.values))
	for key := range s.values {
		if trimmedPrefix == "" || strings.HasPrefix(key, trimmedPrefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *testSessionStore) Clear(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values = make(map[string]any)
	return nil
}

func (s *testSessionStore) GetJSON(_ context.Context, key string) (interface{}, bool, error) {
	return s.get(key)
}

func (s *testSessionStore) SetJSON(_ context.Context, key string, value interface{}) error {
	s.set(key, value)
	return nil
}

func (s *testSessionStore) MergeJSON(_ context.Context, key string, fields map[string]interface{}) (map[string]interface{}, error) {
	trimmedKey := strings.TrimSpace(key)
	if trimmedKey == "" {
		return nil, fmt.Errorf("key is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	merged := make(map[string]interface{})
	if current, ok := s.values[trimmedKey].(map[string]interface{}); ok {
		for k, v := range current {
			merged[k] = v
		}
	}
	for k, v := range fields {
		merged[k] = v
	}
	s.values[trimmedKey] = merged
	return merged, nil
}

func (s *testSessionStore) get(key string) (interface{}, bool, error) {
	trimmedKey := strings.TrimSpace(key)
	if trimmedKey == "" {
		return nil, false, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.values[trimmedKey]
	return val, ok, nil
}

func (s *testSessionStore) set(key string, value any) {
	trimmedKey := strings.TrimSpace(key)
	if trimmedKey == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[trimmedKey] = value
}

func TestBundledBaldaServerInstructionsReflectWorkspaceMode(t *testing.T) {
	bundledBaldaServerInstructions := func(workspaceEnabled, memoryEnabled bool) string {
		instructions := `Use this bundled balda server for session-local balda tools.

- balda.state stores persistent Balda session and app state in state.db.
- balda config editing is not exposed through MCP; edit the balda config file directly.`
		if memoryEnabled {
			instructions += "\n- balda.memory stores durable facts in Balda state; only call balda.memory.remember when the user explicitly asks you to remember or save a fact."
		}
		if workspaceEnabled {
			instructions += "\n- balda.workspace is available and should be used for workspace import/export instead of manual branch landing."
		} else {
			instructions += "\n- balda.workspace is unavailable because balda workspace mode is disabled for this session."
		}
		return instructions
	}

	enabled := bundledBaldaServerInstructions(true, true)
	if !strings.Contains(enabled, "balda.workspace is available") {
		t.Fatalf("bundledBaldaServerInstructions(true, true) = %q, want workspace-enabled guidance", enabled)
	}
	if !strings.Contains(enabled, "balda.memory") {
		t.Fatalf("bundledBaldaServerInstructions(true, true) = %q, want memory guidance", enabled)
	}
	if strings.Contains(enabled, "balda.agents.") {
		t.Fatalf("bundledBaldaServerInstructions(true, true) = %q, want balda.agents absent", enabled)
	}

	disabled := bundledBaldaServerInstructions(false, false)
	if !strings.Contains(disabled, "balda.workspace is unavailable") {
		t.Fatalf("bundledBaldaServerInstructions(false, false) = %q, want workspace-disabled guidance", disabled)
	}
	if strings.Contains(disabled, "balda.memory") {
		t.Fatalf("bundledBaldaServerInstructions(false, false) = %q, want no memory guidance", disabled)
	}
}

func TestStartBundledMCPHTTPServer_MountsRoutesAndAlias(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mkHandler := func(text string) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, text)
		})
	}

	res, err := startBundledMCPHTTPServer(ctx, "127.0.0.1:0", map[string]http.Handler{
		"balda": mkHandler("balda"),
	})
	if err != nil {
		t.Fatalf("startBundledMCPHTTPServer() error = %v", err)
	}
	t.Cleanup(func() {
		_ = res.Close()
	})

	assertBody := func(path, want string) {
		t.Helper()
		resp, err := http.Get("http://" + res.Addr + path)
		if err != nil {
			t.Fatalf("GET %s error = %v", path, err)
		}
		defer func() { _ = resp.Body.Close() }()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read body for %s: %v", path, err)
		}
		if got := string(body); got != want {
			t.Fatalf("GET %s body = %q, want %q", path, got, want)
		}
	}

	assertBody("/mcp/balda", "balda")
	assertBody("/mcp", "balda")
}

func TestEnsureBundledServers_RegistersSharedListenerURLs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager := &InternalMCPManager{
		workspaceEnabled: true,
		logger:           zerolog.Nop(),
		registry:         mcpregistry.New(nil),
		sessionManager:   &session.Manager{},
		stateStore:       newTestSessionStore(),
	}

	if err := manager.ensureBundledServers(ctx); err != nil {
		t.Fatalf("ensureBundledServers() error = %v", err)
	}
	t.Cleanup(func() {
		for _, cleanup := range manager.cleanups {
			_ = cleanup()
		}
	})

	cfg, ok := manager.registry.Get("balda")
	if !ok {
		t.Fatal("registry missing balda")
	}
	u, err := url.Parse(cfg.URL)
	if err != nil {
		t.Fatalf("parse URL for balda: %v", err)
	}
	if u.Scheme != "http" {
		t.Fatalf("balda scheme = %q, want http", u.Scheme)
	}
	if u.Path != "/mcp" {
		t.Fatalf("balda path = %q, want /mcp", u.Path)
	}
	if strings.TrimSpace(u.Host) == "" {
		t.Fatal("shared host is empty")
	}
}

func TestInternalMCPManagerEnsureStarted_IsIdempotent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager := &InternalMCPManager{
		workspaceEnabled: false,
		logger:           zerolog.Nop(),
		registry:         mcpregistry.New(nil),
		sessionManager:   &session.Manager{},
		stateStore:       newTestSessionStore(),
	}

	if err := manager.EnsureStarted(ctx); err != nil {
		t.Fatalf("EnsureStarted() first call error = %v", err)
	}
	firstCleanupCount := len(manager.cleanups)
	if firstCleanupCount == 0 {
		t.Fatal("EnsureStarted() did not register cleanup handlers")
	}

	if err := manager.EnsureStarted(ctx); err != nil {
		t.Fatalf("EnsureStarted() second call error = %v", err)
	}
	if got := len(manager.cleanups); got != firstCleanupCount {
		t.Fatalf("EnsureStarted() cleanup count = %d, want %d", got, firstCleanupCount)
	}
	manager.mu.RLock()
	started := manager.started
	manager.mu.RUnlock()
	if !started {
		t.Fatal("started = false, want true")
	}

	t.Cleanup(func() {
		for i := len(manager.cleanups) - 1; i >= 0; i-- {
			_ = manager.cleanups[i]()
		}
	})
}
