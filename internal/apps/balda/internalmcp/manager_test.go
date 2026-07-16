package internalmcp

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

	"github.com/baldaworks/go-actorlayer"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	"github.com/normahq/balda/internal/apps/balda/controlcmd"
	"github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/sessionmcp"
	"github.com/normahq/runtime/v2/mcpregistry"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

type recordingDispatcher struct {
	envelopes []actorlayer.Envelope
}

func (d *recordingDispatcher) Dispatch(_ context.Context, env actorlayer.Envelope) (*actortransport.DispatchReceipt, error) {
	d.envelopes = append(d.envelopes, env)
	return &actortransport.DispatchReceipt{}, nil
}

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
- balda config editing is not exposed through MCP; edit the balda config file directly.
- balda.control.shutdown gracefully stops the whole Balda process; use it only when the user explicitly asks for restart or shutdown. After installing a new override binary, prefer balda.control.shutdown for restart. Use kill -TERM 1 only as a fallback when the in-process shutdown path is unavailable or broken.`
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

func TestCanonicalSessionLocatorReconstructsTelegramAddress(t *testing.T) {
	t.Parallel()

	got, err := canonicalSessionLocator(sessionmcp.SessionLocatorInput{
		SessionID:   "tg-2317500-536036",
		ChannelType: "telegram",
		AddressKey:  "2317500:536036",
	})
	if err != nil {
		t.Fatalf("canonicalSessionLocator() error = %v", err)
	}
	if got.AddressJSON != `{"chat_id":2317500,"topic_id":536036}` {
		t.Fatalf("address_json = %q, want canonical Telegram topic", got.AddressJSON)
	}
}

func TestCanonicalSessionLocatorRejectsSessionMismatch(t *testing.T) {
	t.Parallel()

	_, err := canonicalSessionLocator(sessionmcp.SessionLocatorInput{
		SessionID:   "tg-2317500-1",
		ChannelType: "telegram",
		AddressKey:  "2317500:536036",
	})
	if err == nil || !strings.Contains(err.Error(), "locator mismatch") {
		t.Fatalf("canonicalSessionLocator() error = %v, want mismatch", err)
	}
}

func TestScheduleSessionWaitReconstructsMissingAddressJSON(t *testing.T) {
	t.Parallel()

	dispatcher := &recordingDispatcher{}
	service := sessionWaitService{dispatcher: dispatcher}
	err := service.ScheduleSessionWait(context.Background(), sessionmcp.SessionWaitInput{
		Locator: sessionmcp.SessionLocatorInput{
			SessionID:   "tg-2317500-536036",
			ChannelType: "telegram",
			AddressKey:  "2317500:536036",
		},
		Content:      "wake me",
		DelaySeconds: 60,
	})
	if err != nil {
		t.Fatalf("ScheduleSessionWait() error = %v", err)
	}
	if len(dispatcher.envelopes) != 1 {
		t.Fatalf("dispatched envelopes = %d, want 1", len(dispatcher.envelopes))
	}
	var payload controlcmd.Payload
	if err := actorlayer.UnmarshalPayload(dispatcher.envelopes[0].Payload, &payload); err != nil {
		t.Fatalf("decode control payload: %v", err)
	}
	if got, want := payload.Locator.AddressJSON, `{"chat_id":2317500,"topic_id":536036}`; got != want {
		t.Fatalf("address_json = %q, want %q", got, want)
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
		shutdowner:       noopShutdowner{},
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
		shutdowner:       noopShutdowner{},
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

type noopShutdowner struct{}

func (noopShutdowner) Shutdown(...fx.ShutdownOption) error {
	return nil
}
