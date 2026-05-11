package handlers

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/normahq/relay/internal/apps/relay/session"
	"github.com/normahq/relay/internal/apps/sessionmcp"
	"github.com/normahq/runtime/mcpregistry"
	"github.com/rs/zerolog"
)

func TestIsBundled(t *testing.T) {
	tests := []struct {
		id   string
		want bool
	}{
		{id: "relay", want: true},
		{id: "norma.tasks", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.id, func(t *testing.T) {
			if got := isBundled(tc.id); got != tc.want {
				t.Fatalf("isBundled(%q) = %t, want %t", tc.id, got, tc.want)
			}
		})
	}
}

func TestBundledRegistryURL(t *testing.T) {
	addr := "127.0.0.1:9010"
	if got := bundledRegistryURL(addr, "relay"); got != "http://127.0.0.1:9010/mcp" {
		t.Fatalf("bundledRegistryURL(relay) = %q, want http://127.0.0.1:9010/mcp", got)
	}
	if got := bundledRoutePath("relay"); got != "/mcp/relay" {
		t.Fatalf("bundledRoutePath(relay) = %q, want /mcp/relay", got)
	}
}

func TestBundledRelayServerInstructionsReflectWorkspaceMode(t *testing.T) {
	enabled := bundledRelayServerInstructions(true)
	if !strings.Contains(enabled, "relay.workspace is available") {
		t.Fatalf("bundledRelayServerInstructions(true) = %q, want workspace-enabled guidance", enabled)
	}
	if strings.Contains(enabled, "relay.agents.") {
		t.Fatalf("bundledRelayServerInstructions(true) = %q, want relay.agents removed", enabled)
	}

	disabled := bundledRelayServerInstructions(false)
	if !strings.Contains(disabled, "relay.workspace is unavailable") {
		t.Fatalf("bundledRelayServerInstructions(false) = %q, want workspace-disabled guidance", disabled)
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
		"relay": mkHandler("relay"),
	})
	if err != nil {
		t.Fatalf("startBundledMCPHTTPServer() error = %v", err)
	}
	if res.server == nil {
		t.Fatal("server is nil")
	}
	if res.server.ReadHeaderTimeout != internalMCPReadHeaderTimeout {
		t.Fatalf("ReadHeaderTimeout = %s, want %s", res.server.ReadHeaderTimeout, internalMCPReadHeaderTimeout)
	}
	if res.server.IdleTimeout != internalMCPIdleTimeout {
		t.Fatalf("IdleTimeout = %s, want %s", res.server.IdleTimeout, internalMCPIdleTimeout)
	}
	if res.server.ReadTimeout != 0 || res.server.WriteTimeout != 0 {
		t.Fatalf("streaming MCP server read/write timeouts = %s/%s, want unset", res.server.ReadTimeout, res.server.WriteTimeout)
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

	assertBody("/mcp/relay", "relay")
	assertBody("/mcp", "relay")
}

func TestEnsureBundledServers_RegistersSharedListenerURLs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workDir := t.TempDir()
	manager := &InternalMCPManager{
		workspaceEnabled: true,
		logger:           zerolog.Nop(),
		registry:         mcpregistry.New(nil),
		workingDir:       workDir,
		sessionManager:   &session.Manager{},
		stateStore:       sessionmcp.NewMemoryStore(),
	}

	if err := manager.ensureBundledServers(ctx); err != nil {
		t.Fatalf("ensureBundledServers() error = %v", err)
	}
	t.Cleanup(func() {
		for _, cleanup := range manager.cleanups {
			_ = cleanup()
		}
	})

	cfg, ok := manager.registry.Get("relay")
	if !ok {
		t.Fatal("registry missing relay")
	}
	u, err := url.Parse(cfg.URL)
	if err != nil {
		t.Fatalf("parse URL for relay: %v", err)
	}
	if u.Scheme != "http" {
		t.Fatalf("relay scheme = %q, want http", u.Scheme)
	}
	if u.Path != "/mcp" {
		t.Fatalf("relay path = %q, want /mcp", u.Path)
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
		workingDir:       t.TempDir(),
		sessionManager:   &session.Manager{},
		stateStore:       sessionmcp.NewMemoryStore(),
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
	if !manager.Started() {
		t.Fatal("Started() = false, want true")
	}

	t.Cleanup(func() {
		for i := len(manager.cleanups) - 1; i >= 0; i-- {
			_ = manager.cleanups[i]()
		}
	})
}
