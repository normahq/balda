package handlers

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/normahq/balda/internal/apps/balda/memory"
	"github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/sessionmcp"
	"github.com/normahq/balda/internal/apps/workspacemcp"
	"github.com/normahq/norma/pkg/runtime/agentconfig"
	"github.com/normahq/norma/pkg/runtime/mcpregistry"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

// InternalMCPManager controls startup/shutdown of internal MCP servers configured for balda.
type InternalMCPManager struct {
	workspaceEnabled bool
	started          bool
	mu               sync.RWMutex
	startMu          sync.Mutex
	logger           zerolog.Logger
	registry         mcpregistry.Registry
	sessionManager   *session.Manager
	stateStore       sessionmcp.Store
	memoryStore      *memory.Store
	cleanups         []func() error
}

const (
	bundledBaldaServerID = "balda"

	internalMCPReadHeaderTimeout = 5 * time.Second
	internalMCPIdleTimeout       = 60 * time.Second
)

type internalMCPParams struct {
	fx.In

	LC               fx.Lifecycle
	WorkspaceEnabled bool `name:"balda_workspace_enabled"`
	Logger           zerolog.Logger
	Registry         *mcpregistry.MapRegistry
	SessionManager   *session.Manager
	StateStore       sessionmcp.Store
	MemoryStore      *memory.Store
}

// NewInternalMCPManager creates an internal MCP lifecycle manager.
func NewInternalMCPManager(params internalMCPParams) *InternalMCPManager {
	manager := &InternalMCPManager{
		workspaceEnabled: params.WorkspaceEnabled,
		logger:           params.Logger.With().Str("component", "balda.internal_mcp").Logger(),
		registry:         params.Registry,
		sessionManager:   params.SessionManager,
		stateStore:       params.StateStore,
		memoryStore:      params.MemoryStore,
	}

	params.LC.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return manager.EnsureStarted(ctx)
		},
		OnStop: func(ctx context.Context) error {
			manager.mu.Lock()
			defer manager.mu.Unlock()

			manager.logger.Info().Int("cleanups", len(manager.cleanups)).Msg("stopping internal MCP servers")
			for i := len(manager.cleanups) - 1; i >= 0; i-- {
				if err := manager.cleanups[i](); err != nil {
					manager.logger.Warn().Err(err).Msg("failed to stop internal MCP server")
				}
			}
			manager.cleanups = nil
			manager.started = false
			return nil
		},
	})

	return manager
}

// EnsureStarted initializes bundled MCP servers exactly once.
func (m *InternalMCPManager) EnsureStarted(ctx context.Context) error {
	m.startMu.Lock()
	defer m.startMu.Unlock()

	m.mu.RLock()
	if m.started {
		m.mu.RUnlock()
		return nil
	}
	m.mu.RUnlock()

	m.logger.Info().Msg("starting bundled internal MCP servers")
	if err := m.ensureBundledServers(ctx); err != nil {
		return fmt.Errorf("ensuring bundled servers: %w", err)
	}

	m.mu.Lock()
	m.started = true
	m.mu.Unlock()
	return nil
}

func (m *InternalMCPManager) ensureBundledServers(ctx context.Context) error {
	if m.stateStore == nil {
		return fmt.Errorf("balda state store is required")
	}
	instructions := `Use this bundled balda server for session-local balda tools.

- balda.state stores persistent Balda session and app state in state.db.
- balda config editing is not exposed through MCP; edit the balda config file directly.`
	if m.memoryStore.MemoryEnabled() {
		instructions += "\n- balda.memory stores durable facts in MEMORY.md; only call balda.memory.remember when the user explicitly asks you to remember or save a fact."
	}
	if m.workspaceEnabled {
		instructions += "\n- balda.workspace is available and should be used for workspace import/export instead of manual branch landing."
	} else {
		instructions += "\n- balda.workspace is unavailable because balda workspace mode is disabled for this session."
	}

	server := mcp.NewServer(
		&mcp.Implementation{
			Name:    "balda",
			Version: "1.0.0",
		},
		&mcp.ServerOptions{Instructions: instructions},
	)

	sessionmcp.RegisterTools(server, m.stateStore)
	memory.RegisterTools(server, m.memoryStore)

	if m.workspaceEnabled {
		workspaceSvc := session.NewWorkspaceMCPServer(m.sessionManager)
		workspacemcp.RegisterTools(server, workspaceSvc)
	} else {
		m.logger.Info().Msg("workspace mode disabled; skipping bundled workspace server")
	}

	handlersByID := map[string]http.Handler{
		bundledBaldaServerID: mcp.NewStreamableHTTPHandler(
			func(_ *http.Request) *mcp.Server { return server },
			&mcp.StreamableHTTPOptions{},
		),
	}
	routes := []string{"/mcp", "/mcp/" + bundledBaldaServerID}

	res, err := startBundledMCPHTTPServer(ctx, "127.0.0.1:0", handlersByID)
	if err != nil {
		return fmt.Errorf("start bundled MCP listener: %w", err)
	}
	m.addCleanup(res.Close)

	m.registry.Set(bundledBaldaServerID, agentconfig.MCPServerConfig{
		Type: agentconfig.MCPServerTypeHTTP,
		URL:  fmt.Sprintf("http://%s/mcp", res.Addr),
	})

	sort.Strings(routes)
	m.logger.Info().
		Str("addr", res.Addr).
		Str("routes", strings.Join(routes, ", ")).
		Msg("bundled MCP listener started")

	return nil
}

type bundledHTTPServerResult struct {
	Addr  string
	Close func() error
}

func startBundledMCPHTTPServer(ctx context.Context, addr string, handlersByID map[string]http.Handler) (*bundledHTTPServerResult, error) {
	mux := http.NewServeMux()

	ids := make([]string, 0, len(handlersByID))
	for id := range handlersByID {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		handler := handlersByID[id]
		mux.Handle("/mcp/"+id, handler)
		if id == bundledBaldaServerID {
			mux.Handle("/mcp", handler)
		}
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %q: %w", addr, err)
	}

	httpServer := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: internalMCPReadHeaderTimeout,
		IdleTimeout:       internalMCPIdleTimeout,
	}

	go func() {
		<-ctx.Done()
		_ = httpServer.Close()
	}()

	go func() {
		_ = httpServer.Serve(listener)
	}()

	return &bundledHTTPServerResult{
		Addr: listener.Addr().String(),
		Close: func() error {
			return httpServer.Close()
		},
	}, nil
}

func (m *InternalMCPManager) addCleanup(f func() error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanups = append(m.cleanups, f)
}
