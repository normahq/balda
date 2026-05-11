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
	"github.com/normahq/relay/internal/apps/relay/auth"
	relaytelegram "github.com/normahq/relay/internal/apps/relay/channel/telegram"
	"github.com/normahq/relay/internal/apps/relay/messenger"
	"github.com/normahq/relay/internal/apps/relay/session"
	"github.com/normahq/relay/internal/apps/sessionmcp"
	"github.com/normahq/relay/internal/apps/workspacemcp"
	"github.com/normahq/runtime/agentconfig"
	"github.com/normahq/runtime/mcpregistry"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

// InternalMCPManager controls startup/shutdown of internal MCP servers configured for relay.
type InternalMCPManager struct {
	workspaceEnabled bool
	started          bool
	mu               sync.RWMutex
	startMu          sync.Mutex
	logger           zerolog.Logger
	registry         mcpregistry.Registry
	workingDir       string
	sessionManager   *session.Manager
	channel          *relaytelegram.Adapter
	messenger        *messenger.Messenger
	ownerStore       *auth.OwnerStore
	stateStore       sessionmcp.Store
	cleanups         []func() error
}

const (
	bundledRelayServerID = "relay"

	internalMCPReadHeaderTimeout = 5 * time.Second
	internalMCPIdleTimeout       = 60 * time.Second
)

func bundledRelayServerInstructions(workspaceEnabled bool) string {
	instructions := `Use this bundled relay server for session-local relay tools.

- relay.state stores persistent relay session and app state in relay.db.
- relay config editing is not exposed through MCP; edit the relay config file directly.`
	if workspaceEnabled {
		instructions += "\n- relay.workspace is available and should be used for workspace import/export instead of manual branch landing."
	} else {
		instructions += "\n- relay.workspace is unavailable because relay workspace mode is disabled for this session."
	}
	return instructions
}

type internalMCPParams struct {
	fx.In

	LC               fx.Lifecycle
	WorkspaceEnabled bool `name:"relay_workspace_enabled"`
	Logger           zerolog.Logger
	Registry         *mcpregistry.MapRegistry
	WorkingDir       string
	SessionManager   *session.Manager
	Channel          *relaytelegram.Adapter
	Messenger        *messenger.Messenger
	OwnerStore       *auth.OwnerStore
	StateStore       sessionmcp.Store
}

// NewInternalMCPManager creates an internal MCP lifecycle manager.
func NewInternalMCPManager(params internalMCPParams) *InternalMCPManager {
	manager := &InternalMCPManager{
		workspaceEnabled: params.WorkspaceEnabled,
		logger:           params.Logger.With().Str("component", "relay.internal_mcp").Logger(),
		registry:         params.Registry,
		workingDir:       params.WorkingDir,
		sessionManager:   params.SessionManager,
		channel:          params.Channel,
		messenger:        params.Messenger,
		ownerStore:       params.OwnerStore,
		stateStore:       params.StateStore,
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
		return fmt.Errorf("relay state store is required")
	}

	server := mcp.NewServer(
		&mcp.Implementation{
			Name:    "relay",
			Version: "1.0.0",
		},
		&mcp.ServerOptions{Instructions: bundledRelayServerInstructions(m.workspaceEnabled)},
	)

	sessionmcp.RegisterTools(server, m.stateStore)

	if m.workspaceEnabled {
		workspaceSvc := session.NewWorkspaceMCPServer(m.sessionManager)
		workspacemcp.RegisterTools(server, workspaceSvc)
	} else {
		m.logger.Info().Msg("workspace mode disabled; skipping bundled workspace server")
	}

	handlersByID := map[string]http.Handler{
		bundledRelayServerID: streamableHandlerForServer(server),
	}
	routes := []string{"/mcp", bundledRoutePath(bundledRelayServerID)}

	res, err := startBundledMCPHTTPServer(ctx, "127.0.0.1:0", handlersByID)
	if err != nil {
		return fmt.Errorf("start bundled MCP listener: %w", err)
	}
	m.addCleanup(res.Close)

	m.registry.Set(bundledRelayServerID, agentconfig.MCPServerConfig{
		Type: agentconfig.MCPServerTypeHTTP,
		URL:  bundledRegistryURL(res.Addr, bundledRelayServerID),
	})

	sort.Strings(routes)
	m.logger.Info().
		Str("addr", res.Addr).
		Str("routes", strings.Join(routes, ", ")).
		Msg("bundled MCP listener started")

	return nil
}

func streamableHandlerForServer(server *mcp.Server) http.Handler {
	return mcp.NewStreamableHTTPHandler(func(_ *http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{})
}

func bundledRoutePath(serverID string) string {
	return "/mcp/" + serverID
}

func bundledRegistryURL(addr, serverID string) string {
	if serverID == bundledRelayServerID {
		return fmt.Sprintf("http://%s/mcp", addr)
	}
	return fmt.Sprintf("http://%s%s", addr, bundledRoutePath(serverID))
}

type bundledHTTPServerResult struct {
	Addr  string
	Close func() error

	server *http.Server
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
		mux.Handle(bundledRoutePath(id), handler)
		if id == bundledRelayServerID {
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
		Addr:   listener.Addr().String(),
		server: httpServer,
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

func isBundled(id string) bool {
	switch id {
	case bundledRelayServerID:
		return true
	default:
		return false
	}
}

// Started reports whether internal MCP startup hook has completed.
func (m *InternalMCPManager) Started() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.started
}
