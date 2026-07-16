package internalmcp

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	actortransport "github.com/baldaworks/go-actorlayer/transport"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/normahq/balda/internal/apps/balda/actorcmd"
	"github.com/normahq/balda/internal/apps/balda/controlcmd"
	"github.com/normahq/balda/internal/apps/balda/controlmcp"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/memory"
	"github.com/normahq/balda/internal/apps/balda/questioncmd"
	"github.com/normahq/balda/internal/apps/balda/questions"
	"github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/sessionmcp"
	"github.com/normahq/balda/internal/apps/workspacemcp"
	"github.com/normahq/runtime/v2/agentconfig"
	"github.com/normahq/runtime/v2/mcpregistry"
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
	shutdowner       fx.Shutdowner
	sessionManager   *session.Manager
	stateStore       sessionmcp.Store
	scheduledJobs    baldastate.ScheduledJobStore
	memoryStore      *memory.Store
	dispatcher       actortransport.Dispatcher
	questionService  *questions.Service
	cleanups         []func() error
}

const (
	bundledBaldaServerID = "balda"

	internalMCPReadHeaderTimeout = 5 * time.Second
	internalMCPIdleTimeout       = 60 * time.Second
)

type internalMCPParams struct {
	fx.In

	WorkspaceEnabled bool `name:"balda_workspace_enabled"`
	Logger           zerolog.Logger
	Registry         *mcpregistry.MapRegistry
	Shutdowner       fx.Shutdowner
	SessionManager   *session.Manager
	StateStore       sessionmcp.Store
	ScheduledJobs    baldastate.ScheduledJobStore
	MemoryStore      *memory.Store
	Dispatcher       actortransport.Dispatcher
	QuestionService  *questions.Service `optional:"true"`
}

// NewInternalMCPManager creates an internal MCP lifecycle manager.
func NewInternalMCPManager(params internalMCPParams) *InternalMCPManager {
	manager := &InternalMCPManager{
		workspaceEnabled: params.WorkspaceEnabled,
		logger:           params.Logger.With().Str("component", "balda.internal_mcp").Logger(),
		registry:         params.Registry,
		shutdowner:       params.Shutdowner,
		sessionManager:   params.SessionManager,
		stateStore:       params.StateStore,
		scheduledJobs:    params.ScheduledJobs,
		memoryStore:      params.MemoryStore,
		dispatcher:       params.Dispatcher,
		questionService:  params.QuestionService,
	}

	return manager
}

// Stop shuts down bundled MCP servers in reverse startup order.
func (m *InternalMCPManager) Stop(context.Context) error {
	m.startMu.Lock()
	defer m.startMu.Unlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	m.logger.Info().Int("cleanups", len(m.cleanups)).Msg("stopping internal MCP servers")
	for i := len(m.cleanups) - 1; i >= 0; i-- {
		if err := m.cleanups[i](); err != nil {
			m.logger.Warn().Err(err).Msg("failed to stop internal MCP server")
		}
	}
	m.cleanups = nil
	m.started = false
	return nil
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
- balda config editing is not exposed through MCP; edit the balda config file directly.
- balda.control.shutdown gracefully stops the whole Balda process; use it only when the user explicitly asks for restart or shutdown. After installing a new override binary, prefer balda.control.shutdown for restart. Use kill -TERM 1 only as a fallback when the in-process shutdown path is unavailable or broken.`
	if m.memoryStore.MemoryEnabled() {
		instructions += "\n- balda.memory stores durable facts in Balda state; only call balda.memory.remember when the user explicitly asks you to remember or save a fact."
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

	sessionmcp.RegisterTools(server, m.stateStore, sessionWaitService{dispatcher: m.dispatcher, jobs: m.scheduledJobs}, sessionQuestionService{service: m.questionService, dispatcher: m.dispatcher})
	memory.RegisterTools(server, m.memoryStore)
	controlmcp.RegisterTools(server, m.shutdowner, m.dispatcher)

	if m.workspaceEnabled {
		workspaceSvc := session.NewWorkspaceService(m.sessionManager)
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

type sessionWaitService struct {
	dispatcher actortransport.Dispatcher
	jobs       baldastate.ScheduledJobStore
}

type sessionQuestionService struct {
	service    *questions.Service
	dispatcher actortransport.Dispatcher
}

func (s sessionWaitService) ScheduleSessionWait(ctx context.Context, in sessionmcp.SessionWaitInput) error {
	locator, err := session.NewSessionLocator(
		in.Locator.ChannelType,
		in.Locator.AddressKey,
		in.Locator.AddressJSON,
		in.Locator.SessionID,
	)
	if err != nil {
		return err
	}
	env, err := controlcmd.ScheduleWaitEnvelope(locator, in.JobID, in.Content, in.DelaySeconds, in.RequestedBy, in.Notify)
	if err != nil {
		return err
	}
	if _, err := s.dispatcher.Dispatch(ctx, env); err != nil {
		return err
	}
	return nil
}

func (s sessionWaitService) ListSessionWaits(ctx context.Context, locator sessionmcp.SessionLocatorInput) ([]sessionmcp.SessionWaitListItem, error) {
	if s.jobs == nil {
		return nil, fmt.Errorf("scheduled job store is required")
	}
	records, err := s.jobs.ListByAddress(ctx, locator.ChannelType, locator.AddressKey)
	if err != nil {
		return nil, err
	}
	items := make([]sessionmcp.SessionWaitListItem, 0, len(records))
	for _, record := range records {
		if strings.TrimSpace(record.ScheduleSpec) != "@once" {
			continue
		}
		if strings.TrimSpace(record.SessionID) != strings.TrimSpace(locator.SessionID) && strings.TrimSpace(locator.SessionID) != "" {
			continue
		}
		items = append(items, sessionmcp.SessionWaitListItem{
			JobID:        record.JobID,
			Content:      record.Content,
			Status:       record.Status,
			ScheduleSpec: record.ScheduleSpec,
			Timezone:     record.Timezone,
			NextRunAt:    formatRFC3339(record.NextRunAt),
			CreatedAt:    formatRFC3339(record.CreatedAt),
			UpdatedAt:    formatRFC3339(record.UpdatedAt),
			LastError:    record.LastError,
		})
	}
	return items, nil
}

func (s sessionQuestionService) AskSessionQuestion(ctx context.Context, in sessionmcp.SessionQuestionInput) (sessionmcp.SessionQuestionOutput, error) {
	if s.service == nil {
		return sessionmcp.SessionQuestionOutput{}, fmt.Errorf("session question service is required")
	}
	options := make([]questions.SessionOption, 0, len(in.Options))
	for _, option := range in.Options {
		options = append(options, questions.SessionOption{
			ID:    strings.TrimSpace(option.ID),
			Label: strings.TrimSpace(option.Label),
		})
	}
	req := questions.SessionRequest{
		Interaction: questioncmd.InteractionContext{
			SessionID:   strings.TrimSpace(in.Locator.SessionID),
			ChannelKind: strings.TrimSpace(in.Locator.ChannelType),
			Locator: deliverycmd.Locator{
				SessionID:   strings.TrimSpace(in.Locator.SessionID),
				ChannelType: strings.TrimSpace(in.Locator.ChannelType),
				AddressKey:  strings.TrimSpace(in.Locator.AddressKey),
				AddressJSON: strings.TrimSpace(in.Locator.AddressJSON),
			},
			RequestedBy: questioncmd.UserRef{UserID: strings.TrimSpace(in.RequestedBy)},
		},
		Resume: questioncmd.ResumeTarget{
			To: actorcmd.ActorTypeSession + ":" + strings.TrimSpace(in.Locator.SessionID),
		},
		Prompt:          strings.TrimSpace(in.Prompt),
		Options:         options,
		DefaultOptionID: strings.TrimSpace(in.DefaultOptionID),
		AllowFreeText:   in.AllowFreeText,
		Timeout:         time.Duration(in.TimeoutSeconds) * time.Second,
		Metadata:        in.Metadata,
	}
	if in.Private && strings.TrimSpace(in.RequestedBy) != "" {
		req.Audience = deliverycmd.QuestionAudience{
			Visibility: deliverycmd.QuestionVisibilityPrivate,
			UserID:     strings.TrimSpace(in.RequestedBy),
		}
	}
	result, err := s.service.AskSession(ctx, s.dispatcher, req)
	if err != nil {
		return sessionmcp.SessionQuestionOutput{}, err
	}
	return sessionmcp.SessionQuestionOutput{
		ToolOutcome: sessionmcp.ToolOutcome{OK: true},
		QuestionID:  result.QuestionID,
		OptionID:    result.OptionID,
		Text:        result.Text,
		Source:      result.Source,
		TimedOut:    result.TimedOut,
		Canceled:    result.Canceled,
	}, nil
}

func (s sessionWaitService) CancelSessionWait(ctx context.Context, locator sessionmcp.SessionLocatorInput, jobID string) (bool, error) {
	if s.jobs == nil {
		return false, fmt.Errorf("scheduled job store is required")
	}
	record, ok, err := s.jobs.GetByID(ctx, jobID)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	if strings.TrimSpace(record.ScheduleSpec) != "@once" {
		return false, nil
	}
	if strings.TrimSpace(record.ChannelType) != strings.TrimSpace(locator.ChannelType) || strings.TrimSpace(record.AddressKey) != strings.TrimSpace(locator.AddressKey) {
		return false, nil
	}
	if strings.TrimSpace(locator.SessionID) != "" && strings.TrimSpace(record.SessionID) != strings.TrimSpace(locator.SessionID) {
		return false, nil
	}
	if err := s.jobs.Delete(ctx, jobID); err != nil {
		return false, err
	}
	return true, nil
}

func formatRFC3339(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
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
