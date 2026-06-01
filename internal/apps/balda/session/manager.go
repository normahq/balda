package session

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	baldaagent "github.com/normahq/balda/internal/apps/balda/agent"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/git"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
	adksession "google.golang.org/adk/session"
)

const cleanupTimeout = 10 * time.Second

const sessionStatusPersisted = "persisted"

const baldaRuntimeAppName = "norma-balda"

const workspaceSyncSkippedNotice = "Workspace was restored without syncing the latest base changes because auto-sync conflicted. Use balda.workspace.import to retry later."

var ErrNoPersistedSession = errors.New("no persisted session")

type agentBuilder interface {
	CreateRuntimeSession(
		ctx context.Context,
		runtime *baldaagent.BuiltRuntime,
		agentName string,
		userID string,
		sessionID string,
		workspaceDir string,
	) (adksession.Session, error)
	GetAgentMetadata(agentName string) baldaagent.AgentMetadata
}

type baldaRuntimeManager interface {
	Runtime(ctx context.Context) (*baldaagent.BuiltRuntime, error)
	ProviderID() string
}

type AgentMetadata = baldaagent.AgentMetadata

// Manager manages balda provider sessions and persists session metadata.
type Manager struct {
	agentBuilder       agentBuilder
	runtimeManager     baldaRuntimeManager
	baldaMCPServerIDs  []string
	baldaProviderName  string
	workingDir         string
	workspaces         *baldaagent.WorkspaceManager
	workspaceEnabled   bool
	workspaceBaseRef   string
	sessionsPersistent bool
	sessionStore       baldastate.SessionStore
	logger             zerolog.Logger

	mu              sync.RWMutex
	sessions        map[string]*TopicSession
	agentSessionSeq uint64
}

// ManagerParams provides dependencies for Manager.
type ManagerParams struct {
	fx.In

	LC                   fx.Lifecycle
	AgentBuilder         *baldaagent.Builder
	RuntimeManager       *baldaagent.RuntimeManager
	BaldaMCPServerIDs    []string `name:"balda_mcp_servers"`
	BaldaProviderID      string   `name:"balda_provider"`
	WorkingDir           string
	StateDir             string `name:"balda_state_dir"`
	WorkspaceEnabled     bool   `name:"balda_workspace_enabled"`
	WorkspaceSessionsDir string `name:"balda_workspace_sessions_dir"`
	WorkspaceBaseRef     string `name:"balda_workspace_base_branch"`
	SessionsPersistent   bool   `name:"balda_sessions_persistent"`
	StateProvider        baldastate.Provider
	Logger               zerolog.Logger
}

// NewManager creates a session Manager.
func NewManager(p ManagerParams) (*Manager, error) {
	if p.StateProvider == nil {
		return nil, fmt.Errorf("balda state provider is required")
	}

	m := &Manager{
		agentBuilder:       p.AgentBuilder,
		runtimeManager:     p.RuntimeManager,
		baldaMCPServerIDs:  append([]string(nil), p.BaldaMCPServerIDs...),
		baldaProviderName:  strings.TrimSpace(p.BaldaProviderID),
		workingDir:         p.WorkingDir,
		workspaces:         baldaagent.NewWorkspaceManagerWithSessionsDir(p.WorkingDir, p.StateDir, p.WorkspaceBaseRef, p.WorkspaceSessionsDir),
		workspaceEnabled:   p.WorkspaceEnabled,
		workspaceBaseRef:   p.WorkspaceBaseRef,
		sessionsPersistent: p.SessionsPersistent,
		sessionStore:       p.StateProvider.Sessions(),
		logger:             p.Logger.With().Str("component", "balda.session_manager").Logger(),
		sessions:           make(map[string]*TopicSession),
	}

	p.LC.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			m.logger.Info().Str("balda_provider", m.getProviderName()).Msg("session manager ready")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			m.logger.Info().Int("active_sessions", len(m.sessions)).Msg("session manager stopping")
			m.stopAllWithContext(ctx)
			return nil
		},
	})

	return m, nil
}

// GetAgentMetadata returns balda-provider metadata with provider-scoped MCP IDs.
func (m *Manager) GetAgentMetadata(agentName string) AgentMetadata {
	m.mu.RLock()
	builder := m.agentBuilder
	baldaMCPServerIDs := append([]string(nil), m.baldaMCPServerIDs...)
	m.mu.RUnlock()
	if builder == nil {
		return AgentMetadata{}
	}
	meta := builder.GetAgentMetadata(agentName)
	if len(baldaMCPServerIDs) == 0 {
		return meta
	}
	out := make([]string, 0, len(meta.MCPServers)+len(baldaMCPServerIDs))
	seen := make(map[string]struct{}, len(meta.MCPServers)+len(baldaMCPServerIDs))
	appendUnique := func(raw string) {
		id := strings.TrimSpace(raw)
		if id == "" {
			return
		}
		if _, exists := seen[id]; exists {
			return
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	for _, id := range meta.MCPServers {
		appendUnique(id)
	}
	for _, id := range baldaMCPServerIDs {
		appendUnique(id)
	}
	meta.MCPServers = out
	return meta
}

// BaldaProviderID returns the configured balda provider ID.
func (m *Manager) BaldaProviderID() string {
	return m.getProviderName()
}

// CreateSession builds an agent for the given locator and stores it in memory.
func (m *Manager) CreateSession(ctx context.Context, sessionCtx SessionContext, agentName string) error {
	return m.createSession(ctx, sessionCtx, agentName, nil)
}

func (m *Manager) createSession(ctx context.Context, sessionCtx SessionContext, agentName string, persisted *baldastate.SessionRecord) error {
	locator := sessionCtx.Locator
	userID := strings.TrimSpace(sessionCtx.UserID)
	if userID == "" {
		return fmt.Errorf("user id is required")
	}

	sessionID := strings.TrimSpace(locator.SessionID)
	m.mu.RLock()
	builder := m.agentBuilder
	m.mu.RUnlock()
	if builder == nil {
		return fmt.Errorf("agent builder is required")
	}

	m.logger.Info().
		Str("user_id", userID).
		Str("agent", agentName).
		Str("session_id", sessionID).
		Str("channel_type", locator.ChannelType).
		Msg("creating session")

	m.mu.Lock()
	if _, exists := m.sessions[sessionID]; exists {
		m.mu.Unlock()
		m.logger.Warn().Str("session_id", sessionID).Msg("session already exists")
		return fmt.Errorf("session already exists for %s", locator.AddressKey)
	}
	m.mu.Unlock()

	branchName := ""
	workspaceDir := m.workingDir
	startupNotice := ""
	if m.workspaceEnabled {
		branchName = fmt.Sprintf("norma/balda/%s", sessionID)
		canonicalPath := m.workspaces.CanonicalWorkspaceDir(sessionID)
		if persisted != nil {
			if persistedBranch := strings.TrimSpace(persisted.BranchName); persistedBranch != "" {
				branchName = persistedBranch
				if !git.BranchExists(ctx, m.workingDir, branchName) {
					return fmt.Errorf("persisted workspace branch %q not found", branchName)
				}
			}
		}

		workspace, err := m.workspaces.EnsureWorkspace(ctx, sessionID, branchName, canonicalPath)
		if err != nil {
			if errors.Is(err, baldaagent.ErrWorkspaceCollision) {
				m.logger.Warn().
					Err(err).
					Str("session_id", sessionID).
					Str("canonical_workspace", canonicalPath).
					Str("branch", branchName).
					Msg("workspace collision detected; force-remounting canonical workspace path")
				workspace, err = m.workspaces.ForceRemountCanonicalWorkspace(ctx, sessionID, branchName)
			}
			if err != nil {
				m.logger.Error().Err(err).Str("session_id", sessionID).Msg("failed to create workspace")
				return fmt.Errorf("create workspace: %w", err)
			}
		}
		workspaceDir = workspace.Dir
		if workspace.SyncSkipped {
			startupNotice = workspaceSyncSkippedNotice
		}
		m.logger.Debug().Str("session_id", sessionID).Str("workspace", workspaceDir).Msg("workspace created")
	}

	runtimeManager := m.runtimeManager
	if runtimeManager == nil {
		if m.workspaceEnabled {
			_ = m.workspaces.CleanupWorkspace(ctx, workspaceDir)
		}
		return fmt.Errorf("balda runtime manager is required")
	}

	rootRuntime, err := runtimeManager.Runtime(ctx)
	if err != nil {
		if m.workspaceEnabled {
			_ = m.workspaces.CleanupWorkspace(ctx, workspaceDir)
		}
		return err
	}
	baldaProvider := strings.TrimSpace(runtimeManager.ProviderID())
	if baldaProvider == "" {
		baldaProvider = m.getProviderName()
	}

	agentSessionID := m.newAgentSessionID(sessionID)
	if m.sessionsPersistent {
		agentSessionID = sessionID
	}
	sess, err := builder.CreateRuntimeSession(
		ctx,
		rootRuntime,
		baldaProvider,
		userID,
		agentSessionID,
		workspaceDir,
	)
	if err != nil {
		m.logger.Error().
			Err(err).
			Str("session_id", sessionID).
			Str("agent_session_id", agentSessionID).
			Str("agent", baldaProvider).
			Str("label", agentName).
			Msg("failed to create runtime session")
		if m.workspaceEnabled {
			_ = m.workspaces.CleanupWorkspace(ctx, workspaceDir)
		}
		return err
	}

	ts := &TopicSession{
		sessionID:      sessionID,
		agentSessionID: agentSessionID,
		userID:         userID,
		locator:        locator,
		agentName:      agentName,
		agent:          rootRuntime.Agent,
		runner:         rootRuntime.Runner,
		sessionSvc:     rootRuntime.SessionSvc,
		sess:           sess,
		workspaceDir:   workspaceDir,
		branchName:     branchName,
		startupNotice:  startupNotice,
	}

	if err := m.persistSessionRecord(ctx, ts, baldastate.SessionStatusActive); err != nil {
		if closeErr := m.cleanupTopicSession(ctx, ts, sessionCleanupOptions{deleteRuntimeSession: true, cleanupWorkspace: true}); closeErr != nil {
			m.logger.Warn().Err(closeErr).Str("session_id", sessionID).Msg("failed to rollback session after persist error")
		}
		return fmt.Errorf("persist session metadata: %w", err)
	}

	m.mu.Lock()
	m.sessions[sessionID] = ts
	m.mu.Unlock()

	m.logger.Info().
		Str("user_id", userID).
		Str("agent", agentName).
		Str("session_id", sessionID).
		Str("channel_type", locator.ChannelType).
		Msg("session created successfully")

	return nil
}

// TakeStartupNotice returns and clears the pending session startup notice.
func (m *Manager) TakeStartupNotice(sessionID string) string {
	trimmedID := strings.TrimSpace(sessionID)
	if trimmedID == "" {
		return ""
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	ts := m.sessions[trimmedID]
	if ts == nil {
		return ""
	}

	notice := strings.TrimSpace(ts.startupNotice)
	ts.startupNotice = ""
	return notice
}

// StopSession removes a session from memory and cleans up.
func (m *Manager) StopSession(locator SessionLocator) {
	sessionID := strings.TrimSpace(locator.SessionID)
	if m.sessionsPersistent {
		m.logger.Info().
			Str("session_id", sessionID).
			Str("channel_type", locator.ChannelType).
			Str("address_key", locator.AddressKey).
			Msg("suspending persistent session")
		m.removeActiveSession(locator, sessionCleanupOptions{cleanupWorkspace: true})
		return
	}
	m.hardDeleteSession(locator)
}

// ResetSession deletes the conversation history for the current session
// while preserving balda metadata so the same chat can start fresh.
func (m *Manager) ResetSession(ctx context.Context, locator SessionLocator) error {
	sessionID := strings.TrimSpace(locator.SessionID)
	if sessionID == "" {
		return fmt.Errorf("session_id is required")
	}

	m.logger.Info().
		Str("session_id", sessionID).
		Str("channel_type", locator.ChannelType).
		Str("address_key", locator.AddressKey).
		Msg("resetting session")

	if m.removeActiveSession(locator, sessionCleanupOptions{deleteRuntimeSession: true}) {
		return nil
	}

	record, ok, err := m.sessionStore.GetBySessionID(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("read session metadata: %w", err)
	}
	if !ok {
		return fmt.Errorf("session %q not found", sessionID)
	}
	if !m.sessionsPersistent {
		return nil
	}

	runtimeManager := m.runtimeManager
	if runtimeManager == nil {
		return fmt.Errorf("balda runtime manager is required")
	}
	rootRuntime, err := runtimeManager.Runtime(ctx)
	if err != nil {
		return err
	}
	if rootRuntime == nil || rootRuntime.SessionSvc == nil {
		return fmt.Errorf("session service is required")
	}
	userID := strings.TrimSpace(record.UserID)
	if userID == "" {
		return fmt.Errorf("persisted session %q has no user_id", sessionID)
	}
	appName := strings.TrimSpace(rootRuntime.AppName)
	if appName == "" {
		appName = baldaRuntimeAppName
	}
	if err := rootRuntime.SessionSvc.Delete(ctx, &adksession.DeleteRequest{
		AppName:   appName,
		UserID:    userID,
		SessionID: sessionID,
	}); err != nil {
		return fmt.Errorf("delete runtime session: %w", err)
	}
	return nil
}

func (m *Manager) hardDeleteSession(locator SessionLocator) {
	sessionID := strings.TrimSpace(locator.SessionID)

	m.logger.Info().
		Str("session_id", sessionID).
		Str("channel_type", locator.ChannelType).
		Str("address_key", locator.AddressKey).
		Msg("stopping session")

	if !m.removeActiveSession(locator, sessionCleanupOptions{deleteRuntimeSession: true, cleanupWorkspace: true}) {
		m.logger.Warn().Str("session_id", sessionID).Msg("session not found for stop")
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	if err := m.sessionStore.DeleteBySessionID(cleanupCtx, sessionID); err != nil {
		m.logger.Warn().Err(err).Str("session_id", sessionID).Msg("failed to delete persisted session metadata")
	}

	m.logger.Info().Str("session_id", sessionID).Msg("session stopped")
}

func (m *Manager) stopAllWithContext(ctx context.Context) {
	m.mu.Lock()
	sessions := make([]*TopicSession, 0, len(m.sessions))
	for _, ts := range m.sessions {
		sessions = append(sessions, ts)
	}
	m.sessions = make(map[string]*TopicSession)
	m.mu.Unlock()

	m.logger.Info().Int("count", len(sessions)).Msg("stopping all sessions")

	opts := sessionCleanupOptions{deleteRuntimeSession: !m.sessionsPersistent, cleanupWorkspace: true}
	for _, ts := range sessions {
		if err := m.cleanupTopicSession(ctx, ts, opts); err != nil {
			m.logger.Warn().Err(err).Str("session_id", ts.sessionID).Msg("failed to close topic session")
		}
	}

	m.logger.Info().Msg("all sessions stopped")
}

type sessionCleanupOptions struct {
	deleteRuntimeSession bool
	cleanupWorkspace     bool
}

func (m *Manager) removeActiveSession(locator SessionLocator, opts sessionCleanupOptions) bool {
	sessionID := strings.TrimSpace(locator.SessionID)
	m.mu.Lock()
	ts, exists := m.sessions[sessionID]
	if exists {
		delete(m.sessions, sessionID)
	}
	m.mu.Unlock()
	if !exists {
		return false
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	if err := m.cleanupTopicSession(cleanupCtx, ts, opts); err != nil {
		m.logger.Warn().Err(err).Str("session_id", sessionID).Msg("failed to cleanup topic session")
	}
	return true
}

func (m *Manager) cleanupTopicSession(ctx context.Context, ts *TopicSession, opts sessionCleanupOptions) error {
	var firstErr error
	if opts.deleteRuntimeSession && ts != nil && ts.sessionSvc != nil {
		sessionID := strings.TrimSpace(ts.GetAgentSessionID())
		userID := strings.TrimSpace(ts.userID)
		appName := baldaRuntimeAppName
		if ts.sess != nil {
			if sessionAppName := strings.TrimSpace(ts.sess.AppName()); sessionAppName != "" {
				appName = sessionAppName
			}
			if sessionUserID := strings.TrimSpace(ts.sess.UserID()); sessionUserID != "" {
				userID = sessionUserID
			}
		}
		if sessionID != "" && userID != "" {
			if err := ts.sessionSvc.Delete(ctx, &adksession.DeleteRequest{
				AppName:   appName,
				UserID:    userID,
				SessionID: sessionID,
			}); err != nil {
				firstErr = fmt.Errorf("delete runtime session: %w", err)
			}
		}
	}
	if opts.cleanupWorkspace && ts != nil && m.workspaceEnabled && ts.workspaceDir != "" {
		if err := m.workspaces.CleanupWorkspace(ctx, ts.workspaceDir); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (m *Manager) persistSessionRecord(ctx context.Context, ts *TopicSession, status string) error {
	if ts == nil {
		return fmt.Errorf("topic session is required")
	}
	if strings.TrimSpace(status) == "" {
		status = baldastate.SessionStatusActive
	}

	return m.sessionStore.Upsert(ctx, baldastate.SessionRecord{
		SessionID:    ts.sessionID,
		UserID:       ts.userID,
		ChannelType:  ts.locator.ChannelType,
		AddressKey:   ts.locator.AddressKey,
		AddressJSON:  ts.locator.AddressJSON,
		AgentName:    ts.agentName,
		WorkspaceDir: ts.workspaceDir,
		BranchName:   ts.branchName,
		Status:       status,
	})
}

func (m *Manager) newAgentSessionID(sessionID string) string {
	seq := atomic.AddUint64(&m.agentSessionSeq, 1)
	return fmt.Sprintf("%s-a%d", strings.TrimSpace(sessionID), seq)
}
