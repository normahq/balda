package session

import (
	"context"
	"fmt"
	"strings"

	baldastate "github.com/normahq/balda/internal/apps/balda/state"
)

// GetSession returns the in-memory session for the given locator.
func (m *Manager) GetSession(locator SessionLocator) (*TopicSession, error) {
	sessionID := strings.TrimSpace(locator.SessionID)

	m.mu.RLock()
	ts := m.sessions[sessionID]
	activeSessions := len(m.sessions)
	m.mu.RUnlock()

	if ts == nil {
		m.logger.Debug().
			Str("session_id", sessionID).
			Str("channel_type", locator.ChannelType).
			Str("address_key", locator.AddressKey).
			Int("active_sessions", activeSessions).
			Msg("session not found")
		return nil, fmt.Errorf("no session for %s", locator.AddressKey)
	}

	return ts, nil
}

// EnsureSession returns the existing session or creates a new one if it doesn't exist.
func (m *Manager) EnsureSession(ctx context.Context, sessionCtx SessionContext, agentName string) (*TopicSession, error) {
	sessionID := strings.TrimSpace(sessionCtx.Locator.SessionID)

	m.mu.RLock()
	ts := m.sessions[sessionID]
	m.mu.RUnlock()

	if ts != nil {
		m.logger.Debug().Str("session_id", sessionID).Msg("returning existing session")
		return ts, nil
	}

	if err := m.CreateSession(ctx, sessionCtx, agentName); err != nil {
		return nil, err
	}
	return m.GetSession(sessionCtx.Locator)
}

// RestoreSession restores a session from persisted metadata when it is not active in memory.
func (m *Manager) RestoreSession(ctx context.Context, sessionCtx SessionContext) (*TopicSession, error) {
	locator := sessionCtx.Locator
	sessionID := strings.TrimSpace(locator.SessionID)

	m.mu.RLock()
	if ts := m.sessions[sessionID]; ts != nil {
		m.mu.RUnlock()
		return ts, nil
	}
	m.mu.RUnlock()

	record, ok, err := m.sessionStore.GetByAddress(ctx, locator.ChannelType, locator.AddressKey)
	if err != nil {
		return nil, fmt.Errorf("read session metadata: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("%w for %s", ErrNoPersistedSession, locator.AddressKey)
	}
	if strings.TrimSpace(record.Status) != "" && record.Status != baldastate.SessionStatusActive {
		return nil, fmt.Errorf("persisted session for %s is not active", locator.AddressKey)
	}

	recordLocator, err := LocatorFromRecord(record)
	if err != nil {
		return nil, fmt.Errorf("decode persisted session locator: %w", err)
	}
	sessionLabel := strings.TrimSpace(record.AgentName)
	if sessionLabel == "" {
		sessionLabel = "auto"
	}

	m.logger.Info().
		Str("session_id", sessionID).
		Str("channel_type", recordLocator.ChannelType).
		Str("address_key", recordLocator.AddressKey).
		Str("label", sessionLabel).
		Msg("restoring session from persisted metadata")

	restoredCtx := SessionContext{
		Locator: recordLocator,
		UserID:  sessionCtx.UserID,
	}
	if userID := strings.TrimSpace(record.UserID); userID != "" {
		restoredCtx.UserID = userID
	}
	if err := m.createSession(ctx, restoredCtx, sessionLabel, &record); err != nil {
		return nil, err
	}
	return m.GetSession(recordLocator)
}

type TopicSessionInfo struct {
	SessionID    string
	UserID       string
	Locator      SessionLocator
	ChannelType  string
	AgentName    string
	WorkspaceDir string
	BranchName   string
	Status       string
}

func (m *Manager) GetSessionInfo(ctx context.Context, sessionID string) (TopicSessionInfo, error) {
	trimmedID := strings.TrimSpace(sessionID)
	if trimmedID == "" {
		return TopicSessionInfo{}, fmt.Errorf("session_id is required")
	}

	m.mu.RLock()
	ts := m.sessions[trimmedID]
	m.mu.RUnlock()
	if ts != nil {
		return TopicSessionInfo{
			SessionID:    ts.sessionID,
			UserID:       ts.userID,
			Locator:      ts.locator,
			ChannelType:  ts.locator.ChannelType,
			AgentName:    ts.agentName,
			WorkspaceDir: ts.workspaceDir,
			BranchName:   ts.branchName,
			Status:       baldastate.SessionStatusActive,
		}, nil
	}

	record, ok, err := m.sessionStore.GetBySessionID(ctx, trimmedID)
	if err != nil {
		return TopicSessionInfo{}, fmt.Errorf("read session metadata: %w", err)
	}
	if !ok {
		return TopicSessionInfo{}, fmt.Errorf("session %q not found", trimmedID)
	}

	locator, err := LocatorFromRecord(record)
	if err != nil {
		return TopicSessionInfo{}, fmt.Errorf("decode persisted session locator for %q: %w", record.SessionID, err)
	}
	info := TopicSessionInfo{
		SessionID:    record.SessionID,
		UserID:       record.UserID,
		Locator:      locator,
		ChannelType:  locator.ChannelType,
		AgentName:    record.AgentName,
		WorkspaceDir: record.WorkspaceDir,
		BranchName:   record.BranchName,
	}
	if strings.TrimSpace(record.Status) == "" || record.Status == baldastate.SessionStatusActive {
		info.Status = sessionStatusPersisted
	} else {
		info.Status = record.Status
	}
	return info, nil
}

func (m *Manager) getProviderName() string {
	if m.runtimeManager != nil {
		if providerID := strings.TrimSpace(m.runtimeManager.ProviderID()); providerID != "" {
			return providerID
		}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.baldaProviderName
}
