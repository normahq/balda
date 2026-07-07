package session

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/runner"
	"google.golang.org/adk/v2/session"
)

// TopicSession represents a single channel session's provider-backed agent session.
type TopicSession struct {
	sessionID      string
	agentSessionID string
	userID         string
	locator        SessionLocator
	agentName      string
	agent          agent.Agent
	runner         *runner.Runner
	sessionSvc     session.Service
	sess           session.Session
	workspaceDir   string
	branchName     string
	startupNotice  string
}

func (s *TopicSession) GetRunner() *runner.Runner {
	return s.runner
}

func (s *TopicSession) GetSessionID() string {
	return s.sessionID
}

func (s *TopicSession) GetAgentSessionID() string {
	agentSessionID := strings.TrimSpace(s.agentSessionID)
	if agentSessionID != "" {
		return agentSessionID
	}
	return s.sessionID
}

func (s *TopicSession) GetUserID() string {
	return s.userID
}

func (s *TopicSession) GetWorkspaceDir() string {
	return s.workspaceDir
}

func (s *TopicSession) GetBranchName() string {
	return s.branchName
}

func (s *TopicSession) GetAgentName() string {
	return s.agentName
}

// RuntimeStateValue returns a value from the current persisted runtime session.
func (s *TopicSession) RuntimeStateValue(ctx context.Context, key string) (any, bool, error) {
	if s == nil || s.sess == nil || s.sessionSvc == nil {
		return nil, false, nil
	}
	current, err := s.sessionSvc.Get(ctx, &session.GetRequest{
		AppName:   s.sess.AppName(),
		UserID:    s.sess.UserID(),
		SessionID: s.GetAgentSessionID(),
	})
	if err != nil {
		return nil, false, fmt.Errorf("get runtime session: %w", err)
	}
	if current == nil || current.Session == nil || current.Session.State() == nil {
		return nil, false, nil
	}
	value, err := current.Session.State().Get(strings.TrimSpace(key))
	if err != nil {
		return nil, false, nil
	}
	return value, true, nil
}
