package session

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	baldaagent "github.com/normahq/balda/internal/apps/balda/agent"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/rs/zerolog"
	adksession "google.golang.org/adk/v2/session"
)

func TestStopAllWithContext_CleansWorkspaceWhenRootContextCanceled(t *testing.T) {
	ctx := context.Background()
	workingDir := t.TempDir()
	initGitRepo(t, ctx, workingDir)

	writeFile(t, filepath.Join(workingDir, "seed.txt"), "seed\n")
	runGit(t, ctx, workingDir, "add", "seed.txt")
	runGit(t, ctx, workingDir, "commit", "-m", "chore: seed")

	workspaceDir := filepath.Join(t.TempDir(), "balda-workspace")
	runGit(t, ctx, workingDir, "worktree", "add", "-b", "norma/balda/tg-1-1", workspaceDir, "HEAD")

	m := &Manager{
		workspaces:       baldaagent.NewWorkspaceManagerWithSessionsDir(workingDir, t.TempDir(), "master", ""),
		workspaceEnabled: true,
		logger:           zerolog.Nop(),
		sessions: map[string]*TopicSession{
			"tg-1-1": {
				sessionID:    "tg-1-1",
				locator:      testTelegramLocator(1, 1),
				workspaceDir: workspaceDir,
			},
		},
	}

	m.stopAllWithContext(context.Background())

	if _, err := os.Stat(workspaceDir); !os.IsNotExist(err) {
		t.Fatalf("workspace still exists after StopAll; stat err = %v", err)
	}
}

func TestStopSession_UsesNonCanceledCleanupContext(t *testing.T) {
	store := &fakeSessionStore{}
	m := &Manager{
		logger:       zerolog.Nop(),
		sessionStore: store,
		sessions: map[string]*TopicSession{
			"tg-10-42": {
				sessionID: "tg-10-42",
				locator:   testTelegramLocator(10, 42),
			},
		},
	}

	m.StopSession(testTelegramLocator(10, 42))

	if store.deletedSessionID != "tg-10-42" {
		t.Fatalf("DeleteBySessionID called with %q, want %q", store.deletedSessionID, "tg-10-42")
	}
	if store.deleteCtxErr != nil {
		t.Fatalf("DeleteBySessionID ctx was canceled: %v", store.deleteCtxErr)
	}
}

func TestStopSession_PersistentModeSuspendsWithoutDeletingMetadata(t *testing.T) {
	store := &fakeSessionStore{}
	m := &Manager{
		logger:             zerolog.Nop(),
		sessionStore:       store,
		sessionsPersistent: true,
		sessions: map[string]*TopicSession{
			"tg-10-42": {
				sessionID: "tg-10-42",
				locator:   testTelegramLocator(10, 42),
			},
		},
	}

	m.StopSession(testTelegramLocator(10, 42))

	if store.deletedSessionID != "" {
		t.Fatalf("DeleteBySessionID called with %q, want no delete", store.deletedSessionID)
	}
	if _, ok := m.sessions["tg-10-42"]; ok {
		t.Fatal("session still active after StopSession")
	}
}

func TestResetSession_DeletesRuntimeHistoryAndPreservesMetadata(t *testing.T) {
	ctx := context.Background()
	store := &fakeSessionStore{}
	svc := adksession.InMemoryService()
	created, err := svc.Create(ctx, &adksession.CreateRequest{
		AppName:   "norma-balda",
		UserID:    "tg-101",
		SessionID: "tg-10-42",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	m := &Manager{
		logger:       zerolog.Nop(),
		sessionStore: store,
		sessions: map[string]*TopicSession{
			"tg-10-42": {
				sessionID:      "tg-10-42",
				agentSessionID: "tg-10-42",
				userID:         "tg-101",
				locator:        testTelegramLocator(10, 42),
				sessionSvc:     svc,
				sess:           created.Session,
			},
		},
	}

	if err := m.ResetSession(ctx, testTelegramLocator(10, 42)); err != nil {
		t.Fatalf("ResetSession() error = %v", err)
	}
	if store.deletedSessionID != "" {
		t.Fatalf("DeleteBySessionID called with %q, want metadata preserved", store.deletedSessionID)
	}
	if _, ok := m.sessions["tg-10-42"]; ok {
		t.Fatal("session still active after ResetSession")
	}
	if _, err := svc.Get(ctx, &adksession.GetRequest{
		AppName:   "norma-balda",
		UserID:    "tg-101",
		SessionID: "tg-10-42",
	}); err == nil {
		t.Fatal("runtime session still exists after ResetSession")
	}
}

func TestGetAgentMetadata_MergesUniqueMCPServers(t *testing.T) {
	m := &Manager{
		agentBuilder: &fakeAgentBuilder{
			agentMetadata: baldaagent.AgentMetadata{
				MCPServers: []string{"balda", "shared"},
			},
		},
		baldaMCPServerIDs: []string{" custom.one ", "shared", "", "custom.two"},
	}

	got := m.GetAgentMetadata("ignored").MCPServers
	want := []string{"balda", "shared", "custom.one", "custom.two"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("GetAgentMetadata().MCPServers = %#v, want %#v", got, want)
	}
}

func TestGetSessionInfo_ReturnsPersistedSession(t *testing.T) {
	store := &fakeSessionStore{
		recordsByID: map[string]baldastate.SessionRecord{
			"tg-10-42": {
				SessionID:    "tg-10-42",
				UserID:       "tg-201",
				ChannelType:  baldastate.ChannelTypeTelegram,
				AddressKey:   "10:42",
				AddressJSON:  `{"chat_id":10,"topic_id":42}`,
				AgentName:    "opencode",
				WorkspaceDir: "/tmp/workspace",
				BranchName:   "norma/balda/tg-10-42",
				Status:       baldastate.SessionStatusActive,
			},
		},
	}

	m := &Manager{
		logger:       zerolog.Nop(),
		sessionStore: store,
		sessions:     map[string]*TopicSession{},
	}

	info, err := m.GetSessionInfo(context.Background(), "tg-10-42")
	if err != nil {
		t.Fatalf("GetSessionInfo() error = %v", err)
	}
	if info.SessionID != "tg-10-42" {
		t.Fatalf("GetSessionInfo() = %+v, want session tg-10-42", info)
	}
	if info.UserID != "tg-201" {
		t.Fatalf("GetSessionInfo() user_id = %q, want tg-201", info.UserID)
	}
	if info.Status != sessionStatusPersisted {
		t.Fatalf("GetSessionInfo() status = %q, want %q", info.Status, sessionStatusPersisted)
	}
}

func TestGetSessionInfo_ReturnsActiveTransportUserID(t *testing.T) {
	m := &Manager{
		logger: zerolog.Nop(),
		sessions: map[string]*TopicSession{
			"tg-10-42": {
				sessionID: "tg-10-42",
				userID:    "tg-101",
				locator:   testTelegramLocator(10, 42),
				agentName: "opencode",
			},
		},
	}

	info, err := m.GetSessionInfo(context.Background(), "tg-10-42")
	if err != nil {
		t.Fatalf("GetSessionInfo() error = %v", err)
	}
	if info.UserID != "tg-101" {
		t.Fatalf("GetSessionInfo() user_id = %q, want tg-101", info.UserID)
	}
}

type fakeAgentBuilder struct {
	agentMetadata                     baldaagent.AgentMetadata
	createRuntimeSessionAgentNames    []string
	createRuntimeSessionUserIDs       []string
	createRuntimeSessionSessionIDs    []string
	createRuntimeSessionWorkspaceDirs []string
	createRuntimeSessionContexts      []baldaagent.RuntimeSessionContext
	createRuntimeSessionErr           error
}

func (f *fakeAgentBuilder) CreateRuntimeSession(
	_ context.Context,
	_ *baldaagent.BuiltRuntime,
	agentName string,
	userID, sessionID, workspaceDir string,
	sessionCtx baldaagent.RuntimeSessionContext,
) (adksession.Session, error) {
	f.createRuntimeSessionAgentNames = append(f.createRuntimeSessionAgentNames, agentName)
	f.createRuntimeSessionUserIDs = append(f.createRuntimeSessionUserIDs, userID)
	f.createRuntimeSessionSessionIDs = append(f.createRuntimeSessionSessionIDs, sessionID)
	f.createRuntimeSessionWorkspaceDirs = append(f.createRuntimeSessionWorkspaceDirs, workspaceDir)
	f.createRuntimeSessionContexts = append(f.createRuntimeSessionContexts, sessionCtx)
	if f.createRuntimeSessionErr != nil {
		return nil, f.createRuntimeSessionErr
	}
	return nil, nil
}

func (f *fakeAgentBuilder) GetAgentMetadata(string) baldaagent.AgentMetadata {
	return f.agentMetadata
}

type fakeBaldaRuntimeManager struct {
	providerID   string
	runtime      *baldaagent.BuiltRuntime
	runtimeErr   error
	runtimeCalls int
}

func (f *fakeBaldaRuntimeManager) Runtime(context.Context) (*baldaagent.BuiltRuntime, error) {
	f.runtimeCalls++
	if f.runtimeErr != nil {
		return nil, f.runtimeErr
	}
	if f.runtime != nil {
		return f.runtime, nil
	}
	return &baldaagent.BuiltRuntime{}, nil
}

func (f *fakeBaldaRuntimeManager) ProviderID() string {
	return f.providerID
}

func TestCreateSession_ReusesSingleRuntimeAndMapsAgentSessions(t *testing.T) {
	builder := &fakeAgentBuilder{}
	runtimeManager := &fakeBaldaRuntimeManager{providerID: "balda-provider"}
	m := &Manager{
		baldaProviderName: "balda-provider",
		runtimeManager:    runtimeManager,
		agentBuilder:      builder,
		workingDir:        t.TempDir(),
		logger:            zerolog.Nop(),
		sessions:          make(map[string]*TopicSession),
		sessionStore:      &fakeSessionStore{},
	}

	first := SessionContext{
		Locator: testTelegramLocator(10, 41),
		UserID:  "tg-201",
	}
	second := SessionContext{
		Locator: testTelegramLocator(10, 42),
		UserID:  "tg-202",
	}

	if err := m.CreateSession(context.Background(), first, "topic-a"); err != nil {
		t.Fatalf("CreateSession(first) error = %v", err)
	}
	if err := m.CreateSession(context.Background(), second, "topic-b"); err != nil {
		t.Fatalf("CreateSession(second) error = %v", err)
	}

	if runtimeManager.runtimeCalls != 2 {
		t.Fatalf("Runtime() calls = %d, want 2", runtimeManager.runtimeCalls)
	}
	if got := len(builder.createRuntimeSessionSessionIDs); got != 2 {
		t.Fatalf("CreateRuntimeSession calls = %d, want 2", got)
	}
	if got := len(builder.createRuntimeSessionContexts); got != 2 {
		t.Fatalf("CreateRuntimeSession contexts = %d, want 2", got)
	}

	firstSessionID := builder.createRuntimeSessionSessionIDs[0]
	secondSessionID := builder.createRuntimeSessionSessionIDs[1]
	if firstSessionID == secondSessionID {
		t.Fatalf("agent session ids are equal (%q), want unique per balda session", firstSessionID)
	}
	if !strings.HasPrefix(firstSessionID, first.Locator.SessionID+"-a") {
		t.Fatalf("first agent session id = %q, want prefix %q", firstSessionID, first.Locator.SessionID+"-a")
	}
	if !strings.HasPrefix(secondSessionID, second.Locator.SessionID+"-a") {
		t.Fatalf("second agent session id = %q, want prefix %q", secondSessionID, second.Locator.SessionID+"-a")
	}
	if got := builder.createRuntimeSessionContexts[0].BaldaSessionID; got != first.Locator.SessionID {
		t.Fatalf("first BaldaSessionID = %q, want %q", got, first.Locator.SessionID)
	}
	if got := builder.createRuntimeSessionContexts[1].BaldaSessionID; got != second.Locator.SessionID {
		t.Fatalf("second BaldaSessionID = %q, want %q", got, second.Locator.SessionID)
	}
}

func TestCreateSession_PersistentModeUsesStableAgentSessionID(t *testing.T) {
	builder := &fakeAgentBuilder{}
	runtimeManager := &fakeBaldaRuntimeManager{providerID: "balda-provider"}
	locator := testTelegramLocator(10, 41)
	m := &Manager{
		baldaProviderName:  "balda-provider",
		runtimeManager:     runtimeManager,
		agentBuilder:       builder,
		workingDir:         t.TempDir(),
		logger:             zerolog.Nop(),
		sessions:           make(map[string]*TopicSession),
		sessionStore:       &fakeSessionStore{},
		sessionsPersistent: true,
	}

	if err := m.CreateSession(context.Background(), SessionContext{
		Locator: locator,
		UserID:  "tg-201",
	}, "topic-a"); err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}

	if got := builder.createRuntimeSessionSessionIDs[0]; got != locator.SessionID {
		t.Fatalf("CreateRuntimeSession sessionID = %q, want stable balda session ID %q", got, locator.SessionID)
	}
	if got := builder.createRuntimeSessionContexts[0].BaldaSessionID; got != locator.SessionID {
		t.Fatalf("CreateRuntimeSession BaldaSessionID = %q, want %q", got, locator.SessionID)
	}
}

func TestCreateSession_UsesBaldaProviderBackend(t *testing.T) {
	builder := &fakeAgentBuilder{}
	runtimeManager := &fakeBaldaRuntimeManager{providerID: "balda-provider"}
	m := &Manager{
		baldaProviderName: "balda-provider",
		runtimeManager:    runtimeManager,
		agentBuilder:      builder,
		logger:            zerolog.Nop(),
		sessions:          make(map[string]*TopicSession),
		sessionStore:      &fakeSessionStore{},
	}

	err := m.CreateSession(context.Background(), SessionContext{
		Locator: testTelegramLocator(10, 42),
		UserID:  "tg-201",
	}, "custom-label")

	if err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}

	if got, want := builder.createRuntimeSessionAgentNames[0], "balda-provider"; got != want {
		t.Fatalf("CreateRuntimeSession provider = %q, want %q", got, want)
	}

	ts := m.sessions[testTelegramLocator(10, 42).SessionID]
	if ts.agentName != "custom-label" {
		t.Fatalf("session label = %q, want %q", ts.agentName, "custom-label")
	}
}

func TestRestoreSession_AlwaysUsesCurrentBaldaProviderBackend(t *testing.T) {
	builder := &fakeAgentBuilder{}
	runtimeManager := &fakeBaldaRuntimeManager{providerID: "new-balda-provider"}
	locator := testTelegramLocator(10, 42)
	store := &fakeSessionStore{
		recordsByAddress: map[string]baldastate.SessionRecord{
			sessionAddressKey(baldastate.ChannelTypeTelegram, "10:42"): {
				SessionID:   locator.SessionID,
				ChannelType: baldastate.ChannelTypeTelegram,
				AddressKey:  "10:42",
				AddressJSON: `{"chat_id":10,"topic_id":42}`,
				AgentName:   "previous-persisted-label",
				Status:      baldastate.SessionStatusActive,
			},
		},
	}

	m := &Manager{
		baldaProviderName: "new-balda-provider",
		runtimeManager:    runtimeManager,
		agentBuilder:      builder,
		logger:            zerolog.Nop(),
		sessions:          make(map[string]*TopicSession),
		sessionStore:      store,
	}

	_, err := m.RestoreSession(context.Background(), SessionContext{
		Locator:                    locator,
		UserID:                     "tg-201",
		AllowBaldaProviderFallback: true,
	})

	if err != nil {
		t.Fatalf("RestoreSession() error = %v", err)
	}

	if got, want := builder.createRuntimeSessionAgentNames[0], "new-balda-provider"; got != want {
		t.Fatalf("CreateRuntimeSession provider = %q, want %q", got, want)
	}

	ts := m.sessions[locator.SessionID]
	if ts.agentName != "previous-persisted-label" {
		t.Fatalf("session label = %q, want %q", ts.agentName, "previous-persisted-label")
	}
}

func TestRestoreSession_UserIDSelection(t *testing.T) {
	tests := []struct {
		name            string
		persistedUserID string
		wantUserID      string
	}{
		{
			name:            "persisted user id",
			persistedUserID: "tg-101",
			wantUserID:      "tg-101",
		},
		{
			name:            "fallback current user id",
			persistedUserID: " ",
			wantUserID:      "tg-202",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := &fakeAgentBuilder{}
			runtimeManager := &fakeBaldaRuntimeManager{providerID: "balda-provider"}
			locator := testTelegramLocator(10, 42)
			store := &fakeSessionStore{
				recordsByAddress: map[string]baldastate.SessionRecord{
					sessionAddressKey(baldastate.ChannelTypeTelegram, "10:42"): {
						SessionID:   locator.SessionID,
						UserID:      tt.persistedUserID,
						ChannelType: baldastate.ChannelTypeTelegram,
						AddressKey:  "10:42",
						AddressJSON: `{"chat_id":10,"topic_id":42}`,
						AgentName:   "persisted-label",
						Status:      baldastate.SessionStatusActive,
					},
				},
			}

			m := &Manager{
				baldaProviderName: "balda-provider",
				runtimeManager:    runtimeManager,
				agentBuilder:      builder,
				logger:            zerolog.Nop(),
				sessions:          make(map[string]*TopicSession),
				sessionStore:      store,
			}

			_, err := m.RestoreSession(context.Background(), SessionContext{
				Locator: locator,
				UserID:  "tg-202",
			})

			if err != nil {
				t.Fatalf("RestoreSession() error = %v", err)
			}
			if got := builder.createRuntimeSessionUserIDs[0]; got != tt.wantUserID {
				t.Fatalf("CreateRuntimeSession userID = %q, want %q", got, tt.wantUserID)
			}
		})
	}
}

func TestRestoreSession_UsesAutoLabelWhenPersistedLabelMissing(t *testing.T) {
	builder := &fakeAgentBuilder{}
	runtimeManager := &fakeBaldaRuntimeManager{providerID: "new-balda-provider"}
	locator := testTelegramLocator(11, 43)
	store := &fakeSessionStore{
		recordsByAddress: map[string]baldastate.SessionRecord{
			sessionAddressKey(baldastate.ChannelTypeTelegram, "11:43"): {
				SessionID:   locator.SessionID,
				ChannelType: baldastate.ChannelTypeTelegram,
				AddressKey:  "11:43",
				AddressJSON: `{"chat_id":11,"topic_id":43}`,
				AgentName:   " ",
				Status:      baldastate.SessionStatusActive,
			},
		},
	}

	m := &Manager{
		baldaProviderName: "new-balda-provider",
		runtimeManager:    runtimeManager,
		agentBuilder:      builder,
		logger:            zerolog.Nop(),
		sessions:          make(map[string]*TopicSession),
		sessionStore:      store,
	}

	_, err := m.RestoreSession(context.Background(), SessionContext{
		Locator: locator,
		UserID:  "tg-201",
	})

	if err != nil {
		t.Fatalf("RestoreSession() error = %v", err)
	}

	ts := m.sessions[locator.SessionID]
	if ts.agentName != "auto" {
		t.Fatalf("session label = %q, want auto", ts.agentName)
	}
}

func TestRestoreSession_FailsWhenPersistedWorkspaceBranchMissing(t *testing.T) {
	ctx := context.Background()
	workingDir := t.TempDir()
	initGitRepo(t, ctx, workingDir)

	writeFile(t, filepath.Join(workingDir, "seed.txt"), "seed\n")
	runGit(t, ctx, workingDir, "add", "seed.txt")
	runGit(t, ctx, workingDir, "commit", "-m", "chore: seed")

	builder := &fakeAgentBuilder{}
	runtimeManager := &fakeBaldaRuntimeManager{providerID: "new-balda-provider"}
	locator := testTelegramLocator(12, 44)
	store := &fakeSessionStore{
		recordsByAddress: map[string]baldastate.SessionRecord{
			sessionAddressKey(baldastate.ChannelTypeTelegram, "12:44"): {
				SessionID:    locator.SessionID,
				ChannelType:  baldastate.ChannelTypeTelegram,
				AddressKey:   "12:44",
				AddressJSON:  `{"chat_id":12,"topic_id":44}`,
				AgentName:    "persisted",
				WorkspaceDir: filepath.Join(t.TempDir(), "missing-workspace"),
				BranchName:   "norma/balda/missing-branch",
				Status:       baldastate.SessionStatusActive,
			},
		},
	}

	m := &Manager{
		baldaProviderName: "new-balda-provider",
		runtimeManager:    runtimeManager,
		agentBuilder:      builder,
		workingDir:        workingDir,
		workspaces:        baldaagent.NewWorkspaceManagerWithSessionsDir(workingDir, t.TempDir(), currentBranch(t, ctx, workingDir), ""),
		workspaceEnabled:  true,
		logger:            zerolog.Nop(),
		sessions:          make(map[string]*TopicSession),
		sessionStore:      store,
	}

	_, err := m.RestoreSession(ctx, SessionContext{
		Locator: locator,
		UserID:  "tg-201",
	})
	if err == nil {
		t.Fatal("RestoreSession() error = nil, want missing persisted branch error")
	}
	if !strings.Contains(err.Error(), `persisted workspace branch "norma/balda/missing-branch" not found`) {
		t.Fatalf("RestoreSession() error = %q, want missing persisted branch context", err.Error())
	}
}

func TestRestoreSession_ForceRemountsCanonicalWorkspaceAfterCollision(t *testing.T) {
	ctx := context.Background()
	workingDir := t.TempDir()
	initGitRepo(t, ctx, workingDir)

	writeFile(t, filepath.Join(workingDir, "seed.txt"), "seed\n")
	runGit(t, ctx, workingDir, "add", "seed.txt")
	runGit(t, ctx, workingDir, "commit", "-m", "chore: seed")

	builder := &fakeAgentBuilder{}
	runtimeManager := &fakeBaldaRuntimeManager{providerID: "new-balda-provider"}
	stateDir := t.TempDir()
	locator := testTelegramLocator(23, 102)
	branchName := "norma/balda/" + locator.SessionID
	canonicalWorkspaceDir := filepath.Join(stateDir, "sessions", locator.SessionID)
	conflictBranch := "feature/conflict-" + locator.SessionID

	runGit(t, ctx, workingDir, "branch", branchName, "HEAD")
	runGit(t, ctx, workingDir, "worktree", "add", "-b", conflictBranch, canonicalWorkspaceDir, "HEAD")
	t.Cleanup(func() {
		_ = runGitAllowError(ctx, workingDir, "worktree", "remove", "--force", canonicalWorkspaceDir)
	})

	store := &fakeSessionStore{
		recordsByAddress: map[string]baldastate.SessionRecord{
			sessionAddressKey(baldastate.ChannelTypeTelegram, locator.AddressKey): {
				SessionID:    locator.SessionID,
				ChannelType:  baldastate.ChannelTypeTelegram,
				AddressKey:   locator.AddressKey,
				AddressJSON:  locator.AddressJSON,
				AgentName:    "persisted",
				WorkspaceDir: canonicalWorkspaceDir,
				BranchName:   branchName,
				Status:       baldastate.SessionStatusActive,
			},
		},
	}

	m := &Manager{
		baldaProviderName: "new-balda-provider",
		runtimeManager:    runtimeManager,
		agentBuilder:      builder,
		workingDir:        workingDir,
		workspaces:        baldaagent.NewWorkspaceManagerWithSessionsDir(workingDir, stateDir, currentBranch(t, ctx, workingDir), ""),
		workspaceEnabled:  true,
		logger:            zerolog.Nop(),
		sessions:          make(map[string]*TopicSession),
		sessionStore:      store,
	}

	ts, err := m.RestoreSession(ctx, SessionContext{
		Locator: locator,
		UserID:  "tg-201",
	})
	if err != nil {
		t.Fatalf("RestoreSession() error = %v", err)
	}

	if got := ts.GetWorkspaceDir(); got != canonicalWorkspaceDir {
		t.Fatalf("workspace dir = %q, want %q", got, canonicalWorkspaceDir)
	}
	if head := strings.TrimSpace(runGit(t, ctx, canonicalWorkspaceDir, "rev-parse", "--abbrev-ref", "HEAD")); head != branchName {
		t.Fatalf("canonical workspace HEAD = %q, want %q", head, branchName)
	}
}

func TestTakeStartupNotice_ReturnsAndClears(t *testing.T) {
	m := &Manager{
		sessions: map[string]*TopicSession{
			"tg-1-1": {
				sessionID:     "tg-1-1",
				startupNotice: "notice text",
			},
		},
	}

	if got := m.TakeStartupNotice("tg-1-1"); got != "notice text" {
		t.Fatalf("TakeStartupNotice(first) = %q, want %q", got, "notice text")
	}
	if got := m.TakeStartupNotice("tg-1-1"); got != "" {
		t.Fatalf("TakeStartupNotice(second) = %q, want empty string", got)
	}
}

func testTelegramLocator(chatID int64, topicID int) SessionLocator {
	address := struct {
		ChatID  int64 `json:"chat_id"`
		TopicID int   `json:"topic_id"`
	}{
		ChatID:  chatID,
		TopicID: topicID,
	}
	raw, _ := json.Marshal(address)
	locator, err := NewSessionLocator(
		baldastate.ChannelTypeTelegram,
		fmt.Sprintf("%d:%d", chatID, topicID),
		string(raw),
		fmt.Sprintf("tg-%d-%d", chatID, topicID),
	)
	if err != nil {
		panic(err)
	}
	return locator
}

type fakeSessionStore struct {
	deletedSessionID string
	deleteCtxErr     error
	getByAddressErr  error
	recordsByAddress map[string]baldastate.SessionRecord
	recordsByID      map[string]baldastate.SessionRecord
	listRecords      []baldastate.SessionRecord
	upsertedRecords  []baldastate.SessionRecord
}

func (f *fakeSessionStore) Upsert(_ context.Context, record baldastate.SessionRecord) error {
	f.upsertedRecords = append(f.upsertedRecords, record)
	return nil
}

func sessionAddressKey(channelType, addressKey string) string {
	return channelType + "|" + addressKey
}

func (f *fakeSessionStore) GetByAddress(_ context.Context, channelType, addressKey string) (baldastate.SessionRecord, bool, error) {
	if f.getByAddressErr != nil {
		return baldastate.SessionRecord{}, false, f.getByAddressErr
	}
	if f.recordsByAddress == nil {
		return baldastate.SessionRecord{}, false, nil
	}
	record, ok := f.recordsByAddress[sessionAddressKey(channelType, addressKey)]
	return record, ok, nil
}

func (f *fakeSessionStore) GetBySessionID(_ context.Context, sessionID string) (baldastate.SessionRecord, bool, error) {
	if f.recordsByID == nil {
		return baldastate.SessionRecord{}, false, nil
	}
	record, ok := f.recordsByID[sessionID]
	return record, ok, nil
}

func (f *fakeSessionStore) DeleteBySessionID(ctx context.Context, sessionID string) error {
	f.deletedSessionID = sessionID
	f.deleteCtxErr = ctx.Err()
	return nil
}

func (f *fakeSessionStore) List(context.Context) ([]baldastate.SessionRecord, error) {
	if f.listRecords == nil {
		return nil, nil
	}
	return append([]baldastate.SessionRecord(nil), f.listRecords...), nil
}

func initGitRepo(t *testing.T, ctx context.Context, workingDir string) {
	t.Helper()
	runGit(t, ctx, workingDir, "init")
	runGit(t, ctx, workingDir, "config", "user.name", "Balda Test")
	runGit(t, ctx, workingDir, "config", "user.email", "balda-test@example.com")
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write file %s: %v", path, err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file %s: %v", path, err)
	}
	return string(data)
}

func runGit(t *testing.T, ctx context.Context, dir string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func runGitAllowError(ctx context.Context, dir string, args ...string) error {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	return cmd.Run()
}

func currentBranch(t *testing.T, ctx context.Context, dir string) string {
	t.Helper()
	return strings.TrimSpace(runGit(t, ctx, dir, "rev-parse", "--abbrev-ref", "HEAD"))
}
