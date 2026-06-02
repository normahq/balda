package handlers

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	baldaagent "github.com/normahq/balda/internal/apps/balda/agent"
	"github.com/normahq/balda/internal/apps/balda/auth"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	"github.com/normahq/balda/internal/apps/balda/messenger"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/rs/zerolog"
	"github.com/tgbotkit/client"
	adksession "google.golang.org/adk/session"
)

type fakeBaldaStartupTGClient struct {
	*fakeTelegramClient

	getMeResp  *client.GetMeResponse
	getMeErr   error
	getMeCalls int
}

const testBaldaStartupBotUsername = "ValeraBot"

func (f *fakeBaldaStartupTGClient) GetMeWithResponse(_ context.Context, _ ...client.RequestEditorFn) (*client.GetMeResponse, error) {
	f.getMeCalls++
	if f.getMeErr != nil {
		return nil, f.getMeErr
	}
	return f.getMeResp, nil
}

func TestBaldaHandlerOnStart_FailsWhenGetMeTransportFails(t *testing.T) {
	handler := newBaldaStartupHandlerForTest(t, &fakeBaldaStartupTGClient{
		getMeErr: errors.New("network timeout"),
	}, "", zerolog.Nop())

	err := handler.onStart(context.Background())
	if err == nil {
		t.Fatal("onStart() error = nil, want startup failure")
	}
	if !strings.Contains(err.Error(), "resolve balda telegram bot identity") {
		t.Fatalf("onStart() error = %q, want bot identity context", err.Error())
	}
	if !strings.Contains(err.Error(), "network timeout") {
		t.Fatalf("onStart() error = %q, want getMe transport error", err.Error())
	}
}

func TestBaldaHandlerOnStart_FailsWhenGetMeUnauthorized(t *testing.T) {
	handler := newBaldaStartupHandlerForTest(t, &fakeBaldaStartupTGClient{
		getMeResp: &client.GetMeResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusUnauthorized, Status: "401 Unauthorized"},
			JSON401: &client.ErrorResponse{
				Description: "Unauthorized",
			},
		},
	}, "", zerolog.Nop())

	err := handler.onStart(context.Background())
	if err == nil {
		t.Fatal("onStart() error = nil, want startup failure")
	}
	if !strings.Contains(err.Error(), "getMe unauthorized") {
		t.Fatalf("onStart() error = %q, want unauthorized context", err.Error())
	}
}

func TestBaldaHandlerOnStart_FailsWhenGetMeUsernameEmpty(t *testing.T) {
	handler := newBaldaStartupHandlerForTest(t, &fakeBaldaStartupTGClient{
		getMeResp: &client.GetMeResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
			JSON200: &struct {
				Ok     client.GetMe200Ok `json:"ok"`
				Result client.User       `json:"result"`
			}{
				Ok: true,
				Result: client.User{
					Id: 42,
				},
			},
		},
	}, "", zerolog.Nop())

	err := handler.onStart(context.Background())
	if err == nil {
		t.Fatal("onStart() error = nil, want startup failure")
	}
	if !strings.Contains(err.Error(), "empty username") {
		t.Fatalf("onStart() error = %q, want empty username error", err.Error())
	}
}

func TestBaldaHandlerOnStart_LoadsBotIdentityWhenGetMeSucceeds(t *testing.T) {
	username := testBaldaStartupBotUsername
	tgClient := &fakeBaldaStartupTGClient{
		getMeResp: &client.GetMeResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
			JSON200: &struct {
				Ok     client.GetMe200Ok `json:"ok"`
				Result client.User       `json:"result"`
			}{
				Ok: true,
				Result: client.User{
					Id:       7791683989,
					Username: &username,
				},
			},
		},
	}
	handler := newBaldaStartupHandlerForTest(t, tgClient, "", zerolog.Nop())

	if err := handler.onStart(context.Background()); err != nil {
		t.Fatalf("onStart() error = %v", err)
	}
	if tgClient.getMeCalls != 1 {
		t.Fatalf("getMe calls = %d, want 1", tgClient.getMeCalls)
	}
	gotBotID, gotUsername := handler.getBotIdentity()
	if gotBotID != 7791683989 {
		t.Fatalf("bot user id = %d, want 7791683989", gotBotID)
	}
	if gotUsername != testBaldaStartupBotUsername {
		t.Fatalf("bot username = %q, want %s", gotUsername, testBaldaStartupBotUsername)
	}
}

func TestBaldaHandlerOnStart_LogsOwnerAuthWhenOwnerUnknown(t *testing.T) {
	username := testBaldaStartupBotUsername
	tgClient := &fakeBaldaStartupTGClient{
		getMeResp: &client.GetMeResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
			JSON200: &struct {
				Ok     client.GetMe200Ok `json:"ok"`
				Result client.User       `json:"result"`
			}{
				Ok: true,
				Result: client.User{
					Id:       7791683989,
					Username: &username,
				},
			},
		},
	}

	var buf bytes.Buffer
	handler := newBaldaStartupHandlerForTest(t, tgClient, "owner-token", zerolog.New(&buf))

	if err := handler.onStart(context.Background()); err != nil {
		t.Fatalf("onStart() error = %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "/start owner=owner-token") {
		t.Fatalf("startup log missing auth command, output=%q", output)
	}
	if !strings.Contains(output, "https://t.me/"+testBaldaStartupBotUsername+"?start=owner_owner-token") {
		t.Fatalf("startup log missing auth link, output=%q", output)
	}
}

func TestBaldaHandlerOnStart_DoesNotLogOwnerAuthWhenOwnerRegistered(t *testing.T) {
	username := testBaldaStartupBotUsername
	tgClient := &fakeBaldaStartupTGClient{
		getMeResp: &client.GetMeResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
			JSON200: &struct {
				Ok     client.GetMe200Ok `json:"ok"`
				Result client.User       `json:"result"`
			}{
				Ok: true,
				Result: client.User{
					Id:       7791683989,
					Username: &username,
				},
			},
		},
	}

	var buf bytes.Buffer
	handler := newBaldaStartupHandlerForTest(t, tgClient, "owner-token", zerolog.New(&buf))
	if _, err := handler.ownerStore.RegisterOwner(42, 0); err != nil {
		t.Fatalf("RegisterOwner() error = %v", err)
	}

	err := handler.onStart(context.Background())
	if err == nil {
		t.Fatal("onStart() error = nil, want missing chat id error")
	}
	if !strings.Contains(err.Error(), "owner.chat_id is required") {
		t.Fatalf("onStart() error = %q, want missing chat id error", err.Error())
	}

	output := buf.String()
	if strings.Contains(output, "/start owner=owner-token") {
		t.Fatalf("startup log unexpectedly included auth command, output=%q", output)
	}
	if strings.Contains(output, "https://t.me/"+testBaldaStartupBotUsername+"?start=owner_owner-token") {
		t.Fatalf("startup log unexpectedly included auth link, output=%q", output)
	}
}

func TestBaldaHandlerOnStart_FailsWhenOwnerBootstrapFails(t *testing.T) {
	handler, _ := newRegisteredOwnerStartupHandler(t)
	sessionManager := &baldasession.Manager{}
	setUnexportedField(t, sessionManager, "agentBuilder", &fakeBaldaStartupFailBuilder{
		metadata: baldaagent.AgentMetadata{
			Type:       "codex_acp",
			Model:      "gpt-5.3-codex",
			MCPServers: []string{"balda"},
		},
		err: errors.New("runtime session creation failed"),
	})
	setUnexportedField(t, sessionManager, "runtimeManager", &fakeBaldaRestoreRuntimeManager{providerID: "balda-provider"})
	setUnexportedField(t, sessionManager, "baldaProviderName", "balda-provider")
	setUnexportedField(t, sessionManager, "sessionStore", &fakeBaldaRestoreSessionStore{})
	setUnexportedField(t, sessionManager, "logger", zerolog.Nop())
	setUnexportedField(t, sessionManager, "sessions", map[string]*baldasession.TopicSession{})
	handler.sessionManager = sessionManager

	err := handler.onStart(context.Background())
	if err == nil {
		t.Fatal("onStart() error = nil, want bootstrap failure")
	}
	if !strings.Contains(err.Error(), "bootstrap owner session during startup") {
		t.Fatalf("onStart() error = %q, want bootstrap failure context", err.Error())
	}
}

func TestBaldaHandlerOnStart_DoesNotFailWhenReadyMessageFails(t *testing.T) {
	handler, tgClient := newRegisteredOwnerStartupHandler(t)
	tgClient.sendErr = errors.New("telegram send failed")

	if err := handler.onStart(context.Background()); err != nil {
		t.Fatalf("onStart() error = %v", err)
	}
}

func TestBootstrapOwnerSession_RestoresPersistedOwnerWorkspaceMetadata(t *testing.T) {
	ctx := context.Background()
	workingDir := t.TempDir()
	initBaldaRestoreGitRepo(t, ctx, workingDir)

	writeBaldaRestoreFile(t, filepath.Join(workingDir, "seed.txt"), "seed\n")
	runBaldaRestoreGit(t, ctx, workingDir, "add", "seed.txt")
	runBaldaRestoreGit(t, ctx, workingDir, "commit", "-m", "chore: seed")

	stateDir := t.TempDir()
	locator := baldatelegram.NewLocator(9001, 0)
	branchName := "persisted/owner-branch"
	workspaceDir := filepath.Join(stateDir, "persisted-owner-workspace")
	canonicalWorkspaceDir := filepath.Join(stateDir, "sessions", locator.SessionID)
	runBaldaRestoreGit(t, ctx, workingDir, "worktree", "add", "-b", branchName, workspaceDir, "HEAD")
	t.Cleanup(func() {
		_ = runBaldaRestoreGitAllowError(ctx, workingDir, "worktree", "remove", "--force", workspaceDir)
		_ = runBaldaRestoreGitAllowError(ctx, workingDir, "worktree", "remove", "--force", canonicalWorkspaceDir)
	})

	store := &fakeBaldaRestoreSessionStore{
		record: baldastate.SessionRecord{
			SessionID:    locator.SessionID,
			UserID:       baldatelegram.UserID(101),
			ChannelType:  locator.ChannelType,
			AddressKey:   locator.AddressKey,
			AddressJSON:  locator.AddressJSON,
			AgentName:    ownerSessionLabel,
			WorkspaceDir: workspaceDir,
			BranchName:   branchName,
			Status:       baldastate.SessionStatusActive,
		},
		foundByAddress: true,
	}

	builder := &fakeBaldaRestoreAgentBuilder{
		metadata: baldaagent.AgentMetadata{
			Type:       "codex_acp",
			Model:      "gpt-5.3-codex",
			MCPServers: []string{"balda"},
		},
	}
	runtimeManager := &fakeBaldaRestoreRuntimeManager{providerID: "balda-provider"}
	sessionManager := &baldasession.Manager{}
	setUnexportedField(t, sessionManager, "agentBuilder", builder)
	setUnexportedField(t, sessionManager, "runtimeManager", runtimeManager)
	setUnexportedField(t, sessionManager, "baldaProviderName", "balda-provider")
	setUnexportedField(t, sessionManager, "workingDir", workingDir)
	setUnexportedField(t, sessionManager, "workspaces", baldaagent.NewWorkspaceManagerWithSessionsDir(workingDir, stateDir, baldaRestoreCurrentBranch(t, ctx, workingDir), ""))
	setUnexportedField(t, sessionManager, "workspaceEnabled", true)
	setUnexportedField(t, sessionManager, "sessionStore", store)
	setUnexportedField(t, sessionManager, "logger", zerolog.Nop())
	setUnexportedField(t, sessionManager, "sessions", map[string]*baldasession.TopicSession{})

	tgClient := &fakeBaldaStartupTGClient{
		fakeTelegramClient: &fakeTelegramClient{},
	}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	handler := &BaldaHandler{
		channel: baldatelegram.NewAdapter(baldatelegram.AdapterParams{
			Messenger: msg,
			TGClient:  tgClient,
			Logger:    zerolog.Nop(),
		}),
		sessionManager:    sessionManager,
		messenger:         msg,
		baldaProviderName: "balda-provider",
		logger:            zerolog.Nop(),
	}

	if err := handler.bootstrapOwnerSession(ctx, 101, 9001); err != nil {
		t.Fatalf("bootstrapOwnerSession() error = %v", err)
	}

	ts, err := sessionManager.GetSession(locator)
	if err != nil {
		t.Fatalf("GetSession() error = %v", err)
	}
	if got := ts.GetBranchName(); got != branchName {
		t.Fatalf("branch name = %q, want %q", got, branchName)
	}
	if got := ts.GetWorkspaceDir(); got != canonicalWorkspaceDir {
		t.Fatalf("workspace dir = %q, want %q", got, canonicalWorkspaceDir)
	}
}

func newBaldaStartupHandlerForTest(t *testing.T, tgClient client.ClientWithResponsesInterface, authToken string, logger zerolog.Logger) *BaldaHandler {
	t.Helper()

	ownerStore, err := auth.NewOwnerStore(&fakeOwnerKVStore{})
	if err != nil {
		t.Fatalf("new owner store: %v", err)
	}

	return &BaldaHandler{
		ownerStore: ownerStore,
		tgClient:   tgClient,
		authToken:  authToken,
		logger:     logger,
	}
}

func newRegisteredOwnerStartupHandler(t *testing.T) (*BaldaHandler, *fakeBaldaStartupTGClient) {
	t.Helper()

	username := testBaldaStartupBotUsername
	tgClient := &fakeBaldaStartupTGClient{
		fakeTelegramClient: &fakeTelegramClient{},
		getMeResp: &client.GetMeResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
			JSON200: &struct {
				Ok     client.GetMe200Ok `json:"ok"`
				Result client.User       `json:"result"`
			}{
				Ok: true,
				Result: client.User{
					Id:       7791683989,
					Username: &username,
				},
			},
		},
	}

	handler := newBaldaStartupHandlerForTest(t, tgClient, "owner-token", zerolog.Nop())
	if _, err := handler.ownerStore.RegisterOwner(42, 9001); err != nil {
		t.Fatalf("RegisterOwner() error = %v", err)
	}

	builder := &fakeBaldaRestoreAgentBuilder{
		metadata: baldaagent.AgentMetadata{
			Type:       "codex_acp",
			Model:      "gpt-5.3-codex",
			MCPServers: []string{"balda"},
		},
	}
	runtimeManager := &fakeBaldaRestoreRuntimeManager{providerID: "balda-provider"}
	sessionManager := newBaldaRestoreSessionManager(t, builder, runtimeManager, &fakeBaldaRestoreSessionStore{})

	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	handler.channel = baldatelegram.NewAdapter(baldatelegram.AdapterParams{
		Messenger: msg,
		TGClient:  tgClient,
		Logger:    zerolog.Nop(),
	})
	handler.sessionManager = sessionManager
	handler.messenger = msg
	handler.baldaProviderName = "balda-provider"
	return handler, tgClient
}

type fakeBaldaStartupFailBuilder struct {
	metadata baldaagent.AgentMetadata
	err      error
}

func (f *fakeBaldaStartupFailBuilder) CreateRuntimeSession(
	context.Context,
	*baldaagent.BuiltRuntime,
	string,
	string,
	string,
	string,
	baldaagent.RuntimeSessionContext,
) (adksession.Session, error) {
	return nil, f.err
}

func (f *fakeBaldaStartupFailBuilder) GetAgentMetadata(string) baldaagent.AgentMetadata {
	return f.metadata
}
