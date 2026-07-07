package handlers

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	baldaagent "github.com/normahq/balda/internal/apps/balda/agent"
	"github.com/normahq/balda/internal/apps/balda/auth"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/rs/zerolog"
	"github.com/tgbotkit/client"
	"github.com/tgbotkit/runtime/events"
	"github.com/tgbotkit/runtime/messagetype"
	adksession "google.golang.org/adk/v2/session"
)

const (
	testBaldaBotUsername   = "testbot"
	testBaldaBranchContent = "branch\n"
)

func TestBaldaHandlerOnMessage_PublicTopicRestoreWelcomeUsesBaldaName(t *testing.T) {
	topicID := 77
	locator := baldatelegram.NewLocator(9001, topicID)
	store := &fakeBaldaRestoreSessionStore{
		record: baldastate.SessionRecord{
			SessionID:   locator.SessionID,
			UserID:      "tg-101",
			ChannelType: locator.ChannelType,
			AddressKey:  locator.AddressKey,
			AddressJSON: locator.AddressJSON,
			AgentName:   "codex",
			Status:      baldastate.SessionStatusActive,
		},
		foundByAddress: true,
	}

	handler, turns, tgClient := newBaldaRestoreHandlerHarness(t, store)

	event := newPublicTopicMessageEvent(topicID, "@"+testBaldaBotUsername+" restore this topic")
	if err := handler.onMessage(context.Background(), event); err != nil {
		t.Fatalf("onMessage() error = %v", err)
	}

	if len(turns.commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(turns.commands))
	}
	assertLastSentContains(t, tgClient, "***Name:*** `balda`")
	if strings.Contains(lastSentText(t, tgClient), "***Name:*** `codex`") {
		t.Fatalf("last message unexpectedly contains persisted label: %q", lastSentText(t, tgClient))
	}
	if got := store.lastUpsert.AgentName; got != "codex" {
		t.Fatalf("persisted label = %q, want codex", got)
	}
}

func TestBaldaHandlerOnMessage_PublicTopicAutoCreateWelcomeUsesBaldaName(t *testing.T) {
	topicID := 88
	store := &fakeBaldaRestoreSessionStore{
		foundByAddress: false,
	}

	handler, turns, tgClient := newBaldaRestoreHandlerHarness(t, store)

	event := newPublicTopicMessageEvent(topicID, "@"+testBaldaBotUsername+" create this topic")
	if err := handler.onMessage(context.Background(), event); err != nil {
		t.Fatalf("onMessage() error = %v", err)
	}

	if len(turns.commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(turns.commands))
	}
	assertLastSentContains(t, tgClient, "***Name:*** `balda`")
	if strings.Contains(lastSentText(t, tgClient), "***Name:*** `auto`") {
		t.Fatalf("last message unexpectedly contains auto label: %q", lastSentText(t, tgClient))
	}
	if got := store.lastUpsert.AgentName; got != "auto" {
		t.Fatalf("persisted label = %q, want auto", got)
	}
}

func TestBaldaHandlerOnMessage_PublicMainChatAutoCreateEnqueuesTurn(t *testing.T) {
	chatID := int64(-5173524191)
	locator := baldatelegram.NewLocator(chatID, 0)
	store := &fakeBaldaRestoreSessionStore{
		foundByAddress: false,
	}

	handler, turns, tgClient := newBaldaRestoreHandlerHarness(t, store)

	event := newPublicMainChatMessageEvent(chatID, "@"+testBaldaBotUsername+" create main chat")
	if err := handler.onMessage(context.Background(), event); err != nil {
		t.Fatalf("onMessage() error = %v", err)
	}

	if len(turns.commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(turns.commands))
	}
	if got := turns.commands[0].SessionID; got != locator.SessionID {
		t.Fatalf("command session = %q, want %q", got, locator.SessionID)
	}
	assertLastSentContains(t, tgClient, "***Name:*** `balda`")
	if got := store.lastUpsert.AgentName; got != "auto" {
		t.Fatalf("persisted label = %q, want auto", got)
	}
	if got := store.lastUpsert.SessionID; got != locator.SessionID {
		t.Fatalf("persisted session = %q, want %q", got, locator.SessionID)
	}
}

func TestBaldaHandlerOnMessage_PublicMainChatAutoCreateWithUnrelatedActiveSession(t *testing.T) {
	chatID := int64(-5173524191)
	locator := baldatelegram.NewLocator(chatID, 0)
	unrelatedLocator := baldatelegram.NewLocator(chatID, 77)
	store := &fakeBaldaRestoreSessionStore{
		foundByAddress: false,
	}

	handler, turns, _ := newBaldaRestoreHandlerHarness(t, store)
	setUnexportedField(t, handler.sessionManager, "sessions", map[string]*baldasession.TopicSession{
		unrelatedLocator.SessionID: newBaldaTopicSession(t, unrelatedLocator.SessionID),
	})

	event := newPublicMainChatMessageEvent(chatID, "@"+testBaldaBotUsername+" use main chat")
	if err := handler.onMessage(context.Background(), event); err != nil {
		t.Fatalf("onMessage() error = %v", err)
	}

	if len(turns.commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(turns.commands))
	}
	if got := turns.commands[0].SessionID; got != locator.SessionID {
		t.Fatalf("command session = %q, want %q", got, locator.SessionID)
	}
	if got := store.lastUpsert.SessionID; got != locator.SessionID {
		t.Fatalf("persisted session = %q, want %q", got, locator.SessionID)
	}
}

func TestBaldaHandlerOnMessage_OwnerDMCreatesOwnerSession(t *testing.T) {
	locator := baldatelegram.NewLocator(9001, 0)
	store := &fakeBaldaRestoreSessionStore{
		foundByAddress: false,
	}

	handler, turns, tgClient := newBaldaRestoreHandlerHarness(t, store)

	event := newPrivateMessageEvent(9001, "hello owner session")
	if err := handler.onMessage(context.Background(), event); err != nil {
		t.Fatalf("onMessage() error = %v", err)
	}

	if len(turns.commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(turns.commands))
	}
	if got := turns.commands[0].SessionID; got != locator.SessionID {
		t.Fatalf("command session = %q, want %q", got, locator.SessionID)
	}
	assertLastSentContains(t, tgClient, "***Name:*** `balda`")
	if got := store.lastUpsert.AgentName; got != "balda" {
		t.Fatalf("persisted label = %q, want balda", got)
	}
}

func TestBaldaHandlerOnMessage_CollaboratorDMCreateFailureUsesGenericSessionMessage(t *testing.T) {
	locator := baldatelegram.NewLocator(9002, 0)
	store := &fakeBaldaRestoreSessionStore{
		foundByAddress: false,
	}

	ownerStore, err := auth.NewOwnerStore(&fakeOwnerKVStore{})
	if err != nil {
		t.Fatalf("NewOwnerStore(): %v", err)
	}
	if _, err := ownerStore.RegisterOwner(101, 9001); err != nil {
		t.Fatalf("RegisterOwner(): %v", err)
	}
	collaboratorStore := auth.NewCollaboratorStore(&fakeCollaboratorBackingStore{
		values: map[string]auth.Collaborator{
			"202": {UserID: "202"},
		},
	})

	builder := &fakeBaldaRestoreAgentBuilder{
		metadata: baldaagent.AgentMetadata{
			Type:       "opencode_acp",
			Model:      "opencode/minimax-m2.5-free",
			MCPServers: []string{"balda", "azure_devops"},
		},
		createErr: errors.New("create runtime session failed"),
	}
	runtimeManager := &fakeBaldaRestoreRuntimeManager{providerID: "balda-provider"}
	sessionManager := newBaldaRestoreSessionManager(t, builder, runtimeManager, store)

	tgClient := &fakeTelegramClient{}
	adapter := newTestTelegramAdapter(tgClient, "none")
	turnDispatcher := &fakeTurnDispatcher{deliveryAdapter: adapter}

	handler := &BaldaHandler{
		ownerStore:        ownerStore,
		collaboratorStore: collaboratorStore,
		channel:           adapter,
		sessionManager:    sessionManager,
		turnDispatcher:    turnDispatcher,
		actorDispatcher:   turnDispatcher,
		logger:            zerolog.Nop(),
	}
	handler.setOwner(101, 9001)
	setUnexportedField(t, handler, "baldaProviderName", "balda-provider")
	handler.botUsername = testBaldaBotUsername
	handler.botUserID = 4242

	event := &events.MessageEvent{
		Type: messagetype.Text,
		Message: &client.Message{
			Chat: client.Chat{
				Id:   9002,
				Type: "private",
			},
			Text: &[]string{"hello collaborator session"}[0],
			From: &client.User{Id: 202},
		},
	}
	if err := handler.onMessage(context.Background(), event); err != nil {
		t.Fatalf("onMessage() error = %v", err)
	}

	if len(turnDispatcher.commands) != 0 {
		t.Fatalf("published commands = %d, want 0", len(turnDispatcher.commands))
	}
	if got := store.lastUpsert.SessionID; got != "" {
		t.Fatalf("persisted session = %q, want empty on create failure", got)
	}
	assertLastSentContains(t, tgClient, "Could not start this session. Please close this chat and try again.")
	assertLastSentNotContains(t, tgClient, "owner session")
	if last := lastSentText(t, tgClient); strings.Contains(last, locator.SessionID) {
		t.Fatalf("last message = %q, must not expose session id", last)
	}
}

func TestBaldaHandlerOnMessage_PublicTopicRestoreWarnsWhenWorkspaceSyncSkipped(t *testing.T) {
	ctx := context.Background()
	workingDir := t.TempDir()
	initBaldaRestoreGitRepo(t, ctx, workingDir)

	writeBaldaRestoreFile(t, filepath.Join(workingDir, "conflict.txt"), "base\n")
	runBaldaRestoreGit(t, ctx, workingDir, "add", "conflict.txt")
	runBaldaRestoreGit(t, ctx, workingDir, "commit", "-m", "chore: seed")

	topicID := 91
	locator := baldatelegram.NewLocator(9001, topicID)
	branchName := "norma/balda/" + locator.SessionID
	stateDir := t.TempDir()
	workspaceDir := filepath.Join(stateDir, "sessions", locator.SessionID)
	runBaldaRestoreGit(t, ctx, workingDir, "worktree", "add", "-b", branchName, workspaceDir, "HEAD")

	runBaldaRestoreGit(t, ctx, workspaceDir, "rm", "conflict.txt")
	runBaldaRestoreGit(t, ctx, workspaceDir, "commit", "-m", "feat: remove conflict file")

	if err := runBaldaRestoreGitAllowError(ctx, workingDir, "worktree", "remove", "--force", workspaceDir); err != nil {
		t.Fatalf("remove worktree: %v", err)
	}
	t.Cleanup(func() {
		_ = runBaldaRestoreGitAllowError(ctx, workingDir, "worktree", "remove", "--force", workspaceDir)
	})

	writeBaldaRestoreFile(t, filepath.Join(workingDir, "conflict.txt"), "main\n")
	runBaldaRestoreGit(t, ctx, workingDir, "add", "conflict.txt")
	runBaldaRestoreGit(t, ctx, workingDir, "commit", "-m", "chore: main conflict")

	store := &fakeBaldaRestoreSessionStore{
		record: baldastate.SessionRecord{
			SessionID:    locator.SessionID,
			UserID:       "tg-101",
			ChannelType:  locator.ChannelType,
			AddressKey:   locator.AddressKey,
			AddressJSON:  locator.AddressJSON,
			AgentName:    "codex",
			WorkspaceDir: workspaceDir,
			BranchName:   branchName,
			Status:       baldastate.SessionStatusActive,
		},
		foundByAddress: true,
	}

	ownerStore, err := auth.NewOwnerStore(&fakeOwnerKVStore{})
	if err != nil {
		t.Fatalf("NewOwnerStore(): %v", err)
	}
	if _, err := ownerStore.RegisterOwner(101, 9001); err != nil {
		t.Fatalf("RegisterOwner(): %v", err)
	}

	builder := &fakeBaldaRestoreAgentBuilder{
		metadata: baldaagent.AgentMetadata{
			Type:       "opencode_acp",
			Model:      "opencode/minimax-m2.5-free",
			MCPServers: []string{"balda", "workspace"},
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

	tgClient := &fakeTelegramClient{}
	adapter := newTestTelegramAdapter(tgClient, "none")
	turnDispatcher := &fakeTurnDispatcher{deliveryAdapter: adapter}
	handler := &BaldaHandler{
		ownerStore:      ownerStore,
		channel:         adapter,
		sessionManager:  sessionManager,
		turnDispatcher:  turnDispatcher,
		actorDispatcher: turnDispatcher,
		logger:          zerolog.Nop(),
	}
	handler.setOwner(101, 9001)
	setUnexportedField(t, handler, "baldaProviderName", "balda-provider")
	handler.botUsername = testBaldaBotUsername
	handler.botUserID = 4242

	event := newPublicTopicMessageEvent(topicID, "@"+testBaldaBotUsername+" restore this topic")
	if err := handler.onMessage(ctx, event); err != nil {
		t.Fatalf("onMessage() error = %v", err)
	}

	if len(turnDispatcher.commands) != 1 {
		t.Fatalf("published commands = %d, want 1", len(turnDispatcher.commands))
	}

	var sawWarning bool
	for _, msg := range tgClient.messages {
		if strings.Contains(msg.Text, "Balda reset the workspace to the last saved session-branch state") {
			sawWarning = true
			break
		}
	}
	if !sawWarning {
		t.Fatalf("sent messages did not include workspace warning: %#v", tgClient.messages)
	}
	for _, msg := range tgClient.messages {
		if strings.Contains(msg.Text, "balda.workspace.import") {
			t.Fatalf("workspace warning must not mention MCP tool name directly: %q", msg.Text)
		}
	}
	for _, msg := range tgClient.messages {
		if strings.Contains(msg.Text, "Could not restore this session") {
			t.Fatalf("unexpected fatal restore message: %q", msg.Text)
		}
	}
}

func newBaldaRestoreHandlerHarness(t *testing.T, store *fakeBaldaRestoreSessionStore) (*BaldaHandler, *fakeTurnDispatcher, *fakeTelegramClient) {
	t.Helper()

	ownerStore, err := auth.NewOwnerStore(&fakeOwnerKVStore{})
	if err != nil {
		t.Fatalf("NewOwnerStore(): %v", err)
	}
	if _, err := ownerStore.RegisterOwner(101, 9001); err != nil {
		t.Fatalf("RegisterOwner(): %v", err)
	}

	builder := &fakeBaldaRestoreAgentBuilder{
		metadata: baldaagent.AgentMetadata{
			Type:       "opencode_acp",
			Model:      "opencode/minimax-m2.5-free",
			MCPServers: []string{"balda", "azure_devops"},
		},
	}
	runtimeManager := &fakeBaldaRestoreRuntimeManager{providerID: "balda-provider"}
	sessionManager := newBaldaRestoreSessionManager(t, builder, runtimeManager, store)

	tgClient := &fakeTelegramClient{}
	adapter := newTestTelegramAdapter(tgClient, "none")
	turnDispatcher := &fakeTurnDispatcher{deliveryAdapter: adapter}

	handler := &BaldaHandler{
		ownerStore:      ownerStore,
		channel:         adapter,
		sessionManager:  sessionManager,
		turnDispatcher:  turnDispatcher,
		actorDispatcher: turnDispatcher,
		logger:          zerolog.Nop(),
	}
	handler.setOwner(101, 9001)
	setUnexportedField(t, handler, "baldaProviderName", "balda-provider")
	handler.botUsername = testBaldaBotUsername
	handler.botUserID = 4242

	return handler, turnDispatcher, tgClient
}

func newBaldaRestoreSessionManager(
	t *testing.T,
	builder *fakeBaldaRestoreAgentBuilder,
	runtimeManager *fakeBaldaRestoreRuntimeManager,
	store *fakeBaldaRestoreSessionStore,
) *baldasession.Manager {
	t.Helper()

	m := &baldasession.Manager{}
	setUnexportedField(t, m, "agentBuilder", builder)
	setUnexportedField(t, m, "runtimeManager", runtimeManager)
	setUnexportedField(t, m, "baldaProviderName", "balda-provider")
	setUnexportedField(t, m, "sessionStore", store)
	setUnexportedField(t, m, "logger", zerolog.Nop())
	setUnexportedField(t, m, "sessions", map[string]*baldasession.TopicSession{})
	return m
}

func newPublicTopicMessageEvent(topicID int, text string) *events.MessageEvent {
	entities := []client.MessageEntity{{Type: "mention", Offset: 0, Length: len("@" + testBaldaBotUsername)}}
	return &events.MessageEvent{
		Type: messagetype.Text,
		Message: &client.Message{
			Chat: client.Chat{
				Id:   9001,
				Type: "supergroup",
			},
			MessageThreadId: &topicID,
			Text:            &text,
			Entities:        &entities,
			From:            &client.User{Id: 101},
		},
	}
}

func newPublicMainChatMessageEvent(chatID int64, text string) *events.MessageEvent {
	entities := []client.MessageEntity{{Type: "mention", Offset: 0, Length: len("@" + testBaldaBotUsername)}}
	return &events.MessageEvent{
		Type: messagetype.Text,
		Message: &client.Message{
			Chat: client.Chat{
				Id:   chatID,
				Type: "supergroup",
			},
			Text:     &text,
			Entities: &entities,
			From:     &client.User{Id: 101},
		},
	}
}

func newPrivateMessageEvent(chatID int64, text string) *events.MessageEvent {
	return &events.MessageEvent{
		Type: messagetype.Text,
		Message: &client.Message{
			Chat: client.Chat{
				Id:   chatID,
				Type: "private",
			},
			Text: &text,
			From: &client.User{Id: 101},
		},
	}
}

func lastSentText(t *testing.T, tgClient *fakeTelegramClient) string {
	t.Helper()
	if len(tgClient.messages) == 0 {
		t.Fatal("sent messages = 0, want at least one")
	}
	return tgClient.messages[len(tgClient.messages)-1].Text
}

type fakeBaldaRestoreSessionStore struct {
	record         baldastate.SessionRecord
	foundByAddress bool
	lastUpsert     baldastate.SessionRecord
}

func (f *fakeBaldaRestoreSessionStore) Upsert(_ context.Context, record baldastate.SessionRecord) error {
	f.lastUpsert = record
	f.record = record
	f.foundByAddress = true
	return nil
}

func (f *fakeBaldaRestoreSessionStore) GetByAddress(_ context.Context, channelType, addressKey string) (baldastate.SessionRecord, bool, error) {
	if !f.foundByAddress {
		return baldastate.SessionRecord{}, false, nil
	}
	if f.record.ChannelType != channelType || f.record.AddressKey != addressKey {
		return baldastate.SessionRecord{}, false, nil
	}
	return f.record, true, nil
}

func (f *fakeBaldaRestoreSessionStore) GetBySessionID(_ context.Context, sessionID string) (baldastate.SessionRecord, bool, error) {
	if !f.foundByAddress || f.record.SessionID != sessionID {
		return baldastate.SessionRecord{}, false, nil
	}
	return f.record, true, nil
}

func (*fakeBaldaRestoreSessionStore) DeleteBySessionID(context.Context, string) error {
	return nil
}

func (f *fakeBaldaRestoreSessionStore) List(context.Context) ([]baldastate.SessionRecord, error) {
	if !f.foundByAddress {
		return nil, nil
	}
	return []baldastate.SessionRecord{f.record}, nil
}

type fakeBaldaRestoreAgentBuilder struct {
	metadata  baldaagent.AgentMetadata
	createErr error
}

func (f *fakeBaldaRestoreAgentBuilder) CreateRuntimeSession(
	context.Context,
	*baldaagent.BuiltRuntime,
	string,
	string,
	string,
	string,
	baldaagent.RuntimeSessionContext,
) (adksession.Session, error) {
	if f.createErr != nil {
		return nil, f.createErr
	}
	return nil, nil
}

func (f *fakeBaldaRestoreAgentBuilder) GetAgentMetadata(string) baldaagent.AgentMetadata {
	return f.metadata
}

type fakeBaldaRestoreRuntimeManager struct {
	providerID string
}

func (*fakeBaldaRestoreRuntimeManager) Runtime(context.Context) (*baldaagent.BuiltRuntime, error) {
	return &baldaagent.BuiltRuntime{}, nil
}

func (f *fakeBaldaRestoreRuntimeManager) ProviderID() string {
	return f.providerID
}

func initBaldaRestoreGitRepo(t *testing.T, ctx context.Context, dir string) {
	t.Helper()
	runBaldaRestoreGit(t, ctx, dir, "init")
	runBaldaRestoreGit(t, ctx, dir, "config", "user.name", "Balda Test")
	runBaldaRestoreGit(t, ctx, dir, "config", "user.email", "balda-test@example.com")
}

func writeBaldaRestoreFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write file %s: %v", path, err)
	}
}

func runBaldaRestoreGit(t *testing.T, ctx context.Context, dir string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func runBaldaRestoreGitAllowError(ctx context.Context, dir string, args ...string) error {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	_, err := cmd.CombinedOutput()
	return err
}

func baldaRestoreCurrentBranch(t *testing.T, ctx context.Context, dir string) string {
	t.Helper()
	return strings.TrimSpace(runBaldaRestoreGit(t, ctx, dir, "rev-parse", "--abbrev-ref", "HEAD"))
}
