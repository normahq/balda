package handlers

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/normahq/balda/internal/apps/balda/auth"
	"github.com/normahq/balda/internal/apps/balda/messenger"
	"github.com/rs/zerolog"
	"github.com/tgbotkit/client"
	"github.com/tgbotkit/runtime/events"
)

type fakeOwnerKVStore struct {
	value any
	ok    bool
	err   error
}

func (s *fakeOwnerKVStore) GetJSON(_ context.Context, _ string) (any, bool, error) {
	if s.err != nil {
		return nil, false, s.err
	}
	return s.value, s.ok, nil
}

func (s *fakeOwnerKVStore) SetJSON(_ context.Context, _ string, value any) error {
	if s.err != nil {
		return s.err
	}
	s.value = value
	s.ok = true
	return nil
}

type fakeInviteKVStore struct {
	values map[string]any
}

func (s *fakeInviteKVStore) GetJSON(_ context.Context, key string) (any, bool, error) {
	value, ok := s.values[key]
	return value, ok, nil
}

func (s *fakeInviteKVStore) SetWithTTL(_ context.Context, key string, value any, _ time.Duration) error {
	if s.values == nil {
		s.values = make(map[string]any)
	}
	s.values[key] = value
	return nil
}

func (s *fakeInviteKVStore) Delete(_ context.Context, key string) error {
	delete(s.values, key)
	return nil
}

func (s *fakeInviteKVStore) List(_ context.Context, prefix string) ([]string, error) {
	keys := make([]string, 0, len(s.values))
	for key := range s.values {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

type fakeCollaboratorBackingStore struct {
	values map[string]auth.Collaborator
}

func (s *fakeCollaboratorBackingStore) AddCollaborator(_ context.Context, c auth.Collaborator) error {
	if s.values == nil {
		s.values = make(map[string]auth.Collaborator)
	}
	s.values[c.UserID] = c
	return nil
}

func (s *fakeCollaboratorBackingStore) RemoveCollaborator(_ context.Context, userID string) error {
	delete(s.values, userID)
	return nil
}

func (s *fakeCollaboratorBackingStore) GetCollaborator(_ context.Context, userID string) (*auth.Collaborator, bool, error) {
	collaborator, ok := s.values[userID]
	if !ok {
		return nil, false, nil
	}
	return &collaborator, true, nil
}

func (s *fakeCollaboratorBackingStore) ListCollaborators(_ context.Context) ([]auth.Collaborator, error) {
	collaborators := make([]auth.Collaborator, 0, len(s.values))
	for _, collaborator := range s.values {
		collaborators = append(collaborators, collaborator)
	}
	return collaborators, nil
}

type fakeTelegramClient struct {
	client.ClientWithResponsesInterface
	sendErr        error
	createTopicErr error
	closeTopicErr  error
	nextTopicID    int
	closedTopicIDs []int
	messages       []client.SendMessageJSONRequestBody
	createdTopics  []client.CreateForumTopicJSONRequestBody
}

func (c *fakeTelegramClient) SendMessageWithResponse(_ context.Context, body client.SendMessageJSONRequestBody, _ ...client.RequestEditorFn) (*client.SendMessageResponse, error) {
	c.messages = append(c.messages, body)
	if c.sendErr != nil {
		return nil, c.sendErr
	}
	return &client.SendMessageResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
		JSON200: &struct {
			Ok     client.SendMessage200Ok `json:"ok"`
			Result client.Message          `json:"result"`
		}{
			Ok:     true,
			Result: client.Message{MessageId: len(c.messages)},
		},
	}, nil
}

func (c *fakeTelegramClient) CreateForumTopicWithResponse(_ context.Context, body client.CreateForumTopicJSONRequestBody, _ ...client.RequestEditorFn) (*client.CreateForumTopicResponse, error) {
	c.createdTopics = append(c.createdTopics, body)
	if c.createTopicErr != nil {
		return nil, c.createTopicErr
	}
	if c.nextTopicID == 0 {
		c.nextTopicID = 123
	}
	return &client.CreateForumTopicResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
		JSON200: &struct {
			Ok     client.CreateForumTopic200Ok `json:"ok"`
			Result client.ForumTopic            `json:"result"`
		}{
			Ok:     true,
			Result: client.ForumTopic{MessageThreadId: c.nextTopicID},
		},
	}, nil
}

func (c *fakeTelegramClient) DeleteForumTopicWithResponse(_ context.Context, body client.DeleteForumTopicJSONRequestBody, _ ...client.RequestEditorFn) (*client.DeleteForumTopicResponse, error) {
	c.closedTopicIDs = append(c.closedTopicIDs, body.MessageThreadId)
	if c.closeTopicErr != nil {
		return nil, c.closeTopicErr
	}
	return &client.DeleteForumTopicResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
		JSON200: &struct {
			Ok     client.DeleteForumTopic200Ok `json:"ok"`
			Result bool                        `json:"result"`
		}{
			Ok:     true,
			Result: true,
		},
	}, nil
}

type fakeBaldaOwnerActivator struct {
	calls []activationCall
	err   error
}

type activationCall struct {
	ownerID int64
	chatID  int64
}

func (f *fakeBaldaOwnerActivator) activateOwner(_ context.Context, ownerID, chatID int64) error {
	f.calls = append(f.calls, activationCall{ownerID: ownerID, chatID: chatID})
	return f.err
}

func TestStartHandlerOnCommand_StrictAuthFlow(t *testing.T) {
	t.Run("accepts owner command as owner bootstrap", func(t *testing.T) {
		handler, store, tgClient := newStartHandlerTestHarness(t, "secret-token")
		balda := &fakeBaldaOwnerActivator{}
		handler.baldaHandler = balda

		err := handler.onCommand(context.Background(), newStartEvent("owner=secret-token", 101, 9001))
		if err != nil {
			t.Fatalf("onCommand(): %v", err)
		}

		if !store.HasOwner() {
			t.Fatal("owner not registered")
		}
		owner := store.GetOwner()
		if owner == nil {
			t.Fatal("owner is nil")
		}
		if owner.UserID != 101 {
			t.Fatalf("owner.UserID = %d, want 101", owner.UserID)
		}
		if len(balda.calls) != 1 {
			t.Fatalf("ActivateOwner calls = %d, want 1", len(balda.calls))
		}
		if got := balda.calls[0]; got.ownerID != 101 || got.chatID != 9001 {
			t.Fatalf("ActivateOwner call = %+v, want owner=101 chat=9001", got)
		}
		assertLastSentContains(t, tgClient, "Congratulations")
		assertLastSentNotContains(t, tgClient, "Balda mode is active.")
	})

	t.Run("accepts owner auth link payload as owner bootstrap", func(t *testing.T) {
		handler, store, tgClient := newStartHandlerTestHarness(t, "secret-token")
		balda := &fakeBaldaOwnerActivator{}
		handler.baldaHandler = balda

		err := handler.onCommand(context.Background(), newStartEvent("owner_secret-token", 101, 9001))
		if err != nil {
			t.Fatalf("onCommand(): %v", err)
		}

		if !store.HasOwner() {
			t.Fatal("owner not registered")
		}
		if len(balda.calls) != 1 {
			t.Fatalf("ActivateOwner calls = %d, want 1", len(balda.calls))
		}
		assertLastSentContains(t, tgClient, "Congratulations")
		assertLastSentNotContains(t, tgClient, "Balda mode is active.")
	})

	t.Run("rejects malformed question-mark token", func(t *testing.T) {
		handler, store, tgClient := newStartHandlerTestHarness(t, "secret-token")

		err := handler.onCommand(context.Background(), newStartEvent("?secret-token", 101, 9001))
		if err != nil {
			t.Fatalf("onCommand(): %v", err)
		}

		if store.HasOwner() {
			t.Fatal("owner registered unexpectedly")
		}
		assertLastSentContains(t, tgClient, "Invalid /start format")
		assertLastSentContains(t, tgClient, "https://t.me/<bot_username>?start=owner_<your_owner_token>")
		assertLastSentContains(t, tgClient, "https://t.me/<bot_username>?start=invite_<your_invite_token>")
	})

	t.Run("rejects unsupported raw token format", func(t *testing.T) {
		handler, store, tgClient := newStartHandlerTestHarness(t, "secret-token")

		err := handler.onCommand(context.Background(), newStartEvent("secret-token", 101, 9001))
		if err != nil {
			t.Fatalf("onCommand(): %v", err)
		}

		if store.HasOwner() {
			t.Fatal("owner registered unexpectedly")
		}
		assertLastSentContains(t, tgClient, "Invalid /start format")
		assertLastSentContains(t, tgClient, "/start owner=<your_owner_token>")
	})

	t.Run("rejects unsupported start assignment", func(t *testing.T) {
		handler, store, tgClient := newStartHandlerTestHarness(t, "secret-token")

		err := handler.onCommand(context.Background(), newStartEvent("start=secret-token", 101, 9001))
		if err != nil {
			t.Fatalf("onCommand(): %v", err)
		}

		if store.HasOwner() {
			t.Fatal("owner registered unexpectedly")
		}
		assertLastSentContains(t, tgClient, "Invalid /start format")
	})

	t.Run("rejects invalid owner token", func(t *testing.T) {
		handler, store, tgClient := newStartHandlerTestHarness(t, "secret-token")

		err := handler.onCommand(context.Background(), newStartEvent("owner=wrong-token", 101, 9001))
		if err != nil {
			t.Fatalf("onCommand(): %v", err)
		}

		if store.HasOwner() {
			t.Fatal("owner registered unexpectedly")
		}
		assertLastSentContains(t, tgClient, "Invalid authentication token")
	})

	t.Run("keeps welcome flow for empty args", func(t *testing.T) {
		handler, store, tgClient := newStartHandlerTestHarness(t, "secret-token")

		err := handler.onCommand(context.Background(), newStartEvent("   ", 101, 9001))
		if err != nil {
			t.Fatalf("onCommand(): %v", err)
		}

		if store.HasOwner() {
			t.Fatal("owner registered unexpectedly")
		}
		assertLastSentContains(t, tgClient, "To authenticate, send /start owner=<your_owner_token>")
	})
}

func TestStartHandlerOnCommand_ExistingOwner_StartsRootWhenMissing(t *testing.T) {
	handler, store, tgClient := newStartHandlerTestHarness(t, "secret-token")
	balda := &fakeBaldaOwnerActivator{}
	handler.baldaHandler = balda

	registered, err := store.RegisterOwner(101, 0, "owner", "Owner", "", true)
	if err != nil {
		t.Fatalf("RegisterOwner(): %v", err)
	}
	if !registered {
		t.Fatal("owner should be newly registered")
	}

	err = handler.onCommand(context.Background(), newStartEvent("", 101, 9001))
	if err != nil {
		t.Fatalf("onCommand(): %v", err)
	}

	if len(balda.calls) != 1 {
		t.Fatalf("ActivateOwner calls = %d, want 1", len(balda.calls))
	}
	assertLastSentContains(t, tgClient, "You are already registered as the bot owner.")
	assertLastSentNotContains(t, tgClient, "Balda mode is active.")
}

func TestStartHandlerOnCommand_ExistingOwnerExplicitOwnerModeReactivates(t *testing.T) {
	handler, store, tgClient := newStartHandlerTestHarness(t, "secret-token")
	balda := &fakeBaldaOwnerActivator{}
	handler.baldaHandler = balda

	registered, err := store.RegisterOwner(101, 0, "owner", "Owner", "", true)
	if err != nil {
		t.Fatalf("RegisterOwner(): %v", err)
	}
	if !registered {
		t.Fatal("owner should be newly registered")
	}

	err = handler.onCommand(context.Background(), newStartEvent("owner=wrong-token", 101, 9001))
	if err != nil {
		t.Fatalf("onCommand(): %v", err)
	}

	if len(balda.calls) != 1 {
		t.Fatalf("ActivateOwner calls = %d, want 1", len(balda.calls))
	}
	assertLastSentContains(t, tgClient, "You are already registered as the bot owner.")
	assertLastSentNotContains(t, tgClient, "Balda mode is active.")
}

func TestStartHandlerOnCommand_ExistingOwnerInviteModeDoesNotReactivate(t *testing.T) {
	handler, store, tgClient := newStartHandlerTestHarness(t, "secret-token")
	balda := &fakeBaldaOwnerActivator{}
	handler.baldaHandler = balda

	registered, err := store.RegisterOwner(101, 0, "owner", "Owner", "", true)
	if err != nil {
		t.Fatalf("RegisterOwner(): %v", err)
	}
	if !registered {
		t.Fatal("owner should be newly registered")
	}

	err = handler.onCommand(context.Background(), newStartEvent("invite=secret-token", 101, 9001))
	if err != nil {
		t.Fatalf("onCommand(): %v", err)
	}

	if len(balda.calls) != 0 {
		t.Fatalf("ActivateOwner calls = %d, want 0", len(balda.calls))
	}
	assertLastSentContains(t, tgClient, "You are already the bot owner.")
}

func TestStartHandlerOnCommand_ExistingOwnerOtherInviteDoesNotConsumeOrReactivate(t *testing.T) {
	handler, store, tgClient := newStartHandlerTestHarness(t, "secret-token")
	balda := &fakeBaldaOwnerActivator{}
	handler.baldaHandler = balda

	registered, err := store.RegisterOwner(101, 0, "owner", "Owner", "", true)
	if err != nil {
		t.Fatalf("RegisterOwner(): %v", err)
	}
	if !registered {
		t.Fatal("owner should be newly registered")
	}

	err = handler.onCommand(context.Background(), newStartEvent("invite=other-token", 101, 9001))
	if err != nil {
		t.Fatalf("onCommand(): %v", err)
	}

	if len(balda.calls) != 0 {
		t.Fatalf("ActivateOwner calls = %d, want 0", len(balda.calls))
	}
	assertLastSentContains(t, tgClient, "You are already the bot owner.")
}

func TestStartHandlerOnCommand_InviteModeRegistersCollaborator(t *testing.T) {
	handler, ownerStore, inviteStore, collaboratorStore, tgClient := newStartHandlerFullTestHarness(t, "secret-token")

	registered, err := ownerStore.RegisterOwner(101, 9001, "owner", "Owner", "", true)
	if err != nil {
		t.Fatalf("RegisterOwner(): %v", err)
	}
	if !registered {
		t.Fatal("owner should be newly registered")
	}

	token, _, err := inviteStore.CreateInvite(context.Background(), "101")
	if err != nil {
		t.Fatalf("CreateInvite(): %v", err)
	}

	err = handler.onCommand(context.Background(), newStartEvent("invite="+token, 202, 9002))
	if err != nil {
		t.Fatalf("onCommand(): %v", err)
	}

	collaborator, ok, err := collaboratorStore.GetCollaborator(context.Background(), "202")
	if err != nil {
		t.Fatalf("GetCollaborator(): %v", err)
	}
	if !ok {
		t.Fatal("collaborator not registered")
	}
	if collaborator.AddedBy != "101" {
		t.Fatalf("collaborator.AddedBy = %q, want %q", collaborator.AddedBy, "101")
	}
	assertLastSentContains(t, tgClient, "Welcome! You are now a bot collaborator.")
}

func TestStartHandlerOnCommand_BaldaActivationFailure_DoesNotClaimBaldaActive(t *testing.T) {
	handler, store, tgClient := newStartHandlerTestHarness(t, "secret-token")
	balda := &fakeBaldaOwnerActivator{err: errors.New("precreate failed")}
	handler.baldaHandler = balda

	err := handler.onCommand(context.Background(), newStartEvent("owner=secret-token", 101, 9001))
	if err != nil {
		t.Fatalf("onCommand(): %v", err)
	}

	if !store.HasOwner() {
		t.Fatal("owner not registered")
	}
	assertLastSentContains(t, tgClient, "Congratulations")
	assertLastSentContains(t, tgClient, "Could not start owner session. Please try again.")
	assertLastSentNotContains(t, tgClient, "Balda mode is active.")
}

func TestStartHandlerOnCommand_ExistingOwnerActivationFailure_DoesNotClaimBaldaActive(t *testing.T) {
	handler, store, tgClient := newStartHandlerTestHarness(t, "secret-token")
	balda := &fakeBaldaOwnerActivator{err: errors.New("precreate failed")}
	handler.baldaHandler = balda

	registered, err := store.RegisterOwner(101, 0, "owner", "Owner", "", true)
	if err != nil {
		t.Fatalf("RegisterOwner(): %v", err)
	}
	if !registered {
		t.Fatal("owner should be newly registered")
	}

	err = handler.onCommand(context.Background(), newStartEvent("", 101, 9001))
	if err != nil {
		t.Fatalf("onCommand(): %v", err)
	}

	if len(balda.calls) != 1 {
		t.Fatalf("ActivateOwner calls = %d, want 1", len(balda.calls))
	}
	assertLastSentContains(t, tgClient, "You are already registered as the bot owner.")
	assertLastSentContains(t, tgClient, "Could not start owner session. Please try again.")
	assertLastSentNotContains(t, tgClient, "Balda mode is active.")
}

func TestStartHandlerOnCommand_SendErrorBubblesUp(t *testing.T) {
	handler, _, tgClient := newStartHandlerTestHarness(t, "secret-token")
	tgClient.sendErr = errors.New("send failed")

	err := handler.onCommand(context.Background(), newStartEvent("   ", 101, 9001))
	if err == nil {
		t.Fatal("onCommand() error = nil, want send error")
	}
}

func newStartHandlerTestHarness(t *testing.T, authToken string) (*StartHandler, *auth.OwnerStore, *fakeTelegramClient) {
	t.Helper()

	handler, ownerStore, _, _, tgClient := newStartHandlerFullTestHarness(t, authToken)
	return handler, ownerStore, tgClient
}

func newStartHandlerFullTestHarness(t *testing.T, authToken string) (*StartHandler, *auth.OwnerStore, *auth.InviteStore, *auth.CollaboratorStore, *fakeTelegramClient) {
	t.Helper()

	stateStore := &fakeOwnerKVStore{}
	ownerStore, err := auth.NewOwnerStore(stateStore)
	if err != nil {
		t.Fatalf("NewOwnerStore(): %v", err)
	}
	inviteStore, err := auth.NewInviteStore(&fakeInviteKVStore{})
	if err != nil {
		t.Fatalf("NewInviteStore(): %v", err)
	}
	collaboratorStore := auth.NewCollaboratorStore(&fakeCollaboratorBackingStore{})

	tgClient := &fakeTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	handler := &StartHandler{
		ownerStore:        ownerStore,
		inviteStore:       inviteStore,
		collaboratorStore: collaboratorStore,
		messenger:         msg,
		authToken:         authToken,
	}

	return handler, ownerStore, inviteStore, collaboratorStore, tgClient
}

func newStartEvent(args string, userID, chatID int64) *events.CommandEvent {
	text := "/start " + strings.TrimSpace(args)
	return &events.CommandEvent{
		Command: "start",
		Args:    args,
		Message: &client.Message{
			Chat: client.Chat{
				Id:   chatID,
				Type: "private",
			},
			From: &client.User{
				Id:        userID,
				FirstName: "Test",
			},
			Text: &text,
		},
	}
}

func assertLastSentContains(t *testing.T, tgClient *fakeTelegramClient, wantSubstring string) {
	t.Helper()
	if len(tgClient.messages) == 0 {
		t.Fatal("no messages were sent")
	}
	last := tgClient.messages[len(tgClient.messages)-1]
	if !strings.Contains(last.Text, wantSubstring) {
		t.Fatalf("last message = %q, want substring %q", last.Text, wantSubstring)
	}
}

func assertLastSentNotContains(t *testing.T, tgClient *fakeTelegramClient, unwantedSubstring string) {
	t.Helper()
	if len(tgClient.messages) == 0 {
		t.Fatal("no messages were sent")
	}
	last := tgClient.messages[len(tgClient.messages)-1]
	if strings.Contains(last.Text, unwantedSubstring) {
		t.Fatalf("last message = %q, must not contain %q", last.Text, unwantedSubstring)
	}
}

type fakeBaldaHandler struct {
	bootstrapCalls []bootstrapCall
	bootstrapErr   error
	ownerID        int64
	chatID         int64
	authToken      string
}

type bootstrapCall struct {
	ownerID int64
	chatID  int64
}

func (f *fakeBaldaHandler) activateOwner(_ context.Context, ownerID, chatID int64) error {
	f.ownerID = ownerID
	f.chatID = chatID
	if f.bootstrapErr != nil {
		return f.bootstrapErr
	}
	f.bootstrapCalls = append(f.bootstrapCalls, bootstrapCall{ownerID: ownerID, chatID: chatID})
	return nil
}

func newBaldaHandlerTestHarness(t *testing.T) (*StartHandler, *auth.OwnerStore, *fakeTelegramClient, *fakeBaldaHandler) {
	t.Helper()

	stateStore := &fakeOwnerKVStore{}
	ownerStore, err := auth.NewOwnerStore(stateStore)
	if err != nil {
		t.Fatalf("NewOwnerStore(): %v", err)
	}

	tgClient := &fakeTelegramClient{}
	msg := messenger.NewMessenger(tgClient, zerolog.Nop())
	baldaHandler := &fakeBaldaHandler{}
	startHandler := &StartHandler{
		ownerStore: ownerStore,
		messenger:  msg,
		authToken:  "",
	}
	startHandler.baldaHandler = baldaHandler

	return startHandler, ownerStore, tgClient, baldaHandler
}

func TestActivateOwner_BootstrapsOwnerSession(t *testing.T) {
	t.Run("calls ActivateOwner and bootstraps session", func(t *testing.T) {
		handler, store, _, baldaHandler := newBaldaHandlerTestHarness(t)
		baldaHandler.authToken = "secret-token"

		registered, err := store.RegisterOwner(101, 0, "owner", "Owner", "", true)
		if err != nil {
			t.Fatalf("RegisterOwner(): %v", err)
		}
		if !registered {
			t.Fatal("owner should be registered")
		}

		err = handler.onCommand(context.Background(), newStartEvent("", 101, 9001))
		if err != nil {
			t.Fatalf("onCommand(): %v", err)
		}

		if len(baldaHandler.bootstrapCalls) != 1 {
			t.Fatalf("bootstrapOwnerSession calls = %d, want 1", len(baldaHandler.bootstrapCalls))
		}
		call := baldaHandler.bootstrapCalls[0]
		if call.ownerID != 101 {
			t.Fatalf("bootstrap ownerID = %d, want 101", call.ownerID)
		}
		if call.chatID != 9001 {
			t.Fatalf("bootstrap chatID = %d, want 9001", call.chatID)
		}
	})

	t.Run("bootstrap failure results in failure message", func(t *testing.T) {
		handler, store, tgClient, baldaHandler := newBaldaHandlerTestHarness(t)
		baldaHandler.authToken = "secret-token"
		baldaHandler.bootstrapErr = errors.New("config reload failed")

		registered, err := store.RegisterOwner(101, 0, "owner", "Owner", "", true)
		if err != nil {
			t.Fatalf("RegisterOwner(): %v", err)
		}
		if !registered {
			t.Fatal("owner should be registered")
		}

		err = handler.onCommand(context.Background(), newStartEvent("", 101, 9001))
		if err != nil {
			t.Fatalf("onCommand(): %v", err)
		}

		assertLastSentContains(t, tgClient, "Could not start owner session. Please try again.")
		assertLastSentNotContains(t, tgClient, "Balda mode is active.")
	})
}
