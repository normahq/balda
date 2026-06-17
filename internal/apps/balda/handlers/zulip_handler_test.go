package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/normahq/balda/internal/apps/balda/actors"
	"github.com/normahq/balda/internal/apps/balda/auth"
	baldazulip "github.com/normahq/balda/internal/apps/balda/channel/zulip"
	"github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	actortransport "github.com/normahq/balda/pkg/actorlayer/transport"
	"github.com/rs/zerolog"
)

func TestZulipBaldaHandlerRejectsInvalidWebhookToken(t *testing.T) {
	handler := &ZulipBaldaHandler{
		webhookToken: "expected-token",
		logger:       zerolog.Nop(),
	}
	req := httptest.NewRequest(http.MethodPost, "/zulip/webhook", strings.NewReader(`{
		"token":"wrong-token",
		"message":{"sender_email":"user@example.com"}
	}`))
	rec := httptest.NewRecorder()

	handler.handleWebhook(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestZulipBaldaHandlerRejectsMissingWebhookTokenConfiguration(t *testing.T) {
	handler := &ZulipBaldaHandler{
		logger: zerolog.Nop(),
	}
	req := httptest.NewRequest(http.MethodPost, "/zulip/webhook", strings.NewReader(`{
		"token":"provided-token",
		"message":{"sender_email":"user@example.com"}
	}`))
	rec := httptest.NewRecorder()

	handler.handleWebhook(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestZulipBaldaHandlerOnStartFailsWhenListenAddressInUse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	ownerStore, err := auth.NewOwnerStore(&fakeOwnerKVStore{})
	if err != nil {
		t.Fatalf("NewOwnerStore() error = %v", err)
	}
	handler := &ZulipBaldaHandler{
		ownerStore: ownerStore,
		enabled:    true,
		listenAddr: ln.Addr().String(),
		logger:     zerolog.Nop(),
	}

	err = handler.onStart(context.Background())
	if err == nil {
		t.Fatal("onStart() error = nil, want listen failure")
	}
	if !strings.Contains(err.Error(), "listen zulip webhook endpoint") {
		t.Fatalf("onStart() error = %v, want listen context", err)
	}
}

func TestZulipBaldaHandlerOnStartConfiguresHTTPTimeouts(t *testing.T) {
	ownerStore, err := auth.NewOwnerStore(&fakeOwnerKVStore{})
	if err != nil {
		t.Fatalf("NewOwnerStore() error = %v", err)
	}
	handler := &ZulipBaldaHandler{
		ownerStore: ownerStore,
		enabled:    true,
		listenAddr: "127.0.0.1:0",
		logger:     zerolog.Nop(),
	}

	if err := handler.onStart(context.Background()); err != nil {
		t.Fatalf("onStart() error = %v", err)
	}
	t.Cleanup(func() { _ = handler.onStop(context.Background()) })

	if handler.server.ReadHeaderTimeout != zulipWebhookReadHeaderTimeout {
		t.Fatalf("ReadHeaderTimeout = %v, want %v", handler.server.ReadHeaderTimeout, zulipWebhookReadHeaderTimeout)
	}
	if handler.server.ReadTimeout != zulipWebhookReadTimeout {
		t.Fatalf("ReadTimeout = %v, want %v", handler.server.ReadTimeout, zulipWebhookReadTimeout)
	}
	if handler.server.WriteTimeout != zulipWebhookWriteTimeout {
		t.Fatalf("WriteTimeout = %v, want %v", handler.server.WriteTimeout, zulipWebhookWriteTimeout)
	}
	if handler.server.IdleTimeout != zulipWebhookIdleTimeout {
		t.Fatalf("IdleTimeout = %v, want %v", handler.server.IdleTimeout, zulipWebhookIdleTimeout)
	}
}

func TestZulipBaldaHandlerRejectsOversizedWebhookBody(t *testing.T) {
	handler := &ZulipBaldaHandler{
		webhookToken: "expected-token",
		logger:       zerolog.Nop(),
	}
	req := httptest.NewRequest(http.MethodPost, "/zulip/webhook", strings.NewReader(strings.Repeat("x", zulipWebhookMaxBodyBytes+1)))
	rec := httptest.NewRecorder()

	handler.handleWebhook(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}
}

func TestZulipBaldaHandlerReturnsBusyWhenProcessingSlotsFull(t *testing.T) {
	handler := &ZulipBaldaHandler{
		webhookToken: "expected-token",
		processSem:   make(chan struct{}, 1),
		logger:       zerolog.Nop(),
	}
	handler.processSem <- struct{}{}
	req := httptest.NewRequest(http.MethodPost, "/zulip/webhook", strings.NewReader(`{
		"token":"expected-token",
		"message":{"sender_id":101,"sender_email":"user@example.com","type":"stream","stream_id":42,"subject":"ops"}
	}`))
	rec := httptest.NewRecorder()

	handler.handleWebhook(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestZulipBaldaHandlerIgnoresBotEchoBeforeProcessingQueue(t *testing.T) {
	handler := &ZulipBaldaHandler{
		webhookToken: "expected-token",
		processSem:   make(chan struct{}, 1),
		logger:       zerolog.Nop(),
	}
	handler.processSem <- struct{}{}
	req := httptest.NewRequest(http.MethodPost, "/zulip/webhook", strings.NewReader(`{
		"bot_email":"bot@example.com",
		"token":"expected-token",
		"message":{"sender_id":101,"sender_email":"bot@example.com","type":"stream","stream_id":42,"subject":"ops","content":"reply"}
	}`))
	rec := httptest.NewRecorder()

	handler.handleWebhook(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != `{"response_not_required": true}` {
		t.Fatalf("body = %q, want no-response payload", got)
	}
	if got := len(handler.processSem); got != 1 {
		t.Fatalf("process slot count = %d, want unchanged full queue", got)
	}
}

func TestZulipBaldaHandlerRejectsInvalidAuthenticatedPayload(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "missing sender id",
			body: `{
				"token":"expected-token",
				"message":{"sender_email":"user@example.com","type":"stream","stream_id":42,"subject":"ops"}
			}`,
		},
		{
			name: "missing sender email",
			body: `{
				"token":"expected-token",
				"message":{"sender_id":101,"type":"stream","stream_id":42,"subject":"ops"}
			}`,
		},
		{
			name: "unsupported message type",
			body: `{
				"token":"expected-token",
				"message":{"sender_id":101,"sender_email":"user@example.com","type":"unknown","stream_id":42,"subject":"ops"}
			}`,
		},
		{
			name: "missing stream id",
			body: `{
				"token":"expected-token",
				"message":{"sender_id":101,"sender_email":"user@example.com","type":"stream","subject":"ops"}
			}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &ZulipBaldaHandler{
				webhookToken: "expected-token",
				processSem:   make(chan struct{}, 1),
				logger:       zerolog.Nop(),
			}
			req := httptest.NewRequest(http.MethodPost, "/zulip/webhook", strings.NewReader(tt.body))
			rec := httptest.NewRecorder()

			handler.handleWebhook(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
			}
			if got := len(handler.processSem); got != 0 {
				t.Fatalf("process slot count = %d, want 0 for rejected payload", got)
			}
		})
	}
}

func TestZulipBaldaHandlerResetRecreatesSessionAndSendsWelcome(t *testing.T) {
	locator := baldazulip.NewStreamLocator(42, "ops")
	manager := &fakeZulipSessionManager{
		baldaProvider: "balda",
		sessionInfo: map[string]session.TopicSessionInfo{
			locator.SessionID: {
				SessionID: locator.SessionID,
				UserID:    "zu-101",
				AgentName: "ops-agent",
			},
		},
		startupNotices: map[string]string{locator.SessionID: "startup ready"},
	}
	dispatcher := &recordingZulipDispatcher{}
	handler := &ZulipBaldaHandler{
		sessionManager:  manager,
		actorDispatcher: dispatcher,
		logger:          zerolog.Nop(),
	}

	handler.handleResetCommand(context.Background(), locator, 101, commandRestart, "", false)

	if len(manager.resetCalls) != 1 || manager.resetCalls[0] != locator.SessionID {
		t.Fatalf("resetCalls = %+v, want [%s]", manager.resetCalls, locator.SessionID)
	}
	if len(manager.createCalls) != 1 {
		t.Fatalf("createCalls = %d, want 1", len(manager.createCalls))
	}
	if got := manager.createCalls[0]; got.SessionID != locator.SessionID || got.UserID != "zu-101" || got.AgentName != "ops-agent" {
		t.Fatalf("CreateSession call = %+v, want restored label/user", got)
	}

	payloads := zulipDeliveryPayloads(t, dispatcher.commands)
	if len(payloads) != 2 {
		t.Fatalf("delivery payloads = %d, want welcome and startup notice", len(payloads))
	}
	if payloads[0].Mode != actors.DeliveryModeMarkdown || !strings.Contains(payloads[0].Text, locator.SessionID) {
		t.Fatalf("welcome payload = %+v, want markdown welcome for restarted session", payloads[0])
	}
	if payloads[1].Mode != actors.DeliveryModePlain || payloads[1].Text != "startup ready" {
		t.Fatalf("startup payload = %+v, want startup notice", payloads[1])
	}
}

func TestZulipBaldaHandlerAutoClaimBareMentionSendsOneWelcome(t *testing.T) {
	ownerStore, err := auth.NewOwnerStore(&fakeOwnerKVStore{})
	if err != nil {
		t.Fatalf("NewOwnerStore() error = %v", err)
	}
	if _, err := ownerStore.RegisterOwner(101, 0); err != nil {
		t.Fatalf("RegisterOwner() error = %v", err)
	}
	locator := baldazulip.NewStreamLocator(42, "ops")
	manager := &fakeZulipSessionManager{baldaProvider: "balda"}
	dispatcher := &recordingZulipDispatcher{}
	handler := &ZulipBaldaHandler{
		ownerStore:        ownerStore,
		sessionManager:    manager,
		actorDispatcher:   dispatcher,
		baldaProviderName: "balda",
		logger:            zerolog.Nop(),
		ownerID:           101,
	}

	handler.handleAutoClaimMention(context.Background(), locator, 101, "owner@example.com", "", false)

	payloads := zulipDeliveryPayloads(t, dispatcher.commands)
	if len(payloads) != 1 {
		t.Fatalf("delivery payloads = %d, want exactly one welcome", len(payloads))
	}
	if !strings.Contains(payloads[0].Text, "auto") {
		t.Fatalf("welcome text = %q, want auto session welcome", payloads[0].Text)
	}
	if len(manager.ensureCalls) != 1 {
		t.Fatalf("ensureCalls = %d, want 1", len(manager.ensureCalls))
	}
}

func TestZulipBaldaHandlerMentionCommandUsesCommandText(t *testing.T) {
	ownerStore, err := auth.NewOwnerStore(&fakeOwnerKVStore{})
	if err != nil {
		t.Fatalf("NewOwnerStore() error = %v", err)
	}
	if _, err := ownerStore.RegisterOwner(101, 0); err != nil {
		t.Fatalf("RegisterOwner() error = %v", err)
	}

	tests := []struct {
		name string
		data string
	}{
		{name: "normal mention", data: "@**Balda** /locator"},
		{name: "silent mention", data: "@_**Balda** /locator"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dispatcher := &recordingZulipDispatcher{}
			handler := &ZulipBaldaHandler{
				ownerStore:      ownerStore,
				actorDispatcher: dispatcher,
				logger:          zerolog.Nop(),
				ownerID:         101,
			}

			handler.processMessage(context.Background(), zulipWebhookPayload{
				Data:    tt.data,
				Trigger: "mention",
				Message: zulipMessage{
					SenderID:    101,
					SenderEmail: "owner@example.com",
					Type:        zulipMessageTypeStream,
					StreamID:    42,
					Subject:     "ops",
				},
			})

			payloads := zulipDeliveryPayloads(t, dispatcher.commands)
			if len(payloads) != 1 {
				t.Fatalf("delivery payloads = %d, want locator reply", len(payloads))
			}
			if !strings.Contains(payloads[0].Text, "Transport: zulip") {
				t.Fatalf("reply = %q, want locator response", payloads[0].Text)
			}
		})
	}
}

func TestZulipBaldaHandlerStartIsDirectMessageOnly(t *testing.T) {
	ownerStore, err := auth.NewOwnerStore(&fakeOwnerKVStore{})
	if err != nil {
		t.Fatalf("NewOwnerStore() error = %v", err)
	}
	locator := baldazulip.NewStreamLocator(42, "ops")
	dispatcher := &recordingZulipDispatcher{}
	handler := &ZulipBaldaHandler{
		ownerStore:      ownerStore,
		authToken:       "owner-token",
		actorDispatcher: dispatcher,
		logger:          zerolog.Nop(),
	}

	handler.handleStartCommand(context.Background(), locator, 101, "owner=owner-token", false)

	if ownerStore.HasOwner() {
		t.Fatal("owner registered from stream /start, want DM-only rejection")
	}
	payloads := zulipDeliveryPayloads(t, dispatcher.commands)
	if len(payloads) != 1 {
		t.Fatalf("delivery payloads = %d, want 1", len(payloads))
	}
	if payloads[0].Text != "This command is only available in direct messages." {
		t.Fatalf("reply = %q, want DM-only rejection", payloads[0].Text)
	}
}

func TestZulipBaldaHandlerCloseIsDirectMessageOnly(t *testing.T) {
	locator := baldazulip.NewStreamLocator(42, "ops")
	manager := &fakeZulipSessionManager{baldaProvider: "balda"}
	dispatcher := &recordingZulipDispatcher{}
	handler := &ZulipBaldaHandler{
		sessionManager:  manager,
		actorDispatcher: dispatcher,
		logger:          zerolog.Nop(),
	}

	handler.handleCloseCommand(context.Background(), locator, 101, "", false)

	if len(manager.resetCalls) != 0 {
		t.Fatalf("resetCalls = %+v, want none for stream /close", manager.resetCalls)
	}
	payloads := zulipDeliveryPayloads(t, dispatcher.commands)
	if len(payloads) != 1 {
		t.Fatalf("delivery payloads = %d, want 1", len(payloads))
	}
	if payloads[0].Text != "This command is only available in direct messages." {
		t.Fatalf("reply = %q, want DM-only rejection", payloads[0].Text)
	}
}

func TestZulipBaldaHandlerCancelRejectsArgsWithoutPublishingControl(t *testing.T) {
	locator := baldazulip.NewStreamLocator(42, "ops")
	dispatcher := &recordingZulipDispatcher{}
	handler := &ZulipBaldaHandler{
		actorDispatcher: dispatcher,
		logger:          zerolog.Nop(),
	}

	handler.handleCancelCommand(context.Background(), locator, 101, "extra")

	if len(dispatcher.commands) != 1 {
		t.Fatalf("commands = %d, want only usage reply", len(dispatcher.commands))
	}
	payloads := zulipDeliveryPayloads(t, dispatcher.commands)
	if len(payloads) != 1 || payloads[0].Text != "Usage: /cancel" {
		t.Fatalf("payloads = %+v, want cancel usage reply", payloads)
	}
	for _, env := range dispatcher.commands {
		if env.Namespace == swarm.NamespaceTaskControl {
			t.Fatalf("published task control command for invalid /cancel: %+v", env)
		}
	}
}

func TestZulipBaldaHandlerLocatorRejectsArgs(t *testing.T) {
	locator := baldazulip.NewStreamLocator(42, "ops")
	dispatcher := &recordingZulipDispatcher{}
	handler := &ZulipBaldaHandler{
		actorDispatcher: dispatcher,
		logger:          zerolog.Nop(),
	}

	handler.handleLocatorCommand(context.Background(), locator, "extra")

	payloads := zulipDeliveryPayloads(t, dispatcher.commands)
	if len(payloads) != 1 || payloads[0].Text != "Usage: /locator" {
		t.Fatalf("payloads = %+v, want locator usage reply", payloads)
	}
	if strings.Contains(payloads[0].Text, "Transport:") {
		t.Fatalf("locator details returned for invalid usage: %q", payloads[0].Text)
	}
}

type fakeZulipSessionManager struct {
	createCalls    []createSessionCall
	ensureCalls    []createSessionCall
	resetCalls     []string
	baldaProvider  string
	metadata       session.AgentMetadata
	sessionInfo    map[string]session.TopicSessionInfo
	startupNotices map[string]string
}

func (f *fakeZulipSessionManager) CreateSession(_ context.Context, sessionCtx session.SessionContext, agentName string) error {
	f.createCalls = append(f.createCalls, createSessionCall{
		SessionID: sessionCtx.Locator.SessionID,
		UserID:    sessionCtx.UserID,
		AgentName: agentName,
	})
	return nil
}

func (f *fakeZulipSessionManager) EnsureSession(_ context.Context, sessionCtx session.SessionContext, agentName string) (*session.TopicSession, error) {
	f.ensureCalls = append(f.ensureCalls, createSessionCall{
		SessionID: sessionCtx.Locator.SessionID,
		UserID:    sessionCtx.UserID,
		AgentName: agentName,
	})
	return &session.TopicSession{}, nil
}

func (f *fakeZulipSessionManager) GetAgentMetadata(string) session.AgentMetadata {
	return f.metadata
}

func (*fakeZulipSessionManager) GetSession(session.SessionLocator) (*session.TopicSession, error) {
	return nil, nil
}

func (f *fakeZulipSessionManager) GetSessionInfo(_ context.Context, sessionID string) (session.TopicSessionInfo, error) {
	info, ok := f.sessionInfo[sessionID]
	if !ok {
		return session.TopicSessionInfo{}, errFakeZulipSessionNotFound
	}
	return info, nil
}

func (*fakeZulipSessionManager) RestoreSession(context.Context, session.SessionContext) (*session.TopicSession, error) {
	return nil, session.ErrNoPersistedSession
}

func (f *fakeZulipSessionManager) BaldaProviderID() string {
	return f.baldaProvider
}

func (f *fakeZulipSessionManager) ResetSession(_ context.Context, locator session.SessionLocator) error {
	f.resetCalls = append(f.resetCalls, locator.SessionID)
	return nil
}

func (f *fakeZulipSessionManager) TakeStartupNotice(sessionID string) string {
	notice := strings.TrimSpace(f.startupNotices[sessionID])
	delete(f.startupNotices, sessionID)
	return notice
}

var errFakeZulipSessionNotFound = errors.New("zulip session not found")

type recordingZulipDispatcher struct {
	commands []swarm.Envelope
}

func (d *recordingZulipDispatcher) Dispatch(_ context.Context, env swarm.Envelope) (*actortransport.DispatchReceipt, error) {
	d.commands = append(d.commands, env)
	return &actortransport.DispatchReceipt{
		Stream:   swarm.DefaultCommandStream,
		Sequence: uint64(len(d.commands)),
		Subject:  swarm.SubjectForEnvelope(env),
		MsgID:    swarm.DedupeKeyOrID(env),
	}, nil
}

func zulipDeliveryPayloads(t *testing.T, envs []swarm.Envelope) []actors.DeliveryPayload {
	t.Helper()
	payloads := make([]actors.DeliveryPayload, 0, len(envs))
	for _, env := range envs {
		if env.To.Target != swarm.ActorTypeDelivery {
			continue
		}
		var payload actors.DeliveryPayload
		if err := json.Unmarshal([]byte(env.PayloadJSON), &payload); err != nil {
			t.Fatalf("decode delivery payload: %v", err)
		}
		payloads = append(payloads, payload)
	}
	return payloads
}
