package handlers

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/baldaworks/go-actorlayer"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	"github.com/normahq/balda/internal/apps/balda/actors"
	"github.com/normahq/balda/internal/apps/balda/auth"
	"github.com/normahq/balda/internal/apps/balda/automode"
	"github.com/normahq/balda/internal/apps/balda/automodecmd"
	baldazulip "github.com/normahq/balda/internal/apps/balda/channel/zulip"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	"github.com/normahq/balda/internal/apps/balda/session"
	"github.com/rs/zerolog"
)

const zulipNotReadyReply = "Balda is not ready right now. Please try again."

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

func TestZulipBaldaHandlerRejectsUnsupportedWebhookMethod(t *testing.T) {
	handler := &ZulipBaldaHandler{
		webhookToken: "expected-token",
		logger:       zerolog.Nop(),
	}
	req := httptest.NewRequest(http.MethodGet, "/zulip/webhook", nil)
	rec := httptest.NewRecorder()

	handler.handleWebhook(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if got := rec.Header().Get("Allow"); got != http.MethodPost {
		t.Fatalf("Allow header = %q, want %q", got, http.MethodPost)
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

func TestZulipBaldaHandlerOnStartHandlesMissingOwnerStore(t *testing.T) {
	handler := &ZulipBaldaHandler{
		enabled:    true,
		listenAddr: "127.0.0.1:0",
		logger:     zerolog.Nop(),
	}

	if err := handler.onStart(context.Background()); err != nil {
		t.Fatalf("onStart() error = %v", err)
	}
	t.Cleanup(func() { _ = handler.onStop(context.Background()) })
}

func TestZulipBaldaHandlerOnStartRejectsInvalidWebhookPath(t *testing.T) {
	ownerStore, err := auth.NewOwnerStore(&fakeOwnerKVStore{})
	if err != nil {
		t.Fatalf("NewOwnerStore() error = %v", err)
	}
	handler := &ZulipBaldaHandler{
		ownerStore:   ownerStore,
		enabled:      true,
		listenAddr:   "127.0.0.1:0",
		webhookPath:  "zulip/webhook",
		webhookToken: "token",
		logger:       zerolog.Nop(),
	}

	err = handler.onStart(context.Background())
	if err == nil {
		t.Fatal("onStart() error = nil, want invalid path error")
	}
	if !strings.Contains(err.Error(), "balda.zulip.webhook.path") {
		t.Fatalf("onStart() error = %v, want webhook path context", err)
	}
}

func TestNormalizeZulipWebhookPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		in        string
		want      string
		wantError bool
	}{
		{name: "default", in: "", want: "/zulip/webhook"},
		{name: "trimmed", in: " /custom/zulip ", want: "/custom/zulip"},
		{name: "relative", in: "zulip/webhook", wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := normalizeZulipWebhookPath(tt.in)
			if tt.wantError {
				if err == nil {
					t.Fatal("normalizeZulipWebhookPath() error = nil, want non-nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizeZulipWebhookPath() error = %v, want nil", err)
			}
			if got != tt.want {
				t.Fatalf("normalizeZulipWebhookPath() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestZulipBaldaHandlerOnStopReturnsShutdownError(t *testing.T) {
	block := make(chan struct{})
	entered := make(chan struct{})
	server := &http.Server{
		Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			close(entered)
			<-block
		}),
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	go func() { _ = server.Serve(ln) }()
	t.Cleanup(func() {
		close(block)
		_ = server.Close()
		_ = ln.Close()
	})

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+ln.Addr().String(), nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	<-entered

	handler := &ZulipBaldaHandler{
		server: server,
		logger: zerolog.Nop(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = handler.onStop(ctx)

	if err == nil {
		t.Fatal("onStop() error = nil, want shutdown error")
	}
	if !strings.Contains(err.Error(), "shutdown zulip webhook server") {
		t.Fatalf("onStop() error = %v, want shutdown context", err)
	}
}

func TestZulipBaldaHandlerOnStopWaitsForWebhookProcessing(t *testing.T) {
	handler := &ZulipBaldaHandler{
		server: &http.Server{},
		logger: zerolog.Nop(),
	}
	handler.processWG.Add(1)
	go func() {
		time.Sleep(10 * time.Millisecond)
		handler.processWG.Done()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := handler.onStop(ctx); err != nil {
		t.Fatalf("onStop() error = %v, want nil", err)
	}
}

func TestZulipBaldaHandlerOnStopReturnsProcessingWaitError(t *testing.T) {
	handler := &ZulipBaldaHandler{
		server: &http.Server{},
		logger: zerolog.Nop(),
	}
	handler.processWG.Add(1)
	defer handler.processWG.Done()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err := handler.onStop(ctx)
	if err == nil {
		t.Fatal("onStop() error = nil, want processing wait error")
	}
	if !strings.Contains(err.Error(), "wait for zulip webhook processing") {
		t.Fatalf("onStop() error = %v, want processing wait context", err)
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

func TestZulipBaldaHandlerRecoversProcessingPanicAndReleasesSlot(t *testing.T) {
	handler := &ZulipBaldaHandler{
		webhookToken: "expected-token",
		processSem:   make(chan struct{}, 1),
		logger:       zerolog.Nop(),
		ownerID:      101,
	}
	req := httptest.NewRequest(http.MethodPost, "/zulip/webhook", strings.NewReader(`{
		"token":"expected-token",
		"message":{"sender_id":101,"sender_email":"owner@example.com","type":"stream","stream_id":42,"subject":"ops","content":"/topic release"}
	}`))
	rec := httptest.NewRecorder()

	handler.handleWebhook(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	for range 1000 {
		if len(handler.processSem) == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("process slot count = %d, want released after recovered panic", len(handler.processSem))
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

func TestValidateZulipWebhookPayloadAllowsEmptyStreamSubject(t *testing.T) {
	err := validateZulipWebhookPayload(zulipWebhookPayload{
		Message: zulipMessage{
			SenderID:    101,
			SenderEmail: "user@example.com",
			Type:        zulipMessageTypeStream,
			StreamID:    42,
		},
	})
	if err != nil {
		t.Fatalf("validateZulipWebhookPayload() error = %v, want nil for empty Zulip topic", err)
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
	dispatcher := &recordingZulipDispatcher{stateManager: manager}
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

func TestZulipBaldaHandlerResetHandlesMissingSessionManager(t *testing.T) {
	locator := baldazulip.NewDMLocator(101)
	dispatcher := &recordingZulipDispatcher{}
	handler := &ZulipBaldaHandler{
		actorDispatcher: dispatcher,
		logger:          zerolog.Nop(),
	}

	handler.handleResetCommand(context.Background(), locator, 101, commandRestart, "", true)

	payloads := zulipDeliveryPayloads(t, dispatcher.commands)
	if len(payloads) != 1 {
		t.Fatalf("delivery payloads = %d, want not-ready reply", len(payloads))
	}
	if payloads[0].Text != zulipNotReadyReply {
		t.Fatalf("reply = %q, want not-ready reply", payloads[0].Text)
	}
}

func TestZulipBaldaHandlerCommandAccessHandlesMissingOwnerStore(t *testing.T) {
	dispatcher := &recordingZulipDispatcher{}
	handler := &ZulipBaldaHandler{
		actorDispatcher: dispatcher,
		logger:          zerolog.Nop(),
	}

	handler.handleCommand(context.Background(), baldazulip.NewDMLocator(101), 101, "/locator", true)

	payloads := zulipDeliveryPayloads(t, dispatcher.commands)
	if len(payloads) != 1 {
		t.Fatalf("delivery payloads = %d, want access denial", len(payloads))
	}
	if payloads[0].Text != "Only the bot owner or collaborators can use this bot." {
		t.Fatalf("reply = %q, want access denial", payloads[0].Text)
	}
}

func TestZulipBaldaHandlerMentionCommandUsesCommandText(t *testing.T) {
	ownerStore, err := auth.NewOwnerStore(&fakeOwnerKVStore{})
	if err != nil {
		t.Fatalf("NewOwnerStore() error = %v", err)
	}
	if _, err := ownerStore.RegisterOwnerSubject(auth.ZulipSubject(101)); err != nil {
		t.Fatalf("RegisterOwnerSubject() error = %v", err)
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

func TestZulipBaldaHandlerUsesMessageContentWhenDataEmpty(t *testing.T) {
	ownerStore, err := auth.NewOwnerStore(&fakeOwnerKVStore{})
	if err != nil {
		t.Fatalf("NewOwnerStore() error = %v", err)
	}
	if _, err := ownerStore.RegisterOwnerSubject(auth.ZulipSubject(101)); err != nil {
		t.Fatalf("RegisterOwnerSubject() error = %v", err)
	}

	dispatcher := &recordingZulipDispatcher{}
	handler := &ZulipBaldaHandler{
		ownerStore:      ownerStore,
		actorDispatcher: dispatcher,
		logger:          zerolog.Nop(),
		ownerID:         101,
	}

	handler.processMessage(context.Background(), zulipWebhookPayload{
		Message: zulipMessage{
			SenderID:    101,
			SenderEmail: "owner@example.com",
			Type:        zulipMessageTypeStream,
			StreamID:    42,
			Subject:     "ops",
			Content:     "/locator",
		},
	})

	payloads := zulipDeliveryPayloads(t, dispatcher.commands)
	if len(payloads) != 1 {
		t.Fatalf("delivery payloads = %d, want locator reply", len(payloads))
	}
	if !strings.Contains(payloads[0].Text, "Transport: zulip") {
		t.Fatalf("reply = %q, want locator response", payloads[0].Text)
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

func TestZulipBaldaHandlerStartOwnerHandlesMissingOwnerStore(t *testing.T) {
	locator := baldazulip.NewDMLocator(101)
	dispatcher := &recordingZulipDispatcher{}
	handler := &ZulipBaldaHandler{
		authToken:       "owner-token",
		actorDispatcher: dispatcher,
		logger:          zerolog.Nop(),
	}

	handler.handleStartCommand(context.Background(), locator, 101, "owner=owner-token", true)

	payloads := zulipDeliveryPayloads(t, dispatcher.commands)
	if len(payloads) != 1 {
		t.Fatalf("delivery payloads = %d, want 1", len(payloads))
	}
	if !strings.Contains(payloads[0].Text, "storage configuration") {
		t.Fatalf("reply = %q, want storage configuration guidance", payloads[0].Text)
	}
}

func TestZulipBaldaHandlerInviteStartHandlesMissingOwnerStore(t *testing.T) {
	locator := baldazulip.NewDMLocator(202)
	dispatcher := &recordingZulipDispatcher{}
	handler := &ZulipBaldaHandler{
		actorDispatcher: dispatcher,
		logger:          zerolog.Nop(),
	}

	handler.handleInviteStart(context.Background(), locator, 202, "invite-token")

	payloads := zulipDeliveryPayloads(t, dispatcher.commands)
	if len(payloads) != 1 {
		t.Fatalf("delivery payloads = %d, want 1", len(payloads))
	}
	if !strings.Contains(payloads[0].Text, "storage configuration") {
		t.Fatalf("reply = %q, want storage configuration guidance", payloads[0].Text)
	}
}

func TestZulipBaldaHandlerInviteLookupErrorDoesNotLogToken(t *testing.T) {
	ownerStore, err := auth.NewOwnerStore(&fakeOwnerKVStore{})
	if err != nil {
		t.Fatalf("NewOwnerStore() error = %v", err)
	}
	inviteStore, err := auth.NewInviteStore(errorInviteKVStore{err: errors.New("backend unavailable")})
	if err != nil {
		t.Fatalf("NewInviteStore() error = %v", err)
	}
	collaboratorStore := auth.NewCollaboratorStore(&fakeCollaboratorBackingStore{})
	var logs bytes.Buffer
	handler := &ZulipBaldaHandler{
		ownerStore:        ownerStore,
		inviteStore:       inviteStore,
		collaboratorStore: collaboratorStore,
		actorDispatcher:   &recordingZulipDispatcher{},
		logger:            zerolog.New(&logs),
	}

	handler.handleInviteStart(context.Background(), baldazulip.NewDMLocator(202), 202, "secret-invite-token")

	if got := logs.String(); strings.Contains(got, "secret-invite-token") {
		t.Fatalf("invite lookup log leaked raw token: %s", got)
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

func TestZulipBaldaHandlerCloseHandlesMissingSessionManager(t *testing.T) {
	locator := baldazulip.NewDMLocator(101)
	dispatcher := &recordingZulipDispatcher{}
	handler := &ZulipBaldaHandler{
		actorDispatcher: dispatcher,
		logger:          zerolog.Nop(),
	}

	handler.handleCloseCommand(context.Background(), locator, 101, "", true)

	payloads := zulipDeliveryPayloads(t, dispatcher.commands)
	if len(payloads) != 1 {
		t.Fatalf("delivery payloads = %d, want not-ready reply", len(payloads))
	}
	if payloads[0].Text != zulipNotReadyReply {
		t.Fatalf("reply = %q, want not-ready reply", payloads[0].Text)
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
		if env.Namespace == baldaexecution.NamespaceJobControl {
			t.Fatalf("published job control command for invalid /cancel: %+v", env)
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

func TestZulipBaldaHandlerUserCommandsHandleMissingCollaboratorStore(t *testing.T) {
	locator := baldazulip.NewDMLocator(101)
	tests := []struct {
		name string
		args string
	}{
		{name: "list", args: "list"},
		{name: "remove", args: "remove 202"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dispatcher := &recordingZulipDispatcher{}
			handler := &ZulipBaldaHandler{
				actorDispatcher: dispatcher,
				logger:          zerolog.Nop(),
				ownerID:         101,
			}

			handler.handleUserCommand(context.Background(), locator, 101, tt.args)

			payloads := zulipDeliveryPayloads(t, dispatcher.commands)
			if len(payloads) != 1 {
				t.Fatalf("delivery payloads = %d, want missing store reply", len(payloads))
			}
			if payloads[0].Text != "Collaborator store is unavailable." {
				t.Fatalf("reply = %q, want missing collaborator store reply", payloads[0].Text)
			}
		})
	}
}

func TestZulipBaldaHandlerTopicHandlesMissingSessionManager(t *testing.T) {
	locator := baldazulip.NewStreamLocator(42, "ops")
	dispatcher := &recordingZulipDispatcher{}
	handler := &ZulipBaldaHandler{
		actorDispatcher: dispatcher,
		logger:          zerolog.Nop(),
	}

	handler.handleTopicCommand(context.Background(), locator, 101, "support", false)

	payloads := zulipDeliveryPayloads(t, dispatcher.commands)
	if len(payloads) != 1 {
		t.Fatalf("delivery payloads = %d, want not-ready reply", len(payloads))
	}
	if payloads[0].Text != "Balda is not ready right now." {
		t.Fatalf("reply = %q, want not-ready reply", payloads[0].Text)
	}
}

func TestZulipBaldaHandlerTopicPublishesWelcomeAndConfirmation(t *testing.T) {
	locator := baldazulip.NewStreamLocator(42, "ops")
	manager := &fakeZulipSessionManager{baldaProvider: "balda"}
	dispatcher := &recordingZulipDispatcher{}
	handler := &ZulipBaldaHandler{
		sessionManager:  manager,
		actorDispatcher: dispatcher,
		logger:          zerolog.Nop(),
	}

	handler.handleTopicCommand(context.Background(), locator, 101, "support", false)

	if len(manager.createCalls) != 1 {
		t.Fatalf("createCalls = %d, want topic session created", len(manager.createCalls))
	}
	payloads := zulipDeliveryPayloads(t, dispatcher.commands)
	if len(payloads) != 2 {
		t.Fatalf("delivery payloads = %d, want welcome and confirmation", len(payloads))
	}
	if payloads[0].Mode != actors.DeliveryModeAgentReply || !strings.Contains(payloads[0].Text, "support") {
		t.Fatalf("welcome payload = %+v, want agent reply welcome", payloads[0])
	}
	if payloads[1].Text != "Session created. Post in topic 'support' to continue." {
		t.Fatalf("confirmation reply = %q, want topic creation confirmation", payloads[1].Text)
	}
}

func TestZulipBaldaHandlerMessagePublishesDirectSessionTurn(t *testing.T) {
	ownerStore, err := auth.NewOwnerStore(&fakeOwnerKVStore{})
	if err != nil {
		t.Fatalf("NewOwnerStore() error = %v", err)
	}
	if _, err := ownerStore.RegisterOwnerSubject(auth.ZulipSubject(101)); err != nil {
		t.Fatalf("RegisterOwnerSubject() error = %v", err)
	}
	locator := baldazulip.NewDMLocator(101)
	dispatcher := &recordingZulipDispatcher{}
	handler := &ZulipBaldaHandler{
		ownerStore:      ownerStore,
		actorDispatcher: dispatcher,
		sessionManager:  &fakeZulipSessionManager{baldaProvider: "alpha"},
		logger:          zerolog.Nop(),
		ownerID:         101,
	}

	handler.handleMessage(context.Background(), locator, 101, 42, "hello", true)

	var env actorlayer.Envelope
	found := false
	for _, candidate := range dispatcher.commands {
		if candidate.To.Target != baldaexecution.ActorTypeSession {
			continue
		}
		env = candidate
		found = true
		break
	}
	if !found {
		t.Fatalf("session command not found in published commands: %+v", dispatcher.commands)
	}
	if got, want := env.DedupeKey, "zulip:42"; got != want {
		t.Fatalf("dedupe_key = %q, want %q", got, want)
	}
	var payload actors.SessionTurnPayload
	if err := actorlayer.UnmarshalPayload(env.Payload, &payload); err != nil {
		t.Fatalf("decode session turn payload: %v", err)
	}
	if payload.Source != "zulip" || !payload.Deliver {
		t.Fatalf("session turn payload = %+v, want zulip deliver=true", payload)
	}
	if got, want := payload.MessageID, 42; got != want {
		t.Fatalf("payload message_id = %d, want %d", got, want)
	}
	if got, want := payload.DedupeKey, "zulip:42"; got != want {
		t.Fatalf("payload dedupe_key = %q, want %q", got, want)
	}
}

func TestZulipBaldaHandlerMessageHandlesMissingSessionManager(t *testing.T) {
	ownerStore, err := auth.NewOwnerStore(&fakeOwnerKVStore{})
	if err != nil {
		t.Fatalf("NewOwnerStore() error = %v", err)
	}
	if _, err := ownerStore.RegisterOwnerSubject(auth.ZulipSubject(101)); err != nil {
		t.Fatalf("RegisterOwnerSubject() error = %v", err)
	}
	locator := baldazulip.NewDMLocator(101)
	dispatcher := &recordingZulipDispatcher{}
	handler := &ZulipBaldaHandler{
		ownerStore:      ownerStore,
		actorDispatcher: dispatcher,
		logger:          zerolog.Nop(),
		ownerID:         101,
	}

	handler.handleMessage(context.Background(), locator, 101, 0, "hello", true)

	payloads := zulipDeliveryPayloads(t, dispatcher.commands)
	if len(payloads) != 1 {
		t.Fatalf("delivery payloads = %d, want not-ready reply", len(payloads))
	}
	if payloads[0].Text != zulipNotReadyReply {
		t.Fatalf("reply = %q, want not-ready reply", payloads[0].Text)
	}
}

type errorInviteKVStore struct {
	err error
}

func (s errorInviteKVStore) GetJSON(context.Context, string) (any, bool, error) {
	return nil, false, s.err
}

func (errorInviteKVStore) SetWithTTL(context.Context, string, any, time.Duration) error {
	return nil
}

func (errorInviteKVStore) Delete(context.Context, string) error {
	return nil
}

func (errorInviteKVStore) List(context.Context, string) ([]string, error) {
	return nil, nil
}

type fakeZulipSessionManager struct {
	createCalls    []createSessionCall
	ensureCalls    []createSessionCall
	resetCalls     []string
	baldaProvider  string
	metadata       session.AgentMetadata
	sessionInfo    map[string]session.TopicSessionInfo
	startupNotices map[string]string
	runtimeState   map[string]map[string]any
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

func (f *fakeZulipSessionManager) RuntimeStateValue(_ context.Context, locator session.SessionLocator, key string) (any, bool, error) {
	if f.runtimeState == nil {
		return nil, false, nil
	}
	state := f.runtimeState[locator.SessionID]
	if state == nil {
		return nil, false, nil
	}
	value, ok := state[key]
	return value, ok, nil
}

func (f *fakeZulipSessionManager) UpdateRuntimeState(_ context.Context, locator session.SessionLocator, state map[string]any) error {
	if f.runtimeState == nil {
		f.runtimeState = map[string]map[string]any{}
	}
	if f.runtimeState[locator.SessionID] == nil {
		f.runtimeState[locator.SessionID] = map[string]any{}
	}
	for key, value := range state {
		f.runtimeState[locator.SessionID][key] = value
	}
	return nil
}

func (f *fakeZulipSessionManager) TakeStartupNotice(sessionID string) string {
	notice := strings.TrimSpace(f.startupNotices[sessionID])
	delete(f.startupNotices, sessionID)
	return notice
}

func TestZulipBaldaHandlerAutoCommandTogglesState(t *testing.T) {
	locator := baldazulip.NewDMLocator(101)
	manager := &fakeZulipSessionManager{}
	dispatcher := &recordingZulipDispatcher{stateManager: manager}
	handler := &ZulipBaldaHandler{
		sessionManager:  manager,
		actorDispatcher: dispatcher,
		logger:          zerolog.Nop(),
	}

	handler.handleAutoCommand(context.Background(), locator, "on")

	payloads := zulipDeliveryPayloads(t, dispatcher.commands)
	if len(payloads) != 1 {
		t.Fatalf("delivery payloads = %d, want 1", len(payloads))
	}
	if !strings.Contains(payloads[0].Text, "Auto mode: on") {
		t.Fatalf("payload text = %q, want auto on", payloads[0].Text)
	}
	if got := manager.runtimeState[locator.SessionID][automode.StateKeyEnabled]; got != true {
		t.Fatalf("runtime auto enabled = %#v, want true", got)
	}
}

var errFakeZulipSessionNotFound = errors.New("zulip session not found")

type recordingZulipDispatcher struct {
	commands     []actorlayer.Envelope
	err          error
	stateManager interface {
		UpdateRuntimeState(context.Context, session.SessionLocator, map[string]any) error
	}
}

func (d *recordingZulipDispatcher) Dispatch(_ context.Context, env actorlayer.Envelope) (*actortransport.DispatchReceipt, error) {
	if env.Namespace == baldaexecution.NamespaceAutoModeCommand && d.stateManager != nil {
		var payload automodecmd.Payload
		if err := actorlayer.UnmarshalPayload(env.Payload, &payload); err != nil {
			return nil, err
		}
		if err := d.stateManager.UpdateRuntimeState(context.Background(), session.SessionLocator(payload.Locator), payload.State); err != nil {
			return nil, err
		}
	}
	d.commands = append(d.commands, env)
	if d.err != nil {
		return nil, d.err
	}
	return &actortransport.DispatchReceipt{
		Stream:   baldaexecution.DefaultCommandStream,
		Sequence: uint64(len(d.commands)),
		Subject:  baldaexecution.SubjectForEnvelope(env),
		MsgID:    actorlayer.DedupeKeyOrID(env),
	}, nil
}

func zulipDeliveryPayloads(t *testing.T, envs []actorlayer.Envelope) []actors.DeliveryPayload {
	t.Helper()
	payloads := make([]actors.DeliveryPayload, 0, len(envs))
	for _, env := range envs {
		if env.To.Target != baldaexecution.ActorTypeDelivery {
			continue
		}
		var payload actors.DeliveryPayload
		if err := actorlayer.UnmarshalPayload(env.Payload, &payload); err != nil {
			t.Fatalf("decode delivery payload: %v", err)
		}
		payloads = append(payloads, payload)
	}
	return payloads
}
