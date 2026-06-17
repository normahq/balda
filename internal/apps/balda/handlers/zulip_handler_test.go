package handlers

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/normahq/balda/internal/apps/balda/auth"
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
