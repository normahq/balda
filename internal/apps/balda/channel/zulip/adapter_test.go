package zulip

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"
)

func TestAdapterSendAgentReplyFallsBackToPlainTextOnContentRejection(t *testing.T) {
	var contents []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm() error = %v", err)
		}
		contents = append(contents, r.Form.Get("content"))
		if len(contents) == 1 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("bad image link"))
			return
		}
		_ = json.NewEncoder(w).Encode(sendMessageResult{Result: "success", ID: 456})
	}))
	t.Cleanup(server.Close)

	adapter := NewAdapter(NewClient(server.URL, "bot@example.com", "api-key"), zerolog.Nop())
	providerMessageID, err := adapter.SendAgentReplyWithProviderMessageID(
		context.Background(),
		NewStreamLocator(42, "ops"),
		"Screenshot: ![broken](https://example.invalid/missing.png)",
	)
	if err != nil {
		t.Fatalf("SendAgentReplyWithProviderMessageID() error = %v", err)
	}
	if providerMessageID != "456" {
		t.Fatalf("provider message ID = %q, want 456", providerMessageID)
	}
	if len(contents) != 2 {
		t.Fatalf("request count = %d, want initial send and fallback", len(contents))
	}
	if contents[0] != "Screenshot: ![broken](https://example.invalid/missing.png)" {
		t.Fatalf("initial content = %q, want original markdown", contents[0])
	}
	if contents[1] != "Screenshot: broken: https://example.invalid/missing.png" {
		t.Fatalf("fallback content = %q, want plain text image reference", contents[1])
	}
}

func TestAdapterSendAgentReplyDoesNotFallbackOnServerError(t *testing.T) {
	var requestCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("temporary upstream failure"))
	}))
	t.Cleanup(server.Close)

	adapter := NewAdapter(NewClient(server.URL, "bot@example.com", "api-key"), zerolog.Nop())
	err := adapter.SendAgentReply(
		context.Background(),
		NewStreamLocator(42, "ops"),
		"Screenshot: ![broken](https://example.invalid/missing.png)",
	)
	if err == nil {
		t.Fatal("SendAgentReply() error = nil, want server error")
	}
	if requestCount != 1 {
		t.Fatalf("request count = %d, want no immediate fallback on server error", requestCount)
	}
}
