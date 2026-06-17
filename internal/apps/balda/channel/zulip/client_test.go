package zulip

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestValidateConfigRequiresReplyCredentials(t *testing.T) {
	tests := []struct {
		name      string
		baseURL   string
		botEmail  string
		apiKey    string
		wantError string
	}{
		{name: "base url", botEmail: "bot@example.com", apiKey: "key", wantError: "server_url"},
		{name: "relative url", baseURL: "zulip.example.com", botEmail: "bot@example.com", apiKey: "key", wantError: "absolute"},
		{name: "unsupported scheme", baseURL: "ftp://zulip.example.com", botEmail: "bot@example.com", apiKey: "key", wantError: "http or https"},
		{name: "bot email", baseURL: "https://zulip.example.com", apiKey: "key", wantError: "bot_email"},
		{name: "api key", baseURL: "https://zulip.example.com", botEmail: "bot@example.com", wantError: "api_key"},
		{name: "valid", baseURL: "https://zulip.example.com", botEmail: "bot@example.com", apiKey: "key"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConfig(tt.baseURL, tt.botEmail, tt.apiKey)
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("ValidateConfig() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidateConfig() error = nil, want %q", tt.wantError)
			}
			if got := err.Error(); !strings.Contains(got, tt.wantError) {
				t.Fatalf("ValidateConfig() error = %q, want marker %q", got, tt.wantError)
			}
		})
	}
}

func TestClientSendStreamMessagePostsExpectedForm(t *testing.T) {
	var sawRequest bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawRequest = true
		if r.URL.Path != "/api/v1/messages" {
			t.Fatalf("request path = %q, want /api/v1/messages", r.URL.Path)
		}
		user, pass, ok := r.BasicAuth()
		if !ok || user != "bot@example.com" || pass != "api-key" {
			t.Fatalf("basic auth = (%q, %q, %v), want bot@example.com/api-key", user, pass, ok)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm() error = %v", err)
		}
		if got := r.Form.Get("type"); got != addressTypeStream {
			t.Fatalf("type form value = %q, want stream", got)
		}
		if got := r.Form.Get("to"); got != "42" {
			t.Fatalf("to form value = %q, want 42", got)
		}
		if got := r.Form.Get("topic"); got != "ops" {
			t.Fatalf("topic form value = %q, want ops", got)
		}
		if got := r.Form.Get("content"); got != "hello" {
			t.Fatalf("content form value = %q, want hello", got)
		}
		_ = json.NewEncoder(w).Encode(sendMessageResult{Result: "success", ID: 123})
	}))
	t.Cleanup(server.Close)

	client := NewClient(server.URL, "bot@example.com", "api-key")
	id, err := client.SendStreamMessage(context.Background(), 42, "ops", "hello")
	if err != nil {
		t.Fatalf("SendStreamMessage() error = %v", err)
	}
	if id != 123 {
		t.Fatalf("SendStreamMessage() id = %d, want 123", id)
	}
	if !sawRequest {
		t.Fatal("test server did not receive request")
	}
}

func TestClientSendStreamMessageRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", maxResponseBodyBytes+1)))
	}))
	t.Cleanup(server.Close)

	client := NewClient(server.URL, "bot@example.com", "api-key")
	_, err := client.SendStreamMessage(context.Background(), 42, "ops", "hello")
	if err == nil {
		t.Fatal("SendStreamMessage() error = nil, want response body size error")
	}
	if got := err.Error(); !strings.Contains(got, "response body too large") {
		t.Fatalf("SendStreamMessage() error = %q, want body size marker", got)
	}
}

func TestClientSendStreamMessageTruncatesHTTPErrorBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(strings.Repeat("x", maxErrorResponseBodyText+100)))
	}))
	t.Cleanup(server.Close)

	client := NewClient(server.URL, "bot@example.com", "api-key")
	_, err := client.SendStreamMessage(context.Background(), 42, "ops", "hello")
	if err == nil {
		t.Fatal("SendStreamMessage() error = nil, want HTTP error")
	}
	got := err.Error()
	if !strings.Contains(got, "HTTP 502") {
		t.Fatalf("SendStreamMessage() error = %q, want HTTP status", got)
	}
	if !strings.Contains(got, "(truncated)") {
		t.Fatalf("SendStreamMessage() error = %q, want truncated marker", got)
	}
	if len(got) > maxErrorResponseBodyText+200 {
		t.Fatalf("SendStreamMessage() error length = %d, want bounded diagnostic", len(got))
	}
}

func TestClientSendStreamMessageParsesStructuredHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(sendMessageResult{
			Result: "error",
			Code:   "BAD_REQUEST",
			Msg:    "invalid image URL",
		})
	}))
	t.Cleanup(server.Close)

	client := NewClient(server.URL, "bot@example.com", "api-key")
	_, err := client.SendStreamMessage(context.Background(), 42, "ops", "hello")
	if err == nil {
		t.Fatal("SendStreamMessage() error = nil, want structured HTTP error")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("SendStreamMessage() error = %T, want APIError", err)
	}
	if apiErr.StatusCode != http.StatusBadRequest || apiErr.Code != "BAD_REQUEST" || apiErr.Message != "invalid image URL" {
		t.Fatalf("APIError = %+v, want parsed status/code/message", apiErr)
	}
	if got := err.Error(); !strings.Contains(got, "HTTP 400 (BAD_REQUEST): invalid image URL") {
		t.Fatalf("SendStreamMessage() error = %q, want structured error text", got)
	}
}
