package zulip

import (
	"context"
	"encoding/json"
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
