package slack

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClientPostMessageThread(t *testing.T) {
	t.Parallel()

	var gotAuth string
	var got postMessageRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat.postMessage" {
			t.Fatalf("path = %q, want /chat.postMessage", r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(postMessageResponse{OK: true, TS: "1712345678.000100"})
	}))
	t.Cleanup(server.Close)

	client := NewClientWithBaseURL(server.URL, "xoxb-token")
	providerID, err := client.PostMessage(t.Context(), "C123", "1712340000.000100", "hello", true)
	if err != nil {
		t.Fatalf("PostMessage() error = %v", err)
	}
	if providerID != "1712345678.000100" {
		t.Fatalf("providerID = %q", providerID)
	}
	if gotAuth != "Bearer xoxb-token" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if got.Channel != "C123" || got.ThreadTS != "1712340000.000100" || got.Text != "hello" || !got.Mrkdwn {
		t.Fatalf("request = %+v", got)
	}
}

func TestClientAuthTest(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/auth.test" {
			t.Fatalf("path = %q, want /auth.test", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(authTestResponse{OK: true, TeamID: "T123", UserID: "U999"})
	}))
	t.Cleanup(server.Close)

	client := NewClientWithBaseURL(server.URL, "xoxb-token")
	teamID, userID, err := client.AuthTest(t.Context())
	if err != nil {
		t.Fatalf("AuthTest() error = %v", err)
	}
	if teamID != "T123" || userID != "U999" {
		t.Fatalf("AuthTest() = %q, %q", teamID, userID)
	}
}
