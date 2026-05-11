package tgbotkit

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/tgbotkit/client"
	"github.com/tgbotkit/runtime/updatepoller"
)

func TestNewUpdateSource_WebhookDisabledFallsBackToPolling(t *testing.T) {
	t.Parallel()

	src, err := NewUpdateSource(
		Config{
			Webhook: WebhookConfig{
				Enabled: false,
				URL:     "https://example.com/webhook",
			},
		},
		newTestTelegramClient(t),
		nil,
		zerolog.Nop(),
	)
	if err != nil {
		t.Fatalf("NewUpdateSource() error = %v", err)
	}

	if _, ok := src.(*updatepoller.Poller); !ok {
		t.Fatalf("update source type = %T, want *updatepoller.Poller fallback", src)
	}
}

func TestNewClientUsesBoundedHTTPClient(t *testing.T) {
	t.Parallel()

	got, err := NewClient(Config{Token: "test-token"})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	withResponses, ok := got.(*client.ClientWithResponses)
	if !ok {
		t.Fatalf("NewClient() type = %T, want *client.ClientWithResponses", got)
	}
	rawClient, ok := withResponses.ClientInterface.(*client.Client)
	if !ok {
		t.Fatalf("ClientInterface type = %T, want *client.Client", withResponses.ClientInterface)
	}
	httpClient, ok := rawClient.Client.(*http.Client)
	if !ok {
		t.Fatalf("HTTP doer type = %T, want *http.Client", rawClient.Client)
	}
	if httpClient.Timeout != telegramHTTPClientTimeout {
		t.Fatalf("HTTP client timeout = %s, want %s", httpClient.Timeout, telegramHTTPClientTimeout)
	}
}

func TestNewUpdateSource_WebhookModeRequiresURL(t *testing.T) {
	t.Parallel()

	_, err := NewUpdateSource(
		Config{
			Webhook: WebhookConfig{
				Enabled: true,
			},
		},
		newTestTelegramClient(t),
		nil,
		zerolog.Nop(),
	)
	if err == nil {
		t.Fatal("NewUpdateSource() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "relay.telegram.webhook.enabled=true") {
		t.Fatalf("NewUpdateSource() error = %q, want contains relay.telegram.webhook.enabled=true", err)
	}
}

func TestNewUpdateSource_WebhookModeReturnsWebhookSource(t *testing.T) {
	t.Parallel()

	src, err := NewUpdateSource(
		Config{
			Webhook: WebhookConfig{
				Enabled:    true,
				ListenAddr: "127.0.0.1:0",
				Path:       "/telegram/webhook",
				URL:        "https://example.com/webhook",
				AuthToken:  "secret",
			},
		},
		newTestTelegramClient(t),
		nil,
		zerolog.Nop(),
	)
	if err != nil {
		t.Fatalf("NewUpdateSource() error = %v", err)
	}

	if _, ok := src.(*webhookUpdateSource); !ok {
		t.Fatalf("update source type = %T, want *webhookUpdateSource", src)
	}
}

func TestWebhookUpdateSource_StartServesConfiguredPathAndToken(t *testing.T) {
	t.Parallel()

	srcAny, err := newWebhookUpdateSource(
		Config{
			Webhook: WebhookConfig{
				Enabled:    true,
				ListenAddr: "127.0.0.1:0",
				Path:       "/telegram/webhook",
				URL:        "https://example.com/webhook",
				AuthToken:  "my-secret-token",
			},
		},
		newTestTelegramClient(t),
		zerolog.Nop(),
	)
	if err != nil {
		t.Fatalf("newWebhookUpdateSource() error = %v", err)
	}
	src := srcAny.(*webhookUpdateSource)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := src.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = src.Stop(context.Background())
	})

	src.mu.Lock()
	addr := src.listener.Addr().String()
	server := src.server
	src.mu.Unlock()
	if server == nil {
		t.Fatal("webhook server is nil after Start")
	}
	if server.ReadHeaderTimeout != webhookReadHeaderTimeout {
		t.Fatalf("ReadHeaderTimeout = %s, want %s", server.ReadHeaderTimeout, webhookReadHeaderTimeout)
	}
	if server.ReadTimeout != webhookReadTimeout {
		t.Fatalf("ReadTimeout = %s, want %s", server.ReadTimeout, webhookReadTimeout)
	}
	if server.WriteTimeout != webhookWriteTimeout {
		t.Fatalf("WriteTimeout = %s, want %s", server.WriteTimeout, webhookWriteTimeout)
	}
	if server.IdleTimeout != webhookIdleTimeout {
		t.Fatalf("IdleTimeout = %s, want %s", server.IdleTimeout, webhookIdleTimeout)
	}
	baseURL := "http://" + addr

	if code := doWebhookRequest(t, baseURL+"/wrong", "my-secret-token"); code != http.StatusNotFound {
		t.Fatalf("wrong path status = %d, want %d", code, http.StatusNotFound)
	}
	if code := doWebhookRequest(t, baseURL+"/telegram/webhook", ""); code != http.StatusUnauthorized {
		t.Fatalf("missing token status = %d, want %d", code, http.StatusUnauthorized)
	}
	if code := doWebhookRequest(t, baseURL+"/telegram/webhook", "wrong"); code != http.StatusUnauthorized {
		t.Fatalf("invalid token status = %d, want %d", code, http.StatusUnauthorized)
	}
	if code := doWebhookRequest(t, baseURL+"/telegram/webhook", "my-secret-token"); code != http.StatusOK {
		t.Fatalf("valid token status = %d, want %d", code, http.StatusOK)
	}
}

func TestNormalizeWebhookPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want string
	}{
		{in: "", want: "/telegram/webhook"},
		{in: "telegram/webhook", want: "/telegram/webhook"},
		{in: "/telegram/webhook", want: "/telegram/webhook"},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			if got := normalizeWebhookPath(tc.in); got != tc.want {
				t.Fatalf("normalizeWebhookPath(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func newTestTelegramClient(t *testing.T) client.ClientWithResponsesInterface {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
	}))
	t.Cleanup(server.Close)

	c, err := client.NewClientWithResponses(server.URL)
	if err != nil {
		t.Fatalf("NewClientWithResponses() error = %v", err)
	}
	return c
}

func doWebhookRequest(t *testing.T, url, token string) int {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBufferString(`{"update_id":1}`))
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("X-Telegram-Bot-Api-Secret-Token", token)
	}

	httpClient := &http.Client{Timeout: 2 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("http request %q error = %v", url, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	return resp.StatusCode
}
