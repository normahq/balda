package handlers

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/normahq/balda/internal/apps/balda/auth"
)

func TestOwnerBindTokenBundleMessageAvoidsTelegramPlaceholderLink(t *testing.T) {
	owner, err := auth.NewOwnerStore(&fakeChannelAuthOwnerKV{values: make(map[string]any)})
	if err != nil {
		t.Fatalf("NewOwnerStore() error = %v", err)
	}
	if _, err := owner.RegisterOwnerSubject("slack:T123:U456"); err != nil {
		t.Fatalf("RegisterOwnerSubject() error = %v", err)
	}
	tokens, err := auth.NewChannelTokenStore(&fakeChannelAuthTokenKV{values: make(map[string]any)})
	if err != nil {
		t.Fatalf("NewChannelTokenStore() error = %v", err)
	}
	service := auth.NewChannelAuthService(tokens, owner)

	message, ok := ownerBindTokenBundleMessage(context.Background(), service, "slack:T123:U456")
	if !ok {
		t.Fatal("ownerBindTokenBundleMessage() ok = false, want true")
	}
	if strings.Contains(message, "<bot_username>") {
		t.Fatalf("message contains Telegram placeholder link: %q", message)
	}
	if !strings.Contains(message, "DM Balda this command: /start balda_") {
		t.Fatalf("message = %q, want Telegram /start token instruction", message)
	}
}

type fakeChannelAuthOwnerKV struct {
	values map[string]any
}

func (s *fakeChannelAuthOwnerKV) GetJSON(_ context.Context, key string) (any, bool, error) {
	value, ok := s.values[key]
	return value, ok, nil
}

func (s *fakeChannelAuthOwnerKV) SetJSON(_ context.Context, key string, value any) error {
	s.values[key] = value
	return nil
}

type fakeChannelAuthTokenKV struct {
	values map[string]any
}

func (s *fakeChannelAuthTokenKV) GetJSON(_ context.Context, key string) (any, bool, error) {
	value, ok := s.values[key]
	return value, ok, nil
}

func (s *fakeChannelAuthTokenKV) SetWithTTL(_ context.Context, key string, value any, _ time.Duration) error {
	s.values[key] = value
	return nil
}

func (s *fakeChannelAuthTokenKV) Delete(_ context.Context, key string) error {
	delete(s.values, key)
	return nil
}
