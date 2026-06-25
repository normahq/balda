package auth

import (
	"context"
	"sync"
	"testing"
	"time"
)

type fakeInviteKVStore struct {
	mu     sync.Mutex
	values map[string]any
}

func (s *fakeInviteKVStore) GetJSON(_ context.Context, key string) (any, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	value, ok := s.values[key]
	return value, ok, nil
}

func (s *fakeInviteKVStore) SetWithTTL(_ context.Context, key string, value any, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.values == nil {
		s.values = make(map[string]any)
	}
	s.values[key] = value
	return nil
}

func (s *fakeInviteKVStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.values, key)
	return nil
}

func (s *fakeInviteKVStore) ConsumeJSON(_ context.Context, key string, shouldConsume func(value any) (bool, error)) (any, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	value, ok := s.values[key]
	if !ok {
		return nil, false, nil
	}
	consume, err := shouldConsume(value)
	if err != nil {
		return nil, false, err
	}
	if !consume {
		return value, false, nil
	}
	delete(s.values, key)
	return value, true, nil
}

func (s *fakeInviteKVStore) List(_ context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]string, 0, len(s.values))
	for key := range s.values {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func TestInviteStoreListInvites_FiltersExpiredInvites(t *testing.T) {
	now := time.Now()
	store := &fakeInviteKVStore{
		values: map[string]any{
			"invite:active": Invite{
				CreatedBy: "101",
				CreatedAt: now.Add(-time.Hour),
				ExpiresAt: now.Add(time.Hour),
			},
			"invite:expired": Invite{
				CreatedBy: "101",
				CreatedAt: now.Add(-2 * time.Hour),
				ExpiresAt: now.Add(-time.Minute),
			},
		},
	}

	invites, err := (&InviteStore{store: store}).ListInvites(context.Background())
	if err != nil {
		t.Fatalf("ListInvites() error = %v", err)
	}
	if len(invites) != 1 {
		t.Fatalf("ListInvites() len = %d, want 1 active invite", len(invites))
	}
	if got := invites[0].ExpiresAt; !got.After(now) {
		t.Fatalf("ListInvites()[0].ExpiresAt = %s, want future time", got)
	}
}

func TestInviteStoreGetInvite_ConsumesInvite(t *testing.T) {
	now := time.Now()
	store := &fakeInviteKVStore{
		values: map[string]any{
			"invite:token": Invite{
				CreatedBy: "101",
				CreatedAt: now,
				ExpiresAt: now.Add(time.Hour),
			},
		},
	}
	inviteStore := &InviteStore{store: store}

	invite, err := inviteStore.GetInvite(context.Background(), "token")
	if err != nil {
		t.Fatalf("GetInvite() error = %v", err)
	}
	if invite == nil {
		t.Fatal("GetInvite() = nil, want invite")
	}
	if _, ok := store.values["invite:token"]; ok {
		t.Fatal("invite key still present after GetInvite consume")
	}
}

func TestInviteStoreGetInvite_RejectsExpiredInvite(t *testing.T) {
	now := time.Now()
	store := &fakeInviteKVStore{
		values: map[string]any{
			"invite:expired": Invite{
				CreatedBy: "101",
				CreatedAt: now.Add(-2 * time.Hour),
				ExpiresAt: now.Add(-time.Minute),
			},
		},
	}
	inviteStore := &InviteStore{store: store}

	invite, err := inviteStore.GetInvite(context.Background(), "expired")
	if err != nil {
		t.Fatalf("GetInvite() error = %v", err)
	}
	if invite != nil {
		t.Fatalf("GetInvite() = %#v, want nil for expired invite", invite)
	}
	if _, ok := store.values["invite:expired"]; ok {
		t.Fatal("expired invite key still present after GetInvite cleanup")
	}
}
