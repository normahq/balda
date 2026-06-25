package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Owner represents the authenticated admin user.
type Owner struct {
	UserID       int64     `json:"user_id"`
	ChatID       int64     `json:"chat_id,omitempty"`
	Subject      string    `json:"subject,omitempty"`
	RegisteredAt time.Time `json:"registered_at"`
}

// OwnerStore manages owner persistence.
type OwnerStore struct {
	store ownerKVStore
	owner *Owner
}

type ownerKVStore interface {
	GetJSON(ctx context.Context, key string) (value any, ok bool, err error)
	SetJSON(ctx context.Context, key string, value any) error
}

const ownerKVKey = "owner"

// NewOwnerStore creates a new owner store backed by key-value state.
func NewOwnerStore(stateStore ownerKVStore) (*OwnerStore, error) {
	if stateStore == nil {
		return nil, fmt.Errorf("owner state store is required")
	}
	store := &OwnerStore{
		store: stateStore,
	}

	// Try to load existing owner.
	if err := store.load(); err != nil {
		return nil, fmt.Errorf("loading owner: %w", err)
	}

	return store, nil
}

// RegisterOwner registers a new owner if none exists.
// Returns true if registered, false if already exists.
func (s *OwnerStore) RegisterOwner(userID, chatID int64) (bool, error) {
	return s.registerOwner(userID, chatID, "")
}

// RegisterOwnerSubject registers a non-numeric transport owner subject.
func (s *OwnerStore) RegisterOwnerSubject(subject string) (bool, error) {
	trimmed := strings.TrimSpace(subject)
	if trimmed == "" {
		return false, fmt.Errorf("owner subject is required")
	}
	return s.registerOwner(0, 0, trimmed)
}

func (s *OwnerStore) registerOwner(userID, chatID int64, subject string) (bool, error) {
	if s.owner != nil {
		return false, nil
	}

	s.owner = &Owner{
		UserID:       userID,
		ChatID:       chatID,
		Subject:      strings.TrimSpace(subject),
		RegisteredAt: time.Now(),
	}

	if err := s.save(); err != nil {
		return false, fmt.Errorf("saving owner: %w", err)
	}

	return true, nil
}

// IsOwner checks if the given user ID is the registered owner.
func (s *OwnerStore) IsOwner(userID int64) bool {
	if s.owner == nil {
		return false
	}
	if userID == 0 || s.owner.UserID == 0 {
		return false
	}
	return s.owner.UserID == userID
}

// IsOwnerSubject checks if the given transport subject is the registered owner.
func (s *OwnerStore) IsOwnerSubject(subject string) bool {
	if s.owner == nil {
		return false
	}
	trimmed := strings.TrimSpace(subject)
	if trimmed == "" {
		return false
	}
	if strings.TrimSpace(s.owner.Subject) != "" {
		return s.owner.Subject == trimmed
	}
	return false
}

// UpdateChatID updates and persists the owner's chat ID.
func (s *OwnerStore) UpdateChatID(chatID int64) error {
	if s.owner == nil {
		return fmt.Errorf("no owner registered")
	}
	s.owner.ChatID = chatID
	return s.save()
}

// GetOwner returns the registered owner, or nil if none exists.
func (s *OwnerStore) GetOwner() *Owner {
	return s.owner
}

// HasOwner returns true if an owner is registered.
func (s *OwnerStore) HasOwner() bool {
	return s.owner != nil
}

func (s *OwnerStore) load() error {
	raw, ok, err := s.store.GetJSON(context.Background(), ownerKVKey)
	if err != nil {
		return fmt.Errorf("get owner state: %w", err)
	}
	if !ok || raw == nil {
		return nil
	}

	data, err := json.Marshal(raw)
	if err != nil {
		return fmt.Errorf("marshal owner state: %w", err)
	}
	var owner Owner
	if err := json.Unmarshal(data, &owner); err != nil {
		return fmt.Errorf("unmarshalling owner: %w", err)
	}

	s.owner = &owner
	return nil
}

func (s *OwnerStore) save() error {
	if err := s.store.SetJSON(context.Background(), ownerKVKey, s.owner); err != nil {
		return fmt.Errorf("set owner state: %w", err)
	}

	return nil
}
