package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Owner represents the authenticated admin user.
type Owner struct {
	UserID       int64     `json:"user_id"`
	ChatID       int64     `json:"chat_id,omitempty"`
	Subject      string    `json:"subject,omitempty"`
	Bindings     []string  `json:"bindings,omitempty"`
	RegisteredAt time.Time `json:"registered_at"`
}

// OwnerStore manages owner persistence.
type OwnerStore struct {
	store ownerKVStore
	mu    sync.RWMutex
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
	subject := ""
	if userID != 0 {
		subject = TelegramSubject(userID)
	}
	return s.registerOwner(userID, chatID, subject)
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
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.owner != nil {
		return false, nil
	}

	bindings := make([]string, 0, 1)
	trimmedSubject := strings.TrimSpace(subject)
	if trimmedSubject != "" {
		bindings = append(bindings, trimmedSubject)
	}
	s.owner = &Owner{
		UserID:       userID,
		ChatID:       chatID,
		Subject:      trimmedSubject,
		Bindings:     bindings,
		RegisteredAt: time.Now(),
	}

	if err := s.saveLocked(); err != nil {
		return false, fmt.Errorf("saving owner: %w", err)
	}

	return true, nil
}

// IsOwner checks if the given user ID is the registered owner.
func (s *OwnerStore) IsOwner(userID int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.owner == nil {
		return false
	}
	if userID == 0 {
		return false
	}
	if s.owner.UserID != 0 && s.owner.UserID == userID {
		return true
	}
	return isOwnerSubject(s.owner, TelegramSubject(userID))
}

// BindOwnerSubject adds a channel-qualified subject to the existing owner.
func (s *OwnerStore) BindOwnerSubject(subject string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.owner == nil {
		return fmt.Errorf("no owner registered")
	}
	trimmed := strings.TrimSpace(subject)
	if trimmed == "" {
		return fmt.Errorf("owner subject is required")
	}
	if isOwnerSubject(s.owner, trimmed) {
		return nil
	}
	s.owner.Bindings = append(s.owner.Bindings, trimmed)
	if strings.TrimSpace(s.owner.Subject) == "" {
		s.owner.Subject = trimmed
	}
	return s.saveLocked()
}

// BindOwnerTelegram adds the Telegram subject for the existing owner and updates chat ID.
func (s *OwnerStore) BindOwnerTelegram(userID, chatID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if userID == 0 {
		return fmt.Errorf("owner user ID is required")
	}
	if s.owner == nil {
		return fmt.Errorf("no owner registered")
	}
	if s.owner.UserID == 0 {
		s.owner.UserID = userID
	}
	if chatID != 0 {
		s.owner.ChatID = chatID
	}
	if !isOwnerSubject(s.owner, TelegramSubject(userID)) {
		s.owner.Bindings = append(s.owner.Bindings, TelegramSubject(userID))
	}
	return s.saveLocked()
}

// OwnerSubjects returns all known channel-qualified owner subjects.
func (s *OwnerStore) OwnerSubjects() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.owner == nil {
		return nil
	}
	out := make([]string, 0, len(s.owner.Bindings)+2)
	seen := map[string]struct{}{}
	add := func(subject string) {
		trimmed := strings.TrimSpace(subject)
		if trimmed == "" {
			return
		}
		if _, ok := seen[trimmed]; ok {
			return
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	add(s.owner.Subject)
	if s.owner.UserID != 0 {
		add(TelegramSubject(s.owner.UserID))
	}
	for _, subject := range s.owner.Bindings {
		add(subject)
	}
	return out
}

// HasOwnerSubject reports whether any subject is bound for the given channel.
func (s *OwnerStore) HasOwnerSubject(channel string) bool {
	prefix := strings.TrimSpace(channel) + ":"
	for _, subject := range s.OwnerSubjects() {
		if strings.HasPrefix(subject, prefix) {
			return true
		}
	}
	return false
}

// IsOwnerSubject checks if the given transport subject is the registered owner.
func (s *OwnerStore) IsOwnerSubject(subject string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return isOwnerSubject(s.owner, subject)
}

func isOwnerSubject(owner *Owner, subject string) bool {
	if owner == nil {
		return false
	}
	trimmed := strings.TrimSpace(subject)
	if trimmed == "" {
		return false
	}
	if strings.TrimSpace(owner.Subject) != "" && owner.Subject == trimmed {
		return true
	}
	for _, binding := range owner.Bindings {
		if strings.TrimSpace(binding) == trimmed {
			return true
		}
	}
	return false
}

// TelegramSubject returns the channel-qualified subject for a Telegram user.
func TelegramSubject(userID int64) string {
	return fmt.Sprintf("telegram:%d", userID)
}

// SlackSubject returns the channel-qualified subject for a Slack user.
func SlackSubject(teamID, userID string) string {
	return fmt.Sprintf("slack:%s:%s", strings.TrimSpace(teamID), strings.TrimSpace(userID))
}

// ZulipSubject returns the channel-qualified subject for a Zulip user.
func ZulipSubject(userID int) string {
	return fmt.Sprintf("zulip:%d", userID)
}

// UpdateChatID updates and persists the owner's chat ID.
func (s *OwnerStore) UpdateChatID(chatID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.owner == nil {
		return fmt.Errorf("no owner registered")
	}
	s.owner.ChatID = chatID
	return s.saveLocked()
}

// GetOwner returns the registered owner, or nil if none exists.
func (s *OwnerStore) GetOwner() *Owner {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return cloneOwner(s.owner)
}

// HasOwner returns true if an owner is registered.
func (s *OwnerStore) HasOwner() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

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
	owner.Bindings = normalizeOwnerBindings(owner)

	s.mu.Lock()
	s.owner = &owner
	s.mu.Unlock()
	return nil
}

func (s *OwnerStore) saveLocked() error {
	if err := s.store.SetJSON(context.Background(), ownerKVKey, cloneOwner(s.owner)); err != nil {
		return fmt.Errorf("set owner state: %w", err)
	}

	return nil
}

func cloneOwner(owner *Owner) *Owner {
	if owner == nil {
		return nil
	}
	out := *owner
	out.Bindings = append([]string(nil), owner.Bindings...)
	return &out
}

func normalizeOwnerBindings(owner Owner) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(owner.Bindings)+2)
	add := func(subject string) {
		trimmed := strings.TrimSpace(subject)
		if trimmed == "" {
			return
		}
		if _, ok := seen[trimmed]; ok {
			return
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	add(owner.Subject)
	if owner.UserID != 0 {
		add(TelegramSubject(owner.UserID))
	}
	for _, subject := range owner.Bindings {
		add(subject)
	}
	return out
}
