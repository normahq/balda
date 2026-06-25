package auth

import (
	"context"
	"testing"
)

func TestOwnerStore_PersistsInKV(t *testing.T) {
	kv := newMemoryOwnerKV()
	store, err := NewOwnerStore(kv)
	if err != nil {
		t.Fatalf("NewOwnerStore() error = %v", err)
	}

	ok, err := store.RegisterOwner(42, 100)
	if err != nil {
		t.Fatalf("RegisterOwner() error = %v", err)
	}
	if !ok {
		t.Fatal("RegisterOwner() ok = false, want true")
	}

	// Re-open with same KV to verify persistence.
	store2, err := NewOwnerStore(kv)
	if err != nil {
		t.Fatalf("NewOwnerStore(reopen) error = %v", err)
	}
	if !store2.HasOwner() {
		t.Fatal("HasOwner() = false, want true")
	}
	owner := store2.GetOwner()
	if owner == nil {
		t.Fatal("GetOwner() = nil, want owner")
	}
	if owner.UserID != 42 {
		t.Fatalf("owner.UserID = %d, want 42", owner.UserID)
	}
}

func TestOwnerStoreSubject(t *testing.T) {
	kv := newMemoryOwnerKV()
	store, err := NewOwnerStore(kv)
	if err != nil {
		t.Fatalf("NewOwnerStore() error = %v", err)
	}

	ok, err := store.RegisterOwnerSubject("slack:T123:U456")
	if err != nil {
		t.Fatalf("RegisterOwnerSubject() error = %v", err)
	}
	if !ok {
		t.Fatal("RegisterOwnerSubject() ok = false, want true")
	}
	if !store.IsOwnerSubject("slack:T123:U456") {
		t.Fatal("IsOwnerSubject() = false, want true")
	}
	if store.IsOwner(0) {
		t.Fatal("IsOwner(0) = true, want false for subject owner")
	}
}

type memoryOwnerKV struct {
	data map[string]any
}

func newMemoryOwnerKV() *memoryOwnerKV {
	return &memoryOwnerKV{data: make(map[string]any)}
}

func (m *memoryOwnerKV) GetJSON(_ context.Context, key string) (any, bool, error) {
	value, ok := m.data[key]
	return value, ok, nil
}

func (m *memoryOwnerKV) SetJSON(_ context.Context, key string, value any) error {
	m.data[key] = value
	return nil
}
