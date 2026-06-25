package auth

import (
	"context"
	"sync"
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

func TestOwnerStoreGetOwnerReturnsCopy(t *testing.T) {
	store, err := NewOwnerStore(newMemoryOwnerKV())
	if err != nil {
		t.Fatalf("NewOwnerStore() error = %v", err)
	}
	if _, err := store.RegisterOwner(42, 100); err != nil {
		t.Fatalf("RegisterOwner() error = %v", err)
	}

	owner := store.GetOwner()
	owner.UserID = 7
	owner.Bindings[0] = "slack:T123:U456"

	if store.IsOwner(7) {
		t.Fatal("IsOwner(7) = true after mutating GetOwner result, want false")
	}
	if !store.IsOwner(42) {
		t.Fatal("IsOwner(42) = false after mutating GetOwner result, want true")
	}
	if store.IsOwnerSubject("slack:T123:U456") {
		t.Fatal("IsOwnerSubject(slack:T123:U456) = true after mutating GetOwner result, want false")
	}
}

func TestOwnerStoreConcurrentBindings(t *testing.T) {
	store, err := NewOwnerStore(newMemoryOwnerKV())
	if err != nil {
		t.Fatalf("NewOwnerStore() error = %v", err)
	}
	if _, err := store.RegisterOwner(42, 100); err != nil {
		t.Fatalf("RegisterOwner() error = %v", err)
	}

	var wg sync.WaitGroup
	subjects := []string{"slack:T123:U1", "zulip:2", "slack:T123:U3", "zulip:4"}
	for _, subject := range subjects {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := store.BindOwnerSubject(subject); err != nil {
				t.Errorf("BindOwnerSubject(%q) error = %v", subject, err)
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = store.OwnerSubjects()
			_ = store.IsOwner(42)
			_ = store.IsOwnerSubject(subject)
		}()
	}
	wg.Wait()

	for _, subject := range subjects {
		if !store.IsOwnerSubject(subject) {
			t.Fatalf("IsOwnerSubject(%q) = false, want true", subject)
		}
	}
}

type memoryOwnerKV struct {
	mu   sync.Mutex
	data map[string]any
}

func newMemoryOwnerKV() *memoryOwnerKV {
	return &memoryOwnerKV{data: make(map[string]any)}
}

func (m *memoryOwnerKV) GetJSON(_ context.Context, key string) (any, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	value, ok := m.data[key]
	return value, ok, nil
}

func (m *memoryOwnerKV) SetJSON(_ context.Context, key string, value any) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = value
	return nil
}
