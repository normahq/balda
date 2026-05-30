package sessionmcp

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Global shared state for in-package test stores.
var (
	sharedMemoryMu sync.Mutex
	sharedMemory   *memoryState
)

type memoryState struct {
	mu     sync.RWMutex
	values map[string]any
}

func newMemoryState() *memoryState {
	return &memoryState{values: make(map[string]any)}
}

func getSharedMemoryState() *memoryState {
	sharedMemoryMu.Lock()
	defer sharedMemoryMu.Unlock()
	if sharedMemory == nil {
		sharedMemory = newMemoryState()
	}
	return sharedMemory
}

func ResetSharedStore() {
	sharedMemoryMu.Lock()
	defer sharedMemoryMu.Unlock()
	sharedMemory = newMemoryState()
}

type MemoryStore struct {
	state *memoryState
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{state: getSharedMemoryState()}
}

func (s *MemoryStore) Get(_ context.Context, key string) (string, bool, error) {
	val, ok := s.get(key)
	if !ok {
		return "", false, nil
	}
	str, ok := val.(string)
	if !ok {
		b, err := json.Marshal(val)
		if err != nil {
			return "", false, fmt.Errorf("marshal value: %w", err)
		}
		return string(b), true, nil
	}
	return str, true, nil
}

func (s *MemoryStore) Set(_ context.Context, key, value string) error {
	s.set(key, value)
	return nil
}

func (s *MemoryStore) Delete(_ context.Context, key string) error {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	delete(s.state.values, strings.TrimSpace(key))
	return nil
}

func (s *MemoryStore) List(_ context.Context, prefix string) ([]string, error) {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()

	trimmedPrefix := strings.TrimSpace(prefix)
	keys := make([]string, 0)
	for key := range s.state.values {
		if trimmedPrefix == "" || strings.HasPrefix(key, trimmedPrefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *MemoryStore) Clear(_ context.Context) error {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	s.state.values = make(map[string]any)
	return nil
}

func (s *MemoryStore) GetJSON(_ context.Context, key string) (interface{}, bool, error) {
	val, ok := s.get(key)
	if !ok {
		return nil, false, nil
	}
	return val, true, nil
}

func (s *MemoryStore) SetJSON(_ context.Context, key string, value interface{}) error {
	s.set(key, value)
	return nil
}

func (s *MemoryStore) MergeJSON(_ context.Context, key string, fields map[string]interface{}) (map[string]interface{}, error) {
	trimmedKey := strings.TrimSpace(key)
	if trimmedKey == "" {
		return nil, fmt.Errorf("key is required")
	}

	var existing map[string]interface{}

	s.state.mu.Lock()
	defer s.state.mu.Unlock()

	if val, ok := s.state.values[trimmedKey]; ok {
		switch v := val.(type) {
		case map[string]interface{}:
			existing = copyStringAnyMap(v)
		default:
			existing = make(map[string]interface{})
		}
	} else {
		existing = make(map[string]interface{})
	}

	for k, v := range fields {
		existing[k] = v
	}

	s.state.values[trimmedKey] = copyStringAnyMap(existing)
	return existing, nil
}

func (s *MemoryStore) get(key string) (any, bool) {
	trimmedKey := strings.TrimSpace(key)
	if trimmedKey == "" {
		return nil, false
	}

	s.state.mu.RLock()
	defer s.state.mu.RUnlock()

	val, ok := s.state.values[trimmedKey]
	return val, ok
}

func (s *MemoryStore) set(key string, value any) {
	trimmedKey := strings.TrimSpace(key)
	if trimmedKey == "" {
		return
	}

	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	s.state.values[trimmedKey] = value
}

func copyStringAnyMap(in map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
