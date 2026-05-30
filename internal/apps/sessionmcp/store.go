package sessionmcp

import (
	"context"
)

// Store is the interface for session state storage drivers.
// It wraps ADK's session.State with additional methods for MCP tools.
type Store interface {
	// Get retrieves a value by key. Returns empty string and false if not found.
	Get(ctx context.Context, key string) (value string, ok bool, err error)
	// Set stores a value by key.
	Set(ctx context.Context, key, value string) error
	// Delete removes a key. No-op if key doesn't exist.
	Delete(ctx context.Context, key string) error
	// List returns all keys, optionally filtered by prefix.
	List(ctx context.Context, prefix string) ([]string, error)
	// Clear removes all keys.
	Clear(ctx context.Context) error
	// GetJSON retrieves a value by key as parsed JSON.
	GetJSON(ctx context.Context, key string) (value interface{}, ok bool, err error)
	// SetJSON stores a value by key as JSON.
	SetJSON(ctx context.Context, key string, value interface{}) error
	// MergeJSON merges fields into an existing JSON object at key.
	MergeJSON(ctx context.Context, key string, fields map[string]interface{}) (merged map[string]interface{}, err error)
}
