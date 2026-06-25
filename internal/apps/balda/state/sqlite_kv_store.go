package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

type sqliteKVStore struct {
	db        *sql.DB
	namespace string
}

func (s *sqliteKVStore) Get(ctx context.Context, key string) (string, bool, error) {
	value, ok, err := s.GetJSON(ctx, key)
	if err != nil || !ok {
		return "", ok, err
	}

	if str, ok := value.(string); ok {
		return str, true, nil
	}

	data, err := json.Marshal(value)
	if err != nil {
		return "", false, fmt.Errorf("marshal json value for key %q: %w", key, err)
	}
	return string(data), true, nil
}

func (s *sqliteKVStore) Set(ctx context.Context, key, value string) error {
	return s.SetJSON(ctx, key, value)
}

func (s *sqliteKVStore) SetWithTTL(ctx context.Context, key string, value any, ttl time.Duration) error {
	trimmedKey := strings.TrimSpace(key)
	if trimmedKey == "" {
		return fmt.Errorf("key is required")
	}

	expiresAt := time.Now().UTC().Add(ttl).Format(time.RFC3339)

	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal json key %q: %w", trimmedKey, err)
	}

	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO balda_app_kv (namespace, key, value_json, expires_at, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(namespace, key)
		DO UPDATE SET value_json = excluded.value_json, expires_at = excluded.expires_at, updated_at = excluded.updated_at`,
		s.namespace, trimmedKey, string(encoded), expiresAt, time.Now().UTC().Format(time.RFC3339),
	); err != nil {
		return fmt.Errorf("set json key with ttl %q: %w", trimmedKey, err)
	}
	return nil
}

func (s *sqliteKVStore) Delete(ctx context.Context, key string) error {
	trimmedKey := strings.TrimSpace(key)
	if trimmedKey == "" {
		return nil
	}

	if _, err := s.db.ExecContext(ctx, `
		DELETE FROM balda_app_kv
		WHERE namespace = ? AND key = ?`,
		s.namespace, trimmedKey,
	); err != nil {
		return fmt.Errorf("delete key %q: %w", trimmedKey, err)
	}
	return nil
}

func (s *sqliteKVStore) List(ctx context.Context, prefix string) ([]string, error) {
	args := []any{s.namespace}
	query := `
		SELECT key
		FROM balda_app_kv
		WHERE namespace = ?`

	trimmedPrefix := strings.TrimSpace(prefix)
	if trimmedPrefix != "" {
		query += ` AND key LIKE ?`
		args = append(args, trimmedPrefix+"%")
	}
	query += ` ORDER BY key`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list keys: %w", err)
	}
	defer func() { _ = rows.Close() }()

	keys := make([]string, 0)
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("scan key: %w", err)
		}

		// Skip expired keys
		if s.isKeyExpired(ctx, key) {
			continue
		}

		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate keys: %w", err)
	}
	return keys, nil
}

func (s *sqliteKVStore) isKeyExpired(ctx context.Context, key string) bool {
	var expiresAt sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT expires_at
		FROM balda_app_kv
		WHERE namespace = ? AND key = ?`,
		s.namespace, key,
	).Scan(&expiresAt)
	if err != nil || !expiresAt.Valid {
		return false // no expiry set
	}

	expTime, err := time.Parse(time.RFC3339, expiresAt.String)
	if err != nil {
		return false
	}

	return time.Now().UTC().After(expTime)
}

func (s *sqliteKVStore) Clear(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `
		DELETE FROM balda_app_kv
		WHERE namespace = ?`,
		s.namespace,
	); err != nil {
		return fmt.Errorf("clear namespace %q: %w", s.namespace, err)
	}
	return nil
}

func (s *sqliteKVStore) GetJSON(ctx context.Context, key string) (any, bool, error) {
	trimmedKey := strings.TrimSpace(key)
	if trimmedKey == "" {
		return nil, false, nil
	}

	var raw string
	var expiresAt sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT value_json, expires_at
		FROM balda_app_kv
		WHERE namespace = ? AND key = ?`,
		s.namespace, trimmedKey,
	).Scan(&raw, &expiresAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get json key %q: %w", trimmedKey, err)
	}

	// Check expiration
	if expiresAt.Valid {
		expTime, parseErr := time.Parse(time.RFC3339, expiresAt.String)
		if parseErr == nil && time.Now().UTC().After(expTime) {
			// Auto-delete expired key
			_ = s.Delete(ctx, trimmedKey)
			return nil, false, nil
		}
	}

	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return nil, false, fmt.Errorf("unmarshal json key %q: %w", trimmedKey, err)
	}
	return value, true, nil
}

func (s *sqliteKVStore) ConsumeJSON(
	ctx context.Context,
	key string,
	shouldConsume func(value any) (bool, error),
) (any, bool, error) {
	trimmedKey := strings.TrimSpace(key)
	if trimmedKey == "" {
		return nil, false, nil
	}
	if shouldConsume == nil {
		return nil, false, fmt.Errorf("consume predicate is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, fmt.Errorf("begin consume json key %q: %w", trimmedKey, err)
	}
	defer func() { _ = tx.Rollback() }()

	var raw string
	var expiresAt sql.NullString
	err = tx.QueryRowContext(ctx, `
		SELECT value_json, expires_at
		FROM balda_app_kv
		WHERE namespace = ? AND key = ?`,
		s.namespace, trimmedKey,
	).Scan(&raw, &expiresAt)
	if err != nil {
		if err == sql.ErrNoRows {
			if commitErr := tx.Commit(); commitErr != nil {
				return nil, false, fmt.Errorf("commit missing consume json key %q: %w", trimmedKey, commitErr)
			}
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get json key %q for consume: %w", trimmedKey, err)
	}

	if expiresAt.Valid {
		expTime, parseErr := time.Parse(time.RFC3339, expiresAt.String)
		if parseErr == nil && time.Now().UTC().After(expTime) {
			if _, err := tx.ExecContext(ctx, `
				DELETE FROM balda_app_kv
				WHERE namespace = ? AND key = ?`,
				s.namespace, trimmedKey,
			); err != nil {
				return nil, false, fmt.Errorf("delete expired json key %q: %w", trimmedKey, err)
			}
			if err := tx.Commit(); err != nil {
				return nil, false, fmt.Errorf("commit expired consume json key %q: %w", trimmedKey, err)
			}
			return nil, false, nil
		}
	}

	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return nil, false, fmt.Errorf("unmarshal json key %q: %w", trimmedKey, err)
	}
	consume, err := shouldConsume(value)
	if err != nil {
		return nil, false, err
	}
	if !consume {
		if err := tx.Commit(); err != nil {
			return nil, false, fmt.Errorf("commit skipped consume json key %q: %w", trimmedKey, err)
		}
		return value, false, nil
	}

	result, err := tx.ExecContext(ctx, `
		DELETE FROM balda_app_kv
		WHERE namespace = ? AND key = ?`,
		s.namespace, trimmedKey,
	)
	if err != nil {
		return nil, false, fmt.Errorf("delete consumed json key %q: %w", trimmedKey, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return nil, false, fmt.Errorf("check consumed json key %q: %w", trimmedKey, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, false, fmt.Errorf("commit consumed json key %q: %w", trimmedKey, err)
	}
	return value, rows == 1, nil
}

func (s *sqliteKVStore) SetJSON(ctx context.Context, key string, value any) error {
	trimmedKey := strings.TrimSpace(key)
	if trimmedKey == "" {
		return fmt.Errorf("key is required")
	}

	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal json key %q: %w", trimmedKey, err)
	}

	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO balda_app_kv (namespace, key, value_json, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(namespace, key)
		DO UPDATE SET value_json = excluded.value_json, expires_at = NULL, updated_at = excluded.updated_at`,
		s.namespace, trimmedKey, string(encoded), time.Now().UTC().Format(time.RFC3339),
	); err != nil {
		return fmt.Errorf("set json key %q: %w", trimmedKey, err)
	}
	return nil
}

func (s *sqliteKVStore) MergeJSON(ctx context.Context, key string, fields map[string]any) (map[string]any, error) {
	trimmedKey := strings.TrimSpace(key)
	if trimmedKey == "" {
		return nil, fmt.Errorf("key is required")
	}
	if len(fields) == 0 {
		return map[string]any{}, nil
	}

	current, ok, err := s.GetJSON(ctx, trimmedKey)
	if err != nil {
		return nil, err
	}

	merged := make(map[string]any)
	if ok {
		if currentMap, ok := current.(map[string]any); ok {
			for k, v := range currentMap {
				merged[k] = v
			}
		}
	}

	for k, v := range fields {
		merged[k] = v
	}

	if err := s.SetJSON(ctx, trimmedKey, merged); err != nil {
		return nil, err
	}

	// Keep deterministic map iteration in tests that compare marshaled JSON.
	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ordered := make(map[string]any, len(merged))
	for _, k := range keys {
		ordered[k] = merged[k]
	}

	return ordered, nil
}
