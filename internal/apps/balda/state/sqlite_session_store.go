package state

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type sqliteSessionStore struct {
	db *sql.DB
}

func (s *sqliteSessionStore) Upsert(ctx context.Context, record SessionRecord) error {
	sessionID := strings.TrimSpace(record.SessionID)
	if sessionID == "" {
		return fmt.Errorf("session_id is required")
	}
	channelType := strings.TrimSpace(record.ChannelType)
	if channelType == "" {
		return fmt.Errorf("channel_type is required")
	}
	addressKey := strings.TrimSpace(record.AddressKey)
	if addressKey == "" {
		return fmt.Errorf("address_key is required")
	}
	addressJSON := strings.TrimSpace(record.AddressJSON)
	if addressJSON == "" {
		return fmt.Errorf("address_json is required")
	}

	if strings.TrimSpace(record.Status) == "" {
		record.Status = SessionStatusActive
	}

	chatID, topicID := legacyTelegramAddress(record)

	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO balda_session_metadata (
			session_id, user_id, chat_id, topic_id, channel_type, address_key, address_json, agent_name, workspace_dir, branch_name, status, updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(session_id) DO UPDATE SET
			user_id = excluded.user_id,
			chat_id = excluded.chat_id,
			topic_id = excluded.topic_id,
			channel_type = excluded.channel_type,
			address_key = excluded.address_key,
			address_json = excluded.address_json,
			agent_name = excluded.agent_name,
			workspace_dir = excluded.workspace_dir,
			branch_name = excluded.branch_name,
			status = excluded.status,
			updated_at = excluded.updated_at`,
		sessionID,
		strings.TrimSpace(record.UserID),
		chatID,
		topicID,
		channelType,
		addressKey,
		addressJSON,
		record.AgentName,
		record.WorkspaceDir,
		record.BranchName,
		record.Status,
		time.Now().UTC().Format(time.RFC3339),
	); err != nil {
		return fmt.Errorf("upsert balda session %q: %w", sessionID, err)
	}

	return nil
}

func legacyTelegramAddress(record SessionRecord) (int64, int64) {
	if strings.TrimSpace(record.ChannelType) != ChannelTypeTelegram {
		return 0, 0
	}

	chatIDRaw, topicIDRaw, ok := strings.Cut(strings.TrimSpace(record.AddressKey), ":")
	if !ok {
		return 0, 0
	}
	chatID, err := strconv.ParseInt(strings.TrimSpace(chatIDRaw), 10, 64)
	if err != nil {
		return 0, 0
	}
	topicID, err := strconv.ParseInt(strings.TrimSpace(topicIDRaw), 10, 64)
	if err != nil {
		return 0, 0
	}
	return chatID, topicID
}

func (s *sqliteSessionStore) GetByAddress(ctx context.Context, channelType, addressKey string) (SessionRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT session_id, user_id, channel_type, address_key, address_json, agent_name, workspace_dir, branch_name, status
		FROM balda_session_metadata
		WHERE channel_type = ? AND address_key = ?`,
		strings.TrimSpace(channelType), strings.TrimSpace(addressKey),
	)

	var record SessionRecord
	if err := row.Scan(
		&record.SessionID,
		&record.UserID,
		&record.ChannelType,
		&record.AddressKey,
		&record.AddressJSON,
		&record.AgentName,
		&record.WorkspaceDir,
		&record.BranchName,
		&record.Status,
	); err != nil {
		if err == sql.ErrNoRows {
			return SessionRecord{}, false, nil
		}
		return SessionRecord{}, false, fmt.Errorf("get balda session by address: %w", err)
	}

	return record, true, nil
}

func (s *sqliteSessionStore) GetBySessionID(ctx context.Context, sessionID string) (SessionRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT session_id, user_id, channel_type, address_key, address_json, agent_name, workspace_dir, branch_name, status
		FROM balda_session_metadata
		WHERE session_id = ?`,
		strings.TrimSpace(sessionID),
	)

	var record SessionRecord
	if err := row.Scan(
		&record.SessionID,
		&record.UserID,
		&record.ChannelType,
		&record.AddressKey,
		&record.AddressJSON,
		&record.AgentName,
		&record.WorkspaceDir,
		&record.BranchName,
		&record.Status,
	); err != nil {
		if err == sql.ErrNoRows {
			return SessionRecord{}, false, nil
		}
		return SessionRecord{}, false, fmt.Errorf("get balda session by session_id: %w", err)
	}

	return record, true, nil
}

func (s *sqliteSessionStore) DeleteBySessionID(ctx context.Context, sessionID string) error {
	trimmed := strings.TrimSpace(sessionID)
	if trimmed == "" {
		return nil
	}

	if _, err := s.db.ExecContext(ctx, `
		DELETE FROM balda_session_metadata
		WHERE session_id = ?`,
		trimmed,
	); err != nil {
		return fmt.Errorf("delete balda session %q: %w", trimmed, err)
	}
	return nil
}

func (s *sqliteSessionStore) List(ctx context.Context) ([]SessionRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT session_id, user_id, channel_type, address_key, address_json, agent_name, workspace_dir, branch_name, status
		FROM balda_session_metadata
		ORDER BY updated_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list balda sessions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]SessionRecord, 0)
	for rows.Next() {
		var record SessionRecord
		if err := rows.Scan(
			&record.SessionID,
			&record.UserID,
			&record.ChannelType,
			&record.AddressKey,
			&record.AddressJSON,
			&record.AgentName,
			&record.WorkspaceDir,
			&record.BranchName,
			&record.Status,
		); err != nil {
			return nil, fmt.Errorf("scan balda session: %w", err)
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate balda sessions: %w", err)
	}

	return out, nil
}
