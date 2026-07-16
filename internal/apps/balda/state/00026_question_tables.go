package state

import (
	"context"
	"database/sql"
	"fmt"
)

func up00026QuestionTables(ctx context.Context, tx *sql.Tx) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS balda_questions (
			question_id TEXT PRIMARY KEY,
			session_id TEXT NOT NULL,
			channel_kind TEXT NOT NULL,
			address_key TEXT NOT NULL,
			address_json TEXT NOT NULL,
			prompt TEXT NOT NULL,
			status TEXT NOT NULL,
			interaction_json TEXT NOT NULL,
			resume_json TEXT NOT NULL,
			request_json TEXT NOT NULL,
			answer_json TEXT NOT NULL DEFAULT '',
			provider TEXT NOT NULL DEFAULT '',
			conversation_key TEXT NOT NULL DEFAULT '',
			provider_message_id TEXT NOT NULL DEFAULT '',
			reply_handle TEXT NOT NULL DEFAULT '',
			control_handle TEXT NOT NULL DEFAULT '',
			expires_at TEXT NOT NULL DEFAULT '',
			answered_at TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_balda_questions_status_created
			ON balda_questions(status, created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_balda_questions_reply_lookup
			ON balda_questions(provider, conversation_key, provider_message_id)`,
		`CREATE INDEX IF NOT EXISTS idx_balda_questions_session_status
			ON balda_questions(session_id, status, created_at)`,
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("question tables migration: %w", err)
		}
	}
	return nil
}

func down00026QuestionTables(ctx context.Context, tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_balda_questions_session_status`,
		`DROP INDEX IF EXISTS idx_balda_questions_reply_lookup`,
		`DROP INDEX IF EXISTS idx_balda_questions_status_created`,
		`DROP TABLE IF EXISTS balda_questions`,
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("drop question tables: %w", err)
		}
	}
	return nil
}
