package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/pressly/goose/v3"
)

var registerBaldaGoMigrationsOnce sync.Once

func registerBaldaGoMigrations() {
	registerBaldaGoMigrationsOnce.Do(func() {
		goose.AddNamedMigrationContext("00020_runtime_session_tables.go", up00020RuntimeSessionTables, down00020RuntimeSessionTables)
		goose.AddNamedMigrationContext("00021_execution_job_tables.go", up00021ExecutionJobTables, down00021ExecutionJobTables)
		goose.AddNamedMigrationContext("00022_execution_job_storage_naming.go", up00022ExecutionJobStorageNaming, down00022ExecutionJobStorageNaming)
		goose.AddNamedMigrationContext("00023_scheduled_job_storage_naming.go", up00023ScheduledJobStorageNaming, down00023ScheduledJobStorageNaming)
		goose.AddNamedMigrationContext("00024_job_event_outbox.go", up00024JobEventOutbox, down00024JobEventOutbox)
		goose.AddNamedMigrationContext("00025_execution_storage_contract_names.go", up00025ExecutionStorageContractNames, down00025ExecutionStorageContractNames)
		goose.AddNamedMigrationContext("00026_question_tables.go", up00026QuestionTables, down00026QuestionTables)
		goose.AddNamedMigrationContext("00027_question_failures.go", up00027QuestionFailures, down00027QuestionFailures)
		goose.AddNamedMigrationContext("00028_question_control_handles.go", up00028QuestionControlHandles, down00028QuestionControlHandles)
	})
}

func up00020RuntimeSessionTables(ctx context.Context, tx *sql.Tx) error {
	runtimeExists, err := sqliteTxTableExists(ctx, tx, "balda_runtime_app_state")
	if err != nil {
		return fmt.Errorf("inspect runtime app state table: %w", err)
	}
	if runtimeExists {
		return ensureRuntimeSessionIndexesTx(ctx, tx)
	}

	previousSchemaExists, err := sqliteTxTableExists(ctx, tx, "balda_adk_app_state")
	if err != nil {
		return fmt.Errorf("inspect pre-runtime app state table: %w", err)
	}
	if !previousSchemaExists {
		return fmt.Errorf("runtime session tables are missing")
	}

	stmts := []string{
		"ALTER TABLE balda_adk_app_state RENAME TO balda_runtime_app_state",
		"ALTER TABLE balda_adk_user_state RENAME TO balda_runtime_user_state",
		"ALTER TABLE balda_adk_sessions RENAME TO balda_runtime_sessions",
		"ALTER TABLE balda_adk_events RENAME TO balda_runtime_events",
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("rename runtime session tables: %w", err)
		}
	}
	return ensureRuntimeSessionIndexesTx(ctx, tx)
}

func down00020RuntimeSessionTables(ctx context.Context, tx *sql.Tx) error {
	runtimeExists, err := sqliteTxTableExists(ctx, tx, "balda_runtime_app_state")
	if err != nil {
		return fmt.Errorf("inspect runtime app state table: %w", err)
	}
	if !runtimeExists {
		return nil
	}

	previousSchemaExists, err := sqliteTxTableExists(ctx, tx, "balda_adk_app_state")
	if err != nil {
		return fmt.Errorf("inspect pre-runtime app state table: %w", err)
	}
	if previousSchemaExists {
		return nil
	}

	stmts := []string{
		"DROP INDEX IF EXISTS idx_balda_runtime_sessions_app_user",
		"DROP INDEX IF EXISTS idx_balda_runtime_events_session_order",
		"ALTER TABLE balda_runtime_events RENAME TO balda_adk_events",
		"ALTER TABLE balda_runtime_sessions RENAME TO balda_adk_sessions",
		"ALTER TABLE balda_runtime_user_state RENAME TO balda_adk_user_state",
		"ALTER TABLE balda_runtime_app_state RENAME TO balda_adk_app_state",
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("revert runtime session tables: %w", err)
		}
	}

	stmts = []string{
		"CREATE INDEX IF NOT EXISTS idx_balda_adk_sessions_app_user ON balda_adk_sessions(app_name, user_id)",
		"CREATE INDEX IF NOT EXISTS idx_balda_adk_events_session_order ON balda_adk_events(app_name, user_id, session_id, timestamp, ordinal)",
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("restore pre-runtime session indexes: %w", err)
		}
	}
	return nil
}

func ensureRuntimeSessionIndexesTx(ctx context.Context, tx *sql.Tx) error {
	hasRuntimeSessionColumns, err := sqliteTxTableHasColumn(ctx, tx, "balda_runtime_sessions", "app_name")
	if err != nil {
		return fmt.Errorf("inspect runtime session columns: %w", err)
	}
	if !hasRuntimeSessionColumns {
		return nil
	}

	stmts := []string{
		"DROP INDEX IF EXISTS idx_balda_adk_sessions_app_user",
		"DROP INDEX IF EXISTS idx_balda_adk_events_session_order",
		"CREATE INDEX IF NOT EXISTS idx_balda_runtime_sessions_app_user ON balda_runtime_sessions(app_name, user_id)",
		"CREATE INDEX IF NOT EXISTS idx_balda_runtime_events_session_order ON balda_runtime_events(app_name, user_id, session_id, timestamp, ordinal)",
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ensure runtime session indexes: %w", err)
		}
	}
	return nil
}

func sqliteTxTableExists(ctx context.Context, tx *sql.Tx, name string) (bool, error) {
	var exists int
	err := tx.QueryRowContext(ctx, `
		SELECT 1
		FROM sqlite_master
		WHERE type = 'table' AND name = ?
		LIMIT 1`,
		name,
	).Scan(&exists)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func sqliteTxTableHasColumn(ctx context.Context, tx *sql.Tx, table string, column string) (bool, error) {
	rows, err := tx.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			cid     int
			name    string
			ctype   string
			notNull int
			dfltVal sql.NullString
			pk      int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notNull, &dfltVal, &pk); err != nil {
			return false, err
		}
		if strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(column)) {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
}
