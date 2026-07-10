package state

import (
	"context"
	"database/sql"
	"fmt"

)

func up00022ExecutionJobStorageNaming(ctx context.Context, tx *sql.Tx) error {
	if err := renameTableIfExists(ctx, tx, "execution_tasks", "execution_jobs"); err != nil {
		return err
	}
	if err := renameTableIfExists(ctx, tx, "execution_task_events", "execution_job_events"); err != nil {
		return err
	}
	if err := renameTableIfExists(ctx, tx, "execution_delivery_outbox", "execution_delivery_outbox"); err != nil {
		return err
	}
	if err := renameTableIfExists(ctx, tx, "execution_agent_steps", "execution_agent_steps"); err != nil {
		return err
	}
	if err := renameColumnIfExists(ctx, tx, "execution_jobs", "parent_task_id", "parent_job_id"); err != nil {
		return err
	}
	if err := renameColumnIfExists(ctx, tx, "execution_job_events", "task_id", "job_id"); err != nil {
		return err
	}
	if err := renameColumnIfExists(ctx, tx, "execution_delivery_outbox", "task_id", "job_id"); err != nil {
		return err
	}
	if err := renameColumnIfExists(ctx, tx, "execution_agent_steps", "task_id", "job_id"); err != nil {
		return err
	}
	if err := renameColumnIfExists(ctx, tx, "balda_scheduled_tasks", "task_id", "job_id"); err != nil {
		return err
	}
	return ensureExecutionJobStorageIndexesTx(ctx, tx)
}

func down00022ExecutionJobStorageNaming(ctx context.Context, tx *sql.Tx) error {
	if err := dropExecutionJobStorageIndexesTx(ctx, tx); err != nil {
		return err
	}
	if err := renameColumnIfExists(ctx, tx, "balda_scheduled_tasks", "job_id", "task_id"); err != nil {
		return err
	}
	if err := renameColumnIfExists(ctx, tx, "execution_agent_steps", "job_id", "task_id"); err != nil {
		return err
	}
	if err := renameColumnIfExists(ctx, tx, "execution_delivery_outbox", "job_id", "task_id"); err != nil {
		return err
	}
	if err := renameColumnIfExists(ctx, tx, "execution_job_events", "job_id", "task_id"); err != nil {
		return err
	}
	if err := renameColumnIfExists(ctx, tx, "execution_jobs", "parent_job_id", "parent_task_id"); err != nil {
		return err
	}
	if err := renameTableIfExists(ctx, tx, "execution_job_events", "execution_task_events"); err != nil {
		return err
	}
	if err := renameTableIfExists(ctx, tx, "execution_jobs", "execution_tasks"); err != nil {
		return err
	}
	return ensureExecutionTaskStorageIndexesForDownTx(ctx, tx)
}

func renameTableIfExists(ctx context.Context, tx *sql.Tx, from string, to string) error {
	if from == to {
		return nil
	}
	toExists, err := sqliteTxTableExists(ctx, tx, to)
	if err != nil {
		return fmt.Errorf("inspect %s table: %w", to, err)
	}
	if toExists {
		return nil
	}
	fromExists, err := sqliteTxTableExists(ctx, tx, from)
	if err != nil {
		return fmt.Errorf("inspect %s table: %w", from, err)
	}
	if !fromExists {
		return nil
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", from, to)); err != nil {
		return fmt.Errorf("rename %s to %s: %w", from, to, err)
	}
	return nil
}

func renameColumnIfExists(ctx context.Context, tx *sql.Tx, table string, from string, to string) error {
	hasTo, err := sqliteTxTableHasColumn(ctx, tx, table, to)
	if err != nil {
		return fmt.Errorf("inspect %s.%s: %w", table, to, err)
	}
	if hasTo {
		return nil
	}
	hasFrom, err := sqliteTxTableHasColumn(ctx, tx, table, from)
	if err != nil {
		return fmt.Errorf("inspect %s.%s: %w", table, from, err)
	}
	if !hasFrom {
		return nil
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", table, from, to)); err != nil {
		return fmt.Errorf("rename %s.%s to %s: %w", table, from, to, err)
	}
	return nil
}

func ensureExecutionJobStorageIndexesTx(ctx context.Context, tx *sql.Tx) error {
	stmts := []string{
		"DROP INDEX IF EXISTS idx_execution_tasks_session_status",
		"DROP INDEX IF EXISTS idx_execution_tasks_status_updated",
		"DROP INDEX IF EXISTS idx_execution_task_events_task",
		"DROP INDEX IF EXISTS idx_execution_delivery_outbox_task",
		"DROP INDEX IF EXISTS idx_execution_agent_steps_task",
		"CREATE INDEX IF NOT EXISTS idx_execution_jobs_session_status ON execution_jobs(session_id, status, updated_at)",
		"CREATE INDEX IF NOT EXISTS idx_execution_jobs_status_updated ON execution_jobs(status, updated_at)",
		"CREATE INDEX IF NOT EXISTS idx_execution_job_events_job ON execution_job_events(job_id, created_at)",
		"CREATE INDEX IF NOT EXISTS idx_execution_delivery_outbox_job ON execution_delivery_outbox(job_id, created_at)",
		"CREATE INDEX IF NOT EXISTS idx_execution_agent_steps_job ON execution_agent_steps(job_id, created_at)",
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ensure execution job storage indexes: %w", err)
		}
	}
	return nil
}

func dropExecutionJobStorageIndexesTx(ctx context.Context, tx *sql.Tx) error {
	stmts := []string{
		"DROP INDEX IF EXISTS idx_execution_jobs_session_status",
		"DROP INDEX IF EXISTS idx_execution_jobs_status_updated",
		"DROP INDEX IF EXISTS idx_execution_job_events_job",
		"DROP INDEX IF EXISTS idx_execution_delivery_outbox_job",
		"DROP INDEX IF EXISTS idx_execution_agent_steps_job",
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("drop execution job storage indexes: %w", err)
		}
	}
	return nil
}

func ensureExecutionTaskStorageIndexesForDownTx(ctx context.Context, tx *sql.Tx) error {
	stmts := []string{
		"CREATE INDEX IF NOT EXISTS idx_execution_tasks_session_status ON execution_tasks(session_id, status, updated_at)",
		"CREATE INDEX IF NOT EXISTS idx_execution_tasks_status_updated ON execution_tasks(status, updated_at)",
		"CREATE INDEX IF NOT EXISTS idx_execution_task_events_task ON execution_task_events(task_id, created_at)",
		"CREATE INDEX IF NOT EXISTS idx_execution_delivery_outbox_task ON execution_delivery_outbox(task_id, created_at)",
		"CREATE INDEX IF NOT EXISTS idx_execution_agent_steps_task ON execution_agent_steps(task_id, created_at)",
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("restore legacy execution task indexes: %w", err)
		}
	}
	return nil
}
