package state

import (
	"context"
	"database/sql"
	"fmt"

)

func up00023ScheduledJobStorageNaming(ctx context.Context, tx *sql.Tx) error {
	if err := renameTableIfExists(ctx, tx, "balda_scheduled_tasks", "balda_scheduled_jobs"); err != nil {
		return err
	}
	return ensureScheduledJobIndexesTx(ctx, tx)
}

func down00023ScheduledJobStorageNaming(ctx context.Context, tx *sql.Tx) error {
	if err := dropScheduledJobIndexesTx(ctx, tx); err != nil {
		return err
	}
	if err := renameTableIfExists(ctx, tx, "balda_scheduled_jobs", "balda_scheduled_tasks"); err != nil {
		return err
	}
	return ensureScheduledTaskIndexesForDownTx(ctx, tx)
}

func ensureScheduledJobIndexesTx(ctx context.Context, tx *sql.Tx) error {
	stmts := []string{
		"DROP INDEX IF EXISTS idx_balda_scheduled_tasks_due",
		"DROP INDEX IF EXISTS idx_balda_scheduled_tasks_locator",
		"CREATE INDEX IF NOT EXISTS idx_balda_scheduled_jobs_due ON balda_scheduled_jobs(status, next_run_at)",
		"CREATE INDEX IF NOT EXISTS idx_balda_scheduled_jobs_locator ON balda_scheduled_jobs(channel_type, address_key)",
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ensure scheduled job indexes: %w", err)
		}
	}
	return nil
}

func dropScheduledJobIndexesTx(ctx context.Context, tx *sql.Tx) error {
	stmts := []string{
		"DROP INDEX IF EXISTS idx_balda_scheduled_jobs_due",
		"DROP INDEX IF EXISTS idx_balda_scheduled_jobs_locator",
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("drop scheduled job indexes: %w", err)
		}
	}
	return nil
}

func ensureScheduledTaskIndexesForDownTx(ctx context.Context, tx *sql.Tx) error {
	stmts := []string{
		"CREATE INDEX IF NOT EXISTS idx_balda_scheduled_tasks_due ON balda_scheduled_tasks(status, next_run_at)",
		"CREATE INDEX IF NOT EXISTS idx_balda_scheduled_tasks_locator ON balda_scheduled_tasks(channel_type, address_key)",
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("restore scheduled task indexes: %w", err)
		}
	}
	return nil
}
