package state

import (
	"context"
	"database/sql"
	"fmt"
)

func up00021ExecutionJobTables(ctx context.Context, tx *sql.Tx) error {
	executionExists, err := sqliteTxTableExists(ctx, tx, "execution_tasks")
	if err != nil {
		return fmt.Errorf("inspect execution task table: %w", err)
	}
	if executionExists {
		return ensureExecutionJobIndexesTx(ctx, tx)
	}
	runtimeExists, err := sqliteTxTableExists(ctx, tx, "runtime_tasks")
	if err != nil {
		return fmt.Errorf("inspect runtime task table: %w", err)
	}
	if runtimeExists {
		stmts := []string{
			"ALTER TABLE runtime_tasks RENAME TO execution_tasks",
			"ALTER TABLE runtime_task_events RENAME TO execution_task_events",
			"ALTER TABLE runtime_delivery_outbox RENAME TO execution_delivery_outbox",
			"ALTER TABLE runtime_agent_steps RENAME TO execution_agent_steps",
		}
		for _, stmt := range stmts {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("rename execution job tables: %w", err)
			}
		}
		return ensureExecutionJobIndexesTx(ctx, tx)
	}
	legacyExists, err := sqliteTxTableExists(ctx, tx, "swarm_tasks")
	if err != nil {
		return fmt.Errorf("inspect legacy task table: %w", err)
	}
	if !legacyExists {
		return fmt.Errorf("execution job tables are missing")
	}
	stmts := []string{
		"ALTER TABLE swarm_tasks RENAME TO execution_tasks",
		"ALTER TABLE swarm_task_events RENAME TO execution_task_events",
		"ALTER TABLE swarm_delivery_outbox RENAME TO execution_delivery_outbox",
		"ALTER TABLE swarm_agent_steps RENAME TO execution_agent_steps",
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("rename execution job tables: %w", err)
		}
	}
	return ensureExecutionJobIndexesTx(ctx, tx)
}

func down00021ExecutionJobTables(ctx context.Context, tx *sql.Tx) error {
	legacyExists, err := sqliteTxTableExists(ctx, tx, "swarm_tasks")
	if err != nil {
		return fmt.Errorf("inspect legacy task table: %w", err)
	}
	if legacyExists {
		return nil
	}
	executionExists, err := sqliteTxTableExists(ctx, tx, "execution_tasks")
	if err != nil {
		return fmt.Errorf("inspect execution task table: %w", err)
	}
	if !executionExists {
		return nil
	}
	stmts := []string{
		"DROP INDEX IF EXISTS idx_execution_tasks_session_status",
		"DROP INDEX IF EXISTS idx_execution_tasks_status_updated",
		"DROP INDEX IF EXISTS idx_execution_task_events_task",
		"DROP INDEX IF EXISTS idx_execution_delivery_outbox_task",
		"DROP INDEX IF EXISTS idx_execution_agent_steps_task",
		"ALTER TABLE execution_agent_steps RENAME TO swarm_agent_steps",
		"ALTER TABLE execution_delivery_outbox RENAME TO swarm_delivery_outbox",
		"ALTER TABLE execution_task_events RENAME TO swarm_task_events",
		"ALTER TABLE execution_tasks RENAME TO swarm_tasks",
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("revert execution job tables: %w", err)
		}
	}
	legacyIdx := []string{
		"CREATE INDEX IF NOT EXISTS idx_swarm_tasks_session_status ON swarm_tasks(session_id, status, updated_at)",
		"CREATE INDEX IF NOT EXISTS idx_swarm_tasks_status_updated ON swarm_tasks(status, updated_at)",
		"CREATE INDEX IF NOT EXISTS idx_swarm_task_events_task ON swarm_task_events(task_id, created_at)",
		"CREATE INDEX IF NOT EXISTS idx_swarm_delivery_outbox_task ON swarm_delivery_outbox(task_id, created_at)",
		"CREATE INDEX IF NOT EXISTS idx_swarm_agent_steps_task ON swarm_agent_steps(task_id, created_at)",
	}
	for _, stmt := range legacyIdx {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("restore legacy job indexes: %w", err)
		}
	}
	return nil
}

func ensureExecutionJobIndexesTx(ctx context.Context, tx *sql.Tx) error {
	stmts := []string{
		"DROP INDEX IF EXISTS idx_swarm_tasks_session_status",
		"DROP INDEX IF EXISTS idx_swarm_tasks_status_updated",
		"DROP INDEX IF EXISTS idx_swarm_task_events_task",
		"DROP INDEX IF EXISTS idx_swarm_delivery_outbox_task",
		"DROP INDEX IF EXISTS idx_swarm_agent_steps_task",
		"CREATE INDEX IF NOT EXISTS idx_execution_tasks_session_status ON execution_tasks(session_id, status, updated_at)",
		"CREATE INDEX IF NOT EXISTS idx_execution_tasks_status_updated ON execution_tasks(status, updated_at)",
		"CREATE INDEX IF NOT EXISTS idx_execution_task_events_task ON execution_task_events(task_id, created_at)",
		"CREATE INDEX IF NOT EXISTS idx_execution_delivery_outbox_task ON execution_delivery_outbox(task_id, created_at)",
		"CREATE INDEX IF NOT EXISTS idx_execution_agent_steps_task ON execution_agent_steps(task_id, created_at)",
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ensure execution job indexes: %w", err)
		}
	}
	return nil
}
