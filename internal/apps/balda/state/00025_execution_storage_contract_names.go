package state

import (
	"context"
	"database/sql"
	"fmt"
)

func up00025ExecutionStorageContractNames(ctx context.Context, tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE execution_jobs ADD COLUMN result TEXT`,
		`UPDATE execution_jobs SET result = result_json WHERE result IS NULL AND result_json IS NOT NULL`,
		`ALTER TABLE execution_job_events ADD COLUMN payload TEXT`,
		`UPDATE execution_job_events SET payload = payload_json WHERE payload IS NULL AND payload_json IS NOT NULL`,
		`ALTER TABLE execution_job_event_outbox ADD COLUMN envelope TEXT`,
		`UPDATE execution_job_event_outbox SET envelope = envelope_json WHERE envelope IS NULL AND envelope_json IS NOT NULL`,
		`ALTER TABLE execution_delivery_outbox ADD COLUMN payload TEXT`,
		`UPDATE execution_delivery_outbox SET payload = payload_json WHERE payload IS NULL AND payload_json IS NOT NULL`,
		`ALTER TABLE execution_agent_steps ADD COLUMN result TEXT`,
		`UPDATE execution_agent_steps SET result = result_json WHERE result IS NULL AND result_json IS NOT NULL`,
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil && !isSQLiteDuplicateColumn(err) {
			return fmt.Errorf("execution storage contract names migration: %w", err)
		}
	}
	return nil
}

func down00025ExecutionStorageContractNames(context.Context, *sql.Tx) error {
	return nil
}

func isSQLiteDuplicateColumn(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == "SQL logic error: duplicate column name: result (1)" ||
		err.Error() == "SQL logic error: duplicate column name: payload (1)" ||
		err.Error() == "SQL logic error: duplicate column name: envelope (1)"
}
