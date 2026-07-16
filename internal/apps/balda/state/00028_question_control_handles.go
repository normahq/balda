package state

import (
	"context"
	"database/sql"
	"fmt"
)

func up00028QuestionControlHandles(ctx context.Context, tx *sql.Tx) error {
	hasColumn, err := sqliteTxTableHasColumn(ctx, tx, "balda_questions", "control_handle")
	if err != nil {
		return fmt.Errorf("inspect balda_questions.control_handle: %w", err)
	}
	if hasColumn {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `ALTER TABLE balda_questions ADD COLUMN control_handle TEXT NOT NULL DEFAULT ''`); err != nil {
		return fmt.Errorf("add balda_questions.control_handle: %w", err)
	}
	return nil
}

func down00028QuestionControlHandles(context.Context, *sql.Tx) error {
	return nil
}
