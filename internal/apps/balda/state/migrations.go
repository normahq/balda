package state

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"

	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var baldaMigrationsFS embed.FS

var requiredBaldaSQLiteTables = []string{
	"balda_app_kv",
	"balda_session_metadata",
	"balda_telegram_offsets",
	"balda_collaborators",
	"balda_adk_app_state",
	"balda_adk_user_state",
	"balda_adk_sessions",
	"balda_adk_events",
	"balda_scheduled_jobs",
}

func migrate(ctx context.Context, db *sql.DB) error {
	migrationsDir, err := fs.Sub(baldaMigrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("open balda migrations fs: %w", err)
	}

	provider, err := goose.NewProvider(goose.DialectSQLite3, db, migrationsDir)
	if err != nil {
		return fmt.Errorf("create balda migration provider: %w", err)
	}

	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("apply balda migrations: %w", err)
	}
	if err := validateBaldaSQLiteSchema(ctx, db); err != nil {
		return err
	}
	return nil
}

func validateBaldaSQLiteSchema(ctx context.Context, db *sql.DB) error {
	for _, table := range requiredBaldaSQLiteTables {
		exists, err := sqliteTableExists(ctx, db, table)
		if err != nil {
			return fmt.Errorf("inspect %s table: %w", table, err)
		}
		if !exists {
			return fmt.Errorf("balda state schema missing %s; back up and remove .config/balda/balda.db, then run balda init again", table)
		}
	}
	return nil
}

func sqliteTableExists(ctx context.Context, db *sql.DB, name string) (bool, error) {
	var exists int
	err := db.QueryRowContext(ctx, `
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
