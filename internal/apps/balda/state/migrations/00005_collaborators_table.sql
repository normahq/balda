-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS relay_collaborators (
    user_id TEXT PRIMARY KEY,
    username TEXT NOT NULL DEFAULT '',
    first_name TEXT NOT NULL DEFAULT '',
    added_by TEXT NOT NULL,
    added_at TEXT NOT NULL
);

INSERT OR IGNORE INTO schema_migrations(version, applied_at)
VALUES(5, datetime('now'));
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DELETE FROM schema_migrations WHERE version = 5;
DROP TABLE IF EXISTS relay_collaborators;
-- +goose StatementEnd