-- +goose Up
-- +goose StatementBegin
ALTER TABLE relay_session_metadata ADD COLUMN user_id TEXT NOT NULL DEFAULT '';

INSERT OR IGNORE INTO schema_migrations(version, applied_at)
VALUES(6, datetime('now'));
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DELETE FROM schema_migrations WHERE version = 6;
ALTER TABLE relay_session_metadata DROP COLUMN user_id;
-- +goose StatementEnd
