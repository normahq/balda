-- +goose Up
-- +goose StatementBegin
ALTER TABLE relay_app_kv ADD COLUMN expires_at TEXT;

INSERT OR IGNORE INTO schema_migrations(version, applied_at)
VALUES(4, datetime('now'));
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DELETE FROM schema_migrations WHERE version = 4;
ALTER TABLE relay_app_kv DROP COLUMN expires_at;
-- +goose StatementEnd