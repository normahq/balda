-- +goose Up
-- +goose StatementBegin
-- Collaborators are stored in relay_app_kv with prefix "collaborator:userID"
-- No additional schema needed - uses existing KV store

INSERT OR IGNORE INTO schema_migrations(version, applied_at)
VALUES(3, datetime('now'));
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DELETE FROM schema_migrations WHERE version = 3;
-- +goose StatementEnd