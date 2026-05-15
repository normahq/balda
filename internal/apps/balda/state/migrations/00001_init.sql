-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS relay_app_kv (
    namespace TEXT NOT NULL,
    key TEXT NOT NULL,
    value_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (namespace, key)
);

CREATE TABLE IF NOT EXISTS relay_session_metadata (
    session_id TEXT PRIMARY KEY,
    chat_id INTEGER NOT NULL,
    topic_id INTEGER NOT NULL,
    agent_name TEXT NOT NULL,
    workspace_dir TEXT NOT NULL,
    branch_name TEXT NOT NULL,
    status TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (chat_id, topic_id)
);

CREATE INDEX IF NOT EXISTS idx_relay_session_metadata_status ON relay_session_metadata(status);

CREATE TABLE IF NOT EXISTS relay_telegram_offsets (
    bot_key TEXT PRIMARY KEY,
    offset INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

INSERT OR IGNORE INTO schema_migrations(version, applied_at)
VALUES(1, datetime('now'));
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DELETE FROM schema_migrations WHERE version = 1;
DROP INDEX IF EXISTS idx_relay_session_metadata_status;
DROP TABLE IF EXISTS relay_telegram_offsets;
DROP TABLE IF EXISTS relay_session_metadata;
DROP TABLE IF EXISTS relay_app_kv;
DROP TABLE IF EXISTS schema_migrations;
-- +goose StatementEnd
