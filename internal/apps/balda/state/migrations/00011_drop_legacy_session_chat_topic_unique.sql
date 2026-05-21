-- +goose Up
-- +goose StatementBegin
PRAGMA defer_foreign_keys = ON;

DROP INDEX IF EXISTS idx_balda_session_metadata_status;
DROP INDEX IF EXISTS idx_balda_session_metadata_channel_address;

CREATE TABLE balda_session_metadata_new (
    session_id TEXT PRIMARY KEY,
    chat_id INTEGER NOT NULL DEFAULT 0,
    topic_id INTEGER NOT NULL DEFAULT 0,
    agent_name TEXT NOT NULL,
    workspace_dir TEXT NOT NULL,
    branch_name TEXT NOT NULL,
    status TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    channel_type TEXT NOT NULL DEFAULT 'telegram',
    address_key TEXT NOT NULL DEFAULT '',
    address_json TEXT NOT NULL DEFAULT '{}',
    user_id TEXT NOT NULL DEFAULT ''
);

INSERT INTO balda_session_metadata_new (
    session_id,
    chat_id,
    topic_id,
    agent_name,
    workspace_dir,
    branch_name,
    status,
    updated_at,
    channel_type,
    address_key,
    address_json,
    user_id
)
SELECT
    session_id,
    CASE
        WHEN channel_type = 'telegram' AND instr(address_key, ':') > 0
            THEN CAST(substr(address_key, 1, instr(address_key, ':') - 1) AS INTEGER)
        ELSE chat_id
    END AS chat_id,
    CASE
        WHEN channel_type = 'telegram' AND instr(address_key, ':') > 0
            THEN CAST(substr(address_key, instr(address_key, ':') + 1) AS INTEGER)
        ELSE topic_id
    END AS topic_id,
    agent_name,
    workspace_dir,
    branch_name,
    status,
    updated_at,
    channel_type,
    address_key,
    address_json,
    user_id
FROM balda_session_metadata;

DROP TABLE balda_session_metadata;
ALTER TABLE balda_session_metadata_new RENAME TO balda_session_metadata;

CREATE INDEX IF NOT EXISTS idx_balda_session_metadata_status
    ON balda_session_metadata(status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_balda_session_metadata_channel_address
    ON balda_session_metadata(channel_type, address_key);

INSERT OR IGNORE INTO schema_migrations(version, applied_at)
VALUES(11, datetime('now'));
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DELETE FROM schema_migrations WHERE version = 11;
-- The removed UNIQUE(chat_id, topic_id) constraint is intentionally not restored.
-- Reintroducing it can fail or lose non-Telegram/channel-address rows created after this migration.
-- +goose StatementEnd
