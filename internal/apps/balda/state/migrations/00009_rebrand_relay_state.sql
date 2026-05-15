-- +goose Up
-- +goose StatementBegin
PRAGMA defer_foreign_keys = ON;

ALTER TABLE relay_app_kv RENAME TO balda_app_kv;
ALTER TABLE relay_session_metadata RENAME TO balda_session_metadata;
ALTER TABLE relay_telegram_offsets RENAME TO balda_telegram_offsets;
ALTER TABLE relay_collaborators RENAME TO balda_collaborators;
ALTER TABLE relay_adk_app_state RENAME TO balda_adk_app_state;
ALTER TABLE relay_adk_user_state RENAME TO balda_adk_user_state;
ALTER TABLE relay_adk_sessions RENAME TO balda_adk_sessions;
ALTER TABLE relay_adk_events RENAME TO balda_adk_events;
ALTER TABLE relay_scheduled_jobs RENAME TO balda_scheduled_jobs;

DROP INDEX IF EXISTS idx_relay_session_metadata_status;
DROP INDEX IF EXISTS idx_relay_session_metadata_channel_address;
DROP INDEX IF EXISTS idx_relay_adk_sessions_app_user;
DROP INDEX IF EXISTS idx_relay_adk_events_session_order;
DROP INDEX IF EXISTS idx_relay_scheduled_jobs_due;
DROP INDEX IF EXISTS idx_relay_scheduled_jobs_locator;

CREATE INDEX IF NOT EXISTS idx_balda_session_metadata_status ON balda_session_metadata(status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_balda_session_metadata_channel_address ON balda_session_metadata(channel_type, address_key);
CREATE INDEX IF NOT EXISTS idx_balda_adk_sessions_app_user ON balda_adk_sessions(app_name, user_id);
CREATE INDEX IF NOT EXISTS idx_balda_adk_events_session_order ON balda_adk_events(app_name, user_id, session_id, timestamp, ordinal);
CREATE INDEX IF NOT EXISTS idx_balda_scheduled_jobs_due ON balda_scheduled_jobs(status, next_run_at);
CREATE INDEX IF NOT EXISTS idx_balda_scheduled_jobs_locator ON balda_scheduled_jobs(channel_type, address_key);

UPDATE balda_telegram_offsets SET bot_key = 'balda-default' WHERE bot_key = 'relay-default';
UPDATE balda_adk_app_state SET app_name = 'norma-balda' WHERE app_name = 'norma-relay';
UPDATE balda_adk_user_state SET app_name = 'norma-balda' WHERE app_name = 'norma-relay';
UPDATE balda_adk_sessions SET app_name = 'norma-balda' WHERE app_name = 'norma-relay';
UPDATE balda_adk_events SET app_name = 'norma-balda' WHERE app_name = 'norma-relay';

INSERT OR IGNORE INTO schema_migrations(version, applied_at)
VALUES(9, datetime('now'));
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
PRAGMA defer_foreign_keys = ON;

DELETE FROM schema_migrations WHERE version = 9;

DROP INDEX IF EXISTS idx_balda_session_metadata_status;
DROP INDEX IF EXISTS idx_balda_session_metadata_channel_address;
DROP INDEX IF EXISTS idx_balda_adk_sessions_app_user;
DROP INDEX IF EXISTS idx_balda_adk_events_session_order;
DROP INDEX IF EXISTS idx_balda_scheduled_jobs_due;
DROP INDEX IF EXISTS idx_balda_scheduled_jobs_locator;

UPDATE balda_telegram_offsets SET bot_key = 'relay-default' WHERE bot_key = 'balda-default';
UPDATE balda_adk_app_state SET app_name = 'norma-relay' WHERE app_name = 'norma-balda';
UPDATE balda_adk_user_state SET app_name = 'norma-relay' WHERE app_name = 'norma-balda';
UPDATE balda_adk_sessions SET app_name = 'norma-relay' WHERE app_name = 'norma-balda';
UPDATE balda_adk_events SET app_name = 'norma-relay' WHERE app_name = 'norma-balda';

ALTER TABLE balda_scheduled_jobs RENAME TO relay_scheduled_jobs;
ALTER TABLE balda_adk_events RENAME TO relay_adk_events;
ALTER TABLE balda_adk_sessions RENAME TO relay_adk_sessions;
ALTER TABLE balda_adk_user_state RENAME TO relay_adk_user_state;
ALTER TABLE balda_adk_app_state RENAME TO relay_adk_app_state;
ALTER TABLE balda_collaborators RENAME TO relay_collaborators;
ALTER TABLE balda_telegram_offsets RENAME TO relay_telegram_offsets;
ALTER TABLE balda_session_metadata RENAME TO relay_session_metadata;
ALTER TABLE balda_app_kv RENAME TO relay_app_kv;

CREATE INDEX IF NOT EXISTS idx_relay_session_metadata_status ON relay_session_metadata(status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_relay_session_metadata_channel_address ON relay_session_metadata(channel_type, address_key);
CREATE INDEX IF NOT EXISTS idx_relay_adk_sessions_app_user ON relay_adk_sessions(app_name, user_id);
CREATE INDEX IF NOT EXISTS idx_relay_adk_events_session_order ON relay_adk_events(app_name, user_id, session_id, timestamp, ordinal);
CREATE INDEX IF NOT EXISTS idx_relay_scheduled_jobs_due ON relay_scheduled_jobs(status, next_run_at);
CREATE INDEX IF NOT EXISTS idx_relay_scheduled_jobs_locator ON relay_scheduled_jobs(channel_type, address_key);
-- +goose StatementEnd
