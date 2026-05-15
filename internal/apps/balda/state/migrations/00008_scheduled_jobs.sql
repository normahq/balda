-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS relay_scheduled_jobs (
    job_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    channel_type TEXT NOT NULL,
    address_key TEXT NOT NULL,
    address_json TEXT NOT NULL,
    prompt TEXT NOT NULL,
    schedule_spec TEXT NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'UTC',
    status TEXT NOT NULL DEFAULT 'active',
    max_retries INTEGER NOT NULL DEFAULT 3,
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_dispatch_key TEXT NOT NULL DEFAULT '',
    next_run_at TEXT NOT NULL,
    last_run_at TEXT NOT NULL DEFAULT '',
    last_error TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_relay_scheduled_jobs_due
    ON relay_scheduled_jobs(status, next_run_at);

CREATE INDEX IF NOT EXISTS idx_relay_scheduled_jobs_locator
    ON relay_scheduled_jobs(channel_type, address_key);

INSERT OR IGNORE INTO schema_migrations(version, applied_at)
VALUES(8, datetime('now'));
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DELETE FROM schema_migrations WHERE version = 8;
DROP INDEX IF EXISTS idx_relay_scheduled_jobs_locator;
DROP INDEX IF EXISTS idx_relay_scheduled_jobs_due;
DROP TABLE IF EXISTS relay_scheduled_jobs;
-- +goose StatementEnd

