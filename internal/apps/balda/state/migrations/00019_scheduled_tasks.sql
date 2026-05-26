-- +goose Up
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_balda_scheduled_jobs_due;
DROP INDEX IF EXISTS idx_balda_scheduled_jobs_locator;

ALTER TABLE balda_scheduled_jobs RENAME TO balda_scheduled_tasks;
ALTER TABLE balda_scheduled_tasks RENAME COLUMN job_id TO task_id;
ALTER TABLE balda_scheduled_tasks RENAME COLUMN prompt TO content;

ALTER TABLE balda_scheduled_tasks ADD COLUMN report_to_enabled INTEGER NOT NULL DEFAULT 0;
ALTER TABLE balda_scheduled_tasks ADD COLUMN report_to_session_id TEXT NOT NULL DEFAULT '';
ALTER TABLE balda_scheduled_tasks ADD COLUMN report_to_channel_type TEXT NOT NULL DEFAULT '';
ALTER TABLE balda_scheduled_tasks ADD COLUMN report_to_address_key TEXT NOT NULL DEFAULT '';
ALTER TABLE balda_scheduled_tasks ADD COLUMN report_to_address_json TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_balda_scheduled_tasks_due
    ON balda_scheduled_tasks(status, next_run_at);

CREATE INDEX IF NOT EXISTS idx_balda_scheduled_tasks_locator
    ON balda_scheduled_tasks(channel_type, address_key);

INSERT OR IGNORE INTO schema_migrations(version, applied_at)
VALUES(19, datetime('now'));
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DELETE FROM schema_migrations WHERE version = 19;

DROP INDEX IF EXISTS idx_balda_scheduled_tasks_locator;
DROP INDEX IF EXISTS idx_balda_scheduled_tasks_due;

CREATE TABLE IF NOT EXISTS balda_scheduled_jobs (
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

INSERT INTO balda_scheduled_jobs (
    job_id, session_id, channel_type, address_key, address_json, prompt, schedule_spec, timezone, status,
    max_retries, retry_count, last_dispatch_key, next_run_at, last_run_at, last_error, created_at, updated_at
)
SELECT
    task_id, session_id, channel_type, address_key, address_json, content, schedule_spec, timezone, status,
    max_retries, retry_count, last_dispatch_key, next_run_at, last_run_at, last_error, created_at, updated_at
FROM balda_scheduled_tasks;

DROP TABLE IF EXISTS balda_scheduled_tasks;

CREATE INDEX IF NOT EXISTS idx_balda_scheduled_jobs_due
    ON balda_scheduled_jobs(status, next_run_at);

CREATE INDEX IF NOT EXISTS idx_balda_scheduled_jobs_locator
    ON balda_scheduled_jobs(channel_type, address_key);
-- +goose StatementEnd
