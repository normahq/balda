package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type sqliteScheduledTaskStore struct {
	db *sql.DB
}

func (s *sqliteScheduledTaskStore) Upsert(ctx context.Context, record ScheduledTaskRecord) error {
	jobID := strings.TrimSpace(record.JobID)
	if jobID == "" {
		return fmt.Errorf("job id is required")
	}
	channelType := strings.TrimSpace(record.ChannelType)
	if channelType == "" {
		return fmt.Errorf("channel_type is required")
	}
	addressKey := strings.TrimSpace(record.AddressKey)
	if addressKey == "" {
		return fmt.Errorf("address_key is required")
	}
	addressJSON := strings.TrimSpace(record.AddressJSON)
	if addressJSON == "" {
		return fmt.Errorf("address_json is required")
	}
	content := strings.TrimSpace(record.Content)
	if content == "" {
		return fmt.Errorf("content is required")
	}
	scheduleSpec := strings.TrimSpace(record.ScheduleSpec)
	if scheduleSpec == "" {
		return fmt.Errorf("schedule_spec is required")
	}
	if record.NextRunAt.IsZero() {
		return fmt.Errorf("next_run_at is required")
	}

	status := strings.TrimSpace(record.Status)
	if status == "" {
		status = ScheduledTaskStatusActive
	}
	timezone := strings.TrimSpace(record.Timezone)
	if timezone == "" {
		timezone = "UTC"
	}
	maxRetries := record.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	retryCount := record.RetryCount
	if retryCount < 0 {
		retryCount = 0
	}

	now := time.Now().UTC()
	createdAt := record.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = now
	}
	updatedAt := now

	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO balda_scheduled_tasks (
			task_id, session_id, channel_type, address_key, address_json,
			report_to_enabled, report_to_session_id, report_to_channel_type, report_to_address_key, report_to_address_json,
			content, schedule_spec, timezone, status,
			max_retries, retry_count, last_dispatch_key, next_run_at, last_run_at, last_error, created_at, updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(task_id) DO UPDATE SET
			session_id = excluded.session_id,
			channel_type = excluded.channel_type,
			address_key = excluded.address_key,
			address_json = excluded.address_json,
			report_to_enabled = excluded.report_to_enabled,
			report_to_session_id = excluded.report_to_session_id,
			report_to_channel_type = excluded.report_to_channel_type,
			report_to_address_key = excluded.report_to_address_key,
			report_to_address_json = excluded.report_to_address_json,
			content = excluded.content,
			schedule_spec = excluded.schedule_spec,
			timezone = excluded.timezone,
			status = excluded.status,
			max_retries = excluded.max_retries,
			retry_count = excluded.retry_count,
			last_dispatch_key = excluded.last_dispatch_key,
			next_run_at = excluded.next_run_at,
			last_run_at = excluded.last_run_at,
			last_error = excluded.last_error,
			updated_at = excluded.updated_at,
			created_at = balda_scheduled_tasks.created_at`,
		jobID,
		strings.TrimSpace(record.SessionID),
		channelType,
		addressKey,
		addressJSON,
		record.ReportToEnabled,
		strings.TrimSpace(record.ReportToSessionID),
		strings.TrimSpace(record.ReportToChannelType),
		strings.TrimSpace(record.ReportToAddressKey),
		strings.TrimSpace(record.ReportToAddressJSON),
		content,
		scheduleSpec,
		timezone,
		status,
		maxRetries,
		retryCount,
		strings.TrimSpace(record.LastDispatchKey),
		record.NextRunAt.UTC().Format(time.RFC3339),
		func() string {
			if record.LastRunAt.IsZero() {
				return ""
			}
			return record.LastRunAt.UTC().Format(time.RFC3339)
		}(),
		strings.TrimSpace(record.LastError),
		createdAt.Format(time.RFC3339),
		updatedAt.Format(time.RFC3339),
	); err != nil {
		return fmt.Errorf("upsert scheduled job %q: %w", jobID, err)
	}

	return nil
}

func (s *sqliteScheduledTaskStore) GetByID(ctx context.Context, jobID string) (ScheduledTaskRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT task_id, session_id, channel_type, address_key, address_json,
		       report_to_enabled, report_to_session_id, report_to_channel_type, report_to_address_key, report_to_address_json,
		       content, schedule_spec, timezone, status,
		       max_retries, retry_count, last_dispatch_key, next_run_at, last_run_at, last_error, created_at, updated_at
		FROM balda_scheduled_tasks
		WHERE task_id = ?`,
		strings.TrimSpace(jobID),
	)

	record, ok, err := scanScheduledTask(row.Scan)
	if err != nil {
		return ScheduledTaskRecord{}, false, err
	}
	return record, ok, nil
}

func (s *sqliteScheduledTaskStore) List(ctx context.Context) ([]ScheduledTaskRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT task_id, session_id, channel_type, address_key, address_json,
		       report_to_enabled, report_to_session_id, report_to_channel_type, report_to_address_key, report_to_address_json,
		       content, schedule_spec, timezone, status,
		       max_retries, retry_count, last_dispatch_key, next_run_at, last_run_at, last_error, created_at, updated_at
		FROM balda_scheduled_tasks
		ORDER BY task_id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list scheduled jobs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return readScheduledTasks(rows)
}

func (s *sqliteScheduledTaskStore) ListByAddress(
	ctx context.Context,
	channelType string,
	addressKey string,
) ([]ScheduledTaskRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT task_id, session_id, channel_type, address_key, address_json,
		       report_to_enabled, report_to_session_id, report_to_channel_type, report_to_address_key, report_to_address_json,
		       content, schedule_spec, timezone, status,
		       max_retries, retry_count, last_dispatch_key, next_run_at, last_run_at, last_error, created_at, updated_at
		FROM balda_scheduled_tasks
		WHERE channel_type = ? AND address_key = ?
		ORDER BY next_run_at ASC`,
		strings.TrimSpace(channelType), strings.TrimSpace(addressKey),
	)
	if err != nil {
		return nil, fmt.Errorf("list scheduled jobs by address: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return readScheduledTasks(rows)
}

func (s *sqliteScheduledTaskStore) ListDue(ctx context.Context, now time.Time, limit int) ([]ScheduledTaskRecord, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT task_id, session_id, channel_type, address_key, address_json,
		       report_to_enabled, report_to_session_id, report_to_channel_type, report_to_address_key, report_to_address_json,
		       content, schedule_spec, timezone, status,
		       max_retries, retry_count, last_dispatch_key, next_run_at, last_run_at, last_error, created_at, updated_at
		FROM balda_scheduled_tasks
		WHERE status = ? AND next_run_at <= ?
		ORDER BY next_run_at ASC
		LIMIT ?`,
		ScheduledTaskStatusActive,
		now.UTC().Format(time.RFC3339),
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list due scheduled jobs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return readScheduledTasks(rows)
}

func (s *sqliteScheduledTaskStore) Delete(ctx context.Context, jobID string) error {
	trimmed := strings.TrimSpace(jobID)
	if trimmed == "" {
		return nil
	}

	if _, err := s.db.ExecContext(ctx, `
		DELETE FROM balda_scheduled_tasks
		WHERE task_id = ?`,
		trimmed,
	); err != nil {
		return fmt.Errorf("delete scheduled job %q: %w", trimmed, err)
	}
	return nil
}

func readScheduledTasks(rows *sql.Rows) ([]ScheduledTaskRecord, error) {
	out := make([]ScheduledTaskRecord, 0)
	for rows.Next() {
		record, ok, err := scanScheduledTask(rows.Scan)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, record)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate scheduled jobs: %w", err)
	}
	return out, nil
}

func scanScheduledTask(scan func(dest ...any) error) (ScheduledTaskRecord, bool, error) {
	var (
		record       ScheduledTaskRecord
		nextRunAtRaw string
		lastRunAtRaw string
		createdAtRaw string
		updatedAtRaw string
	)

	err := scan(
		&record.JobID,
		&record.SessionID,
		&record.ChannelType,
		&record.AddressKey,
		&record.AddressJSON,
		&record.ReportToEnabled,
		&record.ReportToSessionID,
		&record.ReportToChannelType,
		&record.ReportToAddressKey,
		&record.ReportToAddressJSON,
		&record.Content,
		&record.ScheduleSpec,
		&record.Timezone,
		&record.Status,
		&record.MaxRetries,
		&record.RetryCount,
		&record.LastDispatchKey,
		&nextRunAtRaw,
		&lastRunAtRaw,
		&record.LastError,
		&createdAtRaw,
		&updatedAtRaw,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return ScheduledTaskRecord{}, false, nil
		}
		return ScheduledTaskRecord{}, false, fmt.Errorf("scan scheduled job: %w", err)
	}

	nextRunAt, err := parseRequiredRFC3339(nextRunAtRaw)
	if err != nil {
		return ScheduledTaskRecord{}, false, fmt.Errorf("parse next_run_at: %w", err)
	}
	createdAt, err := parseRequiredRFC3339(createdAtRaw)
	if err != nil {
		return ScheduledTaskRecord{}, false, fmt.Errorf("parse created_at: %w", err)
	}
	updatedAt, err := parseRequiredRFC3339(updatedAtRaw)
	if err != nil {
		return ScheduledTaskRecord{}, false, fmt.Errorf("parse updated_at: %w", err)
	}
	lastRunAt, err := parseOptionalRFC3339(lastRunAtRaw)
	if err != nil {
		return ScheduledTaskRecord{}, false, fmt.Errorf("parse last_run_at: %w", err)
	}

	record.NextRunAt = nextRunAt
	record.LastRunAt = lastRunAt
	record.CreatedAt = createdAt
	record.UpdatedAt = updatedAt

	return record, true, nil
}

func parseRequiredRFC3339(raw string) (time.Time, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, fmt.Errorf("timestamp is required")
	}
	parsed, err := time.Parse(time.RFC3339, trimmed)
	if err != nil {
		return time.Time{}, err
	}
	return parsed.UTC(), nil
}

func parseOptionalRFC3339(raw string) (time.Time, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339, trimmed)
	if err != nil {
		return time.Time{}, err
	}
	return parsed.UTC(), nil
}
