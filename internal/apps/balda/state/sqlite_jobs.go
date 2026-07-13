package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type sqliteJobStore struct {
	db *sql.DB
}

type contextExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func (s *sqliteJobStore) CreateJob(ctx context.Context, record JobRecord) (bool, error) {
	now := time.Now().UTC()
	normalized, err := normalizeExecutionJob(record, now)
	if err != nil {
		return false, err
	}
	return insertExecutionJob(ctx, s.db, normalized)
}

func insertExecutionJob(ctx context.Context, exec contextExecer, normalized JobRecord) (bool, error) {
	res, err := exec.ExecContext(ctx, `
		INSERT OR IGNORE INTO execution_jobs (
			id, session_id, parent_job_id, title, objective, status, owner_actor, assigned_actor,
			priority, created_by, result, error,
			created_at, updated_at, started_at, completed_at, canceled_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		normalized.ID,
		nullIfEmpty(normalized.SessionID),
		nullIfEmpty(normalized.ParentJobID),
		nullIfEmpty(normalized.Title),
		normalized.Objective,
		normalized.Status,
		nullIfEmpty(normalized.OwnerActor),
		nullIfEmpty(normalized.AssignedActor),
		normalized.Priority,
		nullIfEmpty(normalized.CreatedBy),
		nullIfEmpty(normalized.Result),
		nullIfEmpty(normalized.Error),
		normalized.CreatedAt.Format(time.RFC3339),
		normalized.UpdatedAt.Format(time.RFC3339),
		optionalTimeValue(normalized.StartedAt),
		optionalTimeValue(normalized.CompletedAt),
		optionalTimeValue(normalized.CanceledAt),
	)
	if err != nil {
		return false, fmt.Errorf("insert runtime job %q: %w", normalized.ID, err)
	}
	count, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("count inserted runtime job %q: %w", normalized.ID, err)
	}
	return count > 0, nil
}

func (s *sqliteJobStore) CreateJobWithEvent(
	ctx context.Context,
	record JobRecord,
	event JobEventOutboxRecord,
) (bool, error) {
	now := time.Now().UTC()
	normalizedJob, err := normalizeExecutionJob(record, now)
	if err != nil {
		return false, err
	}
	normalizedEvent, err := normalizeJobEventOutbox(event, now)
	if err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin create job transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	created, err := insertExecutionJob(ctx, tx, normalizedJob)
	if err != nil {
		return false, err
	}
	if err := enqueueJobEvent(ctx, tx, normalizedEvent); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit create job transaction: %w", err)
	}
	return created, nil
}

func (s *sqliteJobStore) GetJob(ctx context.Context, jobID string) (JobRecord, bool, error) {
	record, ok, err := scanExecutionJob(s.db.QueryRowContext(ctx, executionJobSelectSQL+` WHERE id = ?`, strings.TrimSpace(jobID)).Scan)
	if err != nil {
		return JobRecord{}, false, err
	}
	return record, ok, nil
}

func (s *sqliteJobStore) ListActiveJobsBySession(ctx context.Context, sessionID string) ([]JobRecord, error) {
	trimmed := strings.TrimSpace(sessionID)
	if trimmed == "" {
		return nil, fmt.Errorf("session id is required")
	}
	rows, err := s.db.QueryContext(ctx, executionJobSelectSQL+`
		WHERE session_id = ?
		  AND status NOT IN (?, ?, ?, ?)
		ORDER BY created_at ASC`,
		trimmed,
		JobStatusCompleted,
		JobStatusFailed,
		JobStatusCanceled,
		JobStatusDeadLettered,
	)
	if err != nil {
		return nil, fmt.Errorf("list active runtime jobs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []JobRecord
	for rows.Next() {
		record, ok, err := scanExecutionJob(rows.Scan)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, record)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active runtime jobs: %w", err)
	}
	return out, nil
}

func (s *sqliteJobStore) UpdateJobStatus(ctx context.Context, jobID string, status string, reason string) error {
	trimmedJobID := strings.TrimSpace(jobID)
	if trimmedJobID == "" {
		return fmt.Errorf("job id is required")
	}
	currentStatus, err := s.currentJobStatus(ctx, trimmedJobID)
	if err != nil {
		return err
	}
	normalizedStatus, err := normalizeExecutionJobStatus(status)
	if err != nil {
		return err
	}
	if err := guardJobStatusTransition(currentStatus, normalizedStatus); err != nil {
		return err
	}
	now := time.Now().UTC()
	startedAt, completedAt, canceledAt := statusTimestamps(normalizedStatus, now)
	_, err = s.db.ExecContext(ctx, `
		UPDATE execution_jobs
		SET status = ?,
		    error = ?,
		    updated_at = ?,
		    started_at = COALESCE(started_at, ?),
		    completed_at = COALESCE(completed_at, ?),
		    canceled_at = COALESCE(canceled_at, ?)
		WHERE id = ?`,
		normalizedStatus,
		nullIfEmpty(reason),
		now.Format(time.RFC3339),
		optionalTimeValue(startedAt),
		optionalTimeValue(completedAt),
		optionalTimeValue(canceledAt),
		trimmedJobID,
	)
	if err != nil {
		return fmt.Errorf("update runtime job %q status: %w", trimmedJobID, err)
	}
	return nil
}

func (s *sqliteJobStore) UpdateJobStatusWithEvent(
	ctx context.Context,
	jobID string,
	status string,
	reason string,
	event JobEventOutboxRecord,
) error {
	normalizedEvent, err := normalizeJobEventOutbox(event, time.Now().UTC())
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin update job transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := updateJobStatusTx(ctx, tx, jobID, status, reason); err != nil {
		return err
	}
	if err := enqueueJobEvent(ctx, tx, normalizedEvent); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit update job transaction: %w", err)
	}
	return nil
}

func updateJobStatusTx(ctx context.Context, tx *sql.Tx, jobID string, status string, reason string) error {
	trimmedJobID := strings.TrimSpace(jobID)
	if trimmedJobID == "" {
		return fmt.Errorf("job id is required")
	}
	currentStatus, err := currentJobStatusTx(ctx, tx, trimmedJobID)
	if err != nil {
		return err
	}
	normalizedStatus, err := normalizeExecutionJobStatus(status)
	if err != nil {
		return err
	}
	if err := guardJobStatusTransition(currentStatus, normalizedStatus); err != nil {
		return err
	}
	now := time.Now().UTC()
	startedAt, completedAt, canceledAt := statusTimestamps(normalizedStatus, now)
	if _, err := tx.ExecContext(ctx, `
		UPDATE execution_jobs
		SET status = ?, error = ?, updated_at = ?,
		    started_at = COALESCE(started_at, ?),
		    completed_at = COALESCE(completed_at, ?),
		    canceled_at = COALESCE(canceled_at, ?)
		WHERE id = ?`,
		normalizedStatus,
		nullIfEmpty(reason),
		now.Format(time.RFC3339),
		optionalTimeValue(startedAt),
		optionalTimeValue(completedAt),
		optionalTimeValue(canceledAt),
		trimmedJobID,
	); err != nil {
		return fmt.Errorf("update runtime job %q status: %w", trimmedJobID, err)
	}
	return nil
}

func (s *sqliteJobStore) SetJobResult(ctx context.Context, jobID string, result string, status string, reason string) error {
	trimmedJobID := strings.TrimSpace(jobID)
	if trimmedJobID == "" {
		return fmt.Errorf("job id is required")
	}
	currentStatus, err := s.currentJobStatus(ctx, trimmedJobID)
	if err != nil {
		return err
	}
	normalizedStatus, err := normalizeExecutionJobStatus(status)
	if err != nil {
		return err
	}
	if err := guardJobStatusTransition(currentStatus, normalizedStatus); err != nil {
		return err
	}
	now := time.Now().UTC()
	startedAt, completedAt, canceledAt := statusTimestamps(normalizedStatus, now)
	if _, err := s.db.ExecContext(ctx, `
		UPDATE execution_jobs
		SET status = ?,
		    result = ?,
		    error = ?,
		    updated_at = ?,
		    started_at = COALESCE(started_at, ?),
		    completed_at = COALESCE(completed_at, ?),
		    canceled_at = COALESCE(canceled_at, ?)
		WHERE id = ?`,
		normalizedStatus,
		nullIfEmpty(result),
		nullIfEmpty(reason),
		now.Format(time.RFC3339),
		optionalTimeValue(startedAt),
		optionalTimeValue(completedAt),
		optionalTimeValue(canceledAt),
		trimmedJobID,
	); err != nil {
		return fmt.Errorf("set runtime job %q result: %w", trimmedJobID, err)
	}
	return nil
}

func (s *sqliteJobStore) SetJobResultWithEvent(
	ctx context.Context,
	jobID string,
	resultJSON string,
	status string,
	reason string,
	event JobEventOutboxRecord,
) error {
	normalizedEvent, err := normalizeJobEventOutbox(event, time.Now().UTC())
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin set job result transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := setJobResultTx(ctx, tx, jobID, resultJSON, status, reason); err != nil {
		return err
	}
	if err := enqueueJobEvent(ctx, tx, normalizedEvent); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit set job result transaction: %w", err)
	}
	return nil
}

func setJobResultTx(
	ctx context.Context,
	tx *sql.Tx,
	jobID string,
	result string,
	status string,
	reason string,
) error {
	trimmedJobID := strings.TrimSpace(jobID)
	if trimmedJobID == "" {
		return fmt.Errorf("job id is required")
	}
	currentStatus, err := currentJobStatusTx(ctx, tx, trimmedJobID)
	if err != nil {
		return err
	}
	normalizedStatus, err := normalizeExecutionJobStatus(status)
	if err != nil {
		return err
	}
	if err := guardJobStatusTransition(currentStatus, normalizedStatus); err != nil {
		return err
	}
	now := time.Now().UTC()
	startedAt, completedAt, canceledAt := statusTimestamps(normalizedStatus, now)
	if _, err := tx.ExecContext(ctx, `
		UPDATE execution_jobs
		SET status = ?, result = ?, error = ?, updated_at = ?,
		    started_at = COALESCE(started_at, ?),
		    completed_at = COALESCE(completed_at, ?),
		    canceled_at = COALESCE(canceled_at, ?)
		WHERE id = ?`,
		normalizedStatus,
		nullIfEmpty(result),
		nullIfEmpty(reason),
		now.Format(time.RFC3339),
		optionalTimeValue(startedAt),
		optionalTimeValue(completedAt),
		optionalTimeValue(canceledAt),
		trimmedJobID,
	); err != nil {
		return fmt.Errorf("set runtime job %q result: %w", trimmedJobID, err)
	}
	return nil
}

func (s *sqliteJobStore) AppendJobEvent(ctx context.Context, record JobEventRecord) error {
	now := time.Now().UTC()
	normalized, err := normalizeExecutionJobEvent(record, now)
	if err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `
			INSERT OR IGNORE INTO execution_job_events (id, job_id, event_type, actor, message_id, payload, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
		normalized.ID,
		normalized.JobID,
		normalized.EventType,
		nullIfEmpty(normalized.Actor),
		nullIfEmpty(normalized.MessageID),
		nullIfEmpty(normalized.Payload),
		normalized.CreatedAt.Format(time.RFC3339),
	); err != nil {
		return fmt.Errorf("insert runtime job event %q: %w", normalized.ID, err)
	}
	return nil
}

func (s *sqliteJobStore) ListJobEvents(ctx context.Context, jobID string) ([]JobEventRecord, error) {
	trimmed := strings.TrimSpace(jobID)
	if trimmed == "" {
		return nil, fmt.Errorf("job id is required")
	}
	rows, err := s.db.QueryContext(ctx, executionJobEventSelectSQL+`
		WHERE job_id = ?
		ORDER BY created_at ASC`,
		trimmed,
	)
	if err != nil {
		return nil, fmt.Errorf("list runtime job events: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []JobEventRecord
	for rows.Next() {
		record, err := scanExecutionJobEvent(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate runtime job events: %w", err)
	}
	return out, nil
}

func (s *sqliteJobStore) EnqueueJobEvent(ctx context.Context, event JobEventOutboxRecord) error {
	normalized, err := normalizeJobEventOutbox(event, time.Now().UTC())
	if err != nil {
		return err
	}
	return enqueueJobEvent(ctx, s.db, normalized)
}

func enqueueJobEvent(ctx context.Context, exec contextExecer, event JobEventOutboxRecord) error {
	if _, err := exec.ExecContext(ctx, `
		INSERT OR IGNORE INTO execution_job_event_outbox (
			id, job_id, subject, envelope_json, envelope, attempts, last_error, created_at, published_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.ID,
		event.JobID,
		event.Subject,
		event.Envelope,
		event.Envelope,
		event.Attempts,
		nullIfEmpty(event.LastError),
		event.CreatedAt.Format(time.RFC3339),
		optionalTimeValue(event.PublishedAt),
	); err != nil {
		return fmt.Errorf("enqueue job event %q: %w", event.ID, err)
	}
	return nil
}

func (s *sqliteJobStore) ListPendingJobEvents(ctx context.Context, limit int) ([]JobEventOutboxRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, job_id, subject, COALESCE(envelope, envelope_json, ''), attempts, COALESCE(last_error, ''),
		       created_at, COALESCE(published_at, '')
		FROM execution_job_event_outbox
		WHERE published_at IS NULL
		ORDER BY created_at ASC
		LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("list pending job events: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var records []JobEventOutboxRecord
	for rows.Next() {
		record, err := scanJobEventOutbox(rows.Scan)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending job events: %w", err)
	}
	return records, nil
}

func (s *sqliteJobStore) MarkJobEventPublished(ctx context.Context, eventID string) error {
	trimmed := strings.TrimSpace(eventID)
	if trimmed == "" {
		return fmt.Errorf("job event id is required")
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE execution_job_event_outbox
		SET attempts = attempts + 1, last_error = NULL, published_at = ?
		WHERE id = ?`, time.Now().UTC().Format(time.RFC3339), trimmed); err != nil {
		return fmt.Errorf("mark job event %q published: %w", trimmed, err)
	}
	return nil
}

func (s *sqliteJobStore) MarkJobEventPublishFailed(ctx context.Context, eventID string, reason string) error {
	trimmed := strings.TrimSpace(eventID)
	if trimmed == "" {
		return fmt.Errorf("job event id is required")
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE execution_job_event_outbox
		SET attempts = attempts + 1, last_error = ?
		WHERE id = ?`, nullIfEmpty(reason), trimmed); err != nil {
		return fmt.Errorf("mark job event %q publish failed: %w", trimmed, err)
	}
	return nil
}

func (s *sqliteJobStore) ReserveDelivery(ctx context.Context, record DeliveryRecord) (DeliveryRecord, bool, error) {
	now := time.Now().UTC()
	normalized, err := normalizeExecutionDelivery(record, now)
	if err != nil {
		return DeliveryRecord{}, false, err
	}
	res, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO execution_delivery_outbox (
			id, delivery_key, job_id, session_id, channel, address_key, kind, payload_json, payload,
			payload_hash, status, provider_message_id, sent_at, error, created_at, updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		normalized.ID,
		normalized.DeliveryKey,
		nullIfEmpty(normalized.JobID),
		nullIfEmpty(normalized.SessionID),
		normalized.Channel,
		normalized.AddressKey,
		normalized.Kind,
		normalized.Payload,
		normalized.Payload,
		normalized.PayloadHash,
		normalized.Status,
		nullIfEmpty(normalized.ProviderMessageID),
		optionalTimeValue(normalized.SentAt),
		nullIfEmpty(normalized.Error),
		normalized.CreatedAt.Format(time.RFC3339),
		normalized.UpdatedAt.Format(time.RFC3339),
	)
	if err != nil {
		return DeliveryRecord{}, false, fmt.Errorf("reserve runtime delivery %q: %w", normalized.DeliveryKey, err)
	}
	count, err := res.RowsAffected()
	if err != nil {
		return DeliveryRecord{}, false, fmt.Errorf("count reserved runtime delivery %q: %w", normalized.DeliveryKey, err)
	}
	got, ok, err := s.getDeliveryByKey(ctx, normalized.DeliveryKey)
	if err != nil {
		return DeliveryRecord{}, false, err
	}
	if !ok {
		return DeliveryRecord{}, false, fmt.Errorf("reserved runtime delivery %q not found", normalized.DeliveryKey)
	}
	return got, count > 0, nil
}

func (s *sqliteJobStore) MarkDeliverySending(ctx context.Context, deliveryKey string) error {
	trimmedKey := strings.TrimSpace(deliveryKey)
	if trimmedKey == "" {
		return fmt.Errorf("delivery key is required")
	}
	now := time.Now().UTC()
	if _, err := s.db.ExecContext(ctx, `
			UPDATE execution_delivery_outbox
			SET status = ?,
			    error = NULL,
			    updated_at = ?
			WHERE delivery_key = ?`,
		DeliveryStatusSending,
		now.Format(time.RFC3339),
		trimmedKey,
	); err != nil {
		return fmt.Errorf("mark runtime delivery %q sending: %w", trimmedKey, err)
	}
	return nil
}

func (s *sqliteJobStore) MarkDeliverySent(ctx context.Context, deliveryKey string, providerMessageID string) error {
	trimmedKey := strings.TrimSpace(deliveryKey)
	if trimmedKey == "" {
		return fmt.Errorf("delivery key is required")
	}
	now := time.Now().UTC()
	if _, err := s.db.ExecContext(ctx, `
		UPDATE execution_delivery_outbox
		SET status = ?,
		    provider_message_id = ?,
		    sent_at = COALESCE(sent_at, ?),
		    error = NULL,
		    updated_at = ?
		WHERE delivery_key = ?`,
		DeliveryStatusSent,
		nullIfEmpty(providerMessageID),
		now.Format(time.RFC3339),
		now.Format(time.RFC3339),
		trimmedKey,
	); err != nil {
		return fmt.Errorf("mark runtime delivery %q sent: %w", trimmedKey, err)
	}
	return nil
}

func (s *sqliteJobStore) MarkDeliveryFailed(ctx context.Context, deliveryKey string, reason string) error {
	trimmedKey := strings.TrimSpace(deliveryKey)
	if trimmedKey == "" {
		return fmt.Errorf("delivery key is required")
	}
	now := time.Now().UTC()
	if _, err := s.db.ExecContext(ctx, `
		UPDATE execution_delivery_outbox
		SET status = ?,
		    error = ?,
		    updated_at = ?
		WHERE delivery_key = ?`,
		DeliveryStatusFailed,
		nullIfEmpty(reason),
		now.Format(time.RFC3339),
		trimmedKey,
	); err != nil {
		return fmt.Errorf("mark runtime delivery %q failed: %w", trimmedKey, err)
	}
	return nil
}

func (s *sqliteJobStore) ReserveAgentStep(ctx context.Context, record AgentStepRecord) (AgentStepRecord, bool, error) {
	now := time.Now().UTC()
	normalized, err := normalizeExecutionAgentStep(record, now)
	if err != nil {
		return AgentStepRecord{}, false, err
	}
	res, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO execution_agent_steps (
			id, step_key, job_id, agent_name, role, iteration, payload_hash, status,
			result, error, created_at, updated_at, completed_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		normalized.ID,
		normalized.StepKey,
		normalized.JobID,
		normalized.AgentName,
		normalized.Role,
		normalized.Iteration,
		normalized.PayloadHash,
		normalized.Status,
		nullIfEmpty(normalized.Result),
		nullIfEmpty(normalized.Error),
		normalized.CreatedAt.Format(time.RFC3339),
		normalized.UpdatedAt.Format(time.RFC3339),
		optionalTimeValue(normalized.CompletedAt),
	)
	if err != nil {
		return AgentStepRecord{}, false, fmt.Errorf("reserve runtime agent step %q: %w", normalized.StepKey, err)
	}
	count, err := res.RowsAffected()
	if err != nil {
		return AgentStepRecord{}, false, fmt.Errorf("count reserved runtime agent step %q: %w", normalized.StepKey, err)
	}
	got, ok, err := s.getAgentStepByKey(ctx, normalized.StepKey)
	if err != nil {
		return AgentStepRecord{}, false, err
	}
	if !ok {
		return AgentStepRecord{}, false, fmt.Errorf("reserved runtime agent step %q not found", normalized.StepKey)
	}
	return got, count > 0, nil
}

func (s *sqliteJobStore) CompleteAgentStep(ctx context.Context, stepKey string, result string) error {
	return s.finishAgentStep(ctx, stepKey, AgentStepStatusSucceeded, result, "")
}

func (s *sqliteJobStore) FailAgentStep(ctx context.Context, stepKey string, result string, reason string) error {
	return s.finishAgentStep(ctx, stepKey, AgentStepStatusFailed, result, reason)
}

func (s *sqliteJobStore) finishAgentStep(ctx context.Context, stepKey string, status string, result string, reason string) error {
	trimmedKey := strings.TrimSpace(stepKey)
	if trimmedKey == "" {
		return fmt.Errorf("agent step key is required")
	}
	normalizedStatus, err := normalizeExecutionAgentStepStatus(status)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if _, err := s.db.ExecContext(ctx, `
		UPDATE execution_agent_steps
		SET status = ?,
		    result = ?,
		    error = ?,
		    updated_at = ?,
		    completed_at = COALESCE(completed_at, ?)
		WHERE step_key = ?`,
		normalizedStatus,
		nullIfEmpty(result),
		nullIfEmpty(reason),
		now.Format(time.RFC3339),
		now.Format(time.RFC3339),
		trimmedKey,
	); err != nil {
		return fmt.Errorf("finish runtime agent step %q: %w", trimmedKey, err)
	}
	return nil
}

func (s *sqliteJobStore) getDeliveryByKey(ctx context.Context, deliveryKey string) (DeliveryRecord, bool, error) {
	record, ok, err := scanExecutionDelivery(s.db.QueryRowContext(ctx, executionDeliverySelectSQL+` WHERE delivery_key = ?`, strings.TrimSpace(deliveryKey)).Scan)
	if err != nil {
		return DeliveryRecord{}, false, err
	}
	return record, ok, nil
}

func (s *sqliteJobStore) getAgentStepByKey(ctx context.Context, stepKey string) (AgentStepRecord, bool, error) {
	record, ok, err := scanExecutionAgentStep(s.db.QueryRowContext(ctx, executionAgentStepSelectSQL+` WHERE step_key = ?`, strings.TrimSpace(stepKey)).Scan)
	if err != nil {
		return AgentStepRecord{}, false, err
	}
	return record, ok, nil
}

const executionJobSelectSQL = `
	SELECT id, COALESCE(session_id, ''), COALESCE(parent_job_id, ''), COALESCE(title, ''), objective,
	       status, COALESCE(owner_actor, ''), COALESCE(assigned_actor, ''), priority,
	       COALESCE(created_by, ''), COALESCE(result, result_json, ''), COALESCE(error, ''),
	       created_at, updated_at, COALESCE(started_at, ''), COALESCE(completed_at, ''), COALESCE(canceled_at, '')
	FROM execution_jobs`

const executionJobEventSelectSQL = `
	SELECT id, job_id, event_type, COALESCE(actor, ''), COALESCE(message_id, ''), COALESCE(payload, payload_json, ''), created_at
	FROM execution_job_events`

const executionDeliverySelectSQL = `
	SELECT id, delivery_key, COALESCE(job_id, ''), COALESCE(session_id, ''), channel, address_key, kind,
	       COALESCE(payload, payload_json, ''), payload_hash, status, COALESCE(provider_message_id, ''), COALESCE(sent_at, ''),
	       COALESCE(error, ''), created_at, updated_at
	FROM execution_delivery_outbox`

const executionAgentStepSelectSQL = `
	SELECT id, step_key, job_id, agent_name, role, iteration, payload_hash, status,
	       COALESCE(result, result_json, ''), COALESCE(error, ''), created_at, updated_at, COALESCE(completed_at, '')
	FROM execution_agent_steps`

func scanExecutionJob(scan func(dest ...any) error) (JobRecord, bool, error) {
	var (
		record         JobRecord
		createdAtRaw   string
		updatedAtRaw   string
		startedAtRaw   string
		completedAtRaw string
		canceledAtRaw  string
	)
	err := scan(
		&record.ID,
		&record.SessionID,
		&record.ParentJobID,
		&record.Title,
		&record.Objective,
		&record.Status,
		&record.OwnerActor,
		&record.AssignedActor,
		&record.Priority,
		&record.CreatedBy,
		&record.Result,
		&record.Error,
		&createdAtRaw,
		&updatedAtRaw,
		&startedAtRaw,
		&completedAtRaw,
		&canceledAtRaw,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return JobRecord{}, false, nil
		}
		return JobRecord{}, false, fmt.Errorf("scan runtime job: %w", err)
	}
	createdAt, err := parseRequiredRFC3339(createdAtRaw)
	if err != nil {
		return JobRecord{}, false, fmt.Errorf("parse job created_at: %w", err)
	}
	updatedAt, err := parseRequiredRFC3339(updatedAtRaw)
	if err != nil {
		return JobRecord{}, false, fmt.Errorf("parse job updated_at: %w", err)
	}
	startedAt, err := parseOptionalRFC3339(startedAtRaw)
	if err != nil {
		return JobRecord{}, false, fmt.Errorf("parse job started_at: %w", err)
	}
	completedAt, err := parseOptionalRFC3339(completedAtRaw)
	if err != nil {
		return JobRecord{}, false, fmt.Errorf("parse job completed_at: %w", err)
	}
	canceledAt, err := parseOptionalRFC3339(canceledAtRaw)
	if err != nil {
		return JobRecord{}, false, fmt.Errorf("parse job canceled_at: %w", err)
	}
	record.CreatedAt = createdAt
	record.UpdatedAt = updatedAt
	record.StartedAt = startedAt
	record.CompletedAt = completedAt
	record.CanceledAt = canceledAt
	return record, true, nil
}

func scanExecutionJobEvent(scan func(dest ...any) error) (JobEventRecord, error) {
	var record JobEventRecord
	var createdAtRaw string
	if err := scan(
		&record.ID,
		&record.JobID,
		&record.EventType,
		&record.Actor,
		&record.MessageID,
		&record.Payload,
		&createdAtRaw,
	); err != nil {
		return JobEventRecord{}, fmt.Errorf("scan runtime job event: %w", err)
	}
	createdAt, err := parseRequiredRFC3339(createdAtRaw)
	if err != nil {
		return JobEventRecord{}, fmt.Errorf("parse job event created_at: %w", err)
	}
	record.CreatedAt = createdAt
	return record, nil
}

func scanExecutionDelivery(scan func(dest ...any) error) (DeliveryRecord, bool, error) {
	var (
		record       DeliveryRecord
		sentAtRaw    string
		createdAtRaw string
		updatedAtRaw string
	)
	err := scan(
		&record.ID,
		&record.DeliveryKey,
		&record.JobID,
		&record.SessionID,
		&record.Channel,
		&record.AddressKey,
		&record.Kind,
		&record.Payload,
		&record.PayloadHash,
		&record.Status,
		&record.ProviderMessageID,
		&sentAtRaw,
		&record.Error,
		&createdAtRaw,
		&updatedAtRaw,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return DeliveryRecord{}, false, nil
		}
		return DeliveryRecord{}, false, fmt.Errorf("scan runtime delivery: %w", err)
	}
	createdAt, err := parseRequiredRFC3339(createdAtRaw)
	if err != nil {
		return DeliveryRecord{}, false, fmt.Errorf("parse delivery created_at: %w", err)
	}
	updatedAt, err := parseRequiredRFC3339(updatedAtRaw)
	if err != nil {
		return DeliveryRecord{}, false, fmt.Errorf("parse delivery updated_at: %w", err)
	}
	sentAt, err := parseOptionalRFC3339(sentAtRaw)
	if err != nil {
		return DeliveryRecord{}, false, fmt.Errorf("parse delivery sent_at: %w", err)
	}
	record.CreatedAt = createdAt
	record.UpdatedAt = updatedAt
	record.SentAt = sentAt
	return record, true, nil
}

func scanExecutionAgentStep(scan func(dest ...any) error) (AgentStepRecord, bool, error) {
	var (
		record         AgentStepRecord
		createdAtRaw   string
		updatedAtRaw   string
		completedAtRaw string
	)
	err := scan(
		&record.ID,
		&record.StepKey,
		&record.JobID,
		&record.AgentName,
		&record.Role,
		&record.Iteration,
		&record.PayloadHash,
		&record.Status,
		&record.Result,
		&record.Error,
		&createdAtRaw,
		&updatedAtRaw,
		&completedAtRaw,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return AgentStepRecord{}, false, nil
		}
		return AgentStepRecord{}, false, fmt.Errorf("scan runtime agent step: %w", err)
	}
	createdAt, err := parseRequiredRFC3339(createdAtRaw)
	if err != nil {
		return AgentStepRecord{}, false, fmt.Errorf("parse agent step created_at: %w", err)
	}
	updatedAt, err := parseRequiredRFC3339(updatedAtRaw)
	if err != nil {
		return AgentStepRecord{}, false, fmt.Errorf("parse agent step updated_at: %w", err)
	}
	completedAt, err := parseOptionalRFC3339(completedAtRaw)
	if err != nil {
		return AgentStepRecord{}, false, fmt.Errorf("parse agent step completed_at: %w", err)
	}
	record.CreatedAt = createdAt
	record.UpdatedAt = updatedAt
	record.CompletedAt = completedAt
	return record, true, nil
}

func normalizeExecutionJob(record JobRecord, now time.Time) (JobRecord, error) {
	record.ID = strings.TrimSpace(record.ID)
	if record.ID == "" {
		return JobRecord{}, fmt.Errorf("runtime job id is required")
	}
	record.Objective = strings.TrimSpace(record.Objective)
	if record.Objective == "" {
		return JobRecord{}, fmt.Errorf("runtime job objective is required")
	}
	status, err := normalizeExecutionJobStatus(record.Status)
	if err != nil {
		return JobRecord{}, err
	}
	record.Status = status
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.CreatedAt = record.CreatedAt.UTC()
	record.UpdatedAt = now
	record.SessionID = strings.TrimSpace(record.SessionID)
	record.ParentJobID = strings.TrimSpace(record.ParentJobID)
	record.Title = strings.TrimSpace(record.Title)
	record.OwnerActor = strings.TrimSpace(record.OwnerActor)
	record.AssignedActor = strings.TrimSpace(record.AssignedActor)
	record.CreatedBy = strings.TrimSpace(record.CreatedBy)
	record.Result = strings.TrimSpace(record.Result)
	record.Error = strings.TrimSpace(record.Error)
	return record, nil
}

func normalizeExecutionDelivery(record DeliveryRecord, now time.Time) (DeliveryRecord, error) {
	record.ID = strings.TrimSpace(record.ID)
	if record.ID == "" {
		return DeliveryRecord{}, fmt.Errorf("runtime delivery id is required")
	}
	record.DeliveryKey = strings.TrimSpace(record.DeliveryKey)
	if record.DeliveryKey == "" {
		return DeliveryRecord{}, fmt.Errorf("runtime delivery key is required")
	}
	record.Channel = strings.TrimSpace(record.Channel)
	if record.Channel == "" {
		return DeliveryRecord{}, fmt.Errorf("runtime delivery channel is required")
	}
	record.AddressKey = strings.TrimSpace(record.AddressKey)
	if record.AddressKey == "" {
		return DeliveryRecord{}, fmt.Errorf("runtime delivery address key is required")
	}
	record.Kind = strings.TrimSpace(record.Kind)
	if record.Kind == "" {
		return DeliveryRecord{}, fmt.Errorf("runtime delivery kind is required")
	}
	record.Payload = strings.TrimSpace(record.Payload)
	if record.Payload == "" {
		return DeliveryRecord{}, fmt.Errorf("runtime delivery payload is required")
	}
	record.PayloadHash = strings.TrimSpace(record.PayloadHash)
	if record.PayloadHash == "" {
		return DeliveryRecord{}, fmt.Errorf("runtime delivery payload hash is required")
	}
	status, err := normalizeExecutionDeliveryStatus(record.Status)
	if err != nil {
		return DeliveryRecord{}, err
	}
	record.Status = status
	record.JobID = strings.TrimSpace(record.JobID)
	record.SessionID = strings.TrimSpace(record.SessionID)
	record.ProviderMessageID = strings.TrimSpace(record.ProviderMessageID)
	record.Error = strings.TrimSpace(record.Error)
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.CreatedAt = record.CreatedAt.UTC()
	record.UpdatedAt = now
	record.SentAt = record.SentAt.UTC()
	return record, nil
}

func normalizeExecutionAgentStep(record AgentStepRecord, now time.Time) (AgentStepRecord, error) {
	record.ID = strings.TrimSpace(record.ID)
	if record.ID == "" {
		return AgentStepRecord{}, fmt.Errorf("runtime agent step id is required")
	}
	record.StepKey = strings.TrimSpace(record.StepKey)
	if record.StepKey == "" {
		return AgentStepRecord{}, fmt.Errorf("runtime agent step key is required")
	}
	record.JobID = strings.TrimSpace(record.JobID)
	if record.JobID == "" {
		return AgentStepRecord{}, fmt.Errorf("runtime agent step job id is required")
	}
	record.AgentName = strings.TrimSpace(record.AgentName)
	if record.AgentName == "" {
		return AgentStepRecord{}, fmt.Errorf("runtime agent step agent name is required")
	}
	record.Role = strings.TrimSpace(record.Role)
	if record.Role == "" {
		return AgentStepRecord{}, fmt.Errorf("runtime agent step role is required")
	}
	if record.Iteration <= 0 {
		return AgentStepRecord{}, fmt.Errorf("runtime agent step iteration must be positive")
	}
	record.PayloadHash = strings.TrimSpace(record.PayloadHash)
	if record.PayloadHash == "" {
		return AgentStepRecord{}, fmt.Errorf("runtime agent step payload hash is required")
	}
	status, err := normalizeExecutionAgentStepStatus(record.Status)
	if err != nil {
		return AgentStepRecord{}, err
	}
	record.Status = status
	record.Result = strings.TrimSpace(record.Result)
	record.Error = strings.TrimSpace(record.Error)
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.CreatedAt = record.CreatedAt.UTC()
	record.UpdatedAt = now
	record.CompletedAt = record.CompletedAt.UTC()
	return record, nil
}

func normalizeExecutionJobEvent(record JobEventRecord, now time.Time) (JobEventRecord, error) {
	record.ID = strings.TrimSpace(record.ID)
	if record.ID == "" {
		return JobEventRecord{}, fmt.Errorf("runtime job event id is required")
	}
	record.JobID = strings.TrimSpace(record.JobID)
	if record.JobID == "" {
		return JobEventRecord{}, fmt.Errorf("runtime job event job id is required")
	}
	record.EventType = strings.TrimSpace(record.EventType)
	if record.EventType == "" {
		return JobEventRecord{}, fmt.Errorf("runtime job event type is required")
	}
	record.Actor = strings.TrimSpace(record.Actor)
	record.MessageID = strings.TrimSpace(record.MessageID)
	record.Payload = strings.TrimSpace(record.Payload)
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.CreatedAt = record.CreatedAt.UTC()
	return record, nil
}

func normalizeExecutionAgentStepStatus(status string) (string, error) {
	trimmed := strings.TrimSpace(status)
	if trimmed == "" {
		trimmed = AgentStepStatusRunning
	}
	switch trimmed {
	case AgentStepStatusRunning, AgentStepStatusSucceeded, AgentStepStatusFailed:
		return trimmed, nil
	default:
		return "", fmt.Errorf("invalid runtime agent step status %q", status)
	}
}

func normalizeExecutionDeliveryStatus(status string) (string, error) {
	trimmed := strings.TrimSpace(status)
	if trimmed == "" {
		trimmed = DeliveryStatusPending
	}
	switch trimmed {
	case DeliveryStatusPending, DeliveryStatusSending, DeliveryStatusSent, DeliveryStatusFailed:
		return trimmed, nil
	default:
		return "", fmt.Errorf("invalid runtime delivery status %q", status)
	}
}

func normalizeExecutionJobStatus(status string) (string, error) {
	trimmed := strings.TrimSpace(status)
	if trimmed == "" {
		trimmed = JobStatusCreated
	}
	switch trimmed {
	case JobStatusCreated,
		JobStatusQueued,
		JobStatusRunning,
		JobStatusWaitingForAgent,
		JobStatusWaitingForUser,
		JobStatusValidating,
		JobStatusCompleted,
		JobStatusFailed,
		JobStatusCanceled,
		JobStatusDeadLettered:
		return trimmed, nil
	default:
		return "", fmt.Errorf("invalid runtime job status %q", status)
	}
}

func statusTimestamps(status string, now time.Time) (startedAt time.Time, completedAt time.Time, canceledAt time.Time) {
	switch status {
	case JobStatusRunning,
		JobStatusWaitingForAgent,
		JobStatusWaitingForUser,
		JobStatusValidating:
		return now, time.Time{}, time.Time{}
	case JobStatusCompleted, JobStatusFailed, JobStatusDeadLettered:
		return now, now, time.Time{}
	case JobStatusCanceled:
		return now, time.Time{}, now
	default:
		return time.Time{}, time.Time{}, time.Time{}
	}
}

func nullIfEmpty(value string) any {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return trimmed
}

func optionalTimeValue(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC().Format(time.RFC3339)
}

func (s *sqliteJobStore) currentJobStatus(ctx context.Context, jobID string) (string, error) {
	row := s.db.QueryRowContext(ctx, `SELECT status FROM execution_jobs WHERE id = ?`, jobID)
	return scanCurrentJobStatus(row.Scan, jobID)
}

func currentJobStatusTx(ctx context.Context, tx *sql.Tx, jobID string) (string, error) {
	row := tx.QueryRowContext(ctx, `SELECT status FROM execution_jobs WHERE id = ?`, jobID)
	return scanCurrentJobStatus(row.Scan, jobID)
}

func scanCurrentJobStatus(scan func(...any) error, jobID string) (string, error) {
	var status string
	if err := scan(&status); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("runtime job %q not found", jobID)
		}
		return "", fmt.Errorf("read runtime job %q status: %w", jobID, err)
	}
	normalized, err := normalizeExecutionJobStatus(status)
	if err != nil {
		return "", err
	}
	return normalized, nil
}

func normalizeJobEventOutbox(record JobEventOutboxRecord, now time.Time) (JobEventOutboxRecord, error) {
	record.ID = strings.TrimSpace(record.ID)
	if record.ID == "" {
		return JobEventOutboxRecord{}, fmt.Errorf("job event outbox id is required")
	}
	record.JobID = strings.TrimSpace(record.JobID)
	if record.JobID == "" {
		return JobEventOutboxRecord{}, fmt.Errorf("job event outbox job id is required")
	}
	record.Subject = strings.TrimSpace(record.Subject)
	if record.Subject == "" {
		return JobEventOutboxRecord{}, fmt.Errorf("job event outbox subject is required")
	}
	record.Envelope = strings.TrimSpace(record.Envelope)
	if record.Envelope == "" {
		return JobEventOutboxRecord{}, fmt.Errorf("job event outbox envelope is required")
	}
	if record.Attempts < 0 {
		return JobEventOutboxRecord{}, fmt.Errorf("job event outbox attempts must not be negative")
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.CreatedAt = record.CreatedAt.UTC()
	record.PublishedAt = record.PublishedAt.UTC()
	record.LastError = strings.TrimSpace(record.LastError)
	return record, nil
}

func scanJobEventOutbox(scan func(...any) error) (JobEventOutboxRecord, error) {
	var record JobEventOutboxRecord
	var createdAtRaw, publishedAtRaw string
	if err := scan(
		&record.ID,
		&record.JobID,
		&record.Subject,
		&record.Envelope,
		&record.Attempts,
		&record.LastError,
		&createdAtRaw,
		&publishedAtRaw,
	); err != nil {
		return JobEventOutboxRecord{}, fmt.Errorf("scan job event outbox: %w", err)
	}
	createdAt, err := parseRequiredRFC3339(createdAtRaw)
	if err != nil {
		return JobEventOutboxRecord{}, fmt.Errorf("parse job event outbox created_at: %w", err)
	}
	publishedAt, err := parseOptionalRFC3339(publishedAtRaw)
	if err != nil {
		return JobEventOutboxRecord{}, fmt.Errorf("parse job event outbox published_at: %w", err)
	}
	record.CreatedAt = createdAt
	record.PublishedAt = publishedAt
	return record, nil
}

func guardJobStatusTransition(current string, next string) error {
	current = strings.TrimSpace(current)
	next = strings.TrimSpace(next)
	if current == "" || current == next {
		return nil
	}
	if isTerminalRuntimeJobStatus(current) {
		return fmt.Errorf("invalid runtime job transition %q -> %q: terminal status", current, next)
	}
	if next == JobStatusCreated {
		return fmt.Errorf("invalid runtime job transition %q -> %q", current, next)
	}
	return nil
}

func isTerminalRuntimeJobStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case JobStatusCompleted, JobStatusFailed, JobStatusCanceled, JobStatusDeadLettered:
		return true
	default:
		return false
	}
}
