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

func (s *sqliteJobStore) CreateJob(ctx context.Context, record JobRecord) (bool, error) {
	now := time.Now().UTC()
	normalized, err := normalizeExecutionTask(record, now)
	if err != nil {
		return false, err
	}
	res, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO execution_tasks (
			id, session_id, parent_task_id, title, objective, status, owner_actor, assigned_actor,
			priority, created_by, result_json, error,
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
		nullIfEmpty(normalized.ResultJSON),
		nullIfEmpty(normalized.Error),
		normalized.CreatedAt.Format(time.RFC3339),
		normalized.UpdatedAt.Format(time.RFC3339),
		optionalTimeValue(normalized.StartedAt),
		optionalTimeValue(normalized.CompletedAt),
		optionalTimeValue(normalized.CanceledAt),
	)
	if err != nil {
		return false, fmt.Errorf("insert runtime task %q: %w", normalized.ID, err)
	}
	count, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("count inserted runtime task %q: %w", normalized.ID, err)
	}
	return count > 0, nil
}

func (s *sqliteJobStore) GetJob(ctx context.Context, taskID string) (JobRecord, bool, error) {
	record, ok, err := scanExecutionTask(s.db.QueryRowContext(ctx, executionTaskSelectSQL+` WHERE id = ?`, strings.TrimSpace(taskID)).Scan)
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
	rows, err := s.db.QueryContext(ctx, executionTaskSelectSQL+`
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
		return nil, fmt.Errorf("list active runtime tasks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []JobRecord
	for rows.Next() {
		record, ok, err := scanExecutionTask(rows.Scan)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, record)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active runtime tasks: %w", err)
	}
	return out, nil
}

func (s *sqliteJobStore) UpdateJobStatus(ctx context.Context, taskID string, status string, reason string) error {
	trimmedJobID := strings.TrimSpace(taskID)
	if trimmedJobID == "" {
		return fmt.Errorf("task id is required")
	}
	currentStatus, err := s.currentTaskStatus(ctx, trimmedJobID)
	if err != nil {
		return err
	}
	normalizedStatus, err := normalizeExecutionTaskStatus(status)
	if err != nil {
		return err
	}
	if err := guardTaskStatusTransition(currentStatus, normalizedStatus); err != nil {
		return err
	}
	now := time.Now().UTC()
	startedAt, completedAt, canceledAt := statusTimestamps(normalizedStatus, now)
	_, err = s.db.ExecContext(ctx, `
		UPDATE execution_tasks
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
		return fmt.Errorf("update runtime task %q status: %w", trimmedJobID, err)
	}
	return nil
}

func (s *sqliteJobStore) SetJobResult(ctx context.Context, taskID string, resultJSON string, status string, reason string) error {
	trimmedJobID := strings.TrimSpace(taskID)
	if trimmedJobID == "" {
		return fmt.Errorf("task id is required")
	}
	currentStatus, err := s.currentTaskStatus(ctx, trimmedJobID)
	if err != nil {
		return err
	}
	normalizedStatus, err := normalizeExecutionTaskStatus(status)
	if err != nil {
		return err
	}
	if err := guardTaskStatusTransition(currentStatus, normalizedStatus); err != nil {
		return err
	}
	now := time.Now().UTC()
	startedAt, completedAt, canceledAt := statusTimestamps(normalizedStatus, now)
	if _, err := s.db.ExecContext(ctx, `
		UPDATE execution_tasks
		SET status = ?,
		    result_json = ?,
		    error = ?,
		    updated_at = ?,
		    started_at = COALESCE(started_at, ?),
		    completed_at = COALESCE(completed_at, ?),
		    canceled_at = COALESCE(canceled_at, ?)
		WHERE id = ?`,
		normalizedStatus,
		nullIfEmpty(resultJSON),
		nullIfEmpty(reason),
		now.Format(time.RFC3339),
		optionalTimeValue(startedAt),
		optionalTimeValue(completedAt),
		optionalTimeValue(canceledAt),
		trimmedJobID,
	); err != nil {
		return fmt.Errorf("set runtime task %q result: %w", trimmedJobID, err)
	}
	return nil
}

func (s *sqliteJobStore) AppendJobEvent(ctx context.Context, record JobEventRecord) error {
	now := time.Now().UTC()
	normalized, err := normalizeExecutionTaskEvent(record, now)
	if err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `
			INSERT OR IGNORE INTO execution_task_events (id, task_id, event_type, actor, message_id, payload_json, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
		normalized.ID,
		normalized.JobID,
		normalized.EventType,
		nullIfEmpty(normalized.Actor),
		nullIfEmpty(normalized.MessageID),
		nullIfEmpty(normalized.PayloadJSON),
		normalized.CreatedAt.Format(time.RFC3339),
	); err != nil {
		return fmt.Errorf("insert runtime task event %q: %w", normalized.ID, err)
	}
	return nil
}

func (s *sqliteJobStore) ListJobEvents(ctx context.Context, taskID string) ([]JobEventRecord, error) {
	trimmed := strings.TrimSpace(taskID)
	if trimmed == "" {
		return nil, fmt.Errorf("task id is required")
	}
	rows, err := s.db.QueryContext(ctx, executionTaskEventSelectSQL+`
		WHERE task_id = ?
		ORDER BY created_at ASC`,
		trimmed,
	)
	if err != nil {
		return nil, fmt.Errorf("list runtime task events: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []JobEventRecord
	for rows.Next() {
		record, err := scanExecutionTaskEvent(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate runtime task events: %w", err)
	}
	return out, nil
}

func (s *sqliteJobStore) ReserveDelivery(ctx context.Context, record DeliveryRecord) (DeliveryRecord, bool, error) {
	now := time.Now().UTC()
	normalized, err := normalizeExecutionDelivery(record, now)
	if err != nil {
		return DeliveryRecord{}, false, err
	}
	res, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO execution_delivery_outbox (
			id, delivery_key, task_id, session_id, channel, address_key, kind, payload_json,
			payload_hash, status, provider_message_id, sent_at, error, created_at, updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		normalized.ID,
		normalized.DeliveryKey,
		nullIfEmpty(normalized.JobID),
		nullIfEmpty(normalized.SessionID),
		normalized.Channel,
		normalized.AddressKey,
		normalized.Kind,
		normalized.PayloadJSON,
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
			id, step_key, task_id, agent_name, role, iteration, payload_hash, status,
			result_json, error, created_at, updated_at, completed_at
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
		nullIfEmpty(normalized.ResultJSON),
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

func (s *sqliteJobStore) CompleteAgentStep(ctx context.Context, stepKey string, resultJSON string) error {
	return s.finishAgentStep(ctx, stepKey, AgentStepStatusSucceeded, resultJSON, "")
}

func (s *sqliteJobStore) FailAgentStep(ctx context.Context, stepKey string, resultJSON string, reason string) error {
	return s.finishAgentStep(ctx, stepKey, AgentStepStatusFailed, resultJSON, reason)
}

func (s *sqliteJobStore) finishAgentStep(ctx context.Context, stepKey string, status string, resultJSON string, reason string) error {
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
		    result_json = ?,
		    error = ?,
		    updated_at = ?,
		    completed_at = COALESCE(completed_at, ?)
		WHERE step_key = ?`,
		normalizedStatus,
		nullIfEmpty(resultJSON),
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

const executionTaskSelectSQL = `
	SELECT id, COALESCE(session_id, ''), COALESCE(parent_task_id, ''), COALESCE(title, ''), objective,
	       status, COALESCE(owner_actor, ''), COALESCE(assigned_actor, ''), priority,
	       COALESCE(created_by, ''), COALESCE(result_json, ''), COALESCE(error, ''),
	       created_at, updated_at, COALESCE(started_at, ''), COALESCE(completed_at, ''), COALESCE(canceled_at, '')
	FROM execution_tasks`

const executionTaskEventSelectSQL = `
	SELECT id, task_id, event_type, COALESCE(actor, ''), COALESCE(message_id, ''), COALESCE(payload_json, ''), created_at
	FROM execution_task_events`

const executionDeliverySelectSQL = `
	SELECT id, delivery_key, COALESCE(task_id, ''), COALESCE(session_id, ''), channel, address_key, kind,
	       payload_json, payload_hash, status, COALESCE(provider_message_id, ''), COALESCE(sent_at, ''),
	       COALESCE(error, ''), created_at, updated_at
	FROM execution_delivery_outbox`

const executionAgentStepSelectSQL = `
	SELECT id, step_key, task_id, agent_name, role, iteration, payload_hash, status,
	       COALESCE(result_json, ''), COALESCE(error, ''), created_at, updated_at, COALESCE(completed_at, '')
	FROM execution_agent_steps`

func scanExecutionTask(scan func(dest ...any) error) (JobRecord, bool, error) {
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
		&record.ResultJSON,
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
		return JobRecord{}, false, fmt.Errorf("scan runtime task: %w", err)
	}
	createdAt, err := parseRequiredRFC3339(createdAtRaw)
	if err != nil {
		return JobRecord{}, false, fmt.Errorf("parse task created_at: %w", err)
	}
	updatedAt, err := parseRequiredRFC3339(updatedAtRaw)
	if err != nil {
		return JobRecord{}, false, fmt.Errorf("parse task updated_at: %w", err)
	}
	startedAt, err := parseOptionalRFC3339(startedAtRaw)
	if err != nil {
		return JobRecord{}, false, fmt.Errorf("parse task started_at: %w", err)
	}
	completedAt, err := parseOptionalRFC3339(completedAtRaw)
	if err != nil {
		return JobRecord{}, false, fmt.Errorf("parse task completed_at: %w", err)
	}
	canceledAt, err := parseOptionalRFC3339(canceledAtRaw)
	if err != nil {
		return JobRecord{}, false, fmt.Errorf("parse task canceled_at: %w", err)
	}
	record.CreatedAt = createdAt
	record.UpdatedAt = updatedAt
	record.StartedAt = startedAt
	record.CompletedAt = completedAt
	record.CanceledAt = canceledAt
	return record, true, nil
}

func scanExecutionTaskEvent(scan func(dest ...any) error) (JobEventRecord, error) {
	var record JobEventRecord
	var createdAtRaw string
	if err := scan(
		&record.ID,
		&record.JobID,
		&record.EventType,
		&record.Actor,
		&record.MessageID,
		&record.PayloadJSON,
		&createdAtRaw,
	); err != nil {
		return JobEventRecord{}, fmt.Errorf("scan runtime task event: %w", err)
	}
	createdAt, err := parseRequiredRFC3339(createdAtRaw)
	if err != nil {
		return JobEventRecord{}, fmt.Errorf("parse task event created_at: %w", err)
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
		&record.PayloadJSON,
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
		&record.ResultJSON,
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

func normalizeExecutionTask(record JobRecord, now time.Time) (JobRecord, error) {
	record.ID = strings.TrimSpace(record.ID)
	if record.ID == "" {
		return JobRecord{}, fmt.Errorf("runtime task id is required")
	}
	record.Objective = strings.TrimSpace(record.Objective)
	if record.Objective == "" {
		return JobRecord{}, fmt.Errorf("runtime task objective is required")
	}
	status, err := normalizeExecutionTaskStatus(record.Status)
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
	record.ResultJSON = strings.TrimSpace(record.ResultJSON)
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
	record.PayloadJSON = strings.TrimSpace(record.PayloadJSON)
	if record.PayloadJSON == "" {
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
		return AgentStepRecord{}, fmt.Errorf("runtime agent step task id is required")
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
	record.ResultJSON = strings.TrimSpace(record.ResultJSON)
	record.Error = strings.TrimSpace(record.Error)
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.CreatedAt = record.CreatedAt.UTC()
	record.UpdatedAt = now
	record.CompletedAt = record.CompletedAt.UTC()
	return record, nil
}

func normalizeExecutionTaskEvent(record JobEventRecord, now time.Time) (JobEventRecord, error) {
	record.ID = strings.TrimSpace(record.ID)
	if record.ID == "" {
		return JobEventRecord{}, fmt.Errorf("runtime task event id is required")
	}
	record.JobID = strings.TrimSpace(record.JobID)
	if record.JobID == "" {
		return JobEventRecord{}, fmt.Errorf("runtime task event task id is required")
	}
	record.EventType = strings.TrimSpace(record.EventType)
	if record.EventType == "" {
		return JobEventRecord{}, fmt.Errorf("runtime task event type is required")
	}
	record.Actor = strings.TrimSpace(record.Actor)
	record.MessageID = strings.TrimSpace(record.MessageID)
	record.PayloadJSON = strings.TrimSpace(record.PayloadJSON)
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

func normalizeExecutionTaskStatus(status string) (string, error) {
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
		return "", fmt.Errorf("invalid runtime task status %q", status)
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

func (s *sqliteJobStore) currentTaskStatus(ctx context.Context, taskID string) (string, error) {
	row := s.db.QueryRowContext(ctx, `SELECT status FROM execution_tasks WHERE id = ?`, taskID)
	var status string
	if err := row.Scan(&status); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("runtime task %q not found", taskID)
		}
		return "", fmt.Errorf("read runtime task %q status: %w", taskID, err)
	}
	normalized, err := normalizeExecutionTaskStatus(status)
	if err != nil {
		return "", err
	}
	return normalized, nil
}

func guardTaskStatusTransition(current string, next string) error {
	current = strings.TrimSpace(current)
	next = strings.TrimSpace(next)
	if current == "" || current == next {
		return nil
	}
	if isTerminalRuntimeTaskStatus(current) {
		return fmt.Errorf("invalid runtime task transition %q -> %q: terminal status", current, next)
	}
	if next == JobStatusCreated {
		return fmt.Errorf("invalid runtime task transition %q -> %q", current, next)
	}
	return nil
}

func isTerminalRuntimeTaskStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case JobStatusCompleted, JobStatusFailed, JobStatusCanceled, JobStatusDeadLettered:
		return true
	default:
		return false
	}
}
