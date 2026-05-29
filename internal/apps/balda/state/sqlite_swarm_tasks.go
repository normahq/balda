package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type sqliteSwarmStore struct {
	db *sql.DB
}

func (s *sqliteSwarmStore) CreateTask(ctx context.Context, record SwarmTaskRecord) (bool, error) {
	now := time.Now().UTC()
	normalized, err := normalizeSwarmTask(record, now)
	if err != nil {
		return false, err
	}
	res, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO swarm_tasks (
			id, session_id, parent_task_id, title, objective, status, owner_actor, assigned_actor,
			priority, created_by, created_from, plan_json, result_json, error,
			created_at, updated_at, started_at, completed_at, canceled_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		normalized.ID,
		nullIfEmpty(normalized.SessionID),
		nullIfEmpty(normalized.ParentTaskID),
		nullIfEmpty(normalized.Title),
		normalized.Objective,
		normalized.Status,
		nullIfEmpty(normalized.OwnerActor),
		nullIfEmpty(normalized.AssignedActor),
		normalized.Priority,
		nullIfEmpty(normalized.CreatedBy),
		nullIfEmpty(normalized.CreatedFrom),
		nullIfEmpty(normalized.PlanJSON),
		nullIfEmpty(normalized.ResultJSON),
		nullIfEmpty(normalized.Error),
		normalized.CreatedAt.Format(time.RFC3339),
		normalized.UpdatedAt.Format(time.RFC3339),
		optionalTimeValue(normalized.StartedAt),
		optionalTimeValue(normalized.CompletedAt),
		optionalTimeValue(normalized.CanceledAt),
	)
	if err != nil {
		return false, fmt.Errorf("insert swarm task %q: %w", normalized.ID, err)
	}
	count, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("count inserted swarm task %q: %w", normalized.ID, err)
	}
	return count > 0, nil
}

func (s *sqliteSwarmStore) GetTask(ctx context.Context, taskID string) (SwarmTaskRecord, bool, error) {
	record, ok, err := scanSwarmTask(s.db.QueryRowContext(ctx, swarmTaskSelectSQL+` WHERE id = ?`, strings.TrimSpace(taskID)).Scan)
	if err != nil {
		return SwarmTaskRecord{}, false, err
	}
	return record, ok, nil
}

func (s *sqliteSwarmStore) ListActiveTasksBySession(ctx context.Context, sessionID string) ([]SwarmTaskRecord, error) {
	trimmed := strings.TrimSpace(sessionID)
	if trimmed == "" {
		return nil, fmt.Errorf("session id is required")
	}
	rows, err := s.db.QueryContext(ctx, swarmTaskSelectSQL+`
		WHERE session_id = ?
		  AND status NOT IN (?, ?, ?, ?)
		ORDER BY created_at ASC`,
		trimmed,
		SwarmTaskStatusCompleted,
		SwarmTaskStatusFailed,
		SwarmTaskStatusCanceled,
		SwarmTaskStatusDeadLettered,
	)
	if err != nil {
		return nil, fmt.Errorf("list active swarm tasks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []SwarmTaskRecord
	for rows.Next() {
		record, ok, err := scanSwarmTask(rows.Scan)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, record)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active swarm tasks: %w", err)
	}
	return out, nil
}

func (s *sqliteSwarmStore) ListTaskStatusCounts(ctx context.Context) ([]SwarmStatusCount, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT status, COUNT(*)
		FROM swarm_tasks
		GROUP BY status
		ORDER BY status ASC`)
	if err != nil {
		return nil, fmt.Errorf("list task status counts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []SwarmStatusCount
	for rows.Next() {
		var record SwarmStatusCount
		if err := rows.Scan(&record.Status, &record.Count); err != nil {
			return nil, fmt.Errorf("scan task status count: %w", err)
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate task status counts: %w", err)
	}
	return out, nil
}

func (s *sqliteSwarmStore) ListDeliveryStatusCounts(ctx context.Context) ([]SwarmStatusCount, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT status, COUNT(*)
		FROM swarm_delivery_outbox
		GROUP BY status
		ORDER BY status ASC`)
	if err != nil {
		return nil, fmt.Errorf("list delivery status counts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []SwarmStatusCount
	for rows.Next() {
		var record SwarmStatusCount
		if err := rows.Scan(&record.Status, &record.Count); err != nil {
			return nil, fmt.Errorf("scan delivery status count: %w", err)
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate delivery status counts: %w", err)
	}
	return out, nil
}

func (s *sqliteSwarmStore) UpdateTaskStatus(ctx context.Context, taskID string, status string, reason string) error {
	trimmedTaskID := strings.TrimSpace(taskID)
	if trimmedTaskID == "" {
		return fmt.Errorf("task id is required")
	}
	currentStatus, err := s.currentTaskStatus(ctx, trimmedTaskID)
	if err != nil {
		return err
	}
	normalizedStatus, err := normalizeSwarmTaskStatus(status)
	if err != nil {
		return err
	}
	if err := guardTaskStatusTransition(currentStatus, normalizedStatus); err != nil {
		return err
	}
	now := time.Now().UTC()
	startedAt, completedAt, canceledAt := statusTimestamps(normalizedStatus, now)
	_, err = s.db.ExecContext(ctx, `
		UPDATE swarm_tasks
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
		trimmedTaskID,
	)
	if err != nil {
		return fmt.Errorf("update swarm task %q status: %w", trimmedTaskID, err)
	}
	return nil
}

func (s *sqliteSwarmStore) SetTaskPlan(ctx context.Context, taskID string, planJSON string) error {
	trimmedTaskID := strings.TrimSpace(taskID)
	if trimmedTaskID == "" {
		return fmt.Errorf("task id is required")
	}
	now := time.Now().UTC()
	if _, err := s.db.ExecContext(ctx, `
		UPDATE swarm_tasks
		SET plan_json = ?, updated_at = ?
		WHERE id = ?`,
		nullIfEmpty(planJSON),
		now.Format(time.RFC3339),
		trimmedTaskID,
	); err != nil {
		return fmt.Errorf("set swarm task %q plan: %w", trimmedTaskID, err)
	}
	return nil
}

func (s *sqliteSwarmStore) SetTaskResult(ctx context.Context, taskID string, resultJSON string, status string, reason string) error {
	trimmedTaskID := strings.TrimSpace(taskID)
	if trimmedTaskID == "" {
		return fmt.Errorf("task id is required")
	}
	currentStatus, err := s.currentTaskStatus(ctx, trimmedTaskID)
	if err != nil {
		return err
	}
	normalizedStatus, err := normalizeSwarmTaskStatus(status)
	if err != nil {
		return err
	}
	if err := guardTaskStatusTransition(currentStatus, normalizedStatus); err != nil {
		return err
	}
	now := time.Now().UTC()
	startedAt, completedAt, canceledAt := statusTimestamps(normalizedStatus, now)
	if _, err := s.db.ExecContext(ctx, `
		UPDATE swarm_tasks
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
		trimmedTaskID,
	); err != nil {
		return fmt.Errorf("set swarm task %q result: %w", trimmedTaskID, err)
	}
	return nil
}

func (s *sqliteSwarmStore) AppendTaskEvent(ctx context.Context, record SwarmTaskEventRecord) error {
	now := time.Now().UTC()
	normalized, err := normalizeSwarmTaskEvent(record, now)
	if err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `
			INSERT OR IGNORE INTO swarm_task_events (id, task_id, event_type, actor, message_id, payload_json, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
		normalized.ID,
		normalized.TaskID,
		normalized.EventType,
		nullIfEmpty(normalized.Actor),
		nullIfEmpty(normalized.MessageID),
		nullIfEmpty(normalized.PayloadJSON),
		normalized.CreatedAt.Format(time.RFC3339),
	); err != nil {
		return fmt.Errorf("insert swarm task event %q: %w", normalized.ID, err)
	}
	return nil
}

func (s *sqliteSwarmStore) ListTaskEvents(ctx context.Context, taskID string) ([]SwarmTaskEventRecord, error) {
	trimmed := strings.TrimSpace(taskID)
	if trimmed == "" {
		return nil, fmt.Errorf("task id is required")
	}
	rows, err := s.db.QueryContext(ctx, swarmTaskEventSelectSQL+`
		WHERE task_id = ?
		ORDER BY created_at ASC`,
		trimmed,
	)
	if err != nil {
		return nil, fmt.Errorf("list swarm task events: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []SwarmTaskEventRecord
	for rows.Next() {
		record, err := scanSwarmTaskEvent(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate swarm task events: %w", err)
	}
	return out, nil
}

func (s *sqliteSwarmStore) ReserveDelivery(ctx context.Context, record SwarmDeliveryRecord) (SwarmDeliveryRecord, bool, error) {
	now := time.Now().UTC()
	normalized, err := normalizeSwarmDelivery(record, now)
	if err != nil {
		return SwarmDeliveryRecord{}, false, err
	}
	res, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO swarm_delivery_outbox (
			id, delivery_key, task_id, session_id, channel, address_key, kind, payload_json,
			payload_hash, status, provider_message_id, sent_at, error, created_at, updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		normalized.ID,
		normalized.DeliveryKey,
		nullIfEmpty(normalized.TaskID),
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
		return SwarmDeliveryRecord{}, false, fmt.Errorf("reserve swarm delivery %q: %w", normalized.DeliveryKey, err)
	}
	count, err := res.RowsAffected()
	if err != nil {
		return SwarmDeliveryRecord{}, false, fmt.Errorf("count reserved swarm delivery %q: %w", normalized.DeliveryKey, err)
	}
	got, ok, err := s.getDeliveryByKey(ctx, normalized.DeliveryKey)
	if err != nil {
		return SwarmDeliveryRecord{}, false, err
	}
	if !ok {
		return SwarmDeliveryRecord{}, false, fmt.Errorf("reserved swarm delivery %q not found", normalized.DeliveryKey)
	}
	return got, count > 0, nil
}

func (s *sqliteSwarmStore) MarkDeliverySending(ctx context.Context, deliveryKey string) error {
	trimmedKey := strings.TrimSpace(deliveryKey)
	if trimmedKey == "" {
		return fmt.Errorf("delivery key is required")
	}
	now := time.Now().UTC()
	if _, err := s.db.ExecContext(ctx, `
			UPDATE swarm_delivery_outbox
			SET status = ?,
			    error = NULL,
			    updated_at = ?
			WHERE delivery_key = ?`,
		SwarmDeliveryStatusSending,
		now.Format(time.RFC3339),
		trimmedKey,
	); err != nil {
		return fmt.Errorf("mark swarm delivery %q sending: %w", trimmedKey, err)
	}
	return nil
}

func (s *sqliteSwarmStore) MarkDeliverySent(ctx context.Context, deliveryKey string, providerMessageID string) error {
	trimmedKey := strings.TrimSpace(deliveryKey)
	if trimmedKey == "" {
		return fmt.Errorf("delivery key is required")
	}
	now := time.Now().UTC()
	if _, err := s.db.ExecContext(ctx, `
		UPDATE swarm_delivery_outbox
		SET status = ?,
		    provider_message_id = ?,
		    sent_at = COALESCE(sent_at, ?),
		    error = NULL,
		    updated_at = ?
		WHERE delivery_key = ?`,
		SwarmDeliveryStatusSent,
		nullIfEmpty(providerMessageID),
		now.Format(time.RFC3339),
		now.Format(time.RFC3339),
		trimmedKey,
	); err != nil {
		return fmt.Errorf("mark swarm delivery %q sent: %w", trimmedKey, err)
	}
	return nil
}

func (s *sqliteSwarmStore) MarkDeliveryFailed(ctx context.Context, deliveryKey string, reason string) error {
	trimmedKey := strings.TrimSpace(deliveryKey)
	if trimmedKey == "" {
		return fmt.Errorf("delivery key is required")
	}
	now := time.Now().UTC()
	if _, err := s.db.ExecContext(ctx, `
		UPDATE swarm_delivery_outbox
		SET status = ?,
		    error = ?,
		    updated_at = ?
		WHERE delivery_key = ?`,
		SwarmDeliveryStatusFailed,
		nullIfEmpty(reason),
		now.Format(time.RFC3339),
		trimmedKey,
	); err != nil {
		return fmt.Errorf("mark swarm delivery %q failed: %w", trimmedKey, err)
	}
	return nil
}

func (s *sqliteSwarmStore) ReserveAgentStep(ctx context.Context, record SwarmAgentStepRecord) (SwarmAgentStepRecord, bool, error) {
	now := time.Now().UTC()
	normalized, err := normalizeSwarmAgentStep(record, now)
	if err != nil {
		return SwarmAgentStepRecord{}, false, err
	}
	res, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO swarm_agent_steps (
			id, step_key, task_id, agent_name, role, iteration, payload_hash, status,
			result_json, error, created_at, updated_at, completed_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		normalized.ID,
		normalized.StepKey,
		normalized.TaskID,
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
		return SwarmAgentStepRecord{}, false, fmt.Errorf("reserve swarm agent step %q: %w", normalized.StepKey, err)
	}
	count, err := res.RowsAffected()
	if err != nil {
		return SwarmAgentStepRecord{}, false, fmt.Errorf("count reserved swarm agent step %q: %w", normalized.StepKey, err)
	}
	got, ok, err := s.getAgentStepByKey(ctx, normalized.StepKey)
	if err != nil {
		return SwarmAgentStepRecord{}, false, err
	}
	if !ok {
		return SwarmAgentStepRecord{}, false, fmt.Errorf("reserved swarm agent step %q not found", normalized.StepKey)
	}
	return got, count > 0, nil
}

func (s *sqliteSwarmStore) CompleteAgentStep(ctx context.Context, stepKey string, resultJSON string) error {
	return s.finishAgentStep(ctx, stepKey, SwarmAgentStepStatusSucceeded, resultJSON, "")
}

func (s *sqliteSwarmStore) FailAgentStep(ctx context.Context, stepKey string, resultJSON string, reason string) error {
	return s.finishAgentStep(ctx, stepKey, SwarmAgentStepStatusFailed, resultJSON, reason)
}

func (s *sqliteSwarmStore) finishAgentStep(ctx context.Context, stepKey string, status string, resultJSON string, reason string) error {
	trimmedKey := strings.TrimSpace(stepKey)
	if trimmedKey == "" {
		return fmt.Errorf("agent step key is required")
	}
	normalizedStatus, err := normalizeSwarmAgentStepStatus(status)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if _, err := s.db.ExecContext(ctx, `
		UPDATE swarm_agent_steps
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
		return fmt.Errorf("finish swarm agent step %q: %w", trimmedKey, err)
	}
	return nil
}

func (s *sqliteSwarmStore) getDeliveryByKey(ctx context.Context, deliveryKey string) (SwarmDeliveryRecord, bool, error) {
	record, ok, err := scanSwarmDelivery(s.db.QueryRowContext(ctx, swarmDeliverySelectSQL+` WHERE delivery_key = ?`, strings.TrimSpace(deliveryKey)).Scan)
	if err != nil {
		return SwarmDeliveryRecord{}, false, err
	}
	return record, ok, nil
}

func (s *sqliteSwarmStore) getAgentStepByKey(ctx context.Context, stepKey string) (SwarmAgentStepRecord, bool, error) {
	record, ok, err := scanSwarmAgentStep(s.db.QueryRowContext(ctx, swarmAgentStepSelectSQL+` WHERE step_key = ?`, strings.TrimSpace(stepKey)).Scan)
	if err != nil {
		return SwarmAgentStepRecord{}, false, err
	}
	return record, ok, nil
}

const swarmTaskSelectSQL = `
	SELECT id, COALESCE(session_id, ''), COALESCE(parent_task_id, ''), COALESCE(title, ''), objective,
	       status, COALESCE(owner_actor, ''), COALESCE(assigned_actor, ''), priority,
	       COALESCE(created_by, ''), COALESCE(created_from, ''), COALESCE(plan_json, ''),
	       COALESCE(result_json, ''), COALESCE(error, ''),
	       created_at, updated_at, COALESCE(started_at, ''), COALESCE(completed_at, ''), COALESCE(canceled_at, '')
	FROM swarm_tasks`

const swarmTaskEventSelectSQL = `
	SELECT id, task_id, event_type, COALESCE(actor, ''), COALESCE(message_id, ''), COALESCE(payload_json, ''), created_at
	FROM swarm_task_events`

const swarmDeliverySelectSQL = `
	SELECT id, delivery_key, COALESCE(task_id, ''), COALESCE(session_id, ''), channel, address_key, kind,
	       payload_json, payload_hash, status, COALESCE(provider_message_id, ''), COALESCE(sent_at, ''),
	       COALESCE(error, ''), created_at, updated_at
	FROM swarm_delivery_outbox`

const swarmAgentStepSelectSQL = `
	SELECT id, step_key, task_id, agent_name, role, iteration, payload_hash, status,
	       COALESCE(result_json, ''), COALESCE(error, ''), created_at, updated_at, COALESCE(completed_at, '')
	FROM swarm_agent_steps`

func scanSwarmTask(scan func(dest ...any) error) (SwarmTaskRecord, bool, error) {
	var (
		record         SwarmTaskRecord
		createdAtRaw   string
		updatedAtRaw   string
		startedAtRaw   string
		completedAtRaw string
		canceledAtRaw  string
	)
	err := scan(
		&record.ID,
		&record.SessionID,
		&record.ParentTaskID,
		&record.Title,
		&record.Objective,
		&record.Status,
		&record.OwnerActor,
		&record.AssignedActor,
		&record.Priority,
		&record.CreatedBy,
		&record.CreatedFrom,
		&record.PlanJSON,
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
			return SwarmTaskRecord{}, false, nil
		}
		return SwarmTaskRecord{}, false, fmt.Errorf("scan swarm task: %w", err)
	}
	createdAt, err := parseRequiredRFC3339(createdAtRaw)
	if err != nil {
		return SwarmTaskRecord{}, false, fmt.Errorf("parse task created_at: %w", err)
	}
	updatedAt, err := parseRequiredRFC3339(updatedAtRaw)
	if err != nil {
		return SwarmTaskRecord{}, false, fmt.Errorf("parse task updated_at: %w", err)
	}
	startedAt, err := parseOptionalRFC3339(startedAtRaw)
	if err != nil {
		return SwarmTaskRecord{}, false, fmt.Errorf("parse task started_at: %w", err)
	}
	completedAt, err := parseOptionalRFC3339(completedAtRaw)
	if err != nil {
		return SwarmTaskRecord{}, false, fmt.Errorf("parse task completed_at: %w", err)
	}
	canceledAt, err := parseOptionalRFC3339(canceledAtRaw)
	if err != nil {
		return SwarmTaskRecord{}, false, fmt.Errorf("parse task canceled_at: %w", err)
	}
	record.CreatedAt = createdAt
	record.UpdatedAt = updatedAt
	record.StartedAt = startedAt
	record.CompletedAt = completedAt
	record.CanceledAt = canceledAt
	return record, true, nil
}

func scanSwarmTaskEvent(scan func(dest ...any) error) (SwarmTaskEventRecord, error) {
	var record SwarmTaskEventRecord
	var createdAtRaw string
	if err := scan(
		&record.ID,
		&record.TaskID,
		&record.EventType,
		&record.Actor,
		&record.MessageID,
		&record.PayloadJSON,
		&createdAtRaw,
	); err != nil {
		return SwarmTaskEventRecord{}, fmt.Errorf("scan swarm task event: %w", err)
	}
	createdAt, err := parseRequiredRFC3339(createdAtRaw)
	if err != nil {
		return SwarmTaskEventRecord{}, fmt.Errorf("parse task event created_at: %w", err)
	}
	record.CreatedAt = createdAt
	return record, nil
}

func scanSwarmDelivery(scan func(dest ...any) error) (SwarmDeliveryRecord, bool, error) {
	var (
		record       SwarmDeliveryRecord
		sentAtRaw    string
		createdAtRaw string
		updatedAtRaw string
	)
	err := scan(
		&record.ID,
		&record.DeliveryKey,
		&record.TaskID,
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
			return SwarmDeliveryRecord{}, false, nil
		}
		return SwarmDeliveryRecord{}, false, fmt.Errorf("scan swarm delivery: %w", err)
	}
	createdAt, err := parseRequiredRFC3339(createdAtRaw)
	if err != nil {
		return SwarmDeliveryRecord{}, false, fmt.Errorf("parse delivery created_at: %w", err)
	}
	updatedAt, err := parseRequiredRFC3339(updatedAtRaw)
	if err != nil {
		return SwarmDeliveryRecord{}, false, fmt.Errorf("parse delivery updated_at: %w", err)
	}
	sentAt, err := parseOptionalRFC3339(sentAtRaw)
	if err != nil {
		return SwarmDeliveryRecord{}, false, fmt.Errorf("parse delivery sent_at: %w", err)
	}
	record.CreatedAt = createdAt
	record.UpdatedAt = updatedAt
	record.SentAt = sentAt
	return record, true, nil
}

func scanSwarmAgentStep(scan func(dest ...any) error) (SwarmAgentStepRecord, bool, error) {
	var (
		record         SwarmAgentStepRecord
		createdAtRaw   string
		updatedAtRaw   string
		completedAtRaw string
	)
	err := scan(
		&record.ID,
		&record.StepKey,
		&record.TaskID,
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
			return SwarmAgentStepRecord{}, false, nil
		}
		return SwarmAgentStepRecord{}, false, fmt.Errorf("scan swarm agent step: %w", err)
	}
	createdAt, err := parseRequiredRFC3339(createdAtRaw)
	if err != nil {
		return SwarmAgentStepRecord{}, false, fmt.Errorf("parse agent step created_at: %w", err)
	}
	updatedAt, err := parseRequiredRFC3339(updatedAtRaw)
	if err != nil {
		return SwarmAgentStepRecord{}, false, fmt.Errorf("parse agent step updated_at: %w", err)
	}
	completedAt, err := parseOptionalRFC3339(completedAtRaw)
	if err != nil {
		return SwarmAgentStepRecord{}, false, fmt.Errorf("parse agent step completed_at: %w", err)
	}
	record.CreatedAt = createdAt
	record.UpdatedAt = updatedAt
	record.CompletedAt = completedAt
	return record, true, nil
}

func normalizeSwarmTask(record SwarmTaskRecord, now time.Time) (SwarmTaskRecord, error) {
	record.ID = strings.TrimSpace(record.ID)
	if record.ID == "" {
		return SwarmTaskRecord{}, fmt.Errorf("swarm task id is required")
	}
	record.Objective = strings.TrimSpace(record.Objective)
	if record.Objective == "" {
		return SwarmTaskRecord{}, fmt.Errorf("swarm task objective is required")
	}
	status, err := normalizeSwarmTaskStatus(record.Status)
	if err != nil {
		return SwarmTaskRecord{}, err
	}
	record.Status = status
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.CreatedAt = record.CreatedAt.UTC()
	record.UpdatedAt = now
	record.SessionID = strings.TrimSpace(record.SessionID)
	record.ParentTaskID = strings.TrimSpace(record.ParentTaskID)
	record.Title = strings.TrimSpace(record.Title)
	record.OwnerActor = strings.TrimSpace(record.OwnerActor)
	record.AssignedActor = strings.TrimSpace(record.AssignedActor)
	record.CreatedBy = strings.TrimSpace(record.CreatedBy)
	record.CreatedFrom = strings.TrimSpace(record.CreatedFrom)
	record.PlanJSON = strings.TrimSpace(record.PlanJSON)
	record.ResultJSON = strings.TrimSpace(record.ResultJSON)
	record.Error = strings.TrimSpace(record.Error)
	return record, nil
}

func normalizeSwarmDelivery(record SwarmDeliveryRecord, now time.Time) (SwarmDeliveryRecord, error) {
	record.ID = strings.TrimSpace(record.ID)
	if record.ID == "" {
		return SwarmDeliveryRecord{}, fmt.Errorf("swarm delivery id is required")
	}
	record.DeliveryKey = strings.TrimSpace(record.DeliveryKey)
	if record.DeliveryKey == "" {
		return SwarmDeliveryRecord{}, fmt.Errorf("swarm delivery key is required")
	}
	record.Channel = strings.TrimSpace(record.Channel)
	if record.Channel == "" {
		return SwarmDeliveryRecord{}, fmt.Errorf("swarm delivery channel is required")
	}
	record.AddressKey = strings.TrimSpace(record.AddressKey)
	if record.AddressKey == "" {
		return SwarmDeliveryRecord{}, fmt.Errorf("swarm delivery address key is required")
	}
	record.Kind = strings.TrimSpace(record.Kind)
	if record.Kind == "" {
		return SwarmDeliveryRecord{}, fmt.Errorf("swarm delivery kind is required")
	}
	record.PayloadJSON = strings.TrimSpace(record.PayloadJSON)
	if record.PayloadJSON == "" {
		return SwarmDeliveryRecord{}, fmt.Errorf("swarm delivery payload is required")
	}
	record.PayloadHash = strings.TrimSpace(record.PayloadHash)
	if record.PayloadHash == "" {
		return SwarmDeliveryRecord{}, fmt.Errorf("swarm delivery payload hash is required")
	}
	status, err := normalizeSwarmDeliveryStatus(record.Status)
	if err != nil {
		return SwarmDeliveryRecord{}, err
	}
	record.Status = status
	record.TaskID = strings.TrimSpace(record.TaskID)
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

func normalizeSwarmAgentStep(record SwarmAgentStepRecord, now time.Time) (SwarmAgentStepRecord, error) {
	record.ID = strings.TrimSpace(record.ID)
	if record.ID == "" {
		return SwarmAgentStepRecord{}, fmt.Errorf("swarm agent step id is required")
	}
	record.StepKey = strings.TrimSpace(record.StepKey)
	if record.StepKey == "" {
		return SwarmAgentStepRecord{}, fmt.Errorf("swarm agent step key is required")
	}
	record.TaskID = strings.TrimSpace(record.TaskID)
	if record.TaskID == "" {
		return SwarmAgentStepRecord{}, fmt.Errorf("swarm agent step task id is required")
	}
	record.AgentName = strings.TrimSpace(record.AgentName)
	if record.AgentName == "" {
		return SwarmAgentStepRecord{}, fmt.Errorf("swarm agent step agent name is required")
	}
	record.Role = strings.TrimSpace(record.Role)
	if record.Role == "" {
		return SwarmAgentStepRecord{}, fmt.Errorf("swarm agent step role is required")
	}
	if record.Iteration <= 0 {
		return SwarmAgentStepRecord{}, fmt.Errorf("swarm agent step iteration must be positive")
	}
	record.PayloadHash = strings.TrimSpace(record.PayloadHash)
	if record.PayloadHash == "" {
		return SwarmAgentStepRecord{}, fmt.Errorf("swarm agent step payload hash is required")
	}
	status, err := normalizeSwarmAgentStepStatus(record.Status)
	if err != nil {
		return SwarmAgentStepRecord{}, err
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

func normalizeSwarmTaskEvent(record SwarmTaskEventRecord, now time.Time) (SwarmTaskEventRecord, error) {
	record.ID = strings.TrimSpace(record.ID)
	if record.ID == "" {
		return SwarmTaskEventRecord{}, fmt.Errorf("swarm task event id is required")
	}
	record.TaskID = strings.TrimSpace(record.TaskID)
	if record.TaskID == "" {
		return SwarmTaskEventRecord{}, fmt.Errorf("swarm task event task id is required")
	}
	record.EventType = strings.TrimSpace(record.EventType)
	if record.EventType == "" {
		return SwarmTaskEventRecord{}, fmt.Errorf("swarm task event type is required")
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

func normalizeSwarmAgentStepStatus(status string) (string, error) {
	trimmed := strings.TrimSpace(status)
	if trimmed == "" {
		trimmed = SwarmAgentStepStatusRunning
	}
	switch trimmed {
	case SwarmAgentStepStatusRunning, SwarmAgentStepStatusSucceeded, SwarmAgentStepStatusFailed:
		return trimmed, nil
	default:
		return "", fmt.Errorf("invalid swarm agent step status %q", status)
	}
}

func normalizeSwarmDeliveryStatus(status string) (string, error) {
	trimmed := strings.TrimSpace(status)
	if trimmed == "" {
		trimmed = SwarmDeliveryStatusPending
	}
	switch trimmed {
	case SwarmDeliveryStatusPending, SwarmDeliveryStatusSending, SwarmDeliveryStatusSent, SwarmDeliveryStatusFailed:
		return trimmed, nil
	default:
		return "", fmt.Errorf("invalid swarm delivery status %q", status)
	}
}

func normalizeSwarmTaskStatus(status string) (string, error) {
	trimmed := strings.TrimSpace(status)
	if trimmed == "" {
		trimmed = SwarmTaskStatusCreated
	}
	switch trimmed {
	case SwarmTaskStatusCreated,
		SwarmTaskStatusQueued,
		SwarmTaskStatusRunning,
		SwarmTaskStatusWaitingForAgent,
		SwarmTaskStatusWaitingForUser,
		SwarmTaskStatusValidating,
		SwarmTaskStatusCompleted,
		SwarmTaskStatusFailed,
		SwarmTaskStatusCanceled,
		SwarmTaskStatusDeadLettered:
		return trimmed, nil
	default:
		return "", fmt.Errorf("invalid swarm task status %q", status)
	}
}

func statusTimestamps(status string, now time.Time) (startedAt time.Time, completedAt time.Time, canceledAt time.Time) {
	switch status {
	case SwarmTaskStatusRunning,
		SwarmTaskStatusWaitingForAgent,
		SwarmTaskStatusWaitingForUser,
		SwarmTaskStatusValidating:
		return now, time.Time{}, time.Time{}
	case SwarmTaskStatusCompleted, SwarmTaskStatusFailed, SwarmTaskStatusDeadLettered:
		return now, now, time.Time{}
	case SwarmTaskStatusCanceled:
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

func (s *sqliteSwarmStore) currentTaskStatus(ctx context.Context, taskID string) (string, error) {
	row := s.db.QueryRowContext(ctx, `SELECT status FROM swarm_tasks WHERE id = ?`, taskID)
	var status string
	if err := row.Scan(&status); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("swarm task %q not found", taskID)
		}
		return "", fmt.Errorf("read swarm task %q status: %w", taskID, err)
	}
	normalized, err := normalizeSwarmTaskStatus(status)
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
	if isTerminalSwarmTaskStatus(current) {
		return fmt.Errorf("invalid swarm task transition %q -> %q: terminal status", current, next)
	}
	if next == SwarmTaskStatusCreated {
		return fmt.Errorf("invalid swarm task transition %q -> %q", current, next)
	}
	return nil
}

func isTerminalSwarmTaskStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case SwarmTaskStatusCompleted, SwarmTaskStatusFailed, SwarmTaskStatusCanceled, SwarmTaskStatusDeadLettered:
		return true
	default:
		return false
	}
}
