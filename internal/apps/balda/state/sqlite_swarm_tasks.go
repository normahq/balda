package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

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

func (s *sqliteSwarmStore) UpdateTaskStatus(ctx context.Context, taskID string, status string, reason string) error {
	trimmedTaskID := strings.TrimSpace(taskID)
	if trimmedTaskID == "" {
		return fmt.Errorf("task id is required")
	}
	normalizedStatus, err := normalizeSwarmTaskStatus(status)
	if err != nil {
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
	normalizedStatus, err := normalizeSwarmTaskStatus(status)
	if err != nil {
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
		INSERT INTO swarm_task_events (id, task_id, event_type, actor, message_id, payload_json, created_at)
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
