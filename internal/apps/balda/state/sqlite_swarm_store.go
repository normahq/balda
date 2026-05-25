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

func (s *sqliteSwarmStore) Publish(ctx context.Context, record SwarmMessageRecord) (SwarmPublishResult, error) {
	results, err := s.PublishBatch(ctx, []SwarmMessageRecord{record})
	if err != nil {
		return SwarmPublishResult{}, err
	}
	return results[0], nil
}

func (s *sqliteSwarmStore) PublishBatch(ctx context.Context, records []SwarmMessageRecord) ([]SwarmPublishResult, error) {
	if len(records) == 0 {
		return nil, nil
	}
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin swarm publish: %w", err)
	}
	defer rollbackSwarmTx(tx)

	results := make([]SwarmPublishResult, 0, len(records))
	for _, record := range records {
		normalized, err := normalizeSwarmMessage(record, now)
		if err != nil {
			return nil, err
		}
		result, err := publishSwarmMessage(ctx, tx, normalized)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit swarm publish: %w", err)
	}
	return results, nil
}

func publishSwarmMessage(ctx context.Context, tx *sql.Tx, record SwarmMessageRecord) (SwarmPublishResult, error) {
	res, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO swarm_messages (
			id, mailbox, namespace, kind, from_addr, to_addr, session_id, task_id, correlation_id, causation_id,
			priority, dedupe_key, status, attempt, max_attempts, not_before, expires_at, lease_owner, lease_until,
			payload_json, meta_json, created_at, updated_at, completed_at, error
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.ID,
		record.Mailbox,
		record.Namespace,
		record.Kind,
		record.FromAddr,
		record.ToAddr,
		nullIfEmpty(record.SessionID),
		nullIfEmpty(record.TaskID),
		nullIfEmpty(record.CorrelationID),
		nullIfEmpty(record.CausationID),
		record.Priority,
		nullIfEmpty(record.DedupeKey),
		record.Status,
		nonNegative(record.Attempt),
		defaultMaxAttempts(record.MaxAttempts),
		optionalTimeValue(record.NotBefore),
		optionalTimeValue(record.ExpiresAt),
		nullIfEmpty(record.LeaseOwner),
		optionalTimeValue(record.LeaseUntil),
		record.PayloadJSON,
		nullIfEmpty(record.MetaJSON),
		record.CreatedAt.UTC().Format(time.RFC3339),
		record.UpdatedAt.UTC().Format(time.RFC3339),
		optionalTimeValue(record.CompletedAt),
		nullIfEmpty(record.Error),
	)
	if err != nil {
		return SwarmPublishResult{}, fmt.Errorf("insert swarm message %q: %w", record.ID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return SwarmPublishResult{}, fmt.Errorf("count inserted swarm message %q: %w", record.ID, err)
	}
	return SwarmPublishResult{ID: record.ID, Mailbox: record.Mailbox, Published: rows > 0}, nil
}

func (s *sqliteSwarmStore) Claim(
	ctx context.Context,
	mailbox string,
	owner string,
	limit int,
	lease time.Duration,
) ([]SwarmMessageRecord, error) {
	trimmedMailbox := strings.TrimSpace(mailbox)
	if trimmedMailbox == "" {
		return nil, fmt.Errorf("mailbox is required")
	}
	trimmedOwner := strings.TrimSpace(owner)
	if trimmedOwner == "" {
		return nil, fmt.Errorf("lease owner is required")
	}
	if limit <= 0 {
		limit = 1
	}
	if lease <= 0 {
		lease = 30 * time.Second
	}
	now := time.Now().UTC()
	leaseUntil := now.Add(lease).UTC()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin swarm claim: %w", err)
	}
	defer rollbackSwarmTx(tx)

	rows, err := tx.QueryContext(ctx, `
		SELECT id
		FROM swarm_messages
		WHERE mailbox = ?
		  AND status IN (?, ?)
		  AND (not_before IS NULL OR not_before = '' OR not_before <= ?)
		  AND (expires_at IS NULL OR expires_at = '' OR expires_at > ?)
		ORDER BY priority DESC, created_at ASC
		LIMIT ?`,
		trimmedMailbox,
		SwarmMessageStatusQueued,
		SwarmMessageStatusRetry,
		now.Format(time.RFC3339),
		now.Format(time.RFC3339),
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("select claimable swarm messages: %w", err)
	}
	ids := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan claimable swarm message id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close claimable swarm message rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimable swarm messages: %w", err)
	}

	records := make([]SwarmMessageRecord, 0, len(ids))
	for _, id := range ids {
		res, err := tx.ExecContext(ctx, `
			UPDATE swarm_messages
			SET status = ?, attempt = attempt + 1, lease_owner = ?, lease_until = ?, updated_at = ?
			WHERE id = ? AND mailbox = ? AND status IN (?, ?)`,
			SwarmMessageStatusLeased,
			trimmedOwner,
			leaseUntil.Format(time.RFC3339),
			now.Format(time.RFC3339),
			id,
			trimmedMailbox,
			SwarmMessageStatusQueued,
			SwarmMessageStatusRetry,
		)
		if err != nil {
			return nil, fmt.Errorf("claim swarm message %q: %w", id, err)
		}
		count, err := res.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("count claimed swarm message %q: %w", id, err)
		}
		if count == 0 {
			continue
		}
		record, ok, err := scanSwarmMessage(tx.QueryRowContext(ctx, swarmMessageSelectSQL+` WHERE id = ?`, id).Scan)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("claimed swarm message %q not found", id)
		}
		records = append(records, record)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit swarm claim: %w", err)
	}
	return records, nil
}

func (s *sqliteSwarmStore) Ack(ctx context.Context, mailbox string, messageID string) error {
	return s.finish(ctx, mailbox, messageID, SwarmMessageStatusAcked, "")
}

func (s *sqliteSwarmStore) Retry(ctx context.Context, mailbox string, messageID string, next time.Time, reason string) error {
	trimmedMailbox, trimmedMessageID, err := normalizeMailboxMessageID(mailbox, messageID)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if next.IsZero() {
		next = now
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE swarm_messages
		SET status = ?, not_before = ?, lease_owner = NULL, lease_until = NULL, updated_at = ?, error = ?
		WHERE mailbox = ? AND id = ? AND status <> ?`,
		SwarmMessageStatusRetry,
		next.UTC().Format(time.RFC3339),
		now.Format(time.RFC3339),
		nullIfEmpty(reason),
		trimmedMailbox,
		trimmedMessageID,
		SwarmMessageStatusCanceled,
	); err != nil {
		return fmt.Errorf("retry swarm message %q: %w", trimmedMessageID, err)
	}
	return nil
}

func (s *sqliteSwarmStore) DeadLetter(ctx context.Context, mailbox string, messageID string, reason string) error {
	return s.finish(ctx, mailbox, messageID, SwarmMessageStatusDead, reason)
}

func (s *sqliteSwarmStore) finish(ctx context.Context, mailbox string, messageID string, status string, reason string) error {
	trimmedMailbox, trimmedMessageID, err := normalizeMailboxMessageID(mailbox, messageID)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if _, err := s.db.ExecContext(ctx, `
		UPDATE swarm_messages
		SET status = ?, completed_at = ?, lease_owner = NULL, lease_until = NULL, updated_at = ?, error = ?
		WHERE mailbox = ? AND id = ? AND status <> ?`,
		status,
		now.Format(time.RFC3339),
		now.Format(time.RFC3339),
		nullIfEmpty(reason),
		trimmedMailbox,
		trimmedMessageID,
		SwarmMessageStatusCanceled,
	); err != nil {
		return fmt.Errorf("finish swarm message %q: %w", trimmedMessageID, err)
	}
	return nil
}

func (s *sqliteSwarmStore) CancelByTask(ctx context.Context, taskID string, reason string) (int, error) {
	trimmed := strings.TrimSpace(taskID)
	if trimmed == "" {
		return 0, fmt.Errorf("task id is required")
	}
	return s.cancelWhere(ctx, "task_id = ?", trimmed, reason)
}

func (s *sqliteSwarmStore) CancelBySession(ctx context.Context, sessionID string, reason string) (int, error) {
	trimmed := strings.TrimSpace(sessionID)
	if trimmed == "" {
		return 0, fmt.Errorf("session id is required")
	}
	return s.cancelWhere(ctx, "session_id = ?", trimmed, reason)
}

func (s *sqliteSwarmStore) PendingCount(ctx context.Context, mailbox string) (int, error) {
	trimmed := strings.TrimSpace(mailbox)
	if trimmed == "" {
		return 0, fmt.Errorf("mailbox is required")
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM swarm_messages
		WHERE mailbox = ? AND status IN (?, ?)`,
		trimmed,
		SwarmMessageStatusQueued,
		SwarmMessageStatusRetry,
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("count pending swarm messages: %w", err)
	}
	return count, nil
}

func (s *sqliteSwarmStore) CancelDroppable(
	ctx context.Context,
	mailbox string,
	limit int,
	reason string,
) ([]SwarmMessageRecord, error) {
	trimmed := strings.TrimSpace(mailbox)
	if trimmed == "" {
		return nil, fmt.Errorf("mailbox is required")
	}
	if limit <= 0 {
		return nil, nil
	}
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin cancel droppable swarm messages: %w", err)
	}
	defer rollbackSwarmTx(tx)

	rows, err := tx.QueryContext(ctx, swarmMessageSelectSQL+`
		WHERE mailbox = ? AND status IN (?, ?)
		ORDER BY priority ASC, created_at ASC
		LIMIT ?`,
		trimmed,
		SwarmMessageStatusQueued,
		SwarmMessageStatusRetry,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("select droppable swarm messages: %w", err)
	}
	records := make([]SwarmMessageRecord, 0, limit)
	for rows.Next() {
		record, ok, err := scanSwarmMessage(rows.Scan)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		if ok {
			records = append(records, record)
		}
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close droppable swarm message rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate droppable swarm messages: %w", err)
	}

	for _, record := range records {
		if _, err := tx.ExecContext(ctx, `
			UPDATE swarm_messages
			SET status = ?, completed_at = ?, lease_owner = NULL, lease_until = NULL, updated_at = ?, error = ?
			WHERE mailbox = ? AND id = ? AND status IN (?, ?)`,
			SwarmMessageStatusCanceled,
			now.Format(time.RFC3339),
			now.Format(time.RFC3339),
			nullIfEmpty(reason),
			trimmed,
			record.ID,
			SwarmMessageStatusQueued,
			SwarmMessageStatusRetry,
		); err != nil {
			return nil, fmt.Errorf("cancel droppable swarm message %q: %w", record.ID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit cancel droppable swarm messages: %w", err)
	}
	return records, nil
}

func (s *sqliteSwarmStore) cancelWhere(ctx context.Context, predicate string, arg string, reason string) (int, error) {
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `
		UPDATE swarm_messages
		SET status = ?, completed_at = ?, lease_owner = NULL, lease_until = NULL, updated_at = ?, error = ?
		WHERE `+predicate+` AND status IN (?, ?, ?)`,
		SwarmMessageStatusCanceled,
		now.Format(time.RFC3339),
		now.Format(time.RFC3339),
		nullIfEmpty(reason),
		arg,
		SwarmMessageStatusQueued,
		SwarmMessageStatusRetry,
		SwarmMessageStatusLeased,
	)
	if err != nil {
		return 0, fmt.Errorf("cancel swarm messages: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count canceled swarm messages: %w", err)
	}
	return int(count), nil
}

func (s *sqliteSwarmStore) Recover(ctx context.Context, now time.Time) (SwarmRecoveryResult, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SwarmRecoveryResult{}, fmt.Errorf("begin swarm recovery: %w", err)
	}
	defer rollbackSwarmTx(tx)

	expired, err := tx.ExecContext(ctx, `
		UPDATE swarm_messages
		SET status = ?, completed_at = ?, lease_owner = NULL, lease_until = NULL, updated_at = ?, error = COALESCE(NULLIF(error, ''), 'message expired')
		WHERE status IN (?, ?, ?)
		  AND expires_at IS NOT NULL AND expires_at != '' AND expires_at <= ?`,
		SwarmMessageStatusExpired,
		now.Format(time.RFC3339),
		now.Format(time.RFC3339),
		SwarmMessageStatusQueued,
		SwarmMessageStatusRetry,
		SwarmMessageStatusLeased,
		now.Format(time.RFC3339),
	)
	if err != nil {
		return SwarmRecoveryResult{}, fmt.Errorf("expire swarm messages: %w", err)
	}

	retried, err := tx.ExecContext(ctx, `
		UPDATE swarm_messages
		SET status = ?, not_before = ?, lease_owner = NULL, lease_until = NULL, updated_at = ?, error = COALESCE(NULLIF(error, ''), 'lease expired')
		WHERE status = ? AND lease_until IS NOT NULL AND lease_until != '' AND lease_until < ?`,
		SwarmMessageStatusRetry,
		now.Format(time.RFC3339),
		now.Format(time.RFC3339),
		SwarmMessageStatusLeased,
		now.Format(time.RFC3339),
	)
	if err != nil {
		return SwarmRecoveryResult{}, fmt.Errorf("recover leased swarm messages: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return SwarmRecoveryResult{}, fmt.Errorf("commit swarm recovery: %w", err)
	}

	expiredCount, err := expired.RowsAffected()
	if err != nil {
		return SwarmRecoveryResult{}, fmt.Errorf("count expired swarm messages: %w", err)
	}
	retriedCount, err := retried.RowsAffected()
	if err != nil {
		return SwarmRecoveryResult{}, fmt.Errorf("count recovered swarm messages: %w", err)
	}
	return SwarmRecoveryResult{RetriedLeases: int(retriedCount), Expired: int(expiredCount)}, nil
}

func (s *sqliteSwarmStore) ListReadyMailboxes(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}
	now := time.Now().UTC().Format(time.RFC3339)
	rows, err := s.db.QueryContext(ctx, `
		SELECT mailbox
		FROM swarm_messages
		WHERE status IN (?, ?)
		  AND (not_before IS NULL OR not_before = '' OR not_before <= ?)
		  AND (expires_at IS NULL OR expires_at = '' OR expires_at > ?)
		GROUP BY mailbox
		ORDER BY MAX(priority) DESC, MIN(created_at) ASC
		LIMIT ?`,
		SwarmMessageStatusQueued,
		SwarmMessageStatusRetry,
		now,
		now,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list ready swarm mailboxes: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]string, 0)
	for rows.Next() {
		var mailbox string
		if err := rows.Scan(&mailbox); err != nil {
			return nil, fmt.Errorf("scan ready swarm mailbox: %w", err)
		}
		out = append(out, mailbox)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ready swarm mailboxes: %w", err)
	}
	return out, nil
}

func (s *sqliteSwarmStore) ListMailboxStatusCounts(ctx context.Context, limit int) ([]SwarmMailboxStatusCount, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT mailbox, status, COUNT(*)
		FROM swarm_messages
		WHERE status NOT IN (?, ?)
		GROUP BY mailbox, status
		ORDER BY mailbox ASC, status ASC
		LIMIT ?`,
		SwarmMessageStatusAcked,
		SwarmMessageStatusShadow,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list mailbox status counts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []SwarmMailboxStatusCount
	for rows.Next() {
		var record SwarmMailboxStatusCount
		if err := rows.Scan(&record.Mailbox, &record.Status, &record.Count); err != nil {
			return nil, fmt.Errorf("scan mailbox status count: %w", err)
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate mailbox status counts: %w", err)
	}
	return out, nil
}

func (s *sqliteSwarmStore) GetMessage(ctx context.Context, messageID string) (SwarmMessageRecord, bool, error) {
	record, ok, err := scanSwarmMessage(s.db.QueryRowContext(ctx, swarmMessageSelectSQL+` WHERE id = ?`, strings.TrimSpace(messageID)).Scan)
	if err != nil {
		return SwarmMessageRecord{}, false, err
	}
	return record, ok, nil
}

const swarmMessageSelectSQL = `
	SELECT id, mailbox, namespace, kind, from_addr, to_addr,
	       COALESCE(session_id, ''), COALESCE(task_id, ''), COALESCE(correlation_id, ''), COALESCE(causation_id, ''),
	       priority, COALESCE(dedupe_key, ''), status, attempt, max_attempts,
	       COALESCE(not_before, ''), COALESCE(expires_at, ''), COALESCE(lease_owner, ''), COALESCE(lease_until, ''),
	       payload_json, COALESCE(meta_json, ''), created_at, updated_at, COALESCE(completed_at, ''), COALESCE(error, '')
	FROM swarm_messages`

func scanSwarmMessage(scan func(dest ...any) error) (SwarmMessageRecord, bool, error) {
	var (
		record         SwarmMessageRecord
		notBeforeRaw   string
		expiresAtRaw   string
		leaseUntilRaw  string
		createdAtRaw   string
		updatedAtRaw   string
		completedAtRaw string
	)
	err := scan(
		&record.ID,
		&record.Mailbox,
		&record.Namespace,
		&record.Kind,
		&record.FromAddr,
		&record.ToAddr,
		&record.SessionID,
		&record.TaskID,
		&record.CorrelationID,
		&record.CausationID,
		&record.Priority,
		&record.DedupeKey,
		&record.Status,
		&record.Attempt,
		&record.MaxAttempts,
		&notBeforeRaw,
		&expiresAtRaw,
		&record.LeaseOwner,
		&leaseUntilRaw,
		&record.PayloadJSON,
		&record.MetaJSON,
		&createdAtRaw,
		&updatedAtRaw,
		&completedAtRaw,
		&record.Error,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return SwarmMessageRecord{}, false, nil
		}
		return SwarmMessageRecord{}, false, fmt.Errorf("scan swarm message: %w", err)
	}

	notBefore, err := parseOptionalRFC3339(notBeforeRaw)
	if err != nil {
		return SwarmMessageRecord{}, false, fmt.Errorf("parse not_before: %w", err)
	}
	expiresAt, err := parseOptionalRFC3339(expiresAtRaw)
	if err != nil {
		return SwarmMessageRecord{}, false, fmt.Errorf("parse expires_at: %w", err)
	}
	leaseUntil, err := parseOptionalRFC3339(leaseUntilRaw)
	if err != nil {
		return SwarmMessageRecord{}, false, fmt.Errorf("parse lease_until: %w", err)
	}
	createdAt, err := parseRequiredRFC3339(createdAtRaw)
	if err != nil {
		return SwarmMessageRecord{}, false, fmt.Errorf("parse created_at: %w", err)
	}
	updatedAt, err := parseRequiredRFC3339(updatedAtRaw)
	if err != nil {
		return SwarmMessageRecord{}, false, fmt.Errorf("parse updated_at: %w", err)
	}
	completedAt, err := parseOptionalRFC3339(completedAtRaw)
	if err != nil {
		return SwarmMessageRecord{}, false, fmt.Errorf("parse completed_at: %w", err)
	}

	record.NotBefore = notBefore
	record.ExpiresAt = expiresAt
	record.LeaseUntil = leaseUntil
	record.CreatedAt = createdAt
	record.UpdatedAt = updatedAt
	record.CompletedAt = completedAt
	return record, true, nil
}

func normalizeSwarmMessage(record SwarmMessageRecord, now time.Time) (SwarmMessageRecord, error) {
	record.ID = strings.TrimSpace(record.ID)
	if record.ID == "" {
		return SwarmMessageRecord{}, fmt.Errorf("swarm message id is required")
	}
	record.Mailbox = strings.TrimSpace(record.Mailbox)
	if record.Mailbox == "" {
		return SwarmMessageRecord{}, fmt.Errorf("swarm message mailbox is required")
	}
	record.Namespace = strings.TrimSpace(record.Namespace)
	if record.Namespace == "" {
		return SwarmMessageRecord{}, fmt.Errorf("swarm message namespace is required")
	}
	record.Kind = strings.TrimSpace(record.Kind)
	if record.Kind == "" {
		return SwarmMessageRecord{}, fmt.Errorf("swarm message kind is required")
	}
	record.FromAddr = strings.TrimSpace(record.FromAddr)
	if record.FromAddr == "" {
		return SwarmMessageRecord{}, fmt.Errorf("swarm message from address is required")
	}
	record.ToAddr = strings.TrimSpace(record.ToAddr)
	if record.ToAddr == "" {
		return SwarmMessageRecord{}, fmt.Errorf("swarm message to address is required")
	}
	record.PayloadJSON = strings.TrimSpace(record.PayloadJSON)
	if record.PayloadJSON == "" {
		return SwarmMessageRecord{}, fmt.Errorf("swarm message payload_json is required")
	}
	record.Status = strings.TrimSpace(record.Status)
	if record.Status == "" {
		record.Status = SwarmMessageStatusQueued
	}
	if record.Status != SwarmMessageStatusQueued && record.Status != SwarmMessageStatusShadow {
		return SwarmMessageRecord{}, fmt.Errorf("swarm publish status must be %q or %q", SwarmMessageStatusQueued, SwarmMessageStatusShadow)
	}
	if record.MaxAttempts <= 0 {
		record.MaxAttempts = SwarmMessageDefaultMaxAttempts
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.CreatedAt = record.CreatedAt.UTC()
	record.UpdatedAt = now
	record.SessionID = strings.TrimSpace(record.SessionID)
	record.TaskID = strings.TrimSpace(record.TaskID)
	record.CorrelationID = strings.TrimSpace(record.CorrelationID)
	record.CausationID = strings.TrimSpace(record.CausationID)
	record.DedupeKey = strings.TrimSpace(record.DedupeKey)
	record.LeaseOwner = strings.TrimSpace(record.LeaseOwner)
	record.MetaJSON = strings.TrimSpace(record.MetaJSON)
	record.Error = strings.TrimSpace(record.Error)
	return record, nil
}

func normalizeMailboxMessageID(mailbox string, messageID string) (string, string, error) {
	trimmedMailbox := strings.TrimSpace(mailbox)
	if trimmedMailbox == "" {
		return "", "", fmt.Errorf("mailbox is required")
	}
	trimmedMessageID := strings.TrimSpace(messageID)
	if trimmedMessageID == "" {
		return "", "", fmt.Errorf("message id is required")
	}
	return trimmedMailbox, trimmedMessageID, nil
}

func rollbackSwarmTx(tx *sql.Tx) {
	if tx != nil {
		_ = tx.Rollback()
	}
}

func nonNegative(v int) int {
	if v < 0 {
		return 0
	}
	return v
}

func defaultMaxAttempts(v int) int {
	if v <= 0 {
		return SwarmMessageDefaultMaxAttempts
	}
	return v
}

func nullIfEmpty(raw string) any {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	return trimmed
}

func optionalTimeValue(ts time.Time) any {
	if ts.IsZero() {
		return nil
	}
	return ts.UTC().Format(time.RFC3339)
}
