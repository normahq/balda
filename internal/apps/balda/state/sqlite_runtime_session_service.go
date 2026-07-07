package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"maps"
	"strings"
	"time"

	"google.golang.org/adk/v2/platform"
	adksession "google.golang.org/adk/v2/session"
)

const runtimeSessionTimeFormat = time.RFC3339Nano

type sqliteRuntimeSessionService struct {
	db *sql.DB
}

var _ adksession.Service = (*sqliteRuntimeSessionService)(nil)

// UpdateSessionState updates stored session-scoped state without appending an
// event. Balda uses this to refresh runtime CWD when restoring a persisted chat.
func (s *sqliteRuntimeSessionService) UpdateSessionState(
	ctx context.Context,
	appName string,
	userID string,
	sessionID string,
	state map[string]any,
) (adksession.Session, error) {
	key, err := validateRuntimeSessionKey(appName, userID, sessionID)
	if err != nil {
		return nil, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin update runtime session state: %w", err)
	}
	defer rollbackTx(tx)

	sessionState, updatedAt, err := fetchRuntimeSessionState(ctx, tx, key)
	if err != nil {
		return nil, err
	}
	maps.Copy(sessionState, cloneStateMap(state))
	now := platform.Now(ctx).UTC()
	if err := saveRuntimeSessionState(ctx, tx, key, sessionState, now); err != nil {
		return nil, err
	}
	if updatedAt.After(now) {
		now = updatedAt
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit update runtime session state: %w", err)
	}
	return s.sessionFromStorage(ctx, s.db, key, sessionState, now, nil)
}

func (s *sqliteRuntimeSessionService) Create(ctx context.Context, req *adksession.CreateRequest) (*adksession.CreateResponse, error) {
	if strings.TrimSpace(req.AppName) == "" || strings.TrimSpace(req.UserID) == "" {
		return nil, fmt.Errorf("app_name and user_id are required")
	}

	sessionID := strings.TrimSpace(req.SessionID)
	if sessionID == "" {
		sessionID = platform.NewUUID(ctx)
	}
	key := runtimeSessionKey{
		appName:   strings.TrimSpace(req.AppName),
		userID:    strings.TrimSpace(req.UserID),
		sessionID: sessionID,
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin create runtime session: %w", err)
	}
	defer rollbackTx(tx)

	var one int
	err = tx.QueryRowContext(ctx, `
		SELECT 1
		FROM balda_runtime_sessions
		WHERE app_name = ? AND user_id = ? AND session_id = ?`,
		key.appName, key.userID, key.sessionID,
	).Scan(&one)
	if err == nil {
		return nil, fmt.Errorf("runtime session %q already exists", key.sessionID)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("check runtime session %q exists: %w", key.sessionID, err)
	}

	appState, err := fetchRuntimeAppState(ctx, tx, key.appName)
	if err != nil {
		return nil, err
	}
	userState, err := fetchRuntimeUserState(ctx, tx, key.appName, key.userID)
	if err != nil {
		return nil, err
	}

	appDelta, userDelta, sessionState := splitRuntimeStateDeltas(req.State)
	if len(appDelta) > 0 {
		maps.Copy(appState, appDelta)
		if err := saveRuntimeAppState(ctx, tx, key.appName, appState, platform.Now(ctx).UTC()); err != nil {
			return nil, err
		}
	}
	if len(userDelta) > 0 {
		maps.Copy(userState, userDelta)
		if err := saveRuntimeUserState(ctx, tx, key.appName, key.userID, userState, platform.Now(ctx).UTC()); err != nil {
			return nil, err
		}
	}

	now := platform.Now(ctx).UTC()
	if err := saveRuntimeSessionState(ctx, tx, key, sessionState, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit create runtime session: %w", err)
	}

	return &adksession.CreateResponse{
		Session: newSQLiteRuntimeSession(key, mergeRuntimeStates(appState, userState, sessionState), nil, now),
	}, nil
}

func (s *sqliteRuntimeSessionService) Get(ctx context.Context, req *adksession.GetRequest) (*adksession.GetResponse, error) {
	key, err := validateRuntimeSessionKey(req.AppName, req.UserID, req.SessionID)
	if err != nil {
		return nil, err
	}
	sess, err := s.loadSession(ctx, key, req.NumRecentEvents, req.After)
	if err != nil {
		return nil, err
	}
	return &adksession.GetResponse{Session: sess}, nil
}

func (s *sqliteRuntimeSessionService) List(ctx context.Context, req *adksession.ListRequest) (*adksession.ListResponse, error) {
	appName := strings.TrimSpace(req.AppName)
	if appName == "" {
		return nil, fmt.Errorf("app_name is required")
	}

	query := `
		SELECT user_id, session_id, state_json, updated_at
		FROM balda_runtime_sessions
		WHERE app_name = ?`
	args := []any{appName}
	if userID := strings.TrimSpace(req.UserID); userID != "" {
		query += ` AND user_id = ?`
		args = append(args, userID)
	}
	query += ` ORDER BY updated_at DESC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list runtime sessions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	appState, err := fetchRuntimeAppState(ctx, s.db, appName)
	if err != nil {
		return nil, err
	}

	type listedSession struct {
		userID     string
		sessionID  string
		stateJSON  string
		updatedRaw string
	}
	listed := make([]listedSession, 0)
	for rows.Next() {
		var item listedSession
		if err := rows.Scan(&item.userID, &item.sessionID, &item.stateJSON, &item.updatedRaw); err != nil {
			return nil, fmt.Errorf("scan runtime session: %w", err)
		}
		listed = append(listed, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate runtime sessions: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close runtime session rows: %w", err)
	}

	out := make([]adksession.Session, 0, len(listed))
	for _, item := range listed {
		sessionState, err := decodeStateMap(item.stateJSON)
		if err != nil {
			return nil, fmt.Errorf("decode runtime session %q state: %w", item.sessionID, err)
		}
		updatedAt, err := parseRuntimeTime(item.updatedRaw)
		if err != nil {
			return nil, fmt.Errorf("parse runtime session %q update time: %w", item.sessionID, err)
		}
		userState, err := fetchRuntimeUserState(ctx, s.db, appName, item.userID)
		if err != nil {
			return nil, err
		}
		out = append(out, newSQLiteRuntimeSession(
			runtimeSessionKey{appName: appName, userID: item.userID, sessionID: item.sessionID},
			mergeRuntimeStates(appState, userState, sessionState),
			nil,
			updatedAt,
		))
	}

	return &adksession.ListResponse{Sessions: out}, nil
}

func (s *sqliteRuntimeSessionService) Delete(ctx context.Context, req *adksession.DeleteRequest) error {
	key, err := validateRuntimeSessionKey(req.AppName, req.UserID, req.SessionID)
	if err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `
		DELETE FROM balda_runtime_sessions
		WHERE app_name = ? AND user_id = ? AND session_id = ?`,
		key.appName, key.userID, key.sessionID,
	); err != nil {
		return fmt.Errorf("delete runtime session %q: %w", key.sessionID, err)
	}
	return nil
}

func (s *sqliteRuntimeSessionService) AppendEvent(ctx context.Context, curSession adksession.Session, event *adksession.Event) error {
	if curSession == nil {
		return fmt.Errorf("session is nil")
	}
	if event == nil {
		return fmt.Errorf("event is nil")
	}
	if event.Partial {
		return nil
	}
	key, err := validateRuntimeSessionKey(curSession.AppName(), curSession.UserID(), curSession.ID())
	if err != nil {
		return err
	}
	if strings.TrimSpace(event.ID) == "" {
		event.ID = platform.NewUUID(ctx)
	}
	event.Timestamp = event.Timestamp.UTC().Truncate(time.Microsecond)
	if event.Timestamp.IsZero() {
		event.Timestamp = platform.Now(ctx).UTC().Truncate(time.Microsecond)
	}
	filterTempState(event)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin append runtime event: %w", err)
	}
	defer rollbackTx(tx)

	sessionState, _, err := fetchRuntimeSessionState(ctx, tx, key)
	if err != nil {
		return err
	}
	appState, err := fetchRuntimeAppState(ctx, tx, key.appName)
	if err != nil {
		return err
	}
	userState, err := fetchRuntimeUserState(ctx, tx, key.appName, key.userID)
	if err != nil {
		return err
	}
	appDelta, userDelta, sessionDelta := splitRuntimeStateDeltas(event.Actions.StateDelta)
	if len(appDelta) > 0 {
		maps.Copy(appState, appDelta)
		if err := saveRuntimeAppState(ctx, tx, key.appName, appState, event.Timestamp); err != nil {
			return err
		}
	}
	if len(userDelta) > 0 {
		maps.Copy(userState, userDelta)
		if err := saveRuntimeUserState(ctx, tx, key.appName, key.userID, userState, event.Timestamp); err != nil {
			return err
		}
	}
	if len(sessionDelta) > 0 {
		maps.Copy(sessionState, sessionDelta)
	}

	var ordinal sql.NullInt64
	err = tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(ordinal), 0) + 1
		FROM balda_runtime_events
		WHERE app_name = ? AND user_id = ? AND session_id = ?`,
		key.appName, key.userID, key.sessionID,
	).Scan(&ordinal)
	if err != nil {
		return fmt.Errorf("next runtime event ordinal: %w", err)
	}
	eventOrdinal := int64(1)
	if ordinal.Valid {
		eventOrdinal = ordinal.Int64
	}
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal runtime event %q: %w", event.ID, err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO balda_runtime_events (
			app_name, user_id, session_id, event_id, ordinal, timestamp, event_json
		)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		key.appName,
		key.userID,
		key.sessionID,
		event.ID,
		eventOrdinal,
		event.Timestamp.Format(runtimeSessionTimeFormat),
		string(eventJSON),
	); err != nil {
		return fmt.Errorf("insert runtime event %q: %w", event.ID, err)
	}
	if err := saveRuntimeSessionState(ctx, tx, key, sessionState, event.Timestamp); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit append runtime event: %w", err)
	}

	if sess, ok := curSession.(*sqliteRuntimeSession); ok {
		sess.appendEvent(event, mergeRuntimeStates(appState, userState, sessionState), event.Timestamp)
	}
	return nil
}

func (s *sqliteRuntimeSessionService) loadSession(ctx context.Context, key runtimeSessionKey, limit int, after time.Time) (*sqliteRuntimeSession, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin get runtime session: %w", err)
	}
	defer rollbackTx(tx)

	sessionState, updatedAt, err := fetchRuntimeSessionState(ctx, tx, key)
	if err != nil {
		return nil, err
	}
	events, err := fetchRuntimeEvents(ctx, tx, key, limit, after)
	if err != nil {
		return nil, err
	}
	sess, err := s.sessionFromStorage(ctx, tx, key, sessionState, updatedAt, events)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit get runtime session: %w", err)
	}
	return sess, nil
}

func (s *sqliteRuntimeSessionService) sessionFromStorage(
	ctx context.Context,
	q dbQueryer,
	key runtimeSessionKey,
	sessionState map[string]any,
	updatedAt time.Time,
	events []*adksession.Event,
) (*sqliteRuntimeSession, error) {
	appState, err := fetchRuntimeAppState(ctx, q, key.appName)
	if err != nil {
		return nil, err
	}
	userState, err := fetchRuntimeUserState(ctx, q, key.appName, key.userID)
	if err != nil {
		return nil, err
	}
	return newSQLiteRuntimeSession(key, mergeRuntimeStates(appState, userState, sessionState), events, updatedAt), nil
}

type runtimeSessionKey struct {
	appName   string
	userID    string
	sessionID string
}

func validateRuntimeSessionKey(appName, userID, sessionID string) (runtimeSessionKey, error) {
	key := runtimeSessionKey{
		appName:   strings.TrimSpace(appName),
		userID:    strings.TrimSpace(userID),
		sessionID: strings.TrimSpace(sessionID),
	}
	if key.appName == "" || key.userID == "" || key.sessionID == "" {
		return runtimeSessionKey{}, fmt.Errorf("app_name, user_id, session_id are required")
	}
	return key, nil
}

type sqliteRuntimeSession struct {
	key       runtimeSessionKey
	state     *sqliteRuntimeState
	events    *sqliteRuntimeEvents
	updatedAt time.Time
}

func newSQLiteRuntimeSession(key runtimeSessionKey, state map[string]any, events []*adksession.Event, updatedAt time.Time) *sqliteRuntimeSession {
	return &sqliteRuntimeSession{
		key:       key,
		state:     &sqliteRuntimeState{values: cloneStateMap(state)},
		events:    &sqliteRuntimeEvents{events: cloneRuntimeEvents(events)},
		updatedAt: updatedAt,
	}
}

func (s *sqliteRuntimeSession) ID() string {
	return s.key.sessionID
}

func (s *sqliteRuntimeSession) AppName() string {
	return s.key.appName
}

func (s *sqliteRuntimeSession) UserID() string {
	return s.key.userID
}

func (s *sqliteRuntimeSession) State() adksession.State {
	return s.state
}

func (s *sqliteRuntimeSession) Events() adksession.Events {
	return s.events
}

func (s *sqliteRuntimeSession) LastUpdateTime() time.Time {
	return s.updatedAt
}

func (s *sqliteRuntimeSession) appendEvent(event *adksession.Event, state map[string]any, updatedAt time.Time) {
	s.events.events = append(s.events.events, event)
	s.state.values = cloneStateMap(state)
	s.updatedAt = updatedAt
}

type sqliteRuntimeState struct {
	values map[string]any
}

func (s *sqliteRuntimeState) Get(key string) (any, error) {
	value, ok := s.values[key]
	if !ok {
		return nil, adksession.ErrStateKeyNotExist
	}
	return value, nil
}

func (s *sqliteRuntimeState) Set(key string, value any) error {
	if s.values == nil {
		s.values = make(map[string]any)
	}
	s.values[key] = value
	return nil
}

func (s *sqliteRuntimeState) All() iter.Seq2[string, any] {
	values := cloneStateMap(s.values)
	return func(yield func(string, any) bool) {
		for key, value := range values {
			if !yield(key, value) {
				return
			}
		}
	}
}

type sqliteRuntimeEvents struct {
	events []*adksession.Event
}

func (e *sqliteRuntimeEvents) All() iter.Seq[*adksession.Event] {
	events := cloneRuntimeEvents(e.events)
	return func(yield func(*adksession.Event) bool) {
		for _, event := range events {
			if !yield(event) {
				return
			}
		}
	}
}

func (e *sqliteRuntimeEvents) Len() int {
	return len(e.events)
}

func (e *sqliteRuntimeEvents) At(i int) *adksession.Event {
	return e.events[i]
}

func fetchRuntimeSessionState(ctx context.Context, q dbQueryer, key runtimeSessionKey) (map[string]any, time.Time, error) {
	var stateJSON, updatedRaw string
	err := q.QueryRowContext(ctx, `
		SELECT state_json, updated_at
		FROM balda_runtime_sessions
		WHERE app_name = ? AND user_id = ? AND session_id = ?`,
		key.appName, key.userID, key.sessionID,
	).Scan(&stateJSON, &updatedRaw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, time.Time{}, fmt.Errorf("runtime session %q not found", key.sessionID)
		}
		return nil, time.Time{}, fmt.Errorf("fetch runtime session %q: %w", key.sessionID, err)
	}
	state, err := decodeStateMap(stateJSON)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("decode runtime session %q state: %w", key.sessionID, err)
	}
	updatedAt, err := parseRuntimeTime(updatedRaw)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("parse runtime session %q update time: %w", key.sessionID, err)
	}
	return state, updatedAt, nil
}

func saveRuntimeSessionState(ctx context.Context, tx *sql.Tx, key runtimeSessionKey, state map[string]any, updatedAt time.Time) error {
	stateJSON, err := encodeStateMap(state)
	if err != nil {
		return fmt.Errorf("encode runtime session %q state: %w", key.sessionID, err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO balda_runtime_sessions (app_name, user_id, session_id, state_json, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(app_name, user_id, session_id) DO UPDATE SET
			state_json = excluded.state_json,
			updated_at = excluded.updated_at`,
		key.appName, key.userID, key.sessionID, stateJSON, updatedAt.UTC().Format(runtimeSessionTimeFormat),
	); err != nil {
		return fmt.Errorf("save runtime session %q state: %w", key.sessionID, err)
	}
	return nil
}

func fetchRuntimeAppState(ctx context.Context, q dbQueryer, appName string) (map[string]any, error) {
	var raw string
	err := q.QueryRowContext(ctx, `
		SELECT state_json
		FROM balda_runtime_app_state
		WHERE app_name = ?`,
		strings.TrimSpace(appName),
	).Scan(&raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return map[string]any{}, nil
		}
		return nil, fmt.Errorf("fetch runtime app state: %w", err)
	}
	return decodeStateMap(raw)
}

func saveRuntimeAppState(ctx context.Context, tx *sql.Tx, appName string, state map[string]any, updatedAt time.Time) error {
	stateJSON, err := encodeStateMap(state)
	if err != nil {
		return fmt.Errorf("encode runtime app state: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO balda_runtime_app_state (app_name, state_json, updated_at)
		VALUES (?, ?, ?)
		ON CONFLICT(app_name) DO UPDATE SET
			state_json = excluded.state_json,
			updated_at = excluded.updated_at`,
		strings.TrimSpace(appName), stateJSON, updatedAt.UTC().Format(runtimeSessionTimeFormat),
	); err != nil {
		return fmt.Errorf("save runtime app state: %w", err)
	}
	return nil
}

func fetchRuntimeUserState(ctx context.Context, q dbQueryer, appName, userID string) (map[string]any, error) {
	var raw string
	err := q.QueryRowContext(ctx, `
		SELECT state_json
		FROM balda_runtime_user_state
		WHERE app_name = ? AND user_id = ?`,
		strings.TrimSpace(appName), strings.TrimSpace(userID),
	).Scan(&raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return map[string]any{}, nil
		}
		return nil, fmt.Errorf("fetch runtime user state: %w", err)
	}
	return decodeStateMap(raw)
}

func saveRuntimeUserState(ctx context.Context, tx *sql.Tx, appName, userID string, state map[string]any, updatedAt time.Time) error {
	stateJSON, err := encodeStateMap(state)
	if err != nil {
		return fmt.Errorf("encode runtime user state: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO balda_runtime_user_state (app_name, user_id, state_json, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(app_name, user_id) DO UPDATE SET
			state_json = excluded.state_json,
			updated_at = excluded.updated_at`,
		strings.TrimSpace(appName), strings.TrimSpace(userID), stateJSON, updatedAt.UTC().Format(runtimeSessionTimeFormat),
	); err != nil {
		return fmt.Errorf("save runtime user state: %w", err)
	}
	return nil
}

func fetchRuntimeEvents(ctx context.Context, q dbQueryer, key runtimeSessionKey, limit int, after time.Time) ([]*adksession.Event, error) {
	query := `
		SELECT event_json
		FROM balda_runtime_events
		WHERE app_name = ? AND user_id = ? AND session_id = ?`
	args := []any{key.appName, key.userID, key.sessionID}
	if !after.IsZero() {
		query += ` AND timestamp >= ?`
		args = append(args, after.UTC().Format(runtimeSessionTimeFormat))
	}
	if limit > 0 {
		afterClause := ""
		if !after.IsZero() {
			afterClause = ` AND timestamp >= ?`
		}
		query = `
			SELECT event_json
			FROM (
				SELECT event_json, timestamp, ordinal
				FROM balda_runtime_events
				WHERE app_name = ? AND user_id = ? AND session_id = ?` + afterClause + `
				ORDER BY timestamp DESC, ordinal DESC
				LIMIT ?
			)
			ORDER BY timestamp ASC, ordinal ASC`
		args = []any{key.appName, key.userID, key.sessionID}
		if !after.IsZero() {
			args = append(args, after.UTC().Format(runtimeSessionTimeFormat))
		}
		args = append(args, limit)
	} else {
		query += ` ORDER BY timestamp ASC, ordinal ASC`
	}

	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("fetch runtime events for %q: %w", key.sessionID, err)
	}
	defer func() { _ = rows.Close() }()

	events := make([]*adksession.Event, 0)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan runtime event: %w", err)
		}
		var event adksession.Event
		if err := json.Unmarshal([]byte(raw), &event); err != nil {
			return nil, fmt.Errorf("decode runtime event: %w", err)
		}
		events = append(events, &event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate runtime events: %w", err)
	}
	return events, nil
}

func splitRuntimeStateDeltas(delta map[string]any) (map[string]any, map[string]any, map[string]any) {
	appState := make(map[string]any)
	userState := make(map[string]any)
	sessionState := make(map[string]any)
	for key, value := range delta {
		switch {
		case strings.HasPrefix(key, adksession.KeyPrefixApp):
			appState[strings.TrimPrefix(key, adksession.KeyPrefixApp)] = value
		case strings.HasPrefix(key, adksession.KeyPrefixUser):
			userState[strings.TrimPrefix(key, adksession.KeyPrefixUser)] = value
		case strings.HasPrefix(key, adksession.KeyPrefixTemp):
			continue
		default:
			sessionState[key] = value
		}
	}
	return appState, userState, sessionState
}

func mergeRuntimeStates(appState, userState, sessionState map[string]any) map[string]any {
	out := cloneStateMap(sessionState)
	for key, value := range appState {
		out[adksession.KeyPrefixApp+key] = value
	}
	for key, value := range userState {
		out[adksession.KeyPrefixUser+key] = value
	}
	return out
}

func filterTempState(event *adksession.Event) {
	if event == nil || len(event.Actions.StateDelta) == 0 {
		return
	}
	filtered := make(map[string]any, len(event.Actions.StateDelta))
	for key, value := range event.Actions.StateDelta {
		if strings.HasPrefix(key, adksession.KeyPrefixTemp) {
			continue
		}
		filtered[key] = value
	}
	event.Actions.StateDelta = filtered
}

func encodeStateMap(state map[string]any) (string, error) {
	if state == nil {
		state = map[string]any{}
	}
	data, err := json.Marshal(state)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func decodeStateMap(raw string) (map[string]any, error) {
	if strings.TrimSpace(raw) == "" {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	if out == nil {
		return map[string]any{}, nil
	}
	return out, nil
}

func parseRuntimeTime(raw string) (time.Time, error) {
	if strings.TrimSpace(raw) == "" {
		return time.Time{}, nil
	}
	return time.Parse(runtimeSessionTimeFormat, raw)
}

func cloneStateMap(in map[string]any) map[string]any {
	if in == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	maps.Copy(out, in)
	return out
}

func cloneRuntimeEvents(in []*adksession.Event) []*adksession.Event {
	if in == nil {
		return nil
	}
	out := make([]*adksession.Event, len(in))
	copy(out, in)
	return out
}

type dbQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func rollbackTx(tx *sql.Tx) {
	if tx != nil {
		_ = tx.Rollback()
	}
}
