package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/normahq/balda/internal/apps/balda/auth"
	"github.com/tgbotkit/runtime/updatepoller"
	adksession "google.golang.org/adk/session"
	_ "modernc.org/sqlite" // pure-Go SQLite driver
)

type sqliteProvider struct {
	db         *sql.DB
	appKV      *sqliteKVStore
	mcpKV      *sqliteKVStore
	adkSession *sqliteADKSessionService
	session    *sqliteSessionStore
	tasks      *sqliteScheduledTaskStore
	swarm      *sqliteSwarmStore
	offset     *sqliteOffsetStore
	collab     *auth.CollaboratorStore
}

var _ Provider = (*sqliteProvider)(nil)

func (p *sqliteProvider) AddCollaborator(ctx context.Context, c auth.Collaborator) error {
	if c.UserID == "" {
		return fmt.Errorf("user_id is required")
	}

	_, err := p.db.ExecContext(ctx, `
		INSERT OR REPLACE INTO balda_collaborators (user_id, username, first_name, added_by, added_at)
		VALUES (?, ?, ?, ?, ?)`,
		c.UserID, c.Username, c.FirstName, c.AddedBy, c.AddedAt.Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("add collaborator: %w", err)
	}
	return nil
}

func (p *sqliteProvider) RemoveCollaborator(ctx context.Context, userID string) error {
	if userID == "" {
		return fmt.Errorf("user_id is required")
	}

	_, err := p.db.ExecContext(ctx, `
		DELETE FROM balda_collaborators
		WHERE user_id = ?`,
		userID,
	)
	if err != nil {
		return fmt.Errorf("remove collaborator: %w", err)
	}
	return nil
}

func (p *sqliteProvider) GetCollaborator(ctx context.Context, userID string) (*auth.Collaborator, bool, error) {
	var username, firstName, addedBy, addedAt string
	err := p.db.QueryRowContext(ctx, `
		SELECT username, first_name, added_by, added_at
		FROM balda_collaborators
		WHERE user_id = ?`,
		userID,
	).Scan(&username, &firstName, &addedBy, &addedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get collaborator: %w", err)
	}

	parsedTime, err := time.Parse(time.RFC3339, addedAt)
	if err != nil {
		return nil, false, fmt.Errorf("parse added_at: %w", err)
	}

	return &auth.Collaborator{
		UserID:    userID,
		Username:  username,
		FirstName: firstName,
		AddedBy:   addedBy,
		AddedAt:   parsedTime,
	}, true, nil
}

func (p *sqliteProvider) ListCollaborators(ctx context.Context) ([]auth.Collaborator, error) {
	rows, err := p.db.QueryContext(ctx, `
		SELECT user_id, username, first_name, added_by, added_at
		FROM balda_collaborators
		ORDER BY added_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list collaborators: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var collaborators []auth.Collaborator
	for rows.Next() {
		var c auth.Collaborator
		var addedAt string
		if err := rows.Scan(&c.UserID, &c.Username, &c.FirstName, &c.AddedBy, &addedAt); err != nil {
			return nil, fmt.Errorf("scan collaborator: %w", err)
		}
		parsedTime, err := time.Parse(time.RFC3339, addedAt)
		if err != nil {
			return nil, fmt.Errorf("parse added_at: %w", err)
		}
		c.AddedAt = parsedTime
		collaborators = append(collaborators, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate collaborators: %w", err)
	}

	return collaborators, nil
}

// NewSQLiteProvider initializes balda state storage in a SQLite database.
func NewSQLiteProvider(ctx context.Context, path string) (Provider, error) {
	dbPath := strings.TrimSpace(path)
	if dbPath == "" {
		return nil, fmt.Errorf("sqlite path is required")
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open balda state sqlite db: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := applySQLitePragmas(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}

	var provider = &sqliteProvider{
		db:         db,
		appKV:      &sqliteKVStore{db: db, namespace: NamespaceApp},
		mcpKV:      &sqliteKVStore{db: db, namespace: NamespaceSessionMCP},
		adkSession: &sqliteADKSessionService{db: db},
		session:    &sqliteSessionStore{db: db},
		tasks:      &sqliteScheduledTaskStore{db: db},
		swarm:      &sqliteSwarmStore{db: db},
		offset:     &sqliteOffsetStore{db: db},
	}
	provider.collab = auth.NewCollaboratorStore(provider)
	return provider, nil
}

func (p *sqliteProvider) AppKV() KVStore {
	return p.appKV
}

func (p *sqliteProvider) ADKSessions() adksession.Service {
	return p.adkSession
}

func (p *sqliteProvider) SessionMCPKV() KVStore {
	return p.mcpKV
}

func (p *sqliteProvider) Sessions() SessionStore {
	return p.session
}

func (p *sqliteProvider) ScheduledTasks() ScheduledTaskStore {
	return p.tasks
}

func (p *sqliteProvider) Swarm() SwarmStore {
	return p.swarm
}

func (p *sqliteProvider) PollingOffsetStore() updatepoller.OffsetStore {
	return p.offset
}

func (p *sqliteProvider) Collaborators() CollaboratorStore {
	return p.collab
}

func (p *sqliteProvider) Close() error {
	return p.db.Close()
}

func applySQLitePragmas(ctx context.Context, db *sql.DB) error {
	stmts := []string{
		"PRAGMA foreign_keys=ON;",
		"PRAGMA journal_mode=WAL;",
		"PRAGMA busy_timeout=5000;",
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			// WAL can be unsupported in some environments. Ignore only this one.
			if stmt == "PRAGMA journal_mode=WAL;" {
				continue
			}
			return fmt.Errorf("apply balda state pragma %q: %w", stmt, err)
		}
	}
	return nil
}
