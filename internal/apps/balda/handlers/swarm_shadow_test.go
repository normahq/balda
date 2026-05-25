package handlers

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

type handlerShadowWakeBus struct{}

func (handlerShadowWakeBus) Publish(context.Context, string, swarm.Envelope) error { return nil }
func (handlerShadowWakeBus) Subscribe(context.Context, string, swarm.EventHandler) (swarm.Subscription, error) {
	return swarm.NewNoopEventBus("test").Subscribe(context.Background(), "", nil)
}
func (handlerShadowWakeBus) Request(context.Context, string, swarm.Envelope, time.Duration) (*swarm.Envelope, error) {
	return nil, nil
}
func (handlerShadowWakeBus) Drain(context.Context) error { return nil }

func TestBaldaHandlerSubmitSessionTurn_WebhookLegacyEnqueuesDirectOnly(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	provider, coordinator := newHandlerSwarmCoordinator(t, ctx, dbPath, swarm.Config{
		Enabled:     true,
		Mode:        swarm.ModeShadow,
		WebhookMode: swarm.ModeLegacy,
		Shadow:      swarm.ShadowConfig{Enabled: true},
	})
	t.Cleanup(func() { _ = provider.Close() })

	queue := &fakeSchedulerTurnQueue{}
	handler := &BaldaHandler{
		turnDispatcher:   queue,
		swarmCoordinator: coordinator,
		logger:           zerolog.Nop(),
	}
	locator := baldatelegram.NewLocator(1001, 0)
	if _, err := handler.submitSessionTurn(ctx, sessionTurnPayload{
		Text:      "release event",
		Locator:   locator,
		UserID:    "tg-42",
		Source:    "webhook",
		DedupeKey: "webhook:req-1",
	}); err != nil {
		t.Fatalf("submitSessionTurn() error = %v", err)
	}
	if len(queue.tasks) != 1 {
		t.Fatalf("queued tasks = %d, want 1", len(queue.tasks))
	}
	if got := countSwarmMessages(t, ctx, dbPath); got != 0 {
		t.Fatalf("swarm message count = %d, want 0", got)
	}
}

func TestBaldaHandlerSubmitSessionTurn_ShadowPersistsAndEnqueuesDirect(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	provider, coordinator := newHandlerSwarmCoordinator(t, ctx, dbPath, swarm.Config{
		Enabled:     true,
		Mode:        swarm.ModeLegacy,
		WebhookMode: swarm.ModeShadow,
		Shadow:      swarm.ShadowConfig{Enabled: true},
	})
	t.Cleanup(func() { _ = provider.Close() })

	queue := &fakeSchedulerTurnQueue{}
	handler := &BaldaHandler{
		turnDispatcher:   queue,
		swarmCoordinator: coordinator,
		logger:           zerolog.Nop(),
	}
	locator := baldatelegram.NewLocator(1001, 0)
	position, err := handler.submitSessionTurn(ctx, sessionTurnPayload{
		Text:      "release event",
		Locator:   locator,
		UserID:    "tg-42",
		Source:    "webhook",
		DedupeKey: "webhook:req-1",
	})
	if err != nil {
		t.Fatalf("submitSessionTurn() error = %v", err)
	}
	if position != 0 {
		t.Fatalf("queue position = %d, want 0", position)
	}
	if len(queue.tasks) != 1 {
		t.Fatalf("queued tasks = %d, want 1", len(queue.tasks))
	}
	if got, want := queue.tasks[0].SessionID, locator.SessionID; got != want {
		t.Fatalf("queued session id = %q, want %q", got, want)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer func() { _ = db.Close() }()
	var status, namespace, kind, sessionID, dedupeKey string
	if err := db.QueryRowContext(ctx, `
		SELECT status, namespace, kind, COALESCE(session_id, ''), COALESCE(dedupe_key, '')
		FROM swarm_messages
		LIMIT 1`,
	).Scan(&status, &namespace, &kind, &sessionID, &dedupeKey); err != nil {
		t.Fatalf("query shadow message: %v", err)
	}
	if status != baldastate.SwarmMessageStatusShadow {
		t.Fatalf("shadow status = %q, want %q", status, baldastate.SwarmMessageStatusShadow)
	}
	if namespace != swarm.NamespaceWebhookInbound {
		t.Fatalf("namespace = %q, want %q", namespace, swarm.NamespaceWebhookInbound)
	}
	if kind != swarm.KindWebhookEvent {
		t.Fatalf("kind = %q, want %q", kind, swarm.KindWebhookEvent)
	}
	if sessionID != locator.SessionID {
		t.Fatalf("session_id = %q, want %q", sessionID, locator.SessionID)
	}
	if dedupeKey != "webhook:req-1" {
		t.Fatalf("dedupe_key = %q, want webhook:req-1", dedupeKey)
	}

	snapshot := coordinator.ShadowMetricsSnapshot()
	if got := snapshot[swarm.MetricShadowEnvelopesTotal]; got != 1 {
		t.Fatalf("%s = %d, want 1", swarm.MetricShadowEnvelopesTotal, got)
	}
	if got := snapshot[swarm.MetricShadowDispatchTotal]; got != 1 {
		t.Fatalf("%s = %d, want 1", swarm.MetricShadowDispatchTotal, got)
	}
}

func TestBaldaHandlerSubmitSessionTurn_WebhookMailboxPersistsQueuedOnly(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	provider, coordinator := newHandlerSwarmCoordinator(t, ctx, dbPath, swarm.Config{
		Enabled:     true,
		Mode:        swarm.ModeShadow,
		WebhookMode: swarm.ModeMailbox,
		Shadow:      swarm.ShadowConfig{Enabled: true},
	})
	t.Cleanup(func() { _ = provider.Close() })

	queue := &fakeSchedulerTurnQueue{}
	handler := &BaldaHandler{
		turnDispatcher:   queue,
		swarmCoordinator: coordinator,
		logger:           zerolog.Nop(),
	}
	locator := baldatelegram.NewLocator(1001, 0)
	position, err := handler.submitSessionTurn(ctx, sessionTurnPayload{
		Text:      "release event",
		Locator:   locator,
		UserID:    "tg-42",
		Source:    "webhook",
		DedupeKey: "webhook:req-1",
	})
	if err != nil {
		t.Fatalf("submitSessionTurn() error = %v", err)
	}
	if position != 0 {
		t.Fatalf("queue position = %d, want 0", position)
	}
	if len(queue.tasks) != 0 {
		t.Fatalf("queued tasks = %d, want 0", len(queue.tasks))
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer func() { _ = db.Close() }()
	var status, namespace, kind, sessionID, dedupeKey string
	if err := db.QueryRowContext(ctx, `
		SELECT status, namespace, kind, COALESCE(session_id, ''), COALESCE(dedupe_key, '')
		FROM swarm_messages
		LIMIT 1`,
	).Scan(&status, &namespace, &kind, &sessionID, &dedupeKey); err != nil {
		t.Fatalf("query mailbox message: %v", err)
	}
	if status != baldastate.SwarmMessageStatusQueued {
		t.Fatalf("mailbox status = %q, want %q", status, baldastate.SwarmMessageStatusQueued)
	}
	if namespace != swarm.NamespaceWebhookInbound {
		t.Fatalf("namespace = %q, want %q", namespace, swarm.NamespaceWebhookInbound)
	}
	if kind != swarm.KindWebhookEvent {
		t.Fatalf("kind = %q, want %q", kind, swarm.KindWebhookEvent)
	}
	if sessionID != locator.SessionID {
		t.Fatalf("session_id = %q, want %q", sessionID, locator.SessionID)
	}
	if dedupeKey != "webhook:req-1" {
		t.Fatalf("dedupe_key = %q, want webhook:req-1", dedupeKey)
	}
}

func newHandlerSwarmCoordinator(
	t *testing.T,
	ctx context.Context,
	dbPath string,
	cfg swarm.Config,
) (baldastate.Provider, *swarm.Coordinator) {
	t.Helper()

	var err error
	cfg, err = cfg.Normalized()
	if err != nil {
		t.Fatalf("Normalize swarm config: %v", err)
	}
	provider, err := baldastate.NewSQLiteProvider(ctx, dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	var coordinator *swarm.Coordinator
	app := fxtest.New(t,
		fx.Supply(
			fx.Annotate(provider, fx.As(new(baldastate.Provider))),
			fx.Annotate(handlerShadowWakeBus{}, fx.As(new(swarm.EventBus))),
			cfg,
		),
		fx.Provide(
			swarm.NewShadowMetrics,
			swarm.NewMailboxService,
			swarm.NewCoordinator,
		),
		fx.Populate(&coordinator),
	)
	app.RequireStart()
	t.Cleanup(func() { app.RequireStop() })
	return provider, coordinator
}

func countSwarmMessages(t *testing.T, ctx context.Context, dbPath string) int {
	t.Helper()

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer func() { _ = db.Close() }()

	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM swarm_messages`).Scan(&count); err != nil {
		t.Fatalf("count swarm messages: %v", err)
	}
	return count
}
