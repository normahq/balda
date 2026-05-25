package swarm

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	baldastate "github.com/normahq/balda/internal/apps/balda/state"
)

type recordingWakeBus struct {
	publishCalls atomic.Int64
}

func (b *recordingWakeBus) Publish(context.Context, string, Envelope) error {
	b.publishCalls.Add(1)
	return nil
}

func (*recordingWakeBus) Subscribe(context.Context, string, EventHandler) (Subscription, error) {
	return noopSubscription{}, nil
}

func (*recordingWakeBus) Request(context.Context, string, Envelope, time.Duration) (*Envelope, error) {
	return nil, nil
}

func (*recordingWakeBus) Drain(context.Context) error { return nil }

func TestMailboxService_PublishShadowPersistsWithoutWake(t *testing.T) {
	ctx := context.Background()
	provider, service, bus := newShadowTestMailboxService(t, ctx)

	env := shadowTestEnvelope("shadow-1", ActorAddress{Target: ActorTypeSession, Key: "s-1"})
	submitted, err := service.PublishShadow(ctx, env)
	if err != nil {
		t.Fatalf("PublishShadow() error = %v", err)
	}
	if !submitted.Published {
		t.Fatal("PublishShadow() Published = false, want true")
	}
	if got, want := submitted.MailboxID, "session:s-1"; got != want {
		t.Fatalf("MailboxID = %q, want %q", got, want)
	}
	if got := bus.publishCalls.Load(); got != 0 {
		t.Fatalf("wake publish calls = %d, want 0", got)
	}

	got, ok, err := provider.Swarm().GetMessage(ctx, env.ID)
	if err != nil {
		t.Fatalf("GetMessage() error = %v", err)
	}
	if !ok || got.Status != baldastate.SwarmMessageStatusShadow {
		t.Fatalf("message = %+v, found=%v, want status shadow", got, ok)
	}
	claimed, err := provider.Swarm().Claim(ctx, "session:s-1", "worker-1", 8, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("claimed messages = %+v, want none", claimed)
	}

	snapshot := service.ShadowMetricsSnapshot()
	if got := snapshot[MetricShadowEnvelopesTotal]; got != 1 {
		t.Fatalf("%s = %d, want 1", MetricShadowEnvelopesTotal, got)
	}
	if got := snapshot[MetricShadowMissingSessionTotal]; got != 0 {
		t.Fatalf("%s = %d, want 0", MetricShadowMissingSessionTotal, got)
	}
	if got := snapshot[MetricShadowDedupeHitsTotal]; got != 0 {
		t.Fatalf("%s = %d, want 0", MetricShadowDedupeHitsTotal, got)
	}
}

func TestMailboxService_PublishShadowTracksDedupeAndMissingSession(t *testing.T) {
	ctx := context.Background()
	_, service, _ := newShadowTestMailboxService(t, ctx)

	first := shadowTestEnvelope("shadow-first", ActorAddress{Target: ActorTypeSession, Key: "s-2"})
	first.SessionID = ""
	first.DedupeKey = "webhook:req-1"
	if _, err := service.PublishShadow(ctx, first); err != nil {
		t.Fatalf("PublishShadow(first) error = %v", err)
	}
	duplicate := shadowTestEnvelope("shadow-duplicate", ActorAddress{Target: ActorTypeSession, Key: "s-2"})
	duplicate.SessionID = ""
	duplicate.DedupeKey = "webhook:req-1"
	submitted, err := service.PublishShadow(ctx, duplicate)
	if err != nil {
		t.Fatalf("PublishShadow(duplicate) error = %v", err)
	}
	if submitted.Published {
		t.Fatal("PublishShadow(duplicate) Published = true, want false")
	}

	service.RecordShadowDispatch()
	snapshot := service.ShadowMetricsSnapshot()
	if got := snapshot[MetricShadowEnvelopesTotal]; got != 2 {
		t.Fatalf("%s = %d, want 2", MetricShadowEnvelopesTotal, got)
	}
	if got := snapshot[MetricShadowMissingSessionTotal]; got != 2 {
		t.Fatalf("%s = %d, want 2", MetricShadowMissingSessionTotal, got)
	}
	if got := snapshot[MetricShadowDedupeHitsTotal]; got != 1 {
		t.Fatalf("%s = %d, want 1", MetricShadowDedupeHitsTotal, got)
	}
	if got := snapshot[MetricShadowDispatchTotal]; got != 1 {
		t.Fatalf("%s = %d, want 1", MetricShadowDispatchTotal, got)
	}
}

func TestMailboxService_PublishAppliesDefaultPriority(t *testing.T) {
	ctx := context.Background()
	provider, service, _ := newMailboxTestService(t, ctx, QueueConfig{})

	env := shadowTestEnvelope("webhook-priority", ActorAddress{Target: ActorTypeSession, Key: "s-priority"})
	env.Namespace = NamespaceWebhookInbound
	if _, err := service.Publish(ctx, env); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	claimed, err := provider.Swarm().Claim(ctx, "session:s-priority", "worker-1", 1, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if len(claimed) != 1 || claimed[0].Priority != 80 {
		t.Fatalf("claimed = %+v, want one message with priority 80", claimed)
	}
}

func TestMailboxService_DropNewReturnsQueueFull(t *testing.T) {
	ctx := context.Background()
	_, service, _ := newMailboxTestService(t, ctx, QueueConfig{
		Cap:  1,
		Drop: QueueDropNew,
	})

	if _, err := service.Publish(ctx, sessionTextEnvelope("first", "s-full", "first")); err != nil {
		t.Fatalf("Publish(first) error = %v", err)
	}
	if _, err := service.Publish(ctx, sessionTextEnvelope("second", "s-full", "second")); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("Publish(second) error = %v, want ErrQueueFull", err)
	}
}

func TestMailboxService_DropOldCancelsLowestPriority(t *testing.T) {
	ctx := context.Background()
	provider, service, _ := newMailboxTestService(t, ctx, QueueConfig{
		Cap:  1,
		Drop: QueueDropOld,
	})

	old := sessionTextEnvelope("old", "s-drop-old", "old")
	old.Namespace = NamespaceTelemetry
	newest := sessionTextEnvelope("new", "s-drop-old", "new")
	newest.Namespace = NamespaceHumanInbound
	if _, err := service.Publish(ctx, old); err != nil {
		t.Fatalf("Publish(old) error = %v", err)
	}
	if _, err := service.Publish(ctx, newest); err != nil {
		t.Fatalf("Publish(new) error = %v", err)
	}

	gotOld, ok, err := provider.Swarm().GetMessage(ctx, "old")
	if err != nil {
		t.Fatalf("GetMessage(old) error = %v", err)
	}
	if !ok || gotOld.Status != baldastate.SwarmMessageStatusCanceled {
		t.Fatalf("old message = %+v, found=%v, want canceled", gotOld, ok)
	}
	claimed, err := provider.Swarm().Claim(ctx, "session:s-drop-old", "worker-1", 2, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != "new" {
		t.Fatalf("claimed = %+v, want only new", claimed)
	}
}

func TestMailboxService_DropSummarizeCreatesSessionSummary(t *testing.T) {
	ctx := context.Background()
	provider, service, _ := newMailboxTestService(t, ctx, QueueConfig{
		Cap:  1,
		Drop: QueueDropSummarize,
	})

	if _, err := service.Publish(ctx, sessionTextEnvelope("old", "s-summary", "old text")); err != nil {
		t.Fatalf("Publish(old) error = %v", err)
	}
	if _, err := service.Publish(ctx, sessionTextEnvelope("new", "s-summary", "new text")); err != nil {
		t.Fatalf("Publish(new) error = %v", err)
	}
	old, ok, err := provider.Swarm().GetMessage(ctx, "old")
	if err != nil {
		t.Fatalf("GetMessage(old) error = %v", err)
	}
	if !ok || old.Status != baldastate.SwarmMessageStatusCanceled {
		t.Fatalf("old message = %+v, found=%v, want canceled", old, ok)
	}
	claimed, err := provider.Swarm().Claim(ctx, "session:s-summary", "worker-1", 2, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("Claim() len = %d, want 1", len(claimed))
	}
	if claimed[0].ID == "new" {
		t.Fatalf("claimed id = %q, want synthetic summary id", claimed[0].ID)
	}
	if !strings.Contains(claimed[0].PayloadJSON, "old text") || !strings.Contains(claimed[0].PayloadJSON, "new text") {
		t.Fatalf("summary payload = %s, want old and new text", claimed[0].PayloadJSON)
	}
}

func TestMailboxService_CollectCoalescesSessionMessages(t *testing.T) {
	ctx := context.Background()
	provider, service, bus := newMailboxTestService(t, ctx, QueueConfig{
		DefaultMode: QueueModeCollect,
		DebounceMS:  int(time.Hour.Milliseconds()),
		Cap:         10,
		Drop:        QueueDropSummarize,
	})

	if _, err := service.Publish(ctx, sessionTextEnvelope("one", "s-collect", "one")); err != nil {
		t.Fatalf("Publish(one) error = %v", err)
	}
	if _, err := service.Publish(ctx, sessionTextEnvelope("two", "s-collect", "two")); err != nil {
		t.Fatalf("Publish(two) error = %v", err)
	}
	if got := bus.publishCalls.Load(); got != 0 {
		t.Fatalf("wake calls before flush = %d, want 0", got)
	}
	if service.collect == nil {
		t.Fatal("collect buffer is nil")
	}
	if err := service.collect.FlushAll(); err != nil {
		t.Fatalf("FlushAll() error = %v", err)
	}

	claimed, err := provider.Swarm().Claim(ctx, "session:s-collect", "worker-1", 2, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("Claim() len = %d, want 1", len(claimed))
	}
	if !strings.Contains(claimed[0].PayloadJSON, "one") || !strings.Contains(claimed[0].PayloadJSON, "two") {
		t.Fatalf("collected payload = %s, want both messages", claimed[0].PayloadJSON)
	}
	if got := bus.publishCalls.Load(); got != 1 {
		t.Fatalf("wake calls after flush = %d, want 1", got)
	}
}

func TestMailboxService_ControlBypassesCollectDebounce(t *testing.T) {
	ctx := context.Background()
	provider, service, _ := newMailboxTestService(t, ctx, QueueConfig{
		DefaultMode: QueueModeCollect,
		DebounceMS:  int(time.Hour.Milliseconds()),
		Cap:         10,
		Drop:        QueueDropSummarize,
	})

	env := sessionTextEnvelope("control", "s-control", "cancel")
	env.Namespace = NamespaceTaskControl
	if _, err := service.Publish(ctx, env); err != nil {
		t.Fatalf("Publish(control) error = %v", err)
	}
	claimed, err := provider.Swarm().Claim(ctx, "session:s-control", "worker-1", 1, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != "control" {
		t.Fatalf("claimed = %+v, want control message", claimed)
	}
}

func TestMailboxService_InterruptCancelsQueuedSessionMessages(t *testing.T) {
	ctx := context.Background()
	provider, service, _ := newMailboxTestService(t, ctx, QueueConfig{})

	if _, err := service.Publish(ctx, sessionTextEnvelope("old", "s-interrupt", "old")); err != nil {
		t.Fatalf("Publish(old) error = %v", err)
	}
	control := sessionTextEnvelope("control", "s-interrupt", "control")
	control.Namespace = NamespaceTaskControl
	if _, err := service.Publish(ctx, control); err != nil {
		t.Fatalf("Publish(control) error = %v", err)
	}
	old, ok, err := provider.Swarm().GetMessage(ctx, "old")
	if err != nil {
		t.Fatalf("GetMessage(old) error = %v", err)
	}
	if !ok || old.Status != baldastate.SwarmMessageStatusCanceled {
		t.Fatalf("old message = %+v, found=%v, want canceled", old, ok)
	}
	claimed, err := provider.Swarm().Claim(ctx, "session:s-interrupt", "worker-1", 2, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != "control" {
		t.Fatalf("claimed = %+v, want control only", claimed)
	}
	claimedEnv, err := recordToEnvelope(claimed[0])
	if err != nil {
		t.Fatalf("recordToEnvelope(control) error = %v", err)
	}
	if QueueModeOf(claimedEnv) != QueueModeInterrupt {
		t.Fatalf("QueueModeOf(control) = %q, want %q", QueueModeOf(claimedEnv), QueueModeInterrupt)
	}
}

func newShadowTestMailboxService(
	t *testing.T,
	ctx context.Context,
) (baldastate.Provider, *MailboxService, *recordingWakeBus) {
	t.Helper()

	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	bus := &recordingWakeBus{}
	service := &MailboxService{
		store:   provider.Swarm(),
		bus:     bus,
		cfg:     Config{Enabled: true, Mode: ModeShadow, Shadow: ShadowConfig{Enabled: true}},
		metrics: NewShadowMetrics(),
	}
	return provider, service, bus
}

func newMailboxTestService(
	t *testing.T,
	ctx context.Context,
	queue QueueConfig,
) (baldastate.Provider, *MailboxService, *recordingWakeBus) {
	t.Helper()

	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	bus := &recordingWakeBus{}
	service := &MailboxService{
		store:   provider.Swarm(),
		bus:     bus,
		cfg:     Config{Enabled: true, Mode: ModeMailbox, Queue: queue},
		metrics: NewShadowMetrics(),
	}
	return provider, service, bus
}

func shadowTestEnvelope(id string, to ActorAddress) Envelope {
	return Envelope{
		ID:          id,
		Namespace:   NamespaceHumanInbound,
		Kind:        KindMessage,
		From:        ActorAddress{Target: "test", Key: "source"},
		To:          to,
		SessionID:   to.Key,
		PayloadJSON: `{"ok":true}`,
	}
}

func sessionTextEnvelope(id string, sessionID string, text string) Envelope {
	env := shadowTestEnvelope(id, ActorAddress{Target: ActorTypeSession, Key: sessionID})
	env.Namespace = NamespaceHumanInbound
	env.PayloadJSON = fmt.Sprintf(`{"text":%q,"locator":{"SessionID":%q},"deliver":true,"source":"test"}`, text, sessionID)
	return env
}
