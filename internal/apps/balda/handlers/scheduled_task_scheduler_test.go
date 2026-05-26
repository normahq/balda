package handlers

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/normahq/balda/internal/apps/balda/auth"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/rs/zerolog"
	"google.golang.org/adk/runner"
)

func TestScheduledTaskSchedulerDispatchTask_PublishesCommandAndReschedules(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newSchedulerTaskStore(t)
	locator := baldatelegram.NewLocator(9001, 77)
	now := time.Date(2026, time.May, 14, 12, 0, 0, 0, time.UTC)
	dueAt := now.Add(-time.Second)

	record := baldastate.ScheduledTaskRecord{
		TaskID:       "task-1",
		SessionID:    locator.SessionID,
		ChannelType:  locator.ChannelType,
		AddressKey:   locator.AddressKey,
		AddressJSON:  locator.AddressJSON,
		Content:      "summarize repo health",
		ScheduleSpec: "@every 2s",
		Status:       baldastate.ScheduledTaskStatusActive,
		MaxRetries:   3,
		NextRunAt:    dueAt,
	}
	if err := store.Upsert(ctx, record); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	channel := &fakeSchedulerChannel{}
	bus := &recordingHandlerCommandBus{}
	scheduler := newSchedulerForTest(t, store, bus, channel, now)

	if err := scheduler.dispatchTask(ctx, record, now); err != nil {
		t.Fatalf("dispatchTask() error = %v", err)
	}
	if got := len(bus.commands); got != 1 {
		t.Fatalf("published commands = %d, want 1", got)
	}
	command := bus.commands[0]
	if got, want := command.Namespace, swarm.NamespaceScheduleInbound; got != want {
		t.Fatalf("namespace = %q, want %q", got, want)
	}
	if got, want := command.Kind, swarm.KindScheduledTask; got != want {
		t.Fatalf("kind = %q, want %q", got, want)
	}
	if command.ID == "" {
		t.Fatal("command id is empty")
	}
	if got, want := command.SessionID, locator.SessionID; got != want {
		t.Fatalf("session_id = %q, want %q", got, want)
	}
	if got, want := command.DedupeKey, dispatchAttemptKey(record.TaskID, dueAt); got != want {
		t.Fatalf("dedupe_key = %q, want %q", got, want)
	}

	updated, ok, err := store.GetByID(ctx, record.TaskID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if !ok {
		t.Fatal("GetByID() = not found, want found")
	}
	if got, want := updated.NextRunAt, now.Add(2*time.Second); !got.Equal(want) {
		t.Fatalf("NextRunAt = %s, want %s", got, want)
	}
	wantKey := dispatchAttemptKey(record.TaskID, dueAt)
	if got := updated.LastDispatchKey; got != wantKey {
		t.Fatalf("LastDispatchKey = %q, want %q", got, wantKey)
	}
	if got := updated.RetryCount; got != 0 {
		t.Fatalf("RetryCount = %d, want 0", got)
	}
}

func TestScheduledTaskSchedulerDispatchTask_PublishesWithoutRestoringSession(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newSchedulerTaskStore(t)
	locator := baldatelegram.NewLocator(9001, 88)
	now := time.Date(2026, time.May, 14, 12, 30, 0, 0, time.UTC)

	record := baldastate.ScheduledTaskRecord{
		TaskID:       "task-restore",
		SessionID:    locator.SessionID,
		ChannelType:  locator.ChannelType,
		AddressKey:   locator.AddressKey,
		AddressJSON:  locator.AddressJSON,
		Content:      "restore and run",
		ScheduleSpec: "@every 10s",
		Status:       baldastate.ScheduledTaskStatusActive,
		NextRunAt:    now.Add(-2 * time.Second),
	}
	if err := store.Upsert(ctx, record); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	bus := &recordingHandlerCommandBus{}
	scheduler := newSchedulerForTest(t, store, bus, &fakeSchedulerChannel{}, now)

	if err := scheduler.dispatchTask(ctx, record, now); err != nil {
		t.Fatalf("dispatchTask() error = %v", err)
	}
	if got := len(bus.commands); got != 1 {
		t.Fatalf("published commands = %d, want 1", got)
	}
}

func TestScheduledTaskSchedulerDispatchTask_IdempotentForSameDueSlot(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newSchedulerTaskStore(t)
	locator := baldatelegram.NewLocator(9001, 99)
	now := time.Date(2026, time.May, 14, 13, 0, 0, 0, time.UTC)
	dueAt := now.Add(-time.Second)

	record := baldastate.ScheduledTaskRecord{
		TaskID:       "task-idempotent",
		SessionID:    locator.SessionID,
		ChannelType:  locator.ChannelType,
		AddressKey:   locator.AddressKey,
		AddressJSON:  locator.AddressJSON,
		Content:      "same slot should dispatch once",
		ScheduleSpec: "@every 5s",
		Status:       baldastate.ScheduledTaskStatusActive,
		NextRunAt:    dueAt,
	}
	if err := store.Upsert(ctx, record); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	bus := &recordingHandlerCommandBus{}
	scheduler := newSchedulerForTest(t, store, bus, &fakeSchedulerChannel{}, now)

	if err := scheduler.dispatchTask(ctx, record, now); err != nil {
		t.Fatalf("dispatchTask() first call error = %v", err)
	}
	if err := scheduler.dispatchTask(ctx, record, now); err != nil {
		t.Fatalf("dispatchTask() second call error = %v", err)
	}
	if got := len(bus.commands); got != 1 {
		t.Fatalf("published commands after duplicate dispatch = %d, want 1", got)
	}

	updated, ok, err := store.GetByID(ctx, record.TaskID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if !ok {
		t.Fatal("GetByID() = not found, want found")
	}
	if got, want := updated.LastDispatchKey, dispatchAttemptKey(record.TaskID, dueAt); got != want {
		t.Fatalf("LastDispatchKey = %q, want %q", got, want)
	}
}

func TestScheduledTaskSchedulerMarkFailure_RetryThenPause(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newSchedulerTaskStore(t)
	locator := baldatelegram.NewLocator(9001, 101)
	start := time.Date(2026, time.May, 14, 14, 0, 0, 0, time.UTC)

	record := baldastate.ScheduledTaskRecord{
		TaskID:       "task-fail",
		SessionID:    locator.SessionID,
		ChannelType:  locator.ChannelType,
		AddressKey:   locator.AddressKey,
		AddressJSON:  locator.AddressJSON,
		Content:      "will fail",
		ScheduleSpec: "@every 1m",
		Status:       baldastate.ScheduledTaskStatusActive,
		MaxRetries:   1,
		NextRunAt:    start,
	}
	if err := store.Upsert(ctx, record); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	clock := &schedulerClock{now: start}
	scheduler := &ScheduledTaskScheduler{
		taskStore: store,
		logger:    zerolog.Nop(),
		now:       clock.Now,
	}

	firstCause := errors.New("boom one")
	if err := scheduler.markFailure(ctx, record.TaskID, firstCause); !errors.Is(err, firstCause) {
		t.Fatalf("markFailure() error = %v, want %v", err, firstCause)
	}
	afterFirst, ok, err := store.GetByID(ctx, record.TaskID)
	if err != nil {
		t.Fatalf("GetByID() after first failure error = %v", err)
	}
	if !ok {
		t.Fatal("GetByID() after first failure = not found")
	}
	if got := afterFirst.RetryCount; got != 1 {
		t.Fatalf("RetryCount after first failure = %d, want 1", got)
	}
	if got := afterFirst.Status; got != baldastate.ScheduledTaskStatusActive {
		t.Fatalf("Status after first failure = %q, want active", got)
	}
	if got, want := afterFirst.NextRunAt, start.Add(time.Second); !got.Equal(want) {
		t.Fatalf("NextRunAt after first failure = %s, want %s", got, want)
	}
	if !strings.Contains(afterFirst.LastError, "boom one") {
		t.Fatalf("LastError after first failure = %q, want boom one", afterFirst.LastError)
	}

	clock.now = start.Add(10 * time.Second)
	secondCause := errors.New("boom two")
	if err := scheduler.markFailure(ctx, record.TaskID, secondCause); !errors.Is(err, secondCause) {
		t.Fatalf("markFailure() second error = %v, want %v", err, secondCause)
	}
	afterSecond, ok, err := store.GetByID(ctx, record.TaskID)
	if err != nil {
		t.Fatalf("GetByID() after second failure error = %v", err)
	}
	if !ok {
		t.Fatal("GetByID() after second failure = not found")
	}
	if got := afterSecond.RetryCount; got != 2 {
		t.Fatalf("RetryCount after second failure = %d, want 2", got)
	}
	if got := afterSecond.Status; got != baldastate.ScheduledTaskStatusPaused {
		t.Fatalf("Status after second failure = %q, want paused", got)
	}
}

func TestScheduledTaskSchedulerRecordExecutionFailureDoesNotScheduleRetry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newSchedulerTaskStore(t)
	locator := baldatelegram.NewLocator(9001, 102)
	start := time.Date(2026, time.May, 14, 14, 30, 0, 0, time.UTC)
	nextRun := start.Add(30 * time.Minute)

	record := baldastate.ScheduledTaskRecord{
		TaskID:       "task-exec-fail",
		SessionID:    locator.SessionID,
		ChannelType:  locator.ChannelType,
		AddressKey:   locator.AddressKey,
		AddressJSON:  locator.AddressJSON,
		Content:      "will fail in actor",
		ScheduleSpec: "@every 1h",
		Status:       baldastate.ScheduledTaskStatusActive,
		MaxRetries:   1,
		RetryCount:   1,
		NextRunAt:    nextRun,
	}
	if err := store.Upsert(ctx, record); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	clock := &schedulerClock{now: start}
	scheduler := &ScheduledTaskScheduler{
		taskStore: store,
		logger:    zerolog.Nop(),
		now:       clock.Now,
	}

	cause := errors.New("actor execution failed")
	if err := scheduler.recordExecutionFailure(ctx, record.TaskID, cause); !errors.Is(err, cause) {
		t.Fatalf("recordExecutionFailure() error = %v, want %v", err, cause)
	}
	got, ok, err := store.GetByID(ctx, record.TaskID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if !ok {
		t.Fatal("GetByID() = not found, want found")
	}
	if got.RetryCount != record.RetryCount {
		t.Fatalf("RetryCount = %d, want unchanged %d", got.RetryCount, record.RetryCount)
	}
	if !got.NextRunAt.Equal(nextRun) {
		t.Fatalf("NextRunAt = %s, want unchanged %s", got.NextRunAt, nextRun)
	}
	if got.Status != baldastate.ScheduledTaskStatusActive || !strings.Contains(got.LastError, "actor execution failed") {
		t.Fatalf("task after execution failure = %+v, want active with last error", got)
	}
}

func TestScheduledTaskSchedulerReconcileConfiguredTasks_UpsertsAndDeletes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newSchedulerTaskStore(t)
	now := time.Date(2026, time.May, 14, 16, 0, 0, 0, time.UTC)
	locator := baldatelegram.NewLocator(9001, 222)

	stale := baldastate.ScheduledTaskRecord{
		TaskID:       "stale-task",
		SessionID:    locator.SessionID,
		ChannelType:  locator.ChannelType,
		AddressKey:   locator.AddressKey,
		AddressJSON:  locator.AddressJSON,
		Content:      "remove me",
		ScheduleSpec: "@every 10s",
		Status:       baldastate.ScheduledTaskStatusActive,
		MaxRetries:   3,
		NextRunAt:    now.Add(10 * time.Second),
	}
	if err := store.Upsert(ctx, stale); err != nil {
		t.Fatalf("Upsert(stale) error = %v", err)
	}

	scheduler := &ScheduledTaskScheduler{
		taskStore: store,
		owner:     newOwnerStoreForTest(t, 101, 9001),
		logger:    zerolog.Nop(),
		now:       func() time.Time { return now },
		config: ScheduledTaskSchedulerConfig{
			Tasks: []ConfiguredScheduledTask{
				{
					ID:      "managed-task",
					Cron:    "@every 2s",
					Target:  "alias",
					Key:     "owner",
					Content: "review queue",
					ReportTo: &ConfiguredScheduledTaskTarget{
						Target: "alias",
						Key:    "owner",
					},
				},
			},
		},
	}

	if err := scheduler.reconcileConfiguredTasks(ctx); err != nil {
		t.Fatalf("reconcileConfiguredTasks() error = %v", err)
	}

	managed, ok, err := store.GetByID(ctx, "managed-task")
	if err != nil {
		t.Fatalf("GetByID(managed) error = %v", err)
	}
	if !ok {
		t.Fatal("GetByID(managed) = not found, want found")
	}
	if got, want := managed.ScheduleSpec, "@every 2s"; got != want {
		t.Fatalf("ScheduleSpec = %q, want %q", got, want)
	}
	if got, want := managed.Content, "review queue"; got != want {
		t.Fatalf("Content = %q, want %q", got, want)
	}
	if !managed.ReportToEnabled {
		t.Fatal("ReportToEnabled = false, want true")
	}
	if got, want := managed.ReportToAddressKey, "9001:0"; got != want {
		t.Fatalf("ReportToAddressKey = %q, want %q", got, want)
	}
	if got, want := managed.Status, baldastate.ScheduledTaskStatusActive; got != want {
		t.Fatalf("Status = %q, want %q", got, want)
	}
	if got, want := managed.MaxRetries, defaultSchedulerMaxRetries; got != want {
		t.Fatalf("MaxRetries = %d, want %d", got, want)
	}
	if got, want := managed.NextRunAt, now.Add(2*time.Second); !got.Equal(want) {
		t.Fatalf("NextRunAt = %s, want %s", got, want)
	}

	_, staleExists, err := store.GetByID(ctx, stale.TaskID)
	if err != nil {
		t.Fatalf("GetByID(stale) error = %v", err)
	}
	if staleExists {
		t.Fatal("stale task still exists after reconcile")
	}
}

func TestNextRunAtFromSpec_ParsesCronExpression(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.May, 14, 16, 3, 10, 0, time.UTC)
	nextRunAt, err := nextRunAtFromSpec("*/5 * * * *", now)
	if err != nil {
		t.Fatalf("nextRunAtFromSpec() error = %v", err)
	}
	want := time.Date(2026, time.May, 14, 16, 5, 0, 0, time.UTC)
	if !nextRunAt.Equal(want) {
		t.Fatalf("nextRunAt = %s, want %s", nextRunAt, want)
	}
}

func TestNormalizeScheduledTaskSchedulerConfig_RequiresEnvelopeTarget(t *testing.T) {
	t.Parallel()

	_, err := normalizeScheduledTaskSchedulerConfig(ScheduledTaskSchedulerConfig{
		Tasks: []ConfiguredScheduledTask{
			{
				ID:      "task-1",
				Cron:    "@every 1m",
				Content: "check",
			},
		},
	})
	if err == nil {
		t.Fatal("normalizeScheduledTaskSchedulerConfig() error = nil, want missing target")
	}
	if !strings.Contains(err.Error(), "envelope.target") {
		t.Fatalf("normalizeScheduledTaskSchedulerConfig() error = %v, want envelope.target", err)
	}
}

func TestNormalizeScheduledTaskSchedulerConfig_TrimsEnvelope(t *testing.T) {
	t.Parallel()

	got, err := normalizeScheduledTaskSchedulerConfig(ScheduledTaskSchedulerConfig{
		Tasks: []ConfiguredScheduledTask{
			{
				ID:      " task-1 ",
				Cron:    " @every 1m ",
				Target:  " alias ",
				Key:     " owner ",
				Content: " check ",
				ReportTo: &ConfiguredScheduledTaskTarget{
					Target: " alias ",
					Key:    " owner ",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("normalizeScheduledTaskSchedulerConfig() error = %v", err)
	}
	if len(got.Tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(got.Tasks))
	}
	task := got.Tasks[0]
	if task.ID != "task-1" || task.Target != "alias" || task.Key != "owner" || task.Content != "check" {
		t.Fatalf("task = %+v, want trimmed envelope", task)
	}
	if task.ReportTo == nil || task.ReportTo.Target != "alias" || task.ReportTo.Key != "owner" {
		t.Fatalf("report_to = %+v, want trimmed alias/owner", task.ReportTo)
	}
}

func TestScheduledTaskSchedulerExecuteTaskTurn_SuccessResetsRetryWithoutReply(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newSchedulerTaskStore(t)
	locator := baldatelegram.NewLocator(9001, 111)
	now := time.Date(2026, time.May, 14, 15, 0, 0, 0, time.UTC)

	record := baldastate.ScheduledTaskRecord{
		TaskID:       "task-success",
		SessionID:    locator.SessionID,
		ChannelType:  locator.ChannelType,
		AddressKey:   locator.AddressKey,
		AddressJSON:  locator.AddressJSON,
		Content:      "run once",
		ScheduleSpec: "@every 30s",
		Status:       baldastate.ScheduledTaskStatusActive,
		RetryCount:   2,
		NextRunAt:    now.Add(30 * time.Second),
	}
	if err := store.Upsert(ctx, record); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	adkRunner, adkSessionID := newBaldaRunTurnTestRunner(t)
	ts := newSchedulerTopicSession(t, locator, "tg-101", adkSessionID, adkRunner)
	channel := &fakeSchedulerChannel{}
	scheduler := &ScheduledTaskScheduler{
		taskStore: store,
		channel:   channel,
		logger:    zerolog.Nop(),
		now: func() time.Time {
			return now
		},
	}

	if err := scheduler.executeTaskTurn(ctx, locator, record.TaskID, "ship it", ts); err != nil {
		t.Fatalf("executeTaskTurn() error = %v", err)
	}
	if got := len(channel.agentReplies); got != 0 {
		t.Fatalf("agent reply sends = %d, want 0", got)
	}
	if got := len(channel.plainTexts); got != 0 {
		t.Fatalf("plain sends = %d, want 0", got)
	}

	updated, ok, err := store.GetByID(ctx, record.TaskID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if !ok {
		t.Fatal("GetByID() = not found")
	}
	if got := updated.RetryCount; got != 0 {
		t.Fatalf("RetryCount after success = %d, want 0", got)
	}
	if got := updated.Status; got != baldastate.ScheduledTaskStatusActive {
		t.Fatalf("Status after success = %q, want active", got)
	}
	if got := updated.LastRunAt; !got.Equal(now) {
		t.Fatalf("LastRunAt after success = %s, want %s", got, now)
	}
}

func TestScheduledTaskSchedulerExecuteTaskTurn_CanceledContextDoesNotMarkFailure(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	store := newSchedulerTaskStore(t)
	locator := baldatelegram.NewLocator(9001, 112)
	now := time.Date(2026, time.May, 14, 15, 30, 0, 0, time.UTC)

	record := baldastate.ScheduledTaskRecord{
		TaskID:       "task-canceled",
		SessionID:    locator.SessionID,
		ChannelType:  locator.ChannelType,
		AddressKey:   locator.AddressKey,
		AddressJSON:  locator.AddressJSON,
		Content:      "run once",
		ScheduleSpec: "@every 30s",
		Status:       baldastate.ScheduledTaskStatusActive,
		MaxRetries:   3,
		NextRunAt:    now.Add(30 * time.Second),
	}
	if err := store.Upsert(context.Background(), record); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	ts := newSchedulerTopicSession(t, locator, "tg-101", "adk-canceled", nil)
	channel := &fakeSchedulerChannel{}
	scheduler := &ScheduledTaskScheduler{
		taskStore: store,
		channel:   channel,
		logger:    zerolog.Nop(),
		now: func() time.Time {
			return now
		},
	}

	if err := scheduler.executeTaskTurn(ctx, locator, record.TaskID, "ship it", ts); err != nil {
		t.Fatalf("executeTaskTurn() error = %v, want nil for cancellation", err)
	}
	if got := len(channel.plainTexts); got != 0 {
		t.Fatalf("plain failure messages = %d, want 0", got)
	}
	if got := len(channel.agentReplies); got != 0 {
		t.Fatalf("agent replies = %d, want 0", got)
	}

	updated, ok, err := store.GetByID(context.Background(), record.TaskID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if !ok {
		t.Fatal("GetByID() = not found")
	}
	if got := updated.RetryCount; got != 0 {
		t.Fatalf("RetryCount after cancellation = %d, want 0", got)
	}
	if got := updated.LastError; got != "" {
		t.Fatalf("LastError after cancellation = %q, want empty", got)
	}
	if !updated.LastRunAt.IsZero() {
		t.Fatalf("LastRunAt after cancellation = %s, want zero", updated.LastRunAt)
	}
	if got := updated.Status; got != baldastate.ScheduledTaskStatusActive {
		t.Fatalf("Status after cancellation = %q, want active", got)
	}
}

type fakeSchedulerChannel struct {
	plainTexts   []string
	agentReplies []string
}

func (f *fakeSchedulerChannel) SendPlain(_ context.Context, _ baldasession.SessionLocator, text string) error {
	f.plainTexts = append(f.plainTexts, text)
	return nil
}

func (f *fakeSchedulerChannel) SendAgentReply(_ context.Context, _ baldasession.SessionLocator, text string) error {
	f.agentReplies = append(f.agentReplies, text)
	return nil
}

type schedulerClock struct {
	now time.Time
}

func (c *schedulerClock) Now() time.Time {
	return c.now
}

func newSchedulerForTest(
	t *testing.T,
	store baldastate.ScheduledTaskStore,
	bus *recordingHandlerCommandBus,
	channel schedulerChannel,
	now time.Time,
) *ScheduledTaskScheduler {
	t.Helper()
	if bus == nil {
		bus = &recordingHandlerCommandBus{}
	}
	return &ScheduledTaskScheduler{
		taskStore:    store,
		coordinator:  swarm.NewCoordinator(bus, swarm.Config{Enabled: true}),
		channel:      channel,
		owner:        newOwnerStoreForTest(t, 101, 9001),
		logger:       zerolog.Nop(),
		pollInterval: defaultSchedulerPollInterval,
		dueBatchSize: defaultSchedulerDueBatchSize,
		now:          func() time.Time { return now },
	}
}

func newOwnerStoreForTest(t *testing.T, userID int64, chatID int64) *auth.OwnerStore {
	t.Helper()

	store, err := auth.NewOwnerStore(&fakeOwnerKVStore{})
	if err != nil {
		t.Fatalf("NewOwnerStore() error = %v", err)
	}
	if _, err := store.RegisterOwner(userID, chatID, "owner", "Owner", "", false); err != nil {
		t.Fatalf("RegisterOwner() error = %v", err)
	}
	return store
}

func newSchedulerTaskStore(t *testing.T) baldastate.ScheduledTaskStore {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "state.db")
	provider, err := baldastate.NewSQLiteProvider(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() {
		_ = provider.Close()
	})
	return provider.ScheduledTasks()
}

func newSchedulerTopicSession(
	t *testing.T,
	locator baldasession.SessionLocator,
	userID string,
	agentSessionID string,
	adkRunner *runner.Runner,
) *baldasession.TopicSession {
	t.Helper()

	ts := &baldasession.TopicSession{}
	setUnexportedField(t, ts, "sessionID", locator.SessionID)
	setUnexportedField(t, ts, "locator", locator)
	setUnexportedField(t, ts, "userID", userID)
	setUnexportedField(t, ts, "agentSessionID", agentSessionID)
	setUnexportedField(t, ts, "runner", adkRunner)
	return ts
}
