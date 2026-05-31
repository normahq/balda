package handlers

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/normahq/balda/internal/apps/balda/actors"
	"github.com/normahq/balda/internal/apps/balda/auth"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

const (
	defaultSchedulerPollInterval = 2 * time.Second
	defaultSchedulerDueBatchSize = 100
	defaultSchedulerMaxRetries   = 3
)

// ConfiguredScheduledTask defines a startup-managed recurring task.
type ConfiguredScheduledTask struct {
	ID       string
	Cron     string
	Target   string
	Key      string
	Content  string
	ReportTo *ConfiguredScheduledTaskTarget
}

// ConfiguredScheduledTaskTarget is a configured scheduler envelope address.
type ConfiguredScheduledTaskTarget struct {
	Target string
	Key    string
}

// ScheduledTaskSchedulerConfig controls startup task reconciliation.
type ScheduledTaskSchedulerConfig struct {
	Tasks []ConfiguredScheduledTask
}

type scheduledTaskSchedulerParams struct {
	fx.In

	LC            fx.Lifecycle
	StateProvider baldastate.Provider
	Dispatcher    swarm.ActorDispatcher
	OwnerStore    *auth.OwnerStore
	Logger        zerolog.Logger
	Config        ScheduledTaskSchedulerConfig
}

// ScheduledTaskScheduler publishes due locator-bound recurring tasks as durable task commands.
type ScheduledTaskScheduler struct {
	taskStore  baldastate.ScheduledTaskStore
	dispatcher swarm.ActorDispatcher
	owner      *auth.OwnerStore
	logger     zerolog.Logger
	config     ScheduledTaskSchedulerConfig

	pollInterval time.Duration
	dueBatchSize int
	now          func() time.Time

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (s *ScheduledTaskScheduler) start() {
	if s.cancel != nil {
		return
	}

	runCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		ticker := time.NewTicker(s.pollInterval)
		defer ticker.Stop()

		for {
			if err := s.dispatchDue(runCtx, s.now().UTC()); err != nil {
				s.logger.Warn().Err(err).Msg("failed to dispatch due tasks")
			}

			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (s *ScheduledTaskScheduler) stop(ctx context.Context) error {
	if s.cancel == nil {
		return nil
	}
	s.cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.wg.Wait()
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ScheduledTaskScheduler) reconcileConfiguredTasks(ctx context.Context) error {
	desired := make(map[string]struct{}, len(s.config.Tasks))
	now := s.now().UTC()

	for _, task := range s.config.Tasks {
		target, err := resolveEnvelopeTarget(ctx, s.owner, envelopeTarget{Target: task.Target, Key: task.Key})
		if err != nil {
			return fmt.Errorf("resolve scheduler task %q target: %w", task.ID, err)
		}
		var reportTo *resolvedEnvelopeTarget
		if task.ReportTo != nil {
			resolved, err := resolveEnvelopeTarget(ctx, s.owner, envelopeTarget{Target: task.ReportTo.Target, Key: task.ReportTo.Key})
			if err != nil {
				return fmt.Errorf("resolve scheduler task %q report_to: %w", task.ID, err)
			}
			reportTo = &resolved
		}
		nextRunAt, err := nextRunAtFromSpec(task.Cron, now)
		if err != nil {
			return fmt.Errorf("compute next run for scheduler task %q: %w", task.ID, err)
		}
		record := baldastate.ScheduledTaskRecord{
			TaskID:       task.ID,
			SessionID:    target.Locator.SessionID,
			ChannelType:  target.Locator.ChannelType,
			AddressKey:   target.Locator.AddressKey,
			AddressJSON:  target.Locator.AddressJSON,
			Content:      task.Content,
			ScheduleSpec: task.Cron,
			Timezone:     "UTC",
			Status:       baldastate.ScheduledTaskStatusActive,
			MaxRetries:   defaultSchedulerMaxRetries,
			RetryCount:   0,
			NextRunAt:    nextRunAt,
		}
		if reportTo != nil {
			record.ReportToEnabled = true
			record.ReportToSessionID = reportTo.Locator.SessionID
			record.ReportToChannelType = reportTo.Locator.ChannelType
			record.ReportToAddressKey = reportTo.Locator.AddressKey
			record.ReportToAddressJSON = reportTo.Locator.AddressJSON
		}
		if err := s.taskStore.Upsert(ctx, record); err != nil {
			return fmt.Errorf("upsert scheduler task %q: %w", task.ID, err)
		}
		desired[task.ID] = struct{}{}
	}

	currentTasks, err := s.taskStore.List(ctx)
	if err != nil {
		return fmt.Errorf("list persisted scheduler tasks: %w", err)
	}
	for _, existing := range currentTasks {
		taskID := strings.TrimSpace(existing.TaskID)
		if _, ok := desired[taskID]; ok {
			continue
		}
		if err := s.taskStore.Delete(ctx, taskID); err != nil {
			return fmt.Errorf("delete unmanaged scheduler task %q: %w", taskID, err)
		}
	}

	return nil
}

func (s *ScheduledTaskScheduler) dispatchDue(ctx context.Context, now time.Time) error {
	due, err := s.taskStore.ListDue(ctx, now, s.dueBatchSize)
	if err != nil {
		return fmt.Errorf("list due tasks: %w", err)
	}

	for _, task := range due {
		if err := s.dispatchTask(ctx, task, now); err != nil {
			s.logger.Warn().Err(err).Str("task_id", task.TaskID).Msg("failed to dispatch task")
		}
	}
	return nil
}

func (s *ScheduledTaskScheduler) dispatchTask(ctx context.Context, task baldastate.ScheduledTaskRecord, now time.Time) error {
	taskID := strings.TrimSpace(task.TaskID)
	if taskID == "" {
		return fmt.Errorf("task id is required")
	}

	current, ok, err := s.taskStore.GetByID(ctx, taskID)
	if err != nil {
		return fmt.Errorf("load scheduled task %q: %w", taskID, err)
	}
	if !ok {
		return fmt.Errorf("scheduled task %q not found", taskID)
	}
	if strings.TrimSpace(current.Status) != baldastate.ScheduledTaskStatusActive {
		return nil
	}
	if current.NextRunAt.After(now.UTC()) {
		return nil
	}

	dispatchKey := dispatchAttemptKey(taskID, current.NextRunAt)
	if strings.TrimSpace(current.LastDispatchKey) == dispatchKey {
		return nil
	}

	target, err := s.resolveScheduledTaskTarget(ctx, current)
	if err != nil {
		return s.markFailure(ctx, taskID, fmt.Errorf("resolve scheduler target: %w", err))
	}
	locator := target.Locator

	nextRunAt, err := nextRunAtFromSpec(current.ScheduleSpec, now)
	if err != nil {
		return s.markFailure(ctx, taskID, fmt.Errorf("invalid schedule_spec: %w", err))
	}

	content := strings.TrimSpace(current.Content)
	if err := s.dispatchScheduledTaskTask(ctx, current, target, content, dispatchKey); err != nil {
		return err
	}

	// Mark the slot only after durable command dispatch succeeds.
	current.LastDispatchKey = dispatchKey
	current.LastError = ""
	current.Status = baldastate.ScheduledTaskStatusActive
	current.NextRunAt = nextRunAt
	current.SessionID = locator.SessionID
	current.ChannelType = locator.ChannelType
	current.AddressKey = locator.AddressKey
	current.AddressJSON = locator.AddressJSON
	if err := s.taskStore.Upsert(ctx, current); err != nil {
		return fmt.Errorf("update scheduled task %q after publish: %w", taskID, err)
	}

	return nil
}

func (s *ScheduledTaskScheduler) resolveScheduledTaskTarget(ctx context.Context, task baldastate.ScheduledTaskRecord) (resolvedEnvelopeTarget, error) {
	locator, err := baldasession.NewSessionLocator(task.ChannelType, task.AddressKey, task.AddressJSON, task.SessionID)
	if err != nil {
		return resolveEnvelopeTarget(ctx, s.owner, ownerEnvelopeTarget())
	}
	target := resolvedEnvelopeTarget{Locator: locator}
	if address, ok, decodeErr := baldatelegram.DecodeLocator(locator); decodeErr != nil {
		return resolvedEnvelopeTarget{}, decodeErr
	} else if ok {
		target.TopicID = address.TopicID
	}
	if s.owner != nil {
		if owner := s.owner.GetOwner(); owner != nil && owner.UserID != 0 {
			target.UserID = baldatelegram.UserID(owner.UserID)
		}
	}
	return target, nil
}

func (s *ScheduledTaskScheduler) dispatchScheduledTaskTask(
	ctx context.Context,
	task baldastate.ScheduledTaskRecord,
	target resolvedEnvelopeTarget,
	content string,
	dispatchKey string,
) error {
	reportTo, err := scheduledTaskReportTarget(task)
	if err != nil {
		return s.markFailure(ctx, task.TaskID, err)
	}
	env, err := actors.ScheduledTaskEnvelope(task.TaskID, content, target.Locator, reportTo, target.UserID, target.TopicID, dispatchKey)
	if err != nil {
		return s.markFailure(ctx, task.TaskID, err)
	}
	if _, err := s.dispatcher.Dispatch(ctx, env); err != nil {
		return s.markFailure(ctx, task.TaskID, fmt.Errorf("publish scheduled task command: %w", err))
	}
	return nil
}

func scheduledTaskReportTarget(task baldastate.ScheduledTaskRecord) (*baldasession.SessionLocator, error) {
	if !task.ReportToEnabled {
		return nil, nil
	}
	locator, err := baldasession.NewSessionLocator(task.ReportToChannelType, task.ReportToAddressKey, task.ReportToAddressJSON, task.ReportToSessionID)
	if err != nil {
		return nil, fmt.Errorf("resolve report_to locator: %w", err)
	}
	return &locator, nil
}

func (s *ScheduledTaskScheduler) MarkSuccess(ctx context.Context, taskID string) error {
	task, ok, err := s.taskStore.GetByID(ctx, taskID)
	if err != nil {
		return fmt.Errorf("load scheduled task %q: %w", taskID, err)
	}
	if !ok {
		return fmt.Errorf("scheduled task %q not found", taskID)
	}
	task.LastRunAt = s.now().UTC()
	task.LastError = ""
	task.RetryCount = 0
	task.Status = baldastate.ScheduledTaskStatusActive
	if err := s.taskStore.Upsert(ctx, task); err != nil {
		return fmt.Errorf("upsert scheduled task %q: %w", taskID, err)
	}
	return nil
}

func (s *ScheduledTaskScheduler) markFailure(ctx context.Context, taskID string, cause error) error {
	task, ok, err := s.taskStore.GetByID(ctx, taskID)
	if err != nil {
		return fmt.Errorf("load scheduled task %q: %w", taskID, err)
	}
	if !ok {
		return fmt.Errorf("scheduled task %q not found", taskID)
	}
	now := s.now().UTC()
	task.RetryCount++
	task.LastError = strings.TrimSpace(cause.Error())
	task.LastRunAt = now

	maxRetries := task.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	if task.RetryCount > maxRetries {
		task.Status = baldastate.ScheduledTaskStatusPaused
	} else {
		task.Status = baldastate.ScheduledTaskStatusActive
		retryCount := task.RetryCount
		if retryCount < 1 {
			retryCount = 1
		}
		delay := time.Duration(retryCount) * time.Second
		if delay > 60*time.Second {
			delay = 60 * time.Second
		}
		task.NextRunAt = now.Add(delay)
	}
	if err := s.taskStore.Upsert(ctx, task); err != nil {
		return fmt.Errorf("upsert scheduled task %q: %w", taskID, err)
	}
	return cause
}

func (s *ScheduledTaskScheduler) RecordExecutionFailure(ctx context.Context, taskID string, cause error) error {
	task, ok, err := s.taskStore.GetByID(ctx, taskID)
	if err != nil {
		return fmt.Errorf("load scheduled task %q: %w", taskID, err)
	}
	if !ok {
		return fmt.Errorf("scheduled task %q not found", taskID)
	}
	now := s.now().UTC()
	task.LastError = strings.TrimSpace(cause.Error())
	task.LastRunAt = now
	task.Status = baldastate.ScheduledTaskStatusActive
	if err := s.taskStore.Upsert(ctx, task); err != nil {
		return fmt.Errorf("upsert scheduled task %q execution failure: %w", taskID, err)
	}
	return cause
}

func normalizeScheduledTaskSchedulerConfig(raw ScheduledTaskSchedulerConfig) (ScheduledTaskSchedulerConfig, error) {
	cfg := ScheduledTaskSchedulerConfig{
		Tasks: make([]ConfiguredScheduledTask, 0, len(raw.Tasks)),
	}

	seenTaskIDs := make(map[string]struct{}, len(raw.Tasks))
	for idx, rawTask := range raw.Tasks {
		taskID := strings.TrimSpace(rawTask.ID)
		if taskID == "" {
			return ScheduledTaskSchedulerConfig{}, fmt.Errorf("balda.scheduler.tasks[%d].id is required", idx)
		}
		if _, exists := seenTaskIDs[taskID]; exists {
			return ScheduledTaskSchedulerConfig{}, fmt.Errorf("duplicate balda.scheduler.tasks id %q", taskID)
		}
		seenTaskIDs[taskID] = struct{}{}

		cronSpec := strings.TrimSpace(rawTask.Cron)
		if cronSpec == "" {
			return ScheduledTaskSchedulerConfig{}, fmt.Errorf("balda.scheduler.tasks[%d].cron is required", idx)
		}
		if _, err := parseScheduleSpec(cronSpec); err != nil {
			return ScheduledTaskSchedulerConfig{}, fmt.Errorf("invalid balda.scheduler.tasks[%d].cron: %w", idx, err)
		}

		target := strings.TrimSpace(rawTask.Target)
		if target == "" {
			return ScheduledTaskSchedulerConfig{}, fmt.Errorf("balda.scheduler.tasks[%d].envelope.target is required", idx)
		}
		key := strings.TrimSpace(rawTask.Key)
		if key == "" {
			return ScheduledTaskSchedulerConfig{}, fmt.Errorf("balda.scheduler.tasks[%d].envelope.key is required", idx)
		}
		content := strings.TrimSpace(rawTask.Content)
		if content == "" {
			return ScheduledTaskSchedulerConfig{}, fmt.Errorf("balda.scheduler.tasks[%d].envelope.content is required", idx)
		}
		var reportTo *ConfiguredScheduledTaskTarget
		if rawTask.ReportTo != nil {
			reportTo = &ConfiguredScheduledTaskTarget{
				Target: strings.TrimSpace(rawTask.ReportTo.Target),
				Key:    strings.TrimSpace(rawTask.ReportTo.Key),
			}
			if reportTo.Target == "" {
				return ScheduledTaskSchedulerConfig{}, fmt.Errorf("balda.scheduler.tasks[%d].envelope.report_to.target is required", idx)
			}
			if reportTo.Key == "" {
				return ScheduledTaskSchedulerConfig{}, fmt.Errorf("balda.scheduler.tasks[%d].envelope.report_to.key is required", idx)
			}
		}

		cfg.Tasks = append(cfg.Tasks, ConfiguredScheduledTask{
			ID:       taskID,
			Cron:     cronSpec,
			Target:   target,
			Key:      key,
			Content:  content,
			ReportTo: reportTo,
		})
	}

	sort.Slice(cfg.Tasks, func(i, j int) bool {
		return cfg.Tasks[i].ID < cfg.Tasks[j].ID
	})
	return cfg, nil
}

func nextRunAtFromSpec(spec string, now time.Time) (time.Time, error) {
	schedule, err := parseScheduleSpec(spec)
	if err != nil {
		return time.Time{}, err
	}
	nextRunAt := schedule.Next(now.UTC())
	if nextRunAt.IsZero() {
		return time.Time{}, fmt.Errorf("schedule has no next run")
	}
	return nextRunAt.UTC(), nil
}

func parseScheduleSpec(spec string) (cron.Schedule, error) {
	trimmed := strings.TrimSpace(spec)
	if trimmed == "" {
		return nil, fmt.Errorf("schedule spec is required")
	}

	schedule, err := cron.ParseStandard(trimmed)
	if err != nil {
		return nil, fmt.Errorf("unsupported schedule spec %q", spec)
	}
	return schedule, nil
}

func dispatchAttemptKey(taskID string, dueAt time.Time) string {
	return fmt.Sprintf("%s@%s", strings.TrimSpace(taskID), dueAt.UTC().Format(time.RFC3339Nano))
}
