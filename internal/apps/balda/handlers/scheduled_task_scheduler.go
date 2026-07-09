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
	actortransport "github.com/normahq/balda/pkg/actorlayer/transport"
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

// ScheduledTaskSchedulerConfig controls startup job reconciliation.
type ScheduledTaskSchedulerConfig struct {
	Jobs []ConfiguredScheduledTask
}

type scheduledTaskSchedulerParams struct {
	fx.In

	LC            fx.Lifecycle
	StateProvider baldastate.Provider
	Dispatcher    actortransport.Dispatcher
	OwnerStore    *auth.OwnerStore
	Logger        zerolog.Logger
	Config        ScheduledTaskSchedulerConfig
}

// ScheduledTaskScheduler publishes due locator-bound recurring tasks as durable job commands.
type ScheduledTaskScheduler struct {
	taskStore  baldastate.ScheduledTaskStore
	dispatcher actortransport.Dispatcher
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
	desired := make(map[string]struct{}, len(s.config.Jobs))
	now := s.now().UTC()

	for _, task := range s.config.Jobs {
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
			JobID:        task.ID,
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
			return fmt.Errorf("upsert scheduler job %q: %w", task.ID, err)
		}
		desired[task.ID] = struct{}{}
	}

	currentTasks, err := s.taskStore.List(ctx)
	if err != nil {
		return fmt.Errorf("list persisted scheduler jobs: %w", err)
	}
	for _, existing := range currentTasks {
		jobID := strings.TrimSpace(existing.JobID)
		if _, ok := desired[jobID]; ok {
			continue
		}
		if err := s.taskStore.Delete(ctx, jobID); err != nil {
			return fmt.Errorf("delete unmanaged scheduler job %q: %w", jobID, err)
		}
	}

	return nil
}

func (s *ScheduledTaskScheduler) dispatchDue(ctx context.Context, now time.Time) error {
	due, err := s.taskStore.ListDue(ctx, now, s.dueBatchSize)
	if err != nil {
		return fmt.Errorf("list due jobs: %w", err)
	}

	for _, task := range due {
		if err := s.dispatchTask(ctx, task, now); err != nil {
			s.logger.Warn().Err(err).Str("job_id", task.JobID).Msg("failed to dispatch job")
		}
	}
	return nil
}

func (s *ScheduledTaskScheduler) dispatchTask(ctx context.Context, task baldastate.ScheduledTaskRecord, now time.Time) error {
	jobID := strings.TrimSpace(task.JobID)
	if jobID == "" {
		return fmt.Errorf("job id is required")
	}

	current, ok, err := s.taskStore.GetByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("load scheduled job %q: %w", jobID, err)
	}
	if !ok {
		return fmt.Errorf("scheduled job %q not found", jobID)
	}
	if strings.TrimSpace(current.Status) != baldastate.ScheduledTaskStatusActive {
		return nil
	}
	if current.NextRunAt.After(now.UTC()) {
		return nil
	}

	dispatchKey := fmt.Sprintf("%s@%s", jobID, current.NextRunAt.UTC().Format(time.RFC3339Nano))
	if strings.TrimSpace(current.LastDispatchKey) == dispatchKey {
		return nil
	}

	target, err := s.resolveScheduledTaskTarget(ctx, current)
	if err != nil {
		return s.markFailure(ctx, jobID, fmt.Errorf("resolve scheduler target: %w", err))
	}
	locator := target.Locator

	nextRunAt, err := nextRunAtFromSpec(current.ScheduleSpec, now)
	if err != nil {
		return s.markFailure(ctx, jobID, fmt.Errorf("invalid schedule_spec: %w", err))
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
		return fmt.Errorf("update scheduled job %q after publish: %w", jobID, err)
	}

	return nil
}

func (s *ScheduledTaskScheduler) resolveScheduledTaskTarget(ctx context.Context, task baldastate.ScheduledTaskRecord) (resolvedEnvelopeTarget, error) {
	locator, err := baldasession.NewSessionLocator(task.ChannelType, task.AddressKey, task.AddressJSON, task.SessionID)
	if err != nil {
		return resolveEnvelopeTarget(ctx, s.owner, envelopeTarget{Target: envelopeTargetAlias, Key: envelopeAliasOwner})
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
	var reportTo *baldasession.SessionLocator
	if task.ReportToEnabled {
		locator, err := baldasession.NewSessionLocator(task.ReportToChannelType, task.ReportToAddressKey, task.ReportToAddressJSON, task.ReportToSessionID)
		if err != nil {
			return s.markFailure(ctx, task.JobID, fmt.Errorf("resolve report_to locator: %w", err))
		}
		reportTo = &locator
	}
	env, err := actors.ScheduledJobEnvelope(task.JobID, content, target.Locator, reportTo, target.UserID, target.TopicID, dispatchKey)
	if err != nil {
		return s.markFailure(ctx, task.JobID, err)
	}
	if _, err := s.dispatcher.Dispatch(ctx, env); err != nil {
		return s.markFailure(ctx, task.JobID, fmt.Errorf("publish scheduled job command: %w", err))
	}
	return nil
}

func (s *ScheduledTaskScheduler) MarkSuccess(ctx context.Context, jobID string) error {
	job, ok, err := s.taskStore.GetByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("load scheduled job %q: %w", jobID, err)
	}
	if !ok {
		return fmt.Errorf("scheduled job %q not found", jobID)
	}
	job.LastRunAt = s.now().UTC()
	job.LastError = ""
	job.RetryCount = 0
	job.Status = baldastate.ScheduledTaskStatusActive
	if err := s.taskStore.Upsert(ctx, job); err != nil {
		return fmt.Errorf("upsert scheduled job %q: %w", jobID, err)
	}
	return nil
}

func (s *ScheduledTaskScheduler) markFailure(ctx context.Context, jobID string, cause error) error {
	job, ok, err := s.taskStore.GetByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("load scheduled job %q: %w", jobID, err)
	}
	if !ok {
		return fmt.Errorf("scheduled job %q not found", jobID)
	}
	now := s.now().UTC()
	job.RetryCount++
	job.LastError = strings.TrimSpace(cause.Error())
	job.LastRunAt = now

	maxRetries := job.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	if job.RetryCount > maxRetries {
		job.Status = baldastate.ScheduledTaskStatusPaused
	} else {
		job.Status = baldastate.ScheduledTaskStatusActive
		retryCount := job.RetryCount
		if retryCount < 1 {
			retryCount = 1
		}
		delay := time.Duration(retryCount) * time.Second
		if delay > 60*time.Second {
			delay = 60 * time.Second
		}
		job.NextRunAt = now.Add(delay)
	}
	if err := s.taskStore.Upsert(ctx, job); err != nil {
		return fmt.Errorf("upsert scheduled job %q: %w", jobID, err)
	}
	return cause
}

func (s *ScheduledTaskScheduler) RecordExecutionFailure(ctx context.Context, jobID string, cause error) error {
	job, ok, err := s.taskStore.GetByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("load scheduled job %q: %w", jobID, err)
	}
	if !ok {
		return fmt.Errorf("scheduled job %q not found", jobID)
	}
	now := s.now().UTC()
	job.LastError = strings.TrimSpace(cause.Error())
	job.LastRunAt = now
	job.Status = baldastate.ScheduledTaskStatusActive
	if err := s.taskStore.Upsert(ctx, job); err != nil {
		return fmt.Errorf("upsert scheduled job %q execution failure: %w", jobID, err)
	}
	return cause
}

func normalizeScheduledTaskSchedulerConfig(raw ScheduledTaskSchedulerConfig) (ScheduledTaskSchedulerConfig, error) {
	cfg := ScheduledTaskSchedulerConfig{
		Jobs: make([]ConfiguredScheduledTask, 0, len(raw.Jobs)),
	}

	seenJobIDs := make(map[string]struct{}, len(raw.Jobs))
	for idx, rawTask := range raw.Jobs {
		jobID := strings.TrimSpace(rawTask.ID)
		if jobID == "" {
			return ScheduledTaskSchedulerConfig{}, fmt.Errorf("balda.scheduler.tasks[%d].id is required", idx)
		}
		if _, exists := seenJobIDs[jobID]; exists {
			return ScheduledTaskSchedulerConfig{}, fmt.Errorf("duplicate balda.scheduler.tasks id %q", jobID)
		}
		seenJobIDs[jobID] = struct{}{}

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

		cfg.Jobs = append(cfg.Jobs, ConfiguredScheduledTask{
			ID:       jobID,
			Cron:     cronSpec,
			Target:   target,
			Key:      key,
			Content:  content,
			ReportTo: reportTo,
		})
	}

	sort.Slice(cfg.Jobs, func(i, j int) bool {
		return cfg.Jobs[i].ID < cfg.Jobs[j].ID
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
