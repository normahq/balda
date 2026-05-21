package handlers

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/normahq/balda/internal/apps/balda/auth"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

const (
	defaultSchedulerPollInterval = 2 * time.Second
	defaultSchedulerDueBatchSize = 100
	defaultSchedulerMaxRetries   = 3
)

// ConfiguredScheduledJob defines a startup-managed recurring job.
type ConfiguredScheduledJob struct {
	ID     string
	Cron   string
	Prompt string
}

// JobSchedulerConfig controls startup job reconciliation.
type JobSchedulerConfig struct {
	Jobs []ConfiguredScheduledJob
}

type schedulerSessionManager interface {
	GetSession(locator baldasession.SessionLocator) (*baldasession.TopicSession, error)
	EnsureSession(ctx context.Context, sessionCtx baldasession.SessionContext, label string) (*baldasession.TopicSession, error)
	RestoreSession(ctx context.Context, sessionCtx baldasession.SessionContext) (*baldasession.TopicSession, error)
}

type schedulerChannel interface {
	SendPlain(ctx context.Context, locator baldasession.SessionLocator, text string) error
	SendAgentReply(ctx context.Context, locator baldasession.SessionLocator, text string) error
}

type jobSchedulerParams struct {
	fx.In

	LC             fx.Lifecycle
	StateProvider  baldastate.Provider
	SessionManager *baldasession.Manager
	TurnDispatcher *TurnDispatcher
	Channel        *baldatelegram.Adapter
	OwnerStore     *auth.OwnerStore
	Logger         zerolog.Logger
	Config         JobSchedulerConfig
}

// JobScheduler dispatches due locator-bound recurring jobs into the turn queue.
type JobScheduler struct {
	jobStore baldastate.ScheduledJobStore
	sessions schedulerSessionManager
	dispatch turnQueue
	channel  schedulerChannel
	owner    *auth.OwnerStore
	logger   zerolog.Logger
	config   JobSchedulerConfig

	pollInterval time.Duration
	dueBatchSize int
	now          func() time.Time

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewJobScheduler(params jobSchedulerParams) (*JobScheduler, error) {
	if params.StateProvider == nil {
		return nil, fmt.Errorf("balda state provider is required")
	}
	if params.SessionManager == nil {
		return nil, fmt.Errorf("balda session manager is required")
	}
	if params.TurnDispatcher == nil {
		return nil, fmt.Errorf("balda turn dispatcher is required")
	}
	if params.Channel == nil {
		return nil, fmt.Errorf("balda channel adapter is required")
	}
	config, err := normalizeJobSchedulerConfig(params.Config)
	if err != nil {
		return nil, err
	}
	if len(config.Jobs) > 0 && params.OwnerStore == nil {
		return nil, fmt.Errorf("balda owner store is required for scheduler jobs")
	}

	scheduler := &JobScheduler{
		jobStore:     params.StateProvider.ScheduledJobs(),
		sessions:     params.SessionManager,
		dispatch:     params.TurnDispatcher,
		channel:      params.Channel,
		owner:        params.OwnerStore,
		logger:       params.Logger.With().Str("component", "balda.job_scheduler").Logger(),
		config:       config,
		pollInterval: defaultSchedulerPollInterval,
		dueBatchSize: defaultSchedulerDueBatchSize,
		now:          time.Now,
	}

	params.LC.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := scheduler.reconcileConfiguredJobs(ctx); err != nil {
				return err
			}
			scheduler.start()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return scheduler.stop(ctx)
		},
	})

	return scheduler, nil
}

func (s *JobScheduler) start() {
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
				s.logger.Warn().Err(err).Msg("failed to dispatch due jobs")
			}

			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (s *JobScheduler) stop(ctx context.Context) error {
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

func (s *JobScheduler) reconcileConfiguredJobs(ctx context.Context) error {
	desired := make(map[string]struct{}, len(s.config.Jobs))
	now := s.now().UTC()
	var target resolvedEnvelopeTarget
	if len(s.config.Jobs) > 0 {
		var err error
		target, err = resolveEnvelopeTarget(ctx, s.owner, ownerEnvelopeTarget())
		if err != nil {
			return fmt.Errorf("resolve scheduler target: %w", err)
		}
	}

	for _, job := range s.config.Jobs {
		nextRunAt, err := nextRunAtFromSpec(job.Cron, now)
		if err != nil {
			return fmt.Errorf("compute next run for scheduler job %q: %w", job.ID, err)
		}
		record := baldastate.ScheduledJobRecord{
			JobID:        job.ID,
			SessionID:    target.Locator.SessionID,
			ChannelType:  target.Locator.ChannelType,
			AddressKey:   target.Locator.AddressKey,
			AddressJSON:  target.Locator.AddressJSON,
			Prompt:       job.Prompt,
			ScheduleSpec: job.Cron,
			Timezone:     "UTC",
			Status:       baldastate.ScheduledJobStatusActive,
			MaxRetries:   defaultSchedulerMaxRetries,
			RetryCount:   0,
			NextRunAt:    nextRunAt,
		}
		if err := s.jobStore.Upsert(ctx, record); err != nil {
			return fmt.Errorf("upsert scheduler job %q: %w", job.ID, err)
		}
		desired[job.ID] = struct{}{}
	}

	currentJobs, err := s.jobStore.List(ctx)
	if err != nil {
		return fmt.Errorf("list persisted scheduler jobs: %w", err)
	}
	for _, existing := range currentJobs {
		jobID := strings.TrimSpace(existing.JobID)
		if _, ok := desired[jobID]; ok {
			continue
		}
		if err := s.jobStore.Delete(ctx, jobID); err != nil {
			return fmt.Errorf("delete unmanaged scheduler job %q: %w", jobID, err)
		}
	}

	return nil
}

func (s *JobScheduler) dispatchDue(ctx context.Context, now time.Time) error {
	due, err := s.jobStore.ListDue(ctx, now, s.dueBatchSize)
	if err != nil {
		return fmt.Errorf("list due jobs: %w", err)
	}

	for _, job := range due {
		if err := s.dispatchJob(ctx, job, now); err != nil {
			s.logger.Warn().Err(err).Str("job_id", job.JobID).Msg("failed to dispatch job")
		}
	}
	return nil
}

func (s *JobScheduler) dispatchJob(ctx context.Context, job baldastate.ScheduledJobRecord, now time.Time) error {
	jobID := strings.TrimSpace(job.JobID)
	if jobID == "" {
		return fmt.Errorf("job id is required")
	}

	current, ok, err := s.jobStore.GetByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("load scheduled job %q: %w", jobID, err)
	}
	if !ok {
		return fmt.Errorf("scheduled job %q not found", jobID)
	}
	if strings.TrimSpace(current.Status) != baldastate.ScheduledJobStatusActive {
		return nil
	}
	if current.NextRunAt.After(now.UTC()) {
		return nil
	}

	dispatchKey := dispatchAttemptKey(jobID, current.NextRunAt)
	if strings.TrimSpace(current.LastDispatchKey) == dispatchKey {
		return nil
	}

	target, err := resolveEnvelopeTarget(ctx, s.owner, ownerEnvelopeTarget())
	if err != nil {
		return s.markFailure(ctx, jobID, fmt.Errorf("resolve scheduler target: %w", err))
	}
	locator := target.Locator

	ts, err := s.resolveTopicSession(ctx, target)
	if err != nil {
		return s.markFailure(ctx, jobID, fmt.Errorf("resolve session: %w", err))
	}

	nextRunAt, err := nextRunAtFromSpec(current.ScheduleSpec, now)
	if err != nil {
		return s.markFailure(ctx, jobID, fmt.Errorf("invalid schedule_spec: %w", err))
	}

	// Claim this due-slot before enqueue so duplicate stale due entries do not dispatch twice.
	current.LastDispatchKey = dispatchKey
	current.LastError = ""
	current.Status = baldastate.ScheduledJobStatusActive
	current.NextRunAt = nextRunAt
	current.SessionID = locator.SessionID
	current.ChannelType = locator.ChannelType
	current.AddressKey = locator.AddressKey
	current.AddressJSON = locator.AddressJSON
	if err := s.jobStore.Upsert(ctx, current); err != nil {
		return fmt.Errorf("update job %q before enqueue: %w", jobID, err)
	}

	prompt := strings.TrimSpace(current.Prompt)
	if _, err := s.dispatch.Enqueue(TurnTask{
		SessionID: ts.GetSessionID(),
		Run: func(runCtx context.Context) error {
			return s.executeJobTurn(runCtx, locator, jobID, prompt, ts)
		},
	}); err != nil {
		return s.markFailure(ctx, jobID, fmt.Errorf("enqueue scheduled job: %w", err))
	}

	return nil
}

func (s *JobScheduler) resolveTopicSession(
	ctx context.Context,
	target resolvedEnvelopeTarget,
) (*baldasession.TopicSession, error) {
	locator := target.Locator
	ts, err := s.sessions.GetSession(locator)
	if err == nil {
		return ts, nil
	}

	userID := strings.TrimSpace(target.UserID)
	if userID == "" {
		return nil, fmt.Errorf("owner target user id is required")
	}
	ts, err = s.sessions.RestoreSession(ctx, baldasession.SessionContext{
		Locator: locator,
		UserID:  userID,
	})
	if err == nil {
		return ts, nil
	}
	if !errors.Is(err, baldasession.ErrNoPersistedSession) {
		return nil, err
	}
	return s.sessions.EnsureSession(ctx, baldasession.SessionContext{
		Locator: locator,
		UserID:  userID,
	}, ownerSessionLabel)
}

func (s *JobScheduler) executeJobTurn(
	ctx context.Context,
	locator baldasession.SessionLocator,
	jobID string,
	prompt string,
	ts *baldasession.TopicSession,
) error {
	_, err := runGoalIteration(ctx, ts.GetRunner(), ts.GetUserID(), ts.GetAgentSessionID(), prompt)
	if err != nil {
		if isScheduledJobCancellation(ctx, err) {
			s.logger.Info().Str("job_id", jobID).Msg("scheduled job turn canceled")
			return nil
		}
		_ = s.markFailure(context.Background(), jobID, fmt.Errorf("execute scheduled job: %w", err))
		return err
	}

	if err := s.markSuccess(context.Background(), jobID); err != nil {
		s.logger.Warn().Err(err).Str("job_id", jobID).Msg("failed to mark scheduled job success")
	}
	return nil
}

func isScheduledJobCancellation(ctx context.Context, err error) bool {
	if errors.Is(err, context.Canceled) {
		return true
	}
	if ctx != nil && errors.Is(ctx.Err(), context.Canceled) {
		return true
	}
	return false
}

func (s *JobScheduler) markSuccess(ctx context.Context, jobID string) error {
	job, ok, err := s.jobStore.GetByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("load scheduled job %q: %w", jobID, err)
	}
	if !ok {
		return fmt.Errorf("scheduled job %q not found", jobID)
	}
	job.LastRunAt = s.now().UTC()
	job.LastError = ""
	job.RetryCount = 0
	job.Status = baldastate.ScheduledJobStatusActive
	if err := s.jobStore.Upsert(ctx, job); err != nil {
		return fmt.Errorf("upsert scheduled job %q: %w", jobID, err)
	}
	return nil
}

func (s *JobScheduler) markFailure(ctx context.Context, jobID string, cause error) error {
	job, ok, err := s.jobStore.GetByID(ctx, jobID)
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
		job.Status = baldastate.ScheduledJobStatusPaused
	} else {
		job.Status = baldastate.ScheduledJobStatusActive
		job.NextRunAt = now.Add(retryDelay(job.RetryCount))
	}
	if err := s.jobStore.Upsert(ctx, job); err != nil {
		return fmt.Errorf("upsert scheduled job %q: %w", jobID, err)
	}
	return cause
}

func normalizeJobSchedulerConfig(raw JobSchedulerConfig) (JobSchedulerConfig, error) {
	cfg := JobSchedulerConfig{
		Jobs: make([]ConfiguredScheduledJob, 0, len(raw.Jobs)),
	}

	seenJobIDs := make(map[string]struct{}, len(raw.Jobs))
	for idx, rawJob := range raw.Jobs {
		jobID := strings.TrimSpace(rawJob.ID)
		if jobID == "" {
			return JobSchedulerConfig{}, fmt.Errorf("balda.scheduler.jobs[%d].id is required", idx)
		}
		if _, exists := seenJobIDs[jobID]; exists {
			return JobSchedulerConfig{}, fmt.Errorf("duplicate balda.scheduler.jobs id %q", jobID)
		}
		seenJobIDs[jobID] = struct{}{}

		cronSpec := strings.TrimSpace(rawJob.Cron)
		if cronSpec == "" {
			return JobSchedulerConfig{}, fmt.Errorf("balda.scheduler.jobs[%d].cron is required", idx)
		}
		if _, err := parseScheduleSpec(cronSpec); err != nil {
			return JobSchedulerConfig{}, fmt.Errorf("invalid balda.scheduler.jobs[%d].cron: %w", idx, err)
		}

		prompt := strings.TrimSpace(rawJob.Prompt)
		if prompt == "" {
			return JobSchedulerConfig{}, fmt.Errorf("balda.scheduler.jobs[%d].prompt is required", idx)
		}

		cfg.Jobs = append(cfg.Jobs, ConfiguredScheduledJob{
			ID:     jobID,
			Cron:   cronSpec,
			Prompt: prompt,
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

func retryDelay(retryCount int) time.Duration {
	if retryCount < 1 {
		retryCount = 1
	}
	delay := time.Duration(retryCount) * time.Second
	if delay > 60*time.Second {
		return 60 * time.Second
	}
	return delay
}

func dispatchAttemptKey(jobID string, dueAt time.Time) string {
	return fmt.Sprintf("%s@%s", strings.TrimSpace(jobID), dueAt.UTC().Format(time.RFC3339Nano))
}
