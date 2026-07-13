package jobs

import (
	"context"
	"fmt"
	"strings"

	actortransport "github.com/baldaworks/go-actorlayer/transport"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"go.uber.org/fx"
)

type JobLifecycleService struct {
	store ServiceStore
	bus   actortransport.EventPublisher
}

type jobLifecycleServiceParams struct {
	fx.In

	Store ServiceStore
	Bus   actortransport.EventPublisher `optional:"true"`
}

func NewJobLifecycleService(params jobLifecycleServiceParams) (*JobLifecycleService, error) {
	if params.Store == nil {
		return nil, fmt.Errorf("job lifecycle store is required")
	}
	return &JobLifecycleService{store: params.Store, bus: params.Bus}, nil
}

func (s *JobLifecycleService) Create(ctx context.Context, record baldastate.JobRecord, actor string, payload any) (bool, error) {
	if s == nil {
		return false, nil
	}
	payloadValue, err := marshalPayload(payload)
	if err != nil {
		return false, err
	}
	if strings.TrimSpace(payloadValue) == "" {
		payloadValue = "{}"
	}
	jobID := strings.TrimSpace(record.ID)
	event := baldastate.JobEventRecord{
		ID:        "job:" + jobID + ":event:created",
		JobID:     jobID,
		EventType: JobEventCreated,
		Actor:     strings.TrimSpace(actor),
		Payload:   payloadValue,
	}
	outbox, err := jobEventOutboxRecord(event)
	if err != nil {
		return false, err
	}
	created, err := s.store.CreateJobWithEvent(ctx, record, outbox)
	if err != nil {
		return false, err
	}
	s.publishOutboxBestEffort(ctx, outbox)
	return created, nil
}

func (s *JobLifecycleService) Get(ctx context.Context, jobID string) (baldastate.JobRecord, bool, error) {
	if s == nil {
		return baldastate.JobRecord{}, false, nil
	}
	return s.store.GetJob(ctx, jobID)
}

func (s *JobLifecycleService) ListActiveGoalJobsBySession(ctx context.Context, sessionID string) ([]baldastate.JobRecord, error) {
	if s == nil {
		return nil, nil
	}
	jobs, err := s.store.ListActiveJobsBySession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	out := make([]baldastate.JobRecord, 0, len(jobs))
	for _, job := range jobs {
		if IsGoalJob(job) {
			out = append(out, job)
		}
	}
	return out, nil
}

func (s *JobLifecycleService) MarkStatus(ctx context.Context, jobID string, status string, actor string, messageID string, reason string, payload any) error {
	if s == nil {
		return nil
	}
	eventType := jobStatusEventType(status)
	if eventType == "" {
		return fmt.Errorf("job status %q has no lifecycle event", status)
	}
	event, err := jobEventRecord(jobID, eventType, actor, messageID, mergePayload(payload, map[string]any{
		"status": status,
		"reason": reason,
	}))
	if err != nil {
		return err
	}
	outbox, err := jobEventOutboxRecord(event)
	if err != nil {
		return err
	}
	if err := s.store.UpdateJobStatusWithEvent(ctx, jobID, status, reason, outbox); err != nil {
		return s.suppressStaleTerminalTransition(ctx, jobID, status, err)
	}
	s.publishOutboxBestEffort(ctx, outbox)
	return nil
}

func (s *JobLifecycleService) SetResult(ctx context.Context, jobID string, result any, status string, actor string, reason string) error {
	if s == nil {
		return nil
	}
	data, err := marshalPayload(result)
	if err != nil {
		return err
	}
	eventType := jobStatusEventType(status)
	if eventType == "" {
		return fmt.Errorf("job status %q has no lifecycle event", status)
	}
	event, err := jobEventRecord(jobID, eventType, actor, "", mergePayload(result, map[string]any{
		"status": status,
		"reason": reason,
	}))
	if err != nil {
		return err
	}
	outbox, err := jobEventOutboxRecord(event)
	if err != nil {
		return err
	}
	if err := s.store.SetJobResultWithEvent(ctx, jobID, data, status, reason, outbox); err != nil {
		return s.suppressStaleTerminalTransition(ctx, jobID, status, err)
	}
	s.publishOutboxBestEffort(ctx, outbox)
	return nil
}

func (s *JobLifecycleService) CancelBySession(ctx context.Context, sessionID string, actor string, reason string) ([]string, error) {
	if s == nil {
		return nil, nil
	}
	jobs, err := s.store.ListActiveJobsBySession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(jobs))
	for _, job := range jobs {
		if err := s.MarkStatus(ctx, job.ID, baldastate.JobStatusCanceled, actor, "", reason, nil); err != nil {
			return ids, err
		}
		ids = append(ids, job.ID)
	}
	return ids, nil
}

func (s *JobLifecycleService) CancelJob(ctx context.Context, jobID string, actor string, reason string) error {
	if s == nil {
		return nil
	}
	return s.MarkStatus(ctx, jobID, baldastate.JobStatusCanceled, actor, "", reason, nil)
}

func (s *JobLifecycleService) DeadLetter(ctx context.Context, jobID string, actor string, messageID string, reason string) error {
	return s.MarkStatus(ctx, jobID, baldastate.JobStatusDeadLettered, actor, messageID, reason, nil)
}

func (s *JobLifecycleService) suppressStaleTerminalTransition(ctx context.Context, jobID string, status string, err error) error {
	if err == nil {
		return nil
	}
	if !strings.Contains(err.Error(), "invalid runtime job transition") {
		return err
	}
	if !isTerminalJobStatus(status) {
		return err
	}
	job, ok, getErr := s.Get(ctx, jobID)
	if getErr != nil || !ok {
		return err
	}
	if !isTerminalJobStatus(job.Status) {
		return err
	}
	return nil
}

func (s *JobLifecycleService) publishOutboxBestEffort(ctx context.Context, record baldastate.JobEventOutboxRecord) {
	if s == nil {
		return
	}
	publishOutboxBestEffort(ctx, s.store, s.bus, record)
}
