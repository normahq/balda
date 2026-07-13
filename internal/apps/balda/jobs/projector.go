package jobs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/baldaworks/go-actorlayer"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/actorcmd"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

type EventProjector struct {
	consumer actortransport.EventConsumer
	store    ProjectionStore
	logger   zerolog.Logger

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type eventProjectorParams struct {
	fx.In

	Consumer actortransport.EventConsumer
	Store    ProjectionStore
	Logger   zerolog.Logger
}

// ProjectionStore persists the idempotent job history projection.
type ProjectionStore interface {
	AppendJobEvent(ctx context.Context, record baldastate.JobEventRecord) error
}

func NewEventProjector(params eventProjectorParams) (*EventProjector, error) {
	if params.Store == nil {
		return nil, fmt.Errorf("job event projection store is required")
	}
	if params.Consumer == nil {
		return nil, fmt.Errorf("event projector requires an actor runtime event consumer")
	}
	p := &EventProjector{
		consumer: params.Consumer,
		store:    params.Store,
		logger:   params.Logger.With().Str("component", "balda.jobs.projector").Logger(),
	}
	return p, nil
}

func (p *EventProjector) Start(context.Context) error {
	if p == nil || p.consumer == nil {
		return nil
	}
	runCtx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		if err := p.consumer.RunEventConsumer(runCtx, p.Project); err != nil && !errors.Is(err, context.Canceled) {
			p.logger.Error().Err(err).Msg("event projector stopped")
		}
	}()
	return nil
}

func (p *EventProjector) Stop(ctx context.Context) error {
	if p == nil || p.cancel == nil {
		return nil
	}
	p.cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.wg.Wait()
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *EventProjector) Project(ctx context.Context, subject string, env actorlayer.Envelope) error {
	if p == nil || p.store == nil {
		return nil
	}
	jobID := baldaexecution.EnvelopeJobID(env)
	if jobID == "" {
		return nil
	}
	eventType := ""
	if env.Meta != nil {
		if value := strings.TrimSpace(env.Meta["event_type"]); value != "" {
			eventType = value
		}
	}
	if eventType == "" {
		switch strings.TrimSpace(subject) {
		case baldaexecution.SubjectEventCommandAccepted:
			eventType = "command.accepted"
		case baldaexecution.SubjectEventCommandRunning:
			eventType = "command.running"
		case baldaexecution.SubjectEventCommandInProgress:
			eventType = "command.in_progress"
		case baldaexecution.SubjectEventCommandAcked:
			eventType = "command.acked"
		case baldaexecution.SubjectEventCommandRetrying:
			eventType = "command.retrying"
		case baldaexecution.SubjectEventCommandDeadLettered:
			eventType = "command.deadlettered"
		case baldaexecution.SubjectEventCommandNoop:
			eventType = "command.noop"
		case baldaexecution.SubjectEventCommandDecodeFailed:
			eventType = "command.decode_failed"
		case baldaexecution.SubjectEventJobCreated:
			eventType = JobEventCreated
		case baldaexecution.SubjectEventJobUpdated:
			eventType = JobEventAssigned
		case baldaexecution.SubjectEventJobCompleted:
			eventType = JobEventCompleted
		case baldaexecution.SubjectEventDeliverySent:
			eventType = JobEventDeliverySent
		case baldaexecution.SubjectEventDeliveryFailed:
			eventType = JobEventDeliveryFailed
		}
	}
	if eventType == "" {
		return nil
	}
	actor := strings.TrimSpace(env.Meta["actor"])
	if actor == "" {
		if from, err := env.From.String(); err == nil {
			actor = from
		}
	}
	messageID := strings.TrimSpace(env.Meta["message_id"])
	if messageID == "" {
		messageID = strings.TrimSpace(env.CausationID)
	}
	return p.store.AppendJobEvent(ctx, baldastate.JobEventRecord{
		ID:        strings.TrimSpace(env.ID),
		JobID:     jobID,
		EventType: eventType,
		Actor:     actor,
		MessageID: messageID,
		Payload:   strings.TrimSpace(env.Payload.String()),
	})
}
