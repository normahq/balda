package swarm

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

type EventProjector struct {
	consumer EventConsumer
	store    baldastate.SwarmStore
	logger   zerolog.Logger
	enabled  bool

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type eventProjectorParams struct {
	fx.In

	LC            fx.Lifecycle
	Bus           CommandBus
	Config        Config
	StateProvider baldastate.Provider
	Logger        zerolog.Logger
}

func NewEventProjector(params eventProjectorParams) (*EventProjector, error) {
	if params.StateProvider == nil {
		return nil, fmt.Errorf("balda state provider is required")
	}
	consumer, ok := params.Bus.(EventConsumer)
	if params.Config.Enabled && !ok {
		return nil, fmt.Errorf("event projector requires an event-consumer command bus")
	}
	p := &EventProjector{
		consumer: consumer,
		store:    params.StateProvider.Swarm(),
		logger:   params.Logger.With().Str("component", "balda.swarm.event_projector").Logger(),
		enabled:  params.Config.Enabled,
	}
	params.LC.Append(fx.Hook{OnStart: p.Start, OnStop: p.Stop})
	return p, nil
}

func (p *EventProjector) Start(context.Context) error {
	if p == nil || !p.enabled || p.consumer == nil {
		return nil
	}
	runCtx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		if err := p.consumer.RunEventConsumer(runCtx, p.Project); err != nil && !errors.Is(err, context.Canceled) {
			p.logger.Error().Err(err).Msg("jetstream event projector stopped")
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

func (p *EventProjector) Project(ctx context.Context, subject string, env Envelope) error {
	if p == nil || p.store == nil {
		return nil
	}
	taskID := strings.TrimSpace(env.TaskID)
	if taskID == "" {
		return nil
	}
	eventType := projectedEventType(subject, env)
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
	return p.store.AppendTaskEvent(ctx, baldastate.SwarmTaskEventRecord{
		ID:          strings.TrimSpace(env.ID),
		TaskID:      taskID,
		EventType:   eventType,
		Actor:       actor,
		MessageID:   messageID,
		PayloadJSON: strings.TrimSpace(env.PayloadJSON),
	})
}

func projectedEventType(subject string, env Envelope) string {
	if env.Meta != nil {
		if eventType := strings.TrimSpace(env.Meta["event_type"]); eventType != "" {
			return eventType
		}
	}
	switch strings.TrimSpace(subject) {
	case SubjectEventCommandAccepted:
		return "command.accepted"
	case SubjectEventCommandRunning:
		return "command.running"
	case SubjectEventCommandInProgress:
		return "command.in_progress"
	case SubjectEventCommandAcked:
		return "command.acked"
	case SubjectEventCommandRetrying:
		return "command.retrying"
	case SubjectEventCommandDeadLettered:
		return "command.deadlettered"
	case SubjectEventCommandNoop:
		return "command.noop"
	case SubjectEventTaskCreated:
		return TaskEventTaskCreated
	case SubjectEventTaskUpdated:
		return TaskEventTaskAssigned
	case SubjectEventTaskCompleted:
		return TaskEventTaskCompleted
	case SubjectEventDeliverySent:
		return TaskEventDeliverySent
	default:
		return ""
	}
}
