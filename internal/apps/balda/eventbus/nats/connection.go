package natsbus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	gnats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	baldaeventbus "github.com/normahq/balda/internal/apps/balda/eventbus"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	actorengine "github.com/normahq/norma/pkg/actorlayer/engine"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

type Bus struct {
	cfg           resolvedConfig
	embedded      *EmbeddedNATS
	conn          *gnats.Conn
	js            jetstream.JetStream
	consumer      jetstream.Consumer
	eventConsumer jetstream.Consumer
	logger        zerolog.Logger
}

type Params struct {
	fx.In

	LC         fx.Lifecycle
	Config     baldaeventbus.Config
	Swarm      swarm.Config
	WorkingDir string
	Logger     zerolog.Logger
}

func NewBus(params Params) (*Bus, error) {
	cfg, err := resolveConfig(params.Config, params.Swarm, params.WorkingDir)
	if err != nil {
		return nil, err
	}
	bus := &Bus{cfg: cfg, logger: params.Logger.With().Str("component", "balda.jetstream_bus").Logger()}
	if cfg.NATS.Embedded {
		embedded, err := StartEmbeddedNATS(context.Background(), cfg)
		if err != nil {
			return nil, err
		}
		bus.embedded = embedded
		bus.conn = embedded.Conn
		bus.js = embedded.JS
	} else {
		conn, err := gnats.Connect(
			cfg.NATS.URLs[0],
			gnats.Name("balda-worker"),
			gnats.Timeout(5*time.Second),
		)
		if err != nil {
			return nil, fmt.Errorf("connect nats: %w", err)
		}
		bus.conn = conn
		js, err := jetstream.New(conn)
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("create runtime client: %w", err)
		}
		bus.js = js
	}
	if bus.js == nil {
		_ = bus.Drain(context.Background())
		return nil, fmt.Errorf("runtime transport is required")
	}
	if err := bus.ensureRuntime(context.Background()); err != nil {
		_ = bus.Drain(context.Background())
		return nil, err
	}
	params.LC.Append(fx.Hook{OnStop: bus.Drain})
	return bus, nil
}

func (b *Bus) Dispatch(ctx context.Context, env swarm.Envelope) (*swarm.DispatchReceipt, error) {
	if err := env.Validate(); err != nil {
		return nil, err
	}
	subject := swarm.SubjectForEnvelope(env)
	msg, err := messageFromEnvelope(subject, env)
	if err != nil {
		return nil, err
	}
	msgID := swarm.DedupeKeyOrID(env)
	ack, err := b.js.PublishMsg(ctx, msg, jetstream.WithMsgID(msgID), jetstream.WithExpectStream(b.cfg.Swarm.Commands.Stream))
	if err != nil {
		if isJetStreamQueuePressure(err) {
			return nil, fmt.Errorf("%w: publish command %q: %w", swarm.ErrCommandQueueFull, subject, err)
		}
		return nil, fmt.Errorf("publish command %q: %w", subject, err)
	}
	result := &swarm.DispatchReceipt{Stream: ack.Stream, Sequence: ack.Sequence, Subject: subject, MsgID: msgID, Duplicate: ack.Duplicate}
	logEvt := b.logger.Debug().
		Str("subject", subject).
		Str("envelope_id", strings.TrimSpace(env.ID)).
		Str("task_id", strings.TrimSpace(env.TaskID)).
		Str("session_id", strings.TrimSpace(env.SessionID)).
		Str("correlation_id", strings.TrimSpace(env.CorrelationID)).
		Str("causation_id", strings.TrimSpace(env.CausationID)).
		Str("actor_key", strings.TrimSpace(env.To.Key)).
		Str("stream", ack.Stream).
		Uint64("sequence", ack.Sequence).
		Str("msg_id", msgID).
		Bool("duplicate", ack.Duplicate)
	withDeliveryKey(logEvt, env).Msg("published command to runtime transport")
	if err := b.PublishEvent(ctx, swarm.SubjectEventCommandAccepted, commandEventEnvelope(env, result, "accepted", "", nil)); err != nil {
		logEvt := b.logger.Warn().
			Err(err).
			Str("envelope_id", env.ID).
			Str("task_id", strings.TrimSpace(env.TaskID)).
			Str("session_id", strings.TrimSpace(env.SessionID)).
			Str("correlation_id", strings.TrimSpace(env.CorrelationID)).
			Str("causation_id", strings.TrimSpace(env.CausationID)).
			Str("subject", subject)
		withDeliveryKey(logEvt, env).Msg("failed to publish command accepted event")
	}
	if ack.Duplicate {
		const noopReason = "duplicate publish suppressed"
		if err := b.PublishEvent(ctx, swarm.SubjectEventCommandNoop, commandEventEnvelope(env, result, "noop", noopReason, nil)); err != nil {
			logEvt := b.logger.Warn().
				Err(err).
				Str("envelope_id", env.ID).
				Str("task_id", strings.TrimSpace(env.TaskID)).
				Str("session_id", strings.TrimSpace(env.SessionID)).
				Str("correlation_id", strings.TrimSpace(env.CorrelationID)).
				Str("causation_id", strings.TrimSpace(env.CausationID)).
				Str("subject", subject)
			withDeliveryKey(logEvt, env).Msg("failed to publish command noop event")
		}
	}
	return result, nil
}

var _ actorengine.Source = (*Bus)(nil)

func isJetStreamQueuePressure(err error) bool {
	var jsErr jetstream.JetStreamError
	if errors.As(err, &jsErr) {
		apiErr := jsErr.APIError()
		if apiErr != nil && isQueuePressureText(apiErr.Description) {
			return true
		}
	}
	return isQueuePressureText(err.Error())
}

func isQueuePressureText(raw string) bool {
	text := strings.ToLower(strings.TrimSpace(raw))
	if text == "" {
		return false
	}
	for _, phrase := range []string{
		"maximum messages exceeded",
		"maximum bytes exceeded",
		"max messages exceeded",
		"max bytes exceeded",
		"resource limits exceeded",
		"stream is full",
		"no space left",
		"insufficient storage",
		"discard new",
	} {
		if strings.Contains(text, phrase) {
			return true
		}
	}
	return false
}

func (b *Bus) PublishEvent(ctx context.Context, subject string, env swarm.Envelope) error {
	if err := env.Validate(); err != nil {
		return err
	}
	msg, err := messageFromEnvelope(subject, env)
	if err != nil {
		return err
	}
	_, err = b.js.PublishMsg(ctx, msg, jetstream.WithExpectStream(b.cfg.Swarm.Events.Stream), jetstream.WithMsgID(swarm.DedupeKeyOrID(env)))
	if err != nil {
		return fmt.Errorf("publish event %q: %w", subject, err)
	}
	return nil
}

func (b *Bus) publishDLQ(ctx context.Context, env swarm.Envelope, reason string, emitEvent bool) error {
	msg, err := newDLQMessage(env, reason)
	if err != nil {
		return err
	}
	_, err = b.js.PublishMsg(ctx, msg, jetstream.WithExpectStream(b.cfg.Swarm.DLQ.Stream), jetstream.WithMsgID(swarm.DedupeKeyOrID(env)+":dlq"))
	if err != nil {
		return fmt.Errorf("publish dlq: %w", err)
	}
	if emitEvent {
		if err := b.PublishEvent(ctx, swarm.SubjectEventCommandDeadLettered, commandEventEnvelope(env, nil, "deadlettered", reason, nil)); err != nil {
			logEvt := b.logger.Warn().
				Err(err).
				Str("envelope_id", env.ID).
				Str("task_id", strings.TrimSpace(env.TaskID)).
				Str("session_id", strings.TrimSpace(env.SessionID)).
				Str("correlation_id", strings.TrimSpace(env.CorrelationID)).
				Str("causation_id", strings.TrimSpace(env.CausationID))
			withDeliveryKey(logEvt, env).Msg("failed to publish command deadlettered event")
		}
	}
	return nil
}

func (b *Bus) Drain(ctx context.Context) error {
	if b == nil {
		return nil
	}
	if b.embedded != nil {
		return b.embedded.Drain(ctx)
	}
	if b.conn == nil {
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- b.conn.Drain() }()
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("drain nats connection: %w", err)
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	b.conn.Close()
	return nil
}

func (b *Bus) ensureRuntime(ctx context.Context) error {
	if err := ensureStreams(ctx, b.js, b.cfg); err != nil {
		return err
	}
	consumer, err := b.js.CreateOrUpdateConsumer(ctx, b.cfg.Swarm.Commands.Stream, jetstream.ConsumerConfig{
		Durable:       b.cfg.Swarm.Commands.Consumer,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       b.cfg.AckWait,
		MaxDeliver:    b.cfg.Swarm.Commands.MaxDeliver,
		MaxAckPending: b.cfg.Swarm.Commands.MaxAckPending,
		FilterSubject: swarm.SubjectCommandAll,
	})
	if err != nil {
		return fmt.Errorf("create command consumer: %w", err)
	}
	b.consumer = consumer
	eventConsumer, err := b.js.CreateOrUpdateConsumer(ctx, b.cfg.Swarm.Events.Stream, jetstream.ConsumerConfig{
		Durable:       swarm.DefaultEventProjectorConsumer,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       b.cfg.AckWait,
		MaxDeliver:    b.cfg.Swarm.Commands.MaxDeliver,
		MaxAckPending: b.cfg.Swarm.Commands.MaxAckPending,
		FilterSubject: swarm.SubjectEventAll,
	})
	if err != nil {
		return fmt.Errorf("create event projector consumer: %w", err)
	}
	b.eventConsumer = eventConsumer
	return nil
}
