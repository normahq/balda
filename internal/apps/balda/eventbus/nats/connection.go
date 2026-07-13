package natsbus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/baldaworks/go-actorlayer"
	actorengine "github.com/baldaworks/go-actorlayer/engine"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	gnats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	baldaeventbus "github.com/normahq/balda/internal/apps/balda/eventbus"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
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
	startMu       sync.Mutex
	started       bool
}

type Params struct {
	fx.In

	LC        fx.Lifecycle
	Config    baldaeventbus.Config
	Execution baldaexecution.Config
	StateDir  string `name:"balda_state_dir"`
	Logger    zerolog.Logger
}

func NewBus(params Params) (*Bus, error) {
	cfg, err := resolveConfig(params.Config, params.Execution, params.StateDir)
	if err != nil {
		return nil, err
	}
	return &Bus{cfg: cfg, logger: params.Logger.With().Str("component", "balda.execution_bus").Logger()}, nil
}

// Start connects the bus, ensures its streams and consumers, and makes it
// available to dispatchers and consumers. Construction is intentionally free
// of network and filesystem side effects; the application lifecycle owns this
// operation.
func (b *Bus) Start(ctx context.Context) error {
	if b == nil {
		return nil
	}
	b.startMu.Lock()
	defer b.startMu.Unlock()
	if b.started {
		return nil
	}

	if b.cfg.NATS.Embedded {
		embedded, err := StartEmbeddedNATS(ctx, b.cfg)
		if err != nil {
			return err
		}
		b.embedded = embedded
		b.conn = embedded.Conn
		b.js = embedded.JS
	} else {
		conn, err := gnats.Connect(
			b.cfg.NATS.URLs[0],
			gnats.Name("balda-worker"),
			gnats.Timeout(5*time.Second),
		)
		if err != nil {
			return fmt.Errorf("connect nats: %w", err)
		}
		b.conn = conn
		js, err := jetstream.New(conn)
		if err != nil {
			conn.Close()
			b.conn = nil
			return fmt.Errorf("create runtime client: %w", err)
		}
		b.js = js
	}
	if b.js == nil {
		_ = b.drainResources(context.Background())
		return fmt.Errorf("runtime transport is required")
	}
		if err := b.ensureRuntime(ctx); err != nil {
		_ = b.drainResources(context.Background())
		return err
	}
	b.started = true
	return nil
}

func (b *Bus) ensureStarted(ctx context.Context) error {
	if b == nil {
		return fmt.Errorf("runtime transport is required")
	}
	return b.Start(ctx)
}

func (b *Bus) Dispatch(ctx context.Context, env actorlayer.Envelope) (*actortransport.DispatchReceipt, error) {
	if err := b.ensureStarted(ctx); err != nil {
		return nil, err
	}
	if err := env.Validate(); err != nil {
		return nil, err
	}
	subject := baldaexecution.SubjectForEnvelope(env)
	msg, err := messageFromEnvelope(subject, env)
	if err != nil {
		return nil, err
	}
	msgID := actorlayer.DedupeKeyOrID(env)
	ack, err := b.js.PublishMsg(ctx, msg, jetstream.WithMsgID(msgID), jetstream.WithExpectStream(b.cfg.Execution.Commands.Stream))
	if err != nil {
		if isRuntimeQueuePressure(err) {
			return nil, fmt.Errorf("%w: publish command %q: %w", baldaexecution.ErrCommandQueueFull, subject, err)
		}
		return nil, fmt.Errorf("publish command %q: %w", subject, err)
	}
	result := &actortransport.DispatchReceipt{Stream: ack.Stream, Sequence: ack.Sequence, Subject: subject, MsgID: msgID, Duplicate: ack.Duplicate}
	logEvt := b.logger.Debug().
		Str("subject", subject).
		Str("envelope_id", strings.TrimSpace(env.ID)).
		Str("job_id", baldaexecution.EnvelopeJobID(env)).
		Str("session_id", baldaexecution.EnvelopeSessionID(env)).
		Str("correlation_id", strings.TrimSpace(env.CorrelationID)).
		Str("causation_id", strings.TrimSpace(env.CausationID)).
		Str("actor_key", strings.TrimSpace(env.To.Key)).
		Str("stream", ack.Stream).
		Uint64("sequence", ack.Sequence).
		Str("msg_id", msgID).
		Bool("duplicate", ack.Duplicate)
	withDeliveryKey(logEvt, env).Msg("published command to runtime transport")
	if err := b.PublishEvent(ctx, baldaexecution.SubjectEventCommandAccepted, commandEventEnvelope(env, result, "accepted", "", nil)); err != nil {
		logEvt := b.logger.Warn().
			Err(err).
			Str("envelope_id", env.ID).
			Str("job_id", baldaexecution.EnvelopeJobID(env)).
			Str("session_id", baldaexecution.EnvelopeSessionID(env)).
			Str("correlation_id", strings.TrimSpace(env.CorrelationID)).
			Str("causation_id", strings.TrimSpace(env.CausationID)).
			Str("subject", subject)
		withDeliveryKey(logEvt, env).Msg("failed to publish command accepted event")
	}
	if ack.Duplicate {
		const noopReason = "duplicate publish suppressed"
		if err := b.PublishEvent(ctx, baldaexecution.SubjectEventCommandNoop, commandEventEnvelope(env, result, "noop", noopReason, nil)); err != nil {
			logEvt := b.logger.Warn().
				Err(err).
				Str("envelope_id", env.ID).
				Str("job_id", baldaexecution.EnvelopeJobID(env)).
				Str("session_id", baldaexecution.EnvelopeSessionID(env)).
				Str("correlation_id", strings.TrimSpace(env.CorrelationID)).
				Str("causation_id", strings.TrimSpace(env.CausationID)).
				Str("subject", subject)
			withDeliveryKey(logEvt, env).Msg("failed to publish command noop event")
		}
	}
	return result, nil
}

var _ actorengine.Source = (*Bus)(nil)

func isRuntimeQueuePressure(err error) bool {
	matchesQueuePressure := func(raw string) bool {
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

	var jsErr jetstream.JetStreamError
	if errors.As(err, &jsErr) {
		apiErr := jsErr.APIError()
		if apiErr != nil && matchesQueuePressure(apiErr.Description) {
			return true
		}
	}
	return matchesQueuePressure(err.Error())
}

func (b *Bus) PublishEvent(ctx context.Context, subject string, env actorlayer.Envelope) error {
	if err := b.ensureStarted(ctx); err != nil {
		return err
	}
	if err := env.Validate(); err != nil {
		return err
	}
	msg, err := messageFromEnvelope(subject, env)
	if err != nil {
		return err
	}
	_, err = b.js.PublishMsg(ctx, msg, jetstream.WithExpectStream(b.cfg.Execution.Events.Stream), jetstream.WithMsgID(actorlayer.DedupeKeyOrID(env)))
	if err != nil {
		return fmt.Errorf("publish event %q: %w", subject, err)
	}
	return nil
}

func (b *Bus) publishDLQ(ctx context.Context, env actorlayer.Envelope, reason string, emitEvent bool) error {
	msg, err := messageFromEnvelope(baldaexecution.SubjectDLQCommand, env)
	if err != nil {
		return err
	}
	msg.Header.Set("Balda-DLQ-Reason", reason)
	if env.Meta != nil {
		if value := strings.TrimSpace(env.Meta[dlqMetaErrorClass]); value != "" {
			msg.Header.Set("Balda-DLQ-Error-Class", value)
		}
		if value := strings.TrimSpace(env.Meta[dlqMetaSourceStream]); value != "" {
			msg.Header.Set("Balda-DLQ-Source-Stream", value)
		}
		if value := strings.TrimSpace(env.Meta[dlqMetaSourceCns]); value != "" {
			msg.Header.Set("Balda-DLQ-Source-Consumer", value)
		}
		if value := strings.TrimSpace(env.Meta[dlqMetaSourceSubj]); value != "" {
			msg.Header.Set("Balda-DLQ-Source-Subject", value)
		}
		if value := strings.TrimSpace(env.Meta[dlqMetaDelivered]); value != "" {
			msg.Header.Set("Balda-DLQ-Num-Delivered", value)
		}
	}
	_, err = b.js.PublishMsg(ctx, msg, jetstream.WithExpectStream(b.cfg.Execution.DLQ.Stream), jetstream.WithMsgID(actorlayer.DedupeKeyOrID(env)+":dlq"))
	if err != nil {
		return fmt.Errorf("publish dlq: %w", err)
	}
	if emitEvent {
		if err := b.PublishEvent(ctx, baldaexecution.SubjectEventCommandDeadLettered, commandEventEnvelope(env, nil, "deadlettered", reason, nil)); err != nil {
			logEvt := b.logger.Warn().
				Err(err).
				Str("envelope_id", env.ID).
				Str("job_id", baldaexecution.EnvelopeJobID(env)).
				Str("session_id", baldaexecution.EnvelopeSessionID(env)).
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
	b.startMu.Lock()
	defer b.startMu.Unlock()
	return b.drainResources(ctx)
}

func (b *Bus) drainResources(ctx context.Context) error {
	if b == nil {
		return nil
	}
	if b.embedded != nil {
		err := b.embedded.Drain(ctx)
		b.embedded = nil
		b.started = false
		return err
	}
	if b.conn == nil {
		b.started = false
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
	b.started = false
	return nil
}

func (b *Bus) ensureRuntime(ctx context.Context) error {
	if err := ensureStreams(ctx, b.js, b.cfg); err != nil {
		return err
	}
	consumer, err := b.js.CreateOrUpdateConsumer(ctx, b.cfg.Execution.Commands.Stream, jetstream.ConsumerConfig{
		Durable:       b.cfg.Execution.Commands.Consumer,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       b.cfg.AckWait,
		MaxDeliver:    b.cfg.Execution.Commands.MaxDeliver,
		MaxAckPending: b.cfg.Execution.Commands.MaxAckPending,
		FilterSubject: baldaexecution.SubjectCommandAll,
	})
	if err != nil {
		return fmt.Errorf("create command consumer: %w", err)
	}
	b.consumer = consumer
	eventConsumer, err := b.js.CreateOrUpdateConsumer(ctx, b.cfg.Execution.Events.Stream, jetstream.ConsumerConfig{
		Durable:       baldaexecution.DefaultEventProjectorConsumer,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       b.cfg.AckWait,
		MaxDeliver:    b.cfg.Execution.Commands.MaxDeliver,
		MaxAckPending: b.cfg.Execution.Commands.MaxAckPending,
		FilterSubject: baldaexecution.SubjectEventAll,
	})
	if err != nil {
		return fmt.Errorf("create event projector consumer: %w", err)
	}
	b.eventConsumer = eventConsumer
	return nil
}
