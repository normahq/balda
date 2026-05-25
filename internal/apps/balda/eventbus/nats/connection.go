package natsbus

import (
	"context"
	"fmt"
	"time"

	gnats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	baldaeventbus "github.com/normahq/balda/internal/apps/balda/eventbus"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

type EventBus struct {
	cfg       resolvedConfig
	embedded  *EmbeddedNATS
	conn      *gnats.Conn
	jsMailbox *JetStreamMailbox
	logger    zerolog.Logger
}

type Params struct {
	fx.In

	LC         fx.Lifecycle
	Config     baldaeventbus.Config
	WorkingDir string
	Logger     zerolog.Logger
}

func NewEventBus(params Params) (swarm.EventBus, error) {
	cfg, err := resolveConfig(params.Config, params.WorkingDir)
	if err != nil {
		return nil, err
	}
	if cfg.Mode == baldaeventbus.ModeSQLite {
		return swarm.NewNoopEventBus(cfg.Mode), nil
	}
	bus := &EventBus{cfg: cfg, logger: params.Logger.With().Str("component", "balda.eventbus.nats").Logger()}
	if cfg.NATS.Embedded {
		embedded, err := StartEmbeddedNATS(context.Background(), cfg)
		if err != nil {
			return nil, err
		}
		bus.embedded = embedded
		bus.conn = embedded.Conn
		if cfg.Mode == baldaeventbus.ModeNATSJetStream {
			if err := ensureStreams(context.Background(), embedded.JS, cfg); err != nil {
				_ = embedded.Drain(context.Background())
				return nil, err
			}
			bus.jsMailbox = &JetStreamMailbox{js: embedded.JS, cfg: cfg}
		}
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
		if cfg.Mode == baldaeventbus.ModeNATSJetStream {
			js, err := jetstream.New(conn)
			if err != nil {
				conn.Close()
				return nil, fmt.Errorf("create jetstream client: %w", err)
			}
			if err := ensureStreams(context.Background(), js, cfg); err != nil {
				conn.Close()
				return nil, err
			}
			bus.jsMailbox = &JetStreamMailbox{js: js, cfg: cfg}
		}
	}
	params.LC.Append(fx.Hook{OnStop: bus.Drain})
	return bus, nil
}

func (b *EventBus) Publish(ctx context.Context, subject string, env swarm.Envelope) error {
	msg, err := messageFromEnvelope(subject, env)
	if err != nil {
		return err
	}
	if err := b.conn.PublishMsg(msg); err != nil {
		return fmt.Errorf("publish nats subject %q: %w", subject, err)
	}
	return b.flush(ctx)
}

func (b *EventBus) Subscribe(ctx context.Context, subject string, handler swarm.EventHandler) (swarm.Subscription, error) {
	sub, err := b.conn.Subscribe(subject, func(msg *gnats.Msg) {
		env, err := swarm.DecodeEnvelope(string(msg.Data))
		if err != nil {
			b.logger.Warn().Err(err).Str("subject", msg.Subject).Msg("failed to decode event bus envelope")
			return
		}
		if err := handler(context.Background(), msg.Subject, env); err != nil {
			b.logger.Warn().Err(err).Str("subject", msg.Subject).Str("envelope_id", env.ID).Msg("event bus handler failed")
		}
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe nats subject %q: %w", subject, err)
	}
	if err := b.flush(ctx); err != nil {
		return nil, err
	}
	return subscription{sub: sub}, nil
}

func (b *EventBus) Request(ctx context.Context, subject string, env swarm.Envelope, timeout time.Duration) (*swarm.Envelope, error) {
	msg, err := messageFromEnvelope(subject, env)
	if err != nil {
		return nil, err
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	response, err := b.conn.RequestMsgWithContext(requestCtx, msg)
	if err != nil {
		return nil, fmt.Errorf("request nats subject %q: %w", subject, err)
	}
	out, err := swarm.DecodeEnvelope(string(response.Data))
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (b *EventBus) Drain(ctx context.Context) error {
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

func (b *EventBus) Status() swarm.EventBusStatus {
	status := swarm.EventBusStatus{Mode: b.cfg.Mode, Embedded: b.cfg.NATS.Embedded, JetStream: b.cfg.NATS.JetStream}
	if b.conn != nil && !b.conn.IsClosed() {
		status.Running = true
		status.ClientURL = b.conn.ConnectedUrl()
	}
	if b.embedded != nil && b.embedded.URL != "" {
		status.ClientURL = b.embedded.URL
	}
	return status
}

func (b *EventBus) PublishCommandShadow(ctx context.Context, env swarm.Envelope) error {
	if b == nil || b.jsMailbox == nil {
		return nil
	}
	return b.jsMailbox.PublishCommand(ctx, env)
}

func (b *EventBus) flush(ctx context.Context) error {
	done := make(chan error, 1)
	go func() { done <- b.conn.Flush() }()
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("flush nats: %w", err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type subscription struct {
	sub *gnats.Subscription
}

func (s subscription) Unsubscribe() error {
	if s.sub == nil {
		return nil
	}
	return s.sub.Unsubscribe()
}
