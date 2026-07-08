package memory

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/normahq/balda/pkg/actorlayer"
	"github.com/normahq/balda/pkg/actorlayer/engine"
	"github.com/normahq/balda/pkg/actorlayer/transport"
)

const defaultBuffer = 16

type Transport struct {
	mu           sync.Mutex
	drained      bool
	done         chan struct{}
	drainOnce    sync.Once
	commands     chan *delivery
	events       chan eventMessage
	nextSequence uint64
	deadletters  []actorlayer.Envelope
}

type eventMessage struct {
	subject string
	env     actorlayer.Envelope
}

func New(buffer int) *Transport {
	if buffer <= 0 {
		buffer = defaultBuffer
	}
	return &Transport{
		done:     make(chan struct{}),
		commands: make(chan *delivery, buffer),
		events:   make(chan eventMessage, buffer),
	}
}

func (t *Transport) Dispatch(ctx context.Context, env actorlayer.Envelope) (*transport.DispatchReceipt, error) {
	if t == nil {
		return nil, fmt.Errorf("memory transport is required")
	}
	if err := env.Validate(); err != nil {
		return nil, err
	}
	subject, err := env.To.String()
	if err != nil {
		return nil, actorlayer.DecodeError(err)
	}
	sequence, duplicate := t.next()
	msgID := actorlayer.DedupeKeyOrID(env)
	if err := t.sendCommand(ctx, &delivery{transport: t, env: env, attempt: 1, maxAttempts: env.MaxAttempts}); err != nil {
		return nil, err
	}
	return &transport.DispatchReceipt{
		Stream:    "memory.commands",
		Sequence:  sequence,
		Subject:   subject,
		MsgID:     msgID,
		Duplicate: duplicate,
	}, nil
}

func (t *Transport) Run(ctx context.Context, handler engine.Handler) error {
	if t == nil {
		return fmt.Errorf("memory transport is required")
	}
	if handler == nil {
		return fmt.Errorf("engine handler is required")
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.done:
			return nil
		case delivery := <-t.commands:
			if delivery == nil {
				continue
			}
			if err := handler(ctx, delivery); err != nil {
				return err
			}
		}
	}
}

func (t *Transport) PublishEvent(ctx context.Context, subject string, env actorlayer.Envelope) error {
	if t == nil {
		return fmt.Errorf("memory transport is required")
	}
	if strings.TrimSpace(subject) == "" {
		return fmt.Errorf("event subject is required")
	}
	if err := env.Validate(); err != nil {
		return err
	}
	if t.isDrained() {
		return fmt.Errorf("memory transport is drained")
	}
	msg := eventMessage{subject: strings.TrimSpace(subject), env: env}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.done:
		return fmt.Errorf("memory transport is drained")
	case t.events <- msg:
		return nil
	}
}

func (t *Transport) RunEventConsumer(ctx context.Context, handler transport.EventHandler) error {
	if t == nil {
		return fmt.Errorf("memory transport is required")
	}
	if handler == nil {
		return fmt.Errorf("event handler is required")
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.done:
			return nil
		case msg := <-t.events:
			if err := handler(ctx, msg.subject, msg.env); err != nil {
				return err
			}
		}
	}
}

func (t *Transport) Drain(context.Context) error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	t.drained = true
	t.mu.Unlock()
	t.drainOnce.Do(func() {
		close(t.done)
	})
	return nil
}

func (t *Transport) DeadLetters() []actorlayer.Envelope {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]actorlayer.Envelope(nil), t.deadletters...)
}

func (t *Transport) isDrained() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.drained
}

func (t *Transport) next() (uint64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nextSequence++
	return t.nextSequence, false
}

func (t *Transport) sendCommand(ctx context.Context, delivery *delivery) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if t.isDrained() {
		return fmt.Errorf("memory transport is drained")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.done:
		return fmt.Errorf("memory transport is drained")
	case t.commands <- delivery:
		return nil
	}
}

func (t *Transport) addDeadLetter(env actorlayer.Envelope, reason string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if env.Meta == nil {
		env.Meta = map[string]string{}
	}
	if strings.TrimSpace(reason) != "" {
		env.Meta["reason"] = reason
	}
	t.deadletters = append(t.deadletters, env)
}

type delivery struct {
	transport   *Transport
	env         actorlayer.Envelope
	attempt     int
	maxAttempts int

	mu      sync.Mutex
	settled bool
}

func (d *delivery) Envelope() engine.Envelope {
	env := d.env
	env.Attempt = d.attempt - 1
	env.MaxAttempts = d.maxAttempts
	return env
}

func (d *delivery) Attempt() int {
	if d.attempt <= 0 {
		return 1
	}
	return d.attempt
}

func (d *delivery) MaxAttempts() int {
	return d.maxAttempts
}

func (*delivery) InProgress(context.Context) error {
	return nil
}

func (d *delivery) Ack(context.Context) error {
	return d.settle(func() error { return nil })
}

func (d *delivery) Retry(ctx context.Context, delay time.Duration, reason string) error {
	return d.settle(func() error {
		next := &delivery{
			transport:   d.transport,
			env:         d.env,
			attempt:     d.Attempt() + 1,
			maxAttempts: d.maxAttempts,
		}
		if delay <= 0 {
			return d.transport.sendCommand(ctx, next)
		}
		go func() {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-timer.C:
				_ = d.transport.sendCommand(context.Background(), next)
			case <-d.transport.done:
			}
		}()
		return nil
	})
}

func (d *delivery) DeadLetter(_ context.Context, reason string) error {
	return d.settle(func() error {
		d.transport.addDeadLetter(d.Envelope(), reason)
		return nil
	})
}

func (d *delivery) settle(fn func() error) error {
	d.mu.Lock()
	if d.settled {
		d.mu.Unlock()
		return nil
	}
	d.mu.Unlock()
	if err := fn(); err != nil {
		return err
	}
	d.mu.Lock()
	d.settled = true
	d.mu.Unlock()
	return nil
}
