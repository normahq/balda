package swarm

import (
	"context"
	"fmt"
	"time"
)

type EventHandler func(ctx context.Context, subject string, env Envelope) error

type Subscription interface {
	Unsubscribe() error
}

type EventBus interface {
	Publish(ctx context.Context, subject string, env Envelope) error
	Subscribe(ctx context.Context, subject string, handler EventHandler) (Subscription, error)
	Request(ctx context.Context, subject string, env Envelope, timeout time.Duration) (*Envelope, error)
	Drain(ctx context.Context) error
}

type MessageRef struct {
	Source    string
	Subject   string
	Mailbox   string
	MessageID string
}

type DurableMailbox interface {
	PublishCommand(ctx context.Context, env Envelope) error
	ConsumeCommands(ctx context.Context, actorGroup string, handler EventHandler) error
	Ack(ctx context.Context, msg MessageRef) error
	Retry(ctx context.Context, msg MessageRef, delay time.Duration, reason string) error
	DeadLetter(ctx context.Context, msg MessageRef, reason string) error
}

type CommandShadowPublisher interface {
	PublishCommandShadow(ctx context.Context, env Envelope) error
}

type EventBusStatus struct {
	Mode      string
	Embedded  bool
	Running   bool
	JetStream bool
	ClientURL string
}

type EventBusStatusProvider interface {
	Status() EventBusStatus
}

type NoopEventBus struct {
	mode string
}

func NewNoopEventBus(mode string) *NoopEventBus {
	if mode == "" {
		mode = "sqlite"
	}
	return &NoopEventBus{mode: mode}
}

func (b *NoopEventBus) Publish(context.Context, string, Envelope) error { return nil }

func (b *NoopEventBus) Subscribe(context.Context, string, EventHandler) (Subscription, error) {
	return noopSubscription{}, nil
}

func (b *NoopEventBus) Request(context.Context, string, Envelope, time.Duration) (*Envelope, error) {
	return nil, fmt.Errorf("event bus %q does not support request", b.mode)
}

func (b *NoopEventBus) Drain(context.Context) error { return nil }

func (b *NoopEventBus) Status() EventBusStatus { return EventBusStatus{Mode: b.mode} }

type noopSubscription struct{}

func (noopSubscription) Unsubscribe() error { return nil }
