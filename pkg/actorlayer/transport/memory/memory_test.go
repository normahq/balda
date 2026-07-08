package memory_test

import (
	"context"
	"errors"
	"testing"

	"github.com/normahq/balda/pkg/actorlayer"
	"github.com/normahq/balda/pkg/actorlayer/engine"
	"github.com/normahq/balda/pkg/actorlayer/transport"
	"github.com/normahq/balda/pkg/actorlayer/transport/memory"
)

var (
	_ transport.Dispatcher     = (*memory.Transport)(nil)
	_ transport.EventPublisher = (*memory.Transport)(nil)
	_ transport.EventConsumer  = (*memory.Transport)(nil)
	_ transport.Drainer        = (*memory.Transport)(nil)
	_ engine.Source            = (*memory.Transport)(nil)
)

func TestTransportDispatchRunRetryAndDeadLetter(t *testing.T) {
	t.Parallel()

	bus := memory.New(4)
	env := envelopeForTest("env-1")
	if _, err := bus.Dispatch(context.Background(), env); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	attempts := 0
	if err := bus.Run(context.Background(), func(ctx context.Context, delivery engine.Delivery) error {
		attempts++
		switch attempts {
		case 1:
			if delivery.Attempt() != 1 {
				t.Fatalf("first attempt = %d, want 1", delivery.Attempt())
			}
			return delivery.Retry(ctx, 0, "try again")
		case 2:
			if delivery.Attempt() != 2 {
				t.Fatalf("second attempt = %d, want 2", delivery.Attempt())
			}
			if err := delivery.DeadLetter(ctx, "done"); err != nil {
				return err
			}
			return bus.Drain(ctx)
		default:
			t.Fatalf("unexpected attempt %d", attempts)
		}
		return nil
	}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	deadletters := bus.DeadLetters()
	if len(deadletters) != 1 {
		t.Fatalf("deadletters length = %d, want 1", len(deadletters))
	}
	if deadletters[0].ID != env.ID {
		t.Fatalf("deadletter id = %q, want %q", deadletters[0].ID, env.ID)
	}
	if deadletters[0].Meta["reason"] != "done" {
		t.Fatalf("deadletter reason = %q, want done", deadletters[0].Meta["reason"])
	}
}

func TestTransportPublishAndConsumeEvents(t *testing.T) {
	t.Parallel()

	bus := memory.New(2)
	env := envelopeForTest("event-1")
	if err := bus.PublishEvent(context.Background(), "events.test", env); err != nil {
		t.Fatalf("PublishEvent() error = %v", err)
	}
	if err := bus.RunEventConsumer(context.Background(), func(ctx context.Context, subject string, got actorlayer.Envelope) error {
		if subject != "events.test" {
			t.Fatalf("event subject = %q, want events.test", subject)
		}
		if got.ID != env.ID {
			t.Fatalf("event envelope id = %q, want %q", got.ID, env.ID)
		}
		return bus.Drain(ctx)
	}); err != nil {
		t.Fatalf("RunEventConsumer() error = %v", err)
	}
}

func TestTransportValidationAndDrain(t *testing.T) {
	t.Parallel()

	bus := memory.New(1)
	if _, err := bus.Dispatch(context.Background(), actorlayer.Envelope{}); err == nil {
		t.Fatal("Dispatch(invalid) error = nil, want validation error")
	}
	if err := bus.PublishEvent(context.Background(), "", envelopeForTest("event")); err == nil {
		t.Fatal("PublishEvent(empty subject) error = nil, want error")
	}
	if err := bus.Drain(context.Background()); err != nil {
		t.Fatalf("Drain() error = %v", err)
	}
	if _, err := bus.Dispatch(context.Background(), envelopeForTest("after-drain")); err == nil {
		t.Fatal("Dispatch(after drain) error = nil, want drained error")
	}
	err := bus.Run(context.Background(), func(context.Context, engine.Delivery) error { return nil })
	if err != nil {
		t.Fatalf("Run(after drain) error = %v, want nil", err)
	}
}

func TestTransportRunValidatesInputs(t *testing.T) {
	t.Parallel()

	bus := memory.New(1)
	if err := bus.Run(context.Background(), nil); err == nil {
		t.Fatal("Run(nil handler) error = nil, want error")
	}
	if err := bus.RunEventConsumer(context.Background(), nil); err == nil {
		t.Fatal("RunEventConsumer(nil handler) error = nil, want error")
	}
	var nilBus *memory.Transport
	if _, err := nilBus.Dispatch(context.Background(), envelopeForTest("nil")); err == nil {
		t.Fatal("nil Dispatch() error = nil, want error")
	}
	if err := nilBus.Run(context.Background(), func(context.Context, engine.Delivery) error { return nil }); err == nil {
		t.Fatal("nil Run() error = nil, want error")
	}
	if err := nilBus.PublishEvent(context.Background(), "events", envelopeForTest("nil-event")); err == nil {
		t.Fatal("nil PublishEvent() error = nil, want error")
	}
	if err := nilBus.RunEventConsumer(context.Background(), func(context.Context, string, actorlayer.Envelope) error { return nil }); err == nil {
		t.Fatal("nil RunEventConsumer() error = nil, want error")
	}
	if err := nilBus.Drain(context.Background()); err != nil {
		t.Fatalf("nil Drain() error = %v, want nil", err)
	}
	if deadletters := nilBus.DeadLetters(); deadletters != nil {
		t.Fatalf("nil DeadLetters() = %#v, want nil", deadletters)
	}
}

func TestTransportRunReturnsHandlerError(t *testing.T) {
	t.Parallel()

	bus := memory.New(1)
	if _, err := bus.Dispatch(context.Background(), envelopeForTest("handler-error")); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	errHandler := errors.New("handler failed")
	if err := bus.Run(context.Background(), func(context.Context, engine.Delivery) error {
		return errHandler
	}); !errors.Is(err, errHandler) {
		t.Fatalf("Run() error = %v, want %v", err, errHandler)
	}
}

func envelopeForTest(id string) actorlayer.Envelope {
	return actorlayer.Envelope{
		ID:          id,
		Namespace:   "test.command",
		Kind:        "message",
		From:        actorlayer.SystemAddress("test"),
		To:          actorlayer.ActorAddress{Target: "session", Key: "one"},
		PayloadJSON: `{"ok":true}`,
		MaxAttempts: 3,
	}
}
