package natsbus

import (
	"context"
	"testing"
	"time"

	baldaeventbus "github.com/normahq/balda/internal/apps/balda/eventbus"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/rs/zerolog"
	"go.uber.org/fx/fxtest"
)

func TestEventBus_PublishSubscribeEmbedded(t *testing.T) {
	lc := fxtest.NewLifecycle(t)
	busRaw, err := NewEventBus(Params{
		LC:         lc,
		Config:     baldaeventbus.Config{Mode: baldaeventbus.ModeNATSCore},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewEventBus() error = %v", err)
	}
	bus := busRaw.(*EventBus)
	defer func() { _ = bus.Drain(context.Background()) }()

	got := make(chan swarm.Envelope, 1)
	if _, err := bus.Subscribe(context.Background(), swarm.SubjectWakeupMailbox, func(_ context.Context, _ string, env swarm.Envelope) error {
		got <- env
		return nil
	}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	env := natsTestEnvelope("env-1")
	if err := bus.Publish(context.Background(), swarm.SubjectWakeupMailbox, env); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	select {
	case received := <-got:
		if received.ID != env.ID {
			t.Fatalf("received ID = %q, want %q", received.ID, env.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}
	status := bus.Status()
	if !status.Running || !status.Embedded || status.ClientURL == "" {
		t.Fatalf("Status() = %+v, want running embedded bus", status)
	}
}

func TestEventBus_SQLiteModeReturnsNoop(t *testing.T) {
	bus, err := NewEventBus(Params{Config: baldaeventbus.Config{Mode: baldaeventbus.ModeSQLite}, WorkingDir: t.TempDir(), Logger: zerolog.Nop()})
	if err != nil {
		t.Fatalf("NewEventBus() error = %v", err)
	}
	status := bus.(swarm.EventBusStatusProvider).Status()
	if status.Mode != baldaeventbus.ModeSQLite || status.Running {
		t.Fatalf("Status() = %+v, want sqlite stopped", status)
	}
}

func TestEventBus_JetStreamCreatesStreamsAndPublishesCommand(t *testing.T) {
	busRaw, err := NewEventBus(Params{
		LC: fxtest.NewLifecycle(t),
		Config: baldaeventbus.Config{
			Mode: baldaeventbus.ModeNATSJetStream,
			NATS: baldaeventbus.NATSConfig{StoreDir: t.TempDir()},
		},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewEventBus() error = %v", err)
	}
	bus := busRaw.(*EventBus)
	defer func() { _ = bus.Drain(context.Background()) }()
	env := natsTestEnvelope("env-js")
	if err := bus.PublishCommandShadow(context.Background(), env); err != nil {
		t.Fatalf("PublishCommandShadow() error = %v", err)
	}
	if err := bus.PublishCommandShadow(context.Background(), env); err != nil {
		t.Fatalf("PublishCommandShadow(duplicate) error = %v", err)
	}
}

func natsTestEnvelope(id string) swarm.Envelope {
	return swarm.Envelope{
		ID:          id,
		Namespace:   swarm.NamespaceHumanInbound,
		Kind:        swarm.KindMessage,
		From:        swarm.ActorAddress{Target: "test", Key: "source"},
		To:          swarm.ActorAddress{Target: swarm.ActorTypeSession, Key: "tg-1.2"},
		SessionID:   "tg-1.2",
		DedupeKey:   id,
		PayloadJSON: `{"ok":true}`,
	}
}
