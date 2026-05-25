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

func TestNewCommandBus_DisabledSwarmReturnsUnsupportedBus(t *testing.T) {
	bus, err := NewCommandBus(Params{
		LC:         fxtest.NewLifecycle(t),
		Config:     baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm:      swarm.Config{Enabled: false},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewCommandBus() error = %v", err)
	}
	if _, ok := bus.(swarm.UnsupportedCommandBus); !ok {
		t.Fatalf("bus type = %T, want swarm.UnsupportedCommandBus", bus)
	}
}

func TestBus_PublishCommandAndConsumeEmbeddedJetStream(t *testing.T) {
	busRaw, err := NewCommandBus(Params{
		LC:         fxtest.NewLifecycle(t),
		Config:     baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm:      swarm.Config{Enabled: true},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewCommandBus() error = %v", err)
	}
	bus := busRaw.(*Bus)
	defer func() { _ = bus.Drain(context.Background()) }()

	env := commandTestEnvelope("env-1")
	ack, err := bus.PublishCommand(context.Background(), env)
	if err != nil {
		t.Fatalf("PublishCommand() error = %v", err)
	}
	if ack.Stream != swarm.DefaultCommandStream || ack.Subject != swarm.SubjectCommandTask || ack.Sequence == 0 {
		t.Fatalf("PublishCommand() ack = %+v", ack)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	seen := make(chan swarm.Envelope, 1)
	go func() {
		_ = bus.RunCommandConsumer(ctx, func(_ context.Context, msg swarm.CommandMessage) error {
			seen <- msg.Envelope()
			return nil
		})
	}()
	select {
	case got := <-seen:
		if got.ID != env.ID {
			t.Fatalf("consumed envelope id = %q, want %q", got.ID, env.ID)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for command consumer")
	}
}

func TestBus_CommandDecodeFailurePublishesRawDLQ(t *testing.T) {
	busRaw, err := NewCommandBus(Params{
		LC:         fxtest.NewLifecycle(t),
		Config:     baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm:      swarm.Config{Enabled: true, Commands: swarm.CommandConfig{MaxDeliver: 1, FetchWait: "50ms"}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewCommandBus() error = %v", err)
	}
	bus := busRaw.(*Bus)
	defer func() { _ = bus.Drain(context.Background()) }()

	if err := bus.conn.Publish(swarm.SubjectCommandTask, []byte("{not-json")); err != nil {
		t.Fatalf("raw publish command: %v", err)
	}
	if err := bus.conn.Flush(); err != nil {
		t.Fatalf("flush raw command: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{}, 1)
	go func() {
		_ = bus.RunCommandConsumer(ctx, func(context.Context, swarm.CommandMessage) error {
			t.Error("handler called for poison command")
			return nil
		})
		done <- struct{}{}
	}()
	for {
		status, err := bus.streamStatus(context.Background(), swarm.DefaultDLQStream)
		if err != nil {
			t.Fatalf("DLQ stream status: %v", err)
		}
		if status.Messages == 1 {
			cancel()
			<-done
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("DLQ messages = %d, want 1", status.Messages)
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func TestBus_PublishCommandReportsDuplicate(t *testing.T) {
	busRaw, err := NewCommandBus(Params{
		LC:         fxtest.NewLifecycle(t),
		Config:     baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm:      swarm.Config{Enabled: true},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewCommandBus() error = %v", err)
	}
	bus := busRaw.(*Bus)
	defer func() { _ = bus.Drain(context.Background()) }()

	env := commandTestEnvelope("env-duplicate")
	env.DedupeKey = "dedupe-duplicate"
	first, err := bus.PublishCommand(context.Background(), env)
	if err != nil {
		t.Fatalf("PublishCommand(first) error = %v", err)
	}
	second, err := bus.PublishCommand(context.Background(), env)
	if err != nil {
		t.Fatalf("PublishCommand(second) error = %v", err)
	}
	if first.Duplicate {
		t.Fatalf("first publish duplicate = true, want false")
	}
	if !second.Duplicate {
		t.Fatalf("second publish duplicate = false, want true")
	}
	if second.MsgID != env.DedupeKey {
		t.Fatalf("second msg id = %q, want %q", second.MsgID, env.DedupeKey)
	}
}

func TestBus_PublishEventDeduplicatesByEnvelopeID(t *testing.T) {
	busRaw, err := NewCommandBus(Params{
		LC:         fxtest.NewLifecycle(t),
		Config:     baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm:      swarm.Config{Enabled: true},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewCommandBus() error = %v", err)
	}
	bus := busRaw.(*Bus)
	defer func() { _ = bus.Drain(context.Background()) }()

	env := commandTestEnvelope("event-dedupe")
	env.Namespace = swarm.NamespaceTelemetry
	env.Kind = "task_event"
	env.Meta = map[string]string{"event_type": swarm.TaskEventAgentStarted}
	if err := bus.PublishEvent(context.Background(), swarm.SubjectEventTaskUpdated, env); err != nil {
		t.Fatalf("PublishEvent(first) error = %v", err)
	}
	if err := bus.PublishEvent(context.Background(), swarm.SubjectEventTaskUpdated, env); err != nil {
		t.Fatalf("PublishEvent(second) error = %v", err)
	}
	status, err := bus.streamStatus(context.Background(), swarm.DefaultEventStream)
	if err != nil {
		t.Fatalf("event stream status: %v", err)
	}
	if status.Messages != 1 {
		t.Fatalf("event stream messages = %d, want 1 after duplicate event publish", status.Messages)
	}
}

func TestBus_RetryExhaustionPublishesDLQ(t *testing.T) {
	busRaw, err := NewCommandBus(Params{
		LC:         fxtest.NewLifecycle(t),
		Config:     baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm:      swarm.Config{Enabled: true, Commands: swarm.CommandConfig{MaxDeliver: 1, FetchWait: "50ms"}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewCommandBus() error = %v", err)
	}
	bus := busRaw.(*Bus)
	defer func() { _ = bus.Drain(context.Background()) }()

	env := commandTestEnvelope("env-retry-exhausted")
	if _, err := bus.PublishCommand(context.Background(), env); err != nil {
		t.Fatalf("PublishCommand() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	handled := make(chan struct{}, 1)
	go func() {
		_ = bus.RunCommandConsumer(ctx, func(_ context.Context, msg swarm.CommandMessage) error {
			handled <- struct{}{}
			if msg.DeliveryAttempt() != 1 || msg.MaxDeliveries() != 1 {
				t.Errorf("delivery metadata = %d/%d, want 1/1", msg.DeliveryAttempt(), msg.MaxDeliveries())
			}
			return swarm.TransientError(context.DeadlineExceeded)
		})
	}()
	select {
	case <-handled:
	case <-ctx.Done():
		t.Fatal("timed out waiting for command handler")
	}
	for {
		status, err := bus.streamStatus(context.Background(), swarm.DefaultDLQStream)
		if err != nil {
			t.Fatalf("DLQ stream status: %v", err)
		}
		if status.Messages == 1 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("DLQ messages = %d, want 1", status.Messages)
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func TestBus_EventProjectionPermanentFailurePublishesDLQ(t *testing.T) {
	busRaw, err := NewCommandBus(Params{
		LC:         fxtest.NewLifecycle(t),
		Config:     baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm:      swarm.Config{Enabled: true, Commands: swarm.CommandConfig{MaxDeliver: 1, FetchWait: "50ms"}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewCommandBus() error = %v", err)
	}
	bus := busRaw.(*Bus)
	defer func() { _ = bus.Drain(context.Background()) }()

	env := commandTestEnvelope("event-projection-failed")
	env.Namespace = swarm.NamespaceTelemetry
	env.Kind = "task_event"
	env.Meta = map[string]string{"event_type": swarm.TaskEventAgentProgress}
	if err := bus.PublishEvent(context.Background(), swarm.SubjectEventTaskUpdated, env); err != nil {
		t.Fatalf("PublishEvent() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	handled := make(chan struct{}, 1)
	go func() {
		_ = bus.RunEventConsumer(ctx, func(context.Context, string, swarm.Envelope) error {
			handled <- struct{}{}
			return swarm.PermanentError(context.Canceled)
		})
	}()
	select {
	case <-handled:
	case <-ctx.Done():
		t.Fatal("timed out waiting for event handler")
	}
	for {
		status, err := bus.streamStatus(context.Background(), swarm.DefaultDLQStream)
		if err != nil {
			t.Fatalf("DLQ stream status: %v", err)
		}
		if status.Messages == 1 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("DLQ messages = %d, want 1", status.Messages)
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func TestBus_StatusReportsJetStreamOnly(t *testing.T) {
	busRaw, err := NewCommandBus(Params{
		LC:         fxtest.NewLifecycle(t),
		Config:     baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm:      swarm.Config{Enabled: true},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewCommandBus() error = %v", err)
	}
	bus := busRaw.(*Bus)
	defer func() { _ = bus.Drain(context.Background()) }()
	status, err := bus.Status(context.Background())
	if err != nil {
		t.Fatalf("Status() error = %v", err)
	}
	if status.CommandBus != "jetstream" || status.SQLiteCommandBus || status.ShadowMode || status.LegacyDirectPath {
		t.Fatalf("Status() = %+v, want hard JetStream", status)
	}
}

func commandTestEnvelope(id string) swarm.Envelope {
	return swarm.Envelope{
		ID:          id,
		Namespace:   swarm.NamespaceAgentCommand,
		Kind:        swarm.KindGoal,
		From:        swarm.SystemAddress("test"),
		To:          swarm.ActorAddress{Target: swarm.ActorTypeTask, Key: "task-1"},
		TaskID:      "task-1",
		PayloadJSON: `{"ok":true}`,
	}
}
