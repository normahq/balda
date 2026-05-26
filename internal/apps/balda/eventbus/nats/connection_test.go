package natsbus

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
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

func TestIsJetStreamQueuePressure(t *testing.T) {
	t.Parallel()

	for _, err := range []error{
		fakeJetStreamAPIError{description: "maximum messages exceeded"},
		fakeJetStreamAPIError{description: "resource limits exceeded"},
		errors.New("nats: stream is full"),
	} {
		if !isJetStreamQueuePressure(err) {
			t.Fatalf("isJetStreamQueuePressure(%v) = false, want true", err)
		}
	}
	if isJetStreamQueuePressure(errors.New("stream not found")) {
		t.Fatal("isJetStreamQueuePressure(stream not found) = true, want false")
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

func TestBus_PublishCommandSucceedsWhenAcceptedEventCannotPublish(t *testing.T) {
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
	if err := bus.js.DeleteStream(context.Background(), swarm.DefaultEventStream); err != nil {
		t.Fatalf("DeleteStream(events) error = %v", err)
	}

	ack, err := bus.PublishCommand(context.Background(), commandTestEnvelope("accepted-event-fails"))
	if err != nil {
		t.Fatalf("PublishCommand() error = %v, want nil because command is durable", err)
	}
	if ack.Stream != swarm.DefaultCommandStream || ack.Sequence == 0 {
		t.Fatalf("PublishCommand() ack = %+v, want command stream ack", ack)
	}
}

func TestBus_CommandLifecycleEventsUseDistinctDedupeIDs(t *testing.T) {
	busRaw, err := NewCommandBus(Params{
		LC:         fxtest.NewLifecycle(t),
		Config:     baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm:      swarm.Config{Enabled: true, Commands: swarm.CommandConfig{FetchWait: "50ms"}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewCommandBus() error = %v", err)
	}
	bus := busRaw.(*Bus)
	defer func() { _ = bus.Drain(context.Background()) }()

	env := commandTestEnvelope("lifecycle-dedupe")
	env.DedupeKey = "shared-command-dedupe"
	if _, err := bus.PublishCommand(context.Background(), env); err != nil {
		t.Fatalf("PublishCommand() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	handled := make(chan struct{}, 1)
	go func() {
		_ = bus.RunCommandConsumer(ctx, func(context.Context, swarm.CommandMessage) error {
			handled <- struct{}{}
			return nil
		})
	}()
	select {
	case <-handled:
	case <-ctx.Done():
		t.Fatal("timed out waiting for command handler")
	}
	for {
		status, err := bus.streamStatus(context.Background(), swarm.DefaultEventStream)
		if err != nil {
			t.Fatalf("event stream status: %v", err)
		}
		if status.Messages == 3 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("event stream messages = %d, want accepted/running/acked", status.Messages)
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func TestBus_CommandRunningEventFailureDoesNotBlockCommandHandling(t *testing.T) {
	busRaw, err := NewCommandBus(Params{
		LC:     fxtest.NewLifecycle(t),
		Config: baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm: swarm.Config{Enabled: true, Commands: swarm.CommandConfig{
			AckWait:    "1s",
			FetchWait:  "50ms",
			MaxDeliver: 2,
		}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewCommandBus() error = %v", err)
	}
	bus := busRaw.(*Bus)
	defer func() { _ = bus.Drain(context.Background()) }()

	if _, err := bus.PublishCommand(context.Background(), commandTestEnvelope("running-event-fails")); err != nil {
		t.Fatalf("PublishCommand() error = %v", err)
	}
	if err := bus.js.DeleteStream(context.Background(), swarm.DefaultEventStream); err != nil {
		t.Fatalf("DeleteStream(events) error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	handled := make(chan struct{}, 1)
	var calls atomic.Int32
	go func() {
		_ = bus.RunCommandConsumer(ctx, func(context.Context, swarm.CommandMessage) error {
			calls.Add(1)
			handled <- struct{}{}
			return nil
		})
	}()
	select {
	case <-handled:
	case <-ctx.Done():
		t.Fatal("timed out waiting for command handler")
	}
	for {
		status, err := bus.streamStatus(context.Background(), swarm.DefaultCommandStream)
		if err != nil {
			t.Fatalf("command stream status: %v", err)
		}
		info, err := bus.consumer.Info(context.Background())
		if err != nil {
			t.Fatalf("command consumer info: %v", err)
		}
		if status.Messages == 0 && info.NumAckPending == 0 && info.NumPending == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("command state = messages:%d ack_pending:%d pending:%d, want settled", status.Messages, info.NumAckPending, info.NumPending)
		case <-time.After(25 * time.Millisecond):
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
}

func TestBus_CommandAckedEventFailureDoesNotRedeliverCompletedCommand(t *testing.T) {
	busRaw, err := NewCommandBus(Params{
		LC:     fxtest.NewLifecycle(t),
		Config: baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm: swarm.Config{Enabled: true, Commands: swarm.CommandConfig{
			AckWait:    "100ms",
			FetchWait:  "50ms",
			MaxDeliver: 2,
		}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewCommandBus() error = %v", err)
	}
	bus := busRaw.(*Bus)
	defer func() { _ = bus.Drain(context.Background()) }()

	if _, err := bus.PublishCommand(context.Background(), commandTestEnvelope("acked-event-fails")); err != nil {
		t.Fatalf("PublishCommand() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	handled := make(chan struct{}, 1)
	var calls atomic.Int32
	go func() {
		_ = bus.RunCommandConsumer(ctx, func(context.Context, swarm.CommandMessage) error {
			calls.Add(1)
			if err := bus.js.DeleteStream(context.Background(), swarm.DefaultEventStream); err != nil {
				t.Errorf("DeleteStream(events) error = %v", err)
			}
			handled <- struct{}{}
			return nil
		})
	}()
	select {
	case <-handled:
	case <-ctx.Done():
		t.Fatal("timed out waiting for command handler")
	}
	for {
		status, err := bus.streamStatus(context.Background(), swarm.DefaultCommandStream)
		if err != nil {
			t.Fatalf("command stream status: %v", err)
		}
		if status.Messages == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("command stream messages = %d, want 0 after DoubleAck", status.Messages)
		case <-time.After(25 * time.Millisecond):
		}
	}
	time.Sleep(2 * bus.cfg.AckWait)
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
}

func TestBus_CommandSuccessSettlesWithCanceledParent(t *testing.T) {
	busRaw, err := NewCommandBus(Params{
		LC:     fxtest.NewLifecycle(t),
		Config: baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm: swarm.Config{Enabled: true, Commands: swarm.CommandConfig{
			AckWait:    "100ms",
			FetchWait:  "50ms",
			MaxDeliver: 2,
		}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewCommandBus() error = %v", err)
	}
	bus := busRaw.(*Bus)
	defer func() { _ = bus.Drain(context.Background()) }()

	if _, err := bus.PublishCommand(context.Background(), commandTestEnvelope("settle-success-canceled")); err != nil {
		t.Fatalf("PublishCommand() error = %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	handled := make(chan struct{}, 1)
	done := make(chan error, 1)
	var calls atomic.Int32
	go func() {
		done <- bus.RunCommandConsumer(runCtx, func(ctx context.Context, _ swarm.CommandMessage) error {
			calls.Add(1)
			cancelRun()
			<-ctx.Done()
			handled <- struct{}{}
			return nil
		})
	}()

	waitSignal(t, context.Background(), handled, "command handler")
	assertCommandStreamDrained(t, bus)
	waitConsumerCanceled(t, done)
	time.Sleep(2 * bus.cfg.AckWait)
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
}

func TestBus_CommandDLQSettlesWithCanceledParent(t *testing.T) {
	busRaw, err := NewCommandBus(Params{
		LC:     fxtest.NewLifecycle(t),
		Config: baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm: swarm.Config{Enabled: true, Commands: swarm.CommandConfig{
			AckWait:    "100ms",
			FetchWait:  "50ms",
			MaxDeliver: 2,
		}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewCommandBus() error = %v", err)
	}
	bus := busRaw.(*Bus)
	defer func() { _ = bus.Drain(context.Background()) }()

	if _, err := bus.PublishCommand(context.Background(), commandTestEnvelope("settle-dlq-canceled")); err != nil {
		t.Fatalf("PublishCommand() error = %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	handled := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- bus.RunCommandConsumer(runCtx, func(ctx context.Context, _ swarm.CommandMessage) error {
			cancelRun()
			<-ctx.Done()
			handled <- struct{}{}
			return swarm.PermanentError(errors.New("permanent failure"))
		})
	}()

	waitSignal(t, context.Background(), handled, "command handler")
	waitStreamMessages(t, bus, swarm.DefaultDLQStream, 1)
	assertCommandStreamDrained(t, bus)
	waitConsumerCanceled(t, done)
}

func TestBus_RunCommandConsumerHandlesCommandsConcurrently(t *testing.T) {
	busRaw, err := NewCommandBus(Params{
		LC:     fxtest.NewLifecycle(t),
		Config: baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm: swarm.Config{Enabled: true, Commands: swarm.CommandConfig{
			FetchBatch:    2,
			MaxAckPending: 2,
			FetchWait:     "50ms",
		}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewCommandBus() error = %v", err)
	}
	bus := busRaw.(*Bus)
	defer func() { _ = bus.Drain(context.Background()) }()

	for _, id := range []string{"concurrent-a", "concurrent-b"} {
		env := commandTestEnvelope(id)
		env.TaskID = id
		env.To = swarm.ActorAddress{Target: swarm.ActorTypeTask, Key: id}
		if _, err := bus.PublishCommand(context.Background(), env); err != nil {
			t.Fatalf("PublishCommand(%s) error = %v", id, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan string, 2)
	release := make(chan struct{})
	done := make(chan string, 2)
	go func() {
		_ = bus.RunCommandConsumer(ctx, func(_ context.Context, msg swarm.CommandMessage) error {
			started <- msg.Envelope().ID
			<-release
			done <- msg.Envelope().ID
			return nil
		})
	}()
	first := waitCommandStarted(t, ctx, started)
	second := waitCommandStarted(t, ctx, started)
	if first == second {
		t.Fatalf("started commands = %q/%q, want two distinct commands", first, second)
	}
	close(release)
	for range 2 {
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal("timed out waiting for concurrent command completion")
		}
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

func TestBus_EnsureRuntimeRequiresJetStream(t *testing.T) {
	cfg, err := resolveConfig(
		baldaeventbus.Config{Embedded: true, JetStream: true},
		swarm.Config{Enabled: true},
		t.TempDir(),
	)
	if err != nil {
		t.Fatalf("resolveConfig() error = %v", err)
	}
	bus := &Bus{cfg: cfg}
	err = bus.ensureRuntime(context.Background())
	if err == nil || !strings.Contains(err.Error(), "jetstream is required") {
		t.Fatalf("ensureRuntime() error = %v, want jetstream required", err)
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
	if status.CommandBus != "jetstream" || !status.JetStream {
		t.Fatalf("Status() = %+v, want hard JetStream", status)
	}
}

type fakeJetStreamAPIError struct {
	description string
}

func (e fakeJetStreamAPIError) Error() string {
	return e.description
}

func (e fakeJetStreamAPIError) APIError() *jetstream.APIError {
	return &jetstream.APIError{Code: 503, Description: e.description}
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

func waitCommandStarted(t *testing.T, ctx context.Context, ch <-chan string) string {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-ctx.Done():
		t.Fatal("timed out waiting for command handler start")
		return ""
	}
}

func waitSignal(t *testing.T, parent context.Context, ch <-chan struct{}, label string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for %s", label)
	}
}

func waitConsumerCanceled(t *testing.T, done <-chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("RunCommandConsumer() error = %v, want context canceled", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for command consumer shutdown")
	}
}

func assertCommandStreamDrained(t *testing.T, bus *Bus) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		status, err := bus.streamStatus(context.Background(), swarm.DefaultCommandStream)
		if err != nil {
			t.Fatalf("command stream status: %v", err)
		}
		info, err := bus.consumer.Info(context.Background())
		if err != nil {
			t.Fatalf("command consumer info: %v", err)
		}
		if status.Messages == 0 && info.NumAckPending == 0 && info.NumPending == 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("command state = messages:%d ack_pending:%d pending:%d, want settled", status.Messages, info.NumAckPending, info.NumPending)
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func waitStreamMessages(t *testing.T, bus *Bus, stream string, want uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		status, err := bus.streamStatus(context.Background(), stream)
		if err != nil {
			t.Fatalf("%s stream status: %v", stream, err)
		}
		if status.Messages == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s stream messages = %d, want %d", stream, status.Messages, want)
		case <-time.After(25 * time.Millisecond):
		}
	}
}
