package natsbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	baldaeventbus "github.com/normahq/balda/internal/apps/balda/eventbus"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	actorengine "github.com/normahq/norma/pkg/actorlayer/engine"
	"github.com/rs/zerolog"
	"go.uber.org/fx/fxtest"
)

const testEventKindTask = "task_event"

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

func TestBus_DispatchAndConsumeBuiltInRuntime(t *testing.T) {
	h := StartTestRuntime(t, swarm.Config{})
	bus := h.Bus

	env := commandTestEnvelope("env-1")
	ack := h.Dispatch(t, env)
	if ack.Stream != swarm.DefaultCommandStream || ack.Subject != swarm.SubjectCommandGoal || ack.Sequence == 0 {
		t.Fatalf("Dispatch() ack = %+v", ack)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	seen := make(chan swarm.Envelope, 1)
	go func() {
		_ = bus.Run(ctx, func(_ context.Context, msg actorengine.Delivery) error {
			seen <- testDeliveryEnvelope(t, msg)
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

func TestBus_DispatchSucceedsWhenAcceptedEventCannotPublish(t *testing.T) {
	h := StartTestRuntime(t, swarm.Config{})
	bus := h.Bus
	if err := bus.js.DeleteStream(context.Background(), swarm.DefaultEventStream); err != nil {
		t.Fatalf("DeleteStream(events) error = %v", err)
	}

	ack := h.Dispatch(t, commandTestEnvelope("accepted-event-fails"))
	if ack.Stream != swarm.DefaultCommandStream || ack.Sequence == 0 {
		t.Fatalf("Dispatch() ack = %+v, want command stream ack", ack)
	}
}

func TestBus_CommandLifecycleEventsUseDistinctDedupeIDs(t *testing.T) {
	bus, err := NewBus(Params{
		LC:         fxtest.NewLifecycle(t),
		Config:     baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm:      swarm.Config{Commands: swarm.CommandConfig{FetchWait: "50ms"}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewBus() error = %v", err)
	}
	defer func() { _ = bus.Drain(context.Background()) }()

	env := commandTestEnvelope("lifecycle-dedupe")
	env.DedupeKey = "shared-command-dedupe"
	if _, err := bus.Dispatch(context.Background(), env); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	handled := make(chan struct{}, 1)
	go func() {
		_ = bus.Run(ctx, func(context.Context, actorengine.Delivery) error {
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
	bus, err := NewBus(Params{
		LC:     fxtest.NewLifecycle(t),
		Config: baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm: swarm.Config{Commands: swarm.CommandConfig{
			AckWait:    "1s",
			FetchWait:  "50ms",
			MaxDeliver: 2,
		}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewBus() error = %v", err)
	}
	defer func() { _ = bus.Drain(context.Background()) }()

	if _, err := bus.Dispatch(context.Background(), commandTestEnvelope("running-event-fails")); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	if err := bus.js.DeleteStream(context.Background(), swarm.DefaultEventStream); err != nil {
		t.Fatalf("DeleteStream(events) error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	handled := make(chan struct{}, 1)
	var calls atomic.Int32
	go func() {
		_ = bus.Run(ctx, func(context.Context, actorengine.Delivery) error {
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
	bus, err := NewBus(Params{
		LC:     fxtest.NewLifecycle(t),
		Config: baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm: swarm.Config{Commands: swarm.CommandConfig{
			AckWait:    "100ms",
			FetchWait:  "50ms",
			MaxDeliver: 2,
		}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewBus() error = %v", err)
	}
	defer func() { _ = bus.Drain(context.Background()) }()

	if _, err := bus.Dispatch(context.Background(), commandTestEnvelope("acked-event-fails")); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	handled := make(chan struct{}, 1)
	var calls atomic.Int32
	go func() {
		_ = bus.Run(ctx, func(context.Context, actorengine.Delivery) error {
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

func TestBus_CommandRetryingEventFailureStillRedeliversAndSettles(t *testing.T) {
	bus, err := NewBus(Params{
		LC:     fxtest.NewLifecycle(t),
		Config: baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm: swarm.Config{Commands: swarm.CommandConfig{
			AckWait:    "100ms",
			FetchWait:  "50ms",
			MaxDeliver: 2,
		}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewBus() error = %v", err)
	}
	defer func() { _ = bus.Drain(context.Background()) }()

	if _, err := bus.Dispatch(context.Background(), commandTestEnvelope("retrying-event-fails")); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var calls atomic.Int32
	done := make(chan struct{}, 1)
	go func() {
		_ = bus.Run(ctx, func(context.Context, actorengine.Delivery) error {
			call := calls.Add(1)
			if call == 1 {
				if err := bus.js.DeleteStream(context.Background(), swarm.DefaultEventStream); err != nil {
					t.Errorf("DeleteStream(events) error = %v", err)
				}
				return swarm.TransientError(errors.New("retry please"))
			}
			done <- struct{}{}
			return nil
		})
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out waiting for command redelivery")
	}
	assertCommandStreamDrained(t, bus)
	if got := calls.Load(); got != 2 {
		t.Fatalf("handler calls = %d, want 2 (retry + success)", got)
	}
}

func TestBus_CommandRetryingEventIncludesNextAttemptMetadata(t *testing.T) {
	bus, err := NewBus(Params{
		LC:     fxtest.NewLifecycle(t),
		Config: baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm: swarm.Config{Commands: swarm.CommandConfig{
			AckWait:    "100ms",
			FetchWait:  "50ms",
			MaxDeliver: 2,
		}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewBus() error = %v", err)
	}
	defer func() { _ = bus.Drain(context.Background()) }()

	if _, err := bus.Dispatch(context.Background(), commandTestEnvelope("retrying-metadata")); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var calls atomic.Int32
	done := make(chan struct{}, 1)
	go func() {
		_ = bus.Run(ctx, func(context.Context, actorengine.Delivery) error {
			if calls.Add(1) == 1 {
				return swarm.TransientError(errors.New("retry please"))
			}
			done <- struct{}{}
			return nil
		})
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out waiting for command redelivery")
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("handler calls = %d, want 2 (retry + success)", got)
	}

	retryingConsumer, err := bus.js.CreateOrUpdateConsumer(context.Background(), swarm.DefaultEventStream, jetstream.ConsumerConfig{
		Durable:       "retrying-metadata-inspector",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubject: swarm.SubjectEventCommandRetrying,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateConsumer(retrying-metadata-inspector) error = %v", err)
	}
	batch, err := retryingConsumer.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("Fetch(command.retrying) error = %v", err)
	}
	msg, ok := <-batch.Messages()
	if !ok {
		t.Fatal("command.retrying event message not found")
	}
	got, err := swarm.DecodeEnvelope(string(msg.Data()))
	if err != nil {
		t.Fatalf("DecodeEnvelope(command.retrying) error = %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(got.PayloadJSON), &payload); err != nil {
		t.Fatalf("Unmarshal(command.retrying payload) error = %v", err)
	}
	delayMS, ok := payload["retry_delay_ms"].(float64)
	if !ok || delayMS <= 0 {
		t.Fatalf("retry_delay_ms = %v, want positive number", payload["retry_delay_ms"])
	}
	nextAttemptAt, ok := payload["next_attempt_at"].(string)
	if !ok || strings.TrimSpace(nextAttemptAt) == "" {
		t.Fatalf("next_attempt_at = %v, want non-empty RFC3339 timestamp", payload["next_attempt_at"])
	}
	if _, err := time.Parse(time.RFC3339Nano, nextAttemptAt); err != nil {
		t.Fatalf("next_attempt_at parse error = %v", err)
	}
	if err := msg.DoubleAck(context.Background()); err != nil {
		t.Fatalf("DoubleAck(command.retrying event) error = %v", err)
	}
}

func TestBus_CommandDeadletteredEventFailureStillSettlesDLQ(t *testing.T) {
	bus, err := NewBus(Params{
		LC:     fxtest.NewLifecycle(t),
		Config: baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm: swarm.Config{Commands: swarm.CommandConfig{
			AckWait:    "100ms",
			FetchWait:  "50ms",
			MaxDeliver: 1,
		}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewBus() error = %v", err)
	}
	defer func() { _ = bus.Drain(context.Background()) }()

	if _, err := bus.Dispatch(context.Background(), commandTestEnvelope("deadlettered-event-fails")); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	handled := make(chan struct{}, 1)
	go func() {
		_ = bus.Run(ctx, func(context.Context, actorengine.Delivery) error {
			if err := bus.js.DeleteStream(context.Background(), swarm.DefaultEventStream); err != nil {
				t.Errorf("DeleteStream(events) error = %v", err)
			}
			handled <- struct{}{}
			return swarm.PermanentError(errors.New("permanent failure"))
		})
	}()
	select {
	case <-handled:
	case <-ctx.Done():
		t.Fatal("timed out waiting for command handler")
	}
	waitStreamMessages(t, bus, swarm.DefaultDLQStream, 1)
	assertCommandStreamDrained(t, bus)
}

func TestRetryDelayAppliesExponentialDelayWithJitter(t *testing.T) {
	t.Parallel()

	low := swarm.RetryDelay(0)
	baseDelay := time.Second
	if low < baseDelay || low > baseDelay+(baseDelay/4) {
		t.Fatalf("RetryDelay(0) = %s, want in [%s, %s]", low, baseDelay, baseDelay+(baseDelay/4))
	}

	high := swarm.RetryDelay(16)
	maxDelay := time.Minute
	if high < maxDelay || high > maxDelay+(maxDelay/4) {
		t.Fatalf("RetryDelay(16) = %s, want in [%s, %s]", high, maxDelay, maxDelay+(maxDelay/4))
	}
}

func TestBus_CommandSuccessSettlesWithCanceledParent(t *testing.T) {
	bus, err := NewBus(Params{
		LC:     fxtest.NewLifecycle(t),
		Config: baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm: swarm.Config{Commands: swarm.CommandConfig{
			AckWait:    "100ms",
			FetchWait:  "50ms",
			MaxDeliver: 2,
		}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewBus() error = %v", err)
	}
	defer func() { _ = bus.Drain(context.Background()) }()

	if _, err := bus.Dispatch(context.Background(), commandTestEnvelope("settle-success-canceled")); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	handled := make(chan struct{}, 1)
	done := make(chan error, 1)
	var calls atomic.Int32
	go func() {
		done <- bus.Run(runCtx, func(ctx context.Context, _ actorengine.Delivery) error {
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
	bus, err := NewBus(Params{
		LC:     fxtest.NewLifecycle(t),
		Config: baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm: swarm.Config{Commands: swarm.CommandConfig{
			AckWait:    "100ms",
			FetchWait:  "50ms",
			MaxDeliver: 2,
		}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewBus() error = %v", err)
	}
	defer func() { _ = bus.Drain(context.Background()) }()

	if _, err := bus.Dispatch(context.Background(), commandTestEnvelope("settle-dlq-canceled")); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	handled := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- bus.Run(runCtx, func(ctx context.Context, _ actorengine.Delivery) error {
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

func TestBus_RunHandlesCommandsConcurrently(t *testing.T) {
	bus, err := NewBus(Params{
		LC:     fxtest.NewLifecycle(t),
		Config: baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm: swarm.Config{Commands: swarm.CommandConfig{
			FetchBatch:    2,
			MaxAckPending: 2,
			FetchWait:     "50ms",
		}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewBus() error = %v", err)
	}
	defer func() { _ = bus.Drain(context.Background()) }()

	for _, id := range []string{"concurrent-a", "concurrent-b"} {
		env := commandTestEnvelope(id)
		env.TaskID = id
		env.To = swarm.ActorAddress{Target: swarm.ActorTypeTask, Key: id}
		if _, err := bus.Dispatch(context.Background(), env); err != nil {
			t.Fatalf("Dispatch(%s) error = %v", id, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan string, 2)
	release := make(chan struct{})
	done := make(chan string, 2)
	go func() {
		_ = bus.Run(ctx, func(_ context.Context, msg actorengine.Delivery) error {
			started <- testDeliveryEnvelope(t, msg).ID
			<-release
			done <- testDeliveryEnvelope(t, msg).ID
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

func TestBus_RunLimitsInFlightToFetchBatch(t *testing.T) {
	bus, err := NewBus(Params{
		LC:     fxtest.NewLifecycle(t),
		Config: baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm: swarm.Config{Commands: swarm.CommandConfig{
			FetchBatch:    2,
			MaxAckPending: 8,
			FetchWait:     "50ms",
		}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewBus() error = %v", err)
	}
	defer func() { _ = bus.Drain(context.Background()) }()

	for i := 0; i < 6; i++ {
		id := fmt.Sprintf("bounded-%d", i)
		env := commandTestEnvelope(id)
		env.TaskID = id
		env.To = swarm.ActorAddress{Target: swarm.ActorTypeTask, Key: id}
		if _, err := bus.Dispatch(context.Background(), env); err != nil {
			t.Fatalf("Dispatch(%d) error = %v", i, err)
		}
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	started := atomic.Int64{}
	done := make(chan error, 1)
	go func() {
		done <- bus.Run(runCtx, func(_ context.Context, _ actorengine.Delivery) error {
			started.Add(1)
			<-release
			return nil
		})
	}()

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	for started.Load() < 2 {
		select {
		case <-waitCtx.Done():
			t.Fatalf("started handlers = %d, want at least 2", started.Load())
		case <-time.After(10 * time.Millisecond):
		}
	}

	// With fetch_batch=2 local fan-out must stay bounded even when max_ack_pending is larger.
	select {
	case <-time.After(200 * time.Millisecond):
	case <-waitCtx.Done():
		t.Fatalf("timed out waiting for bounded fan-out check: %v", waitCtx.Err())
	}
	if got := started.Load(); got != 2 {
		t.Fatalf("started handlers = %d, want 2 before release", got)
	}
	info, err := bus.consumer.Info(context.Background())
	if err != nil {
		t.Fatalf("command consumer info: %v", err)
	}
	if info.NumAckPending > 2 {
		t.Fatalf("ack_pending = %d, want <= 2", info.NumAckPending)
	}

	close(release)
	assertCommandStreamDrained(t, bus)
	cancel()
	waitConsumerCanceled(t, done)
}

func TestBus_CommandWorkerLimitUsesFetchBatch(t *testing.T) {
	t.Parallel()

	bus := &Bus{cfg: resolvedConfig{
		Swarm: swarm.Config{Commands: swarm.CommandConfig{
			FetchBatch:    3,
			MaxAckPending: 11,
		}},
	}}
	if got, want := bus.commandWorkerLimit(), 3; got != want {
		t.Fatalf("commandWorkerLimit() = %d, want %d", got, want)
	}
}

func TestBus_CommandDecodeFailurePublishesRawDLQAndDecodeEvent(t *testing.T) {
	bus, err := NewBus(Params{
		LC:         fxtest.NewLifecycle(t),
		Config:     baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm:      swarm.Config{Commands: swarm.CommandConfig{MaxDeliver: 1, FetchWait: "50ms"}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewBus() error = %v", err)
	}
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
		_ = bus.Run(ctx, func(context.Context, actorengine.Delivery) error {
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
		eventStatus, err := bus.streamStatus(context.Background(), swarm.DefaultEventStream)
		if err != nil {
			t.Fatalf("event stream status: %v", err)
		}
		if status.Messages == 1 && eventStatus.Messages == 1 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("DLQ/event messages = %d/%d, want 1/1", status.Messages, eventStatus.Messages)
		case <-time.After(25 * time.Millisecond):
		}
	}
	eventConsumer, err := bus.js.CreateOrUpdateConsumer(context.Background(), swarm.DefaultEventStream, jetstream.ConsumerConfig{
		Durable:       "decode-failed-inspector",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubject: swarm.SubjectEventCommandDecodeFailed,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateConsumer(decode-failed-inspector) error = %v", err)
	}
	msgBatch, err := eventConsumer.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("Fetch(decode_failed) error = %v", err)
	}
	msg, ok := <-msgBatch.Messages()
	if !ok {
		t.Fatal("decode_failed event message not found")
	}
	got, err := swarm.DecodeEnvelope(string(msg.Data()))
	if err != nil {
		t.Fatalf("DecodeEnvelope(decode_failed event) error = %v", err)
	}
	if got.Meta["event_type"] != "command.decode_failed" {
		t.Fatalf("decode_failed event_type = %q, want %q", got.Meta["event_type"], "command.decode_failed")
	}
	if err := msg.DoubleAck(context.Background()); err != nil {
		t.Fatalf("DoubleAck(decode_failed event) error = %v", err)
	}
	cancel()
	<-done
}

func TestBus_DispatchReportsDuplicate(t *testing.T) {
	bus, err := NewBus(Params{
		LC:         fxtest.NewLifecycle(t),
		Config:     baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm:      swarm.Config{},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewBus() error = %v", err)
	}
	defer func() { _ = bus.Drain(context.Background()) }()

	env := commandTestEnvelope("env-duplicate")
	env.DedupeKey = "dedupe-duplicate"
	env.CorrelationID = "corr-duplicate"
	env.CausationID = "cause-duplicate"
	first, err := bus.Dispatch(context.Background(), env)
	if err != nil {
		t.Fatalf("Dispatch(first) error = %v", err)
	}
	second, err := bus.Dispatch(context.Background(), env)
	if err != nil {
		t.Fatalf("Dispatch(second) error = %v", err)
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

	noopConsumer, err := bus.js.CreateOrUpdateConsumer(context.Background(), swarm.DefaultEventStream, jetstream.ConsumerConfig{
		Durable:       "noop-dedupe-inspector",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubject: swarm.SubjectEventCommandNoop,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateConsumer(noop-dedupe-inspector) error = %v", err)
	}
	batch, err := noopConsumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatalf("Fetch(command.noop) error = %v", err)
	}
	msg, ok := <-batch.Messages()
	if !ok {
		t.Fatal("command.noop event message = nil, want duplicate noop lifecycle event")
	}
	got, err := swarm.DecodeEnvelope(string(msg.Data()))
	if err != nil {
		t.Fatalf("DecodeEnvelope(command.noop) error = %v", err)
	}
	if got.Meta["event_type"] != "command.noop" {
		t.Fatalf("command.noop event_type = %q, want %q", got.Meta["event_type"], "command.noop")
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(got.PayloadJSON), &payload); err != nil {
		t.Fatalf("Unmarshal(command.noop payload) error = %v", err)
	}
	if reason, _ := payload["reason"].(string); reason != "duplicate publish suppressed" {
		t.Fatalf("command.noop payload reason = %q, want %q", reason, "duplicate publish suppressed")
	}
	if correlationID, _ := payload["correlation_id"].(string); correlationID != env.CorrelationID {
		t.Fatalf("command.noop payload correlation_id = %q, want %q", correlationID, env.CorrelationID)
	}
	if causationID, _ := payload["causation_id"].(string); causationID != env.CausationID {
		t.Fatalf("command.noop payload causation_id = %q, want %q", causationID, env.CausationID)
	}
	if actorKey, _ := payload["actor_key"].(string); actorKey != env.To.Key {
		t.Fatalf("command.noop payload actor_key = %q, want %q", actorKey, env.To.Key)
	}
	if err := msg.DoubleAck(context.Background()); err != nil {
		t.Fatalf("DoubleAck(command.noop) error = %v", err)
	}
}

func TestBus_PublishEventDeduplicatesByEnvelopeID(t *testing.T) {
	bus, err := NewBus(Params{
		LC:         fxtest.NewLifecycle(t),
		Config:     baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm:      swarm.Config{},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewBus() error = %v", err)
	}
	defer func() { _ = bus.Drain(context.Background()) }()

	env := commandTestEnvelope("event-dedupe")
	env.Namespace = swarm.NamespaceTelemetry
	env.Kind = testEventKindTask
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
	bus, err := NewBus(Params{
		LC:         fxtest.NewLifecycle(t),
		Config:     baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm:      swarm.Config{Commands: swarm.CommandConfig{MaxDeliver: 1, FetchWait: "50ms"}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewBus() error = %v", err)
	}
	defer func() { _ = bus.Drain(context.Background()) }()

	env := commandTestEnvelope("env-retry-exhausted")
	if _, err := bus.Dispatch(context.Background(), env); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	handled := make(chan struct{}, 1)
	go func() {
		_ = bus.Run(ctx, func(_ context.Context, msg actorengine.Delivery) error {
			handled <- struct{}{}
			if msg.Attempt() != 1 || msg.MaxAttempts() != 1 {
				t.Errorf("delivery metadata = %d/%d, want 1/1", msg.Attempt(), msg.MaxAttempts())
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

func TestBus_PublishDLQIncludesOriginalEnvelopeAndReason(t *testing.T) {
	bus, err := NewBus(Params{
		LC:         fxtest.NewLifecycle(t),
		Config:     baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm:      swarm.Config{},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewBus() error = %v", err)
	}
	defer func() { _ = bus.Drain(context.Background()) }()

	env := commandTestEnvelope("dlq-shape")
	reason := "permanent failure: policy denied"
	if err := bus.publishDLQ(context.Background(), env, reason, true); err != nil {
		t.Fatalf("publishDLQ() error = %v", err)
	}

	dlqConsumer, err := bus.js.CreateOrUpdateConsumer(context.Background(), swarm.DefaultDLQStream, jetstream.ConsumerConfig{
		Durable:       "dlq-shape-inspector",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubject: swarm.SubjectDLQCommand,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateConsumer(dlq-shape-inspector) error = %v", err)
	}
	msgBatch, err := dlqConsumer.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("Fetch(dlq message) error = %v", err)
	}
	msg, ok := <-msgBatch.Messages()
	if !ok {
		t.Fatal("dlq message not found")
	}
	got, err := swarm.DecodeEnvelope(string(msg.Data()))
	if err != nil {
		t.Fatalf("DecodeEnvelope(dlq message) error = %v", err)
	}
	if got.ID != env.ID || got.Namespace != env.Namespace || got.Kind != env.Kind {
		t.Fatalf("dlq envelope identity = %+v, want original id/namespace/kind", got)
	}
	if got.From != env.From || got.To != env.To {
		t.Fatalf("dlq envelope routing = from:%+v to:%+v, want from:%+v to:%+v", got.From, got.To, env.From, env.To)
	}
	if got.TaskID != env.TaskID || got.PayloadJSON != env.PayloadJSON {
		t.Fatalf("dlq envelope payload = task:%q payload:%q, want task:%q payload:%q", got.TaskID, got.PayloadJSON, env.TaskID, env.PayloadJSON)
	}
	if gotReason := msg.Headers().Get("Balda-DLQ-Reason"); gotReason != reason {
		t.Fatalf("dlq header reason = %q, want %q", gotReason, reason)
	}
	if err := msg.DoubleAck(context.Background()); err != nil {
		t.Fatalf("DoubleAck(dlq message) error = %v", err)
	}
}

func TestBus_DLQIncludesErrorClassAndSourceMetadata(t *testing.T) {
	bus, err := NewBus(Params{
		LC:     fxtest.NewLifecycle(t),
		Config: baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm: swarm.Config{Commands: swarm.CommandConfig{
			AckWait:    "100ms",
			FetchWait:  "50ms",
			MaxDeliver: 1,
		}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewBus() error = %v", err)
	}
	defer func() { _ = bus.Drain(context.Background()) }()

	env := commandTestEnvelope("dlq-metadata")
	if _, err := bus.Dispatch(context.Background(), env); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan struct{}, 1)
	go func() {
		_ = bus.Run(ctx, func(context.Context, actorengine.Delivery) error {
			done <- struct{}{}
			return swarm.PermanentError(errors.New("policy denied"))
		})
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out waiting for command handler")
	}
	waitStreamMessages(t, bus, swarm.DefaultDLQStream, 1)

	dlqConsumer, err := bus.js.CreateOrUpdateConsumer(context.Background(), swarm.DefaultDLQStream, jetstream.ConsumerConfig{
		Durable:       "dlq-metadata-inspector",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubject: swarm.SubjectDLQCommand,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateConsumer(dlq-metadata-inspector) error = %v", err)
	}
	msgBatch, err := dlqConsumer.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("Fetch(dlq metadata message) error = %v", err)
	}
	msg, ok := <-msgBatch.Messages()
	if !ok {
		t.Fatal("dlq metadata message not found")
	}
	if got := msg.Headers().Get("Balda-DLQ-Error-Class"); got != string(swarm.ErrorKindPermanent) {
		t.Fatalf("Balda-DLQ-Error-Class = %q, want %q", got, swarm.ErrorKindPermanent)
	}
	if got := msg.Headers().Get("Balda-DLQ-Source-Stream"); got != swarm.DefaultCommandStream {
		t.Fatalf("Balda-DLQ-Source-Stream = %q, want %q", got, swarm.DefaultCommandStream)
	}
	if got := msg.Headers().Get("Balda-DLQ-Source-Consumer"); got != swarm.DefaultCommandConsumer {
		t.Fatalf("Balda-DLQ-Source-Consumer = %q, want %q", got, swarm.DefaultCommandConsumer)
	}
	if got := msg.Headers().Get("Balda-DLQ-Source-Subject"); got != swarm.SubjectCommandGoal {
		t.Fatalf("Balda-DLQ-Source-Subject = %q, want %q", got, swarm.SubjectCommandGoal)
	}
	if got := msg.Headers().Get("Balda-DLQ-Num-Delivered"); got != "1" {
		t.Fatalf("Balda-DLQ-Num-Delivered = %q, want %q", got, "1")
	}
	if err := msg.DoubleAck(context.Background()); err != nil {
		t.Fatalf("DoubleAck(dlq metadata message) error = %v", err)
	}
}

func TestBus_EventProjectionPermanentFailurePublishesDLQ(t *testing.T) {
	bus, err := NewBus(Params{
		LC:         fxtest.NewLifecycle(t),
		Config:     baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm:      swarm.Config{Commands: swarm.CommandConfig{MaxDeliver: 1, FetchWait: "50ms"}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewBus() error = %v", err)
	}
	defer func() { _ = bus.Drain(context.Background()) }()

	env := commandTestEnvelope("event-projection-failed")
	env.Namespace = swarm.NamespaceTelemetry
	env.Kind = testEventKindTask
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

func TestBus_EventProjectionFailureDoesNotBlockCommandExecution(t *testing.T) {
	bus, err := NewBus(Params{
		LC:         fxtest.NewLifecycle(t),
		Config:     baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm:      swarm.Config{Commands: swarm.CommandConfig{MaxDeliver: 1, FetchWait: "50ms"}},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewBus() error = %v", err)
	}
	defer func() { _ = bus.Drain(context.Background()) }()

	projectionCtx, projectionCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer projectionCancel()
	projectionHandled := make(chan struct{}, 1)
	go func() {
		_ = bus.RunEventConsumer(projectionCtx, func(context.Context, string, swarm.Envelope) error {
			select {
			case projectionHandled <- struct{}{}:
			default:
			}
			return swarm.PermanentError(errors.New("projection failed"))
		})
	}()
	eventEnv := commandTestEnvelope("projection-failure-does-not-block")
	eventEnv.Namespace = swarm.NamespaceTelemetry
	eventEnv.Kind = testEventKindTask
	eventEnv.Meta = map[string]string{"event_type": swarm.TaskEventAgentProgress}
	if err := bus.PublishEvent(context.Background(), swarm.SubjectEventTaskUpdated, eventEnv); err != nil {
		t.Fatalf("PublishEvent() error = %v", err)
	}
	select {
	case <-projectionHandled:
	case <-projectionCtx.Done():
		t.Fatal("timed out waiting for projection failure")
	}

	commandCtx, commandCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer commandCancel()
	commandHandled := make(chan struct{}, 1)
	go func() {
		_ = bus.Run(commandCtx, func(context.Context, actorengine.Delivery) error {
			commandHandled <- struct{}{}
			return nil
		})
	}()
	if _, err := bus.Dispatch(context.Background(), commandTestEnvelope("command-after-projection-failure")); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	select {
	case <-commandHandled:
	case <-commandCtx.Done():
		t.Fatal("timed out waiting for command handling after projection failure")
	}
}

func TestBus_EnsureRuntimeRequiresTransport(t *testing.T) {
	cfg, err := resolveConfig(
		baldaeventbus.Config{Embedded: true, JetStream: true},
		swarm.Config{},
		t.TempDir(),
	)
	if err != nil {
		t.Fatalf("resolveConfig() error = %v", err)
	}
	bus := &Bus{cfg: cfg}
	err = bus.ensureRuntime(context.Background())
	if err == nil || !strings.Contains(err.Error(), "runtime transport is required") {
		t.Fatalf("ensureRuntime() error = %v, want runtime transport required", err)
	}
}

func TestBus_EnsureRuntimeCreatesRequiredStreamsAndConsumers(t *testing.T) {
	bus, err := NewBus(Params{
		LC:         fxtest.NewLifecycle(t),
		Config:     baldaeventbus.Config{Embedded: true, JetStream: true},
		Swarm:      swarm.Config{},
		WorkingDir: t.TempDir(),
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewBus() error = %v", err)
	}
	defer func() { _ = bus.Drain(context.Background()) }()

	commandStream, err := bus.js.Stream(context.Background(), swarm.DefaultCommandStream)
	if err != nil {
		t.Fatalf("Stream(%s) error = %v", swarm.DefaultCommandStream, err)
	}
	commandInfo, err := commandStream.Info(context.Background())
	if err != nil {
		t.Fatalf("Info(%s) error = %v", swarm.DefaultCommandStream, err)
	}
	if commandInfo.Config.Retention != jetstream.WorkQueuePolicy {
		t.Fatalf("%s retention = %v, want %v", swarm.DefaultCommandStream, commandInfo.Config.Retention, jetstream.WorkQueuePolicy)
	}
	if !slices.Equal(commandInfo.Config.Subjects, []string{swarm.SubjectCommandAll}) {
		t.Fatalf("%s subjects = %#v, want %#v", swarm.DefaultCommandStream, commandInfo.Config.Subjects, []string{swarm.SubjectCommandAll})
	}

	eventStream, err := bus.js.Stream(context.Background(), swarm.DefaultEventStream)
	if err != nil {
		t.Fatalf("Stream(%s) error = %v", swarm.DefaultEventStream, err)
	}
	eventInfo, err := eventStream.Info(context.Background())
	if err != nil {
		t.Fatalf("Info(%s) error = %v", swarm.DefaultEventStream, err)
	}
	if eventInfo.Config.Retention != jetstream.LimitsPolicy {
		t.Fatalf("%s retention = %v, want %v", swarm.DefaultEventStream, eventInfo.Config.Retention, jetstream.LimitsPolicy)
	}
	if !slices.Equal(eventInfo.Config.Subjects, []string{swarm.SubjectEventAll}) {
		t.Fatalf("%s subjects = %#v, want %#v", swarm.DefaultEventStream, eventInfo.Config.Subjects, []string{swarm.SubjectEventAll})
	}

	dlqStream, err := bus.js.Stream(context.Background(), swarm.DefaultDLQStream)
	if err != nil {
		t.Fatalf("Stream(%s) error = %v", swarm.DefaultDLQStream, err)
	}
	dlqInfo, err := dlqStream.Info(context.Background())
	if err != nil {
		t.Fatalf("Info(%s) error = %v", swarm.DefaultDLQStream, err)
	}
	if dlqInfo.Config.Retention != jetstream.LimitsPolicy {
		t.Fatalf("%s retention = %v, want %v", swarm.DefaultDLQStream, dlqInfo.Config.Retention, jetstream.LimitsPolicy)
	}
	if !slices.Equal(dlqInfo.Config.Subjects, []string{swarm.SubjectDLQAll}) {
		t.Fatalf("%s subjects = %#v, want %#v", swarm.DefaultDLQStream, dlqInfo.Config.Subjects, []string{swarm.SubjectDLQAll})
	}

	workerInfo, err := bus.consumer.Info(context.Background())
	if err != nil {
		t.Fatalf("worker consumer Info() error = %v", err)
	}
	if workerInfo.Name != swarm.DefaultCommandConsumer {
		t.Fatalf("worker consumer name = %q, want %q", workerInfo.Name, swarm.DefaultCommandConsumer)
	}
	if workerInfo.Config.FilterSubject != swarm.SubjectCommandAll {
		t.Fatalf("worker filter subject = %q, want %q", workerInfo.Config.FilterSubject, swarm.SubjectCommandAll)
	}
	if workerInfo.Config.AckPolicy != jetstream.AckExplicitPolicy {
		t.Fatalf("worker ack policy = %v, want %v", workerInfo.Config.AckPolicy, jetstream.AckExplicitPolicy)
	}

	projectorInfo, err := bus.eventConsumer.Info(context.Background())
	if err != nil {
		t.Fatalf("projector consumer Info() error = %v", err)
	}
	if projectorInfo.Name != swarm.DefaultEventProjectorConsumer {
		t.Fatalf("projector consumer name = %q, want %q", projectorInfo.Name, swarm.DefaultEventProjectorConsumer)
	}
	if projectorInfo.Config.FilterSubject != swarm.SubjectEventAll {
		t.Fatalf("projector filter subject = %q, want %q", projectorInfo.Config.FilterSubject, swarm.SubjectEventAll)
	}
	if projectorInfo.Config.AckPolicy != jetstream.AckExplicitPolicy {
		t.Fatalf("projector ack policy = %v, want %v", projectorInfo.Config.AckPolicy, jetstream.AckExplicitPolicy)
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
		Namespace:   swarm.NamespaceGoalCommand,
		Kind:        swarm.KindGoal,
		From:        swarm.SystemAddress("test"),
		To:          swarm.ActorAddress{Target: swarm.ActorTypeGoal, Key: "task-1"},
		TaskID:      "task-1",
		PayloadJSON: `{"ok":true}`,
	}
}

func testDeliveryEnvelope(t *testing.T, delivery actorengine.Delivery) swarm.Envelope {
	t.Helper()
	env, ok := delivery.Envelope().(swarm.Envelope)
	if !ok {
		t.Fatalf("delivery envelope type = %T, want swarm.Envelope", delivery.Envelope())
	}
	return env
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
			t.Fatalf("Run() error = %v, want context canceled", err)
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
