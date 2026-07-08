package engine_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/normahq/balda/pkg/actorlayer"
	"github.com/normahq/balda/pkg/actorlayer/dispatch"
	"github.com/normahq/balda/pkg/actorlayer/engine"
)

type testDelivery struct {
	env         engine.Envelope
	attempt     int
	maxAttempts int

	mu               sync.Mutex
	acked            bool
	ackErr           error
	retryErr         error
	deadLetterErr    error
	retryDelay       time.Duration
	retryReason      string
	deadLetterReason string
}

func (d *testDelivery) Envelope() engine.Envelope { return d.env }

func (d *testDelivery) Attempt() int {
	if d.attempt <= 0 {
		return 1
	}
	return d.attempt
}

func (d *testDelivery) MaxAttempts() int { return d.maxAttempts }

func (*testDelivery) InProgress(context.Context) error { return nil }

func (d *testDelivery) Ack(context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.ackErr != nil {
		return d.ackErr
	}
	d.acked = true
	return nil
}

func (d *testDelivery) Retry(_ context.Context, delay time.Duration, reason string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.retryErr != nil {
		return d.retryErr
	}
	d.retryDelay = delay
	d.retryReason = reason
	return nil
}

func (d *testDelivery) DeadLetter(_ context.Context, reason string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.deadLetterErr != nil {
		return d.deadLetterErr
	}
	d.deadLetterReason = reason
	return nil
}

type laneResolver struct{}

func (laneResolver) LaneKey(delivery engine.Delivery) string {
	if delivery == nil {
		return ""
	}
	return delivery.Envelope().To.Key
}

type recordingSink struct {
	mu     sync.Mutex
	events []engine.Event
}

func (s *recordingSink) Publish(_ context.Context, event engine.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

func (s *recordingSink) types() []engine.EventType {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]engine.EventType, 0, len(s.events))
	for _, event := range s.events {
		out = append(out, event.Type)
	}
	return out
}

func (s *recordingSink) snapshot() []engine.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]engine.Event(nil), s.events...)
}

func TestRuntimeHandleAcksSuccessfulDelivery(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	runtime := newRuntimeForTest(t, sink)
	delivery := newDelivery("ack", "lane-1")
	if err := runtime.Handle(context.Background(), delivery, func(context.Context, engine.Delivery) error {
		return nil
	}); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if !delivery.acked {
		t.Fatal("Ack() was not called")
	}
	if got, want := sink.types(), []engine.EventType{engine.EventRunning, engine.EventAcked}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for _, event := range sink.snapshot() {
		if event.EnvelopeID != "ack" {
			t.Fatalf("event envelope id = %q, want %q", event.EnvelopeID, "ack")
		}
		if event.Namespace != "test.command" {
			t.Fatalf("event namespace = %q, want %q", event.Namespace, "test.command")
		}
		if event.Kind != "message" {
			t.Fatalf("event kind = %q, want %q", event.Kind, "message")
		}
		if event.LaneKey != "lane-1" {
			t.Fatalf("event lane key = %q, want %q", event.LaneKey, "lane-1")
		}
		if event.From != (engine.ActorAddress{Target: "test", Key: "source"}) {
			t.Fatalf("event from = %#v, want test/source", event.From)
		}
		if event.To != (engine.ActorAddress{Target: "session", Key: "lane-1"}) {
			t.Fatalf("event to = %#v, want session/lane-1", event.To)
		}
		if event.Attempt != 1 {
			t.Fatalf("event attempt = %d, want 1", event.Attempt)
		}
	}
}

func TestRuntimeHandleAckFailureDoesNotEmitAcked(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	runtime := newRuntimeForTest(t, sink)
	errAck := errors.New("ack failed")
	delivery := newDelivery("ack-failure", "lane-1")
	delivery.ackErr = errAck
	err := runtime.Handle(context.Background(), delivery, func(context.Context, engine.Delivery) error {
		return nil
	})
	if !errors.Is(err, errAck) {
		t.Fatalf("Handle() error = %v, want %v", err, errAck)
	}
	if got, want := sink.types(), []engine.EventType{engine.EventRunning}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestRuntimeHandleRetriesRetryableDelivery(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	runtime := newRuntimeForTest(t, sink)
	delivery := newDelivery("retry", "lane-1")
	errTemporary := errors.New("temporary")
	if err := runtime.Handle(context.Background(), delivery, func(context.Context, engine.Delivery) error {
		return errTemporary
	}); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if delivery.retryDelay != 7*time.Millisecond {
		t.Fatalf("retry delay = %s, want 7ms", delivery.retryDelay)
	}
	if delivery.retryReason != errTemporary.Error() {
		t.Fatalf("retry reason = %q, want %q", delivery.retryReason, errTemporary.Error())
	}
	if got, want := sink.types(), []engine.EventType{engine.EventRunning, engine.EventRetrying}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestRuntimeHandleRetryFailureDoesNotEmitRetrying(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	runtime := newRuntimeForTest(t, sink)
	errRetry := errors.New("retry failed")
	delivery := newDelivery("retry-failure", "lane-1")
	delivery.retryErr = errRetry
	err := runtime.Handle(context.Background(), delivery, func(context.Context, engine.Delivery) error {
		return errors.New("temporary")
	})
	if !errors.Is(err, errRetry) {
		t.Fatalf("Handle() error = %v, want %v", err, errRetry)
	}
	if got, want := sink.types(), []engine.EventType{engine.EventRunning}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestRuntimeHandleDeadLettersNonRetryableDelivery(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	runtime := newRuntimeForTest(t, sink)
	delivery := newDelivery("deadletter", "lane-1")
	errPolicy := actorlayer.PolicyError(errors.New("not allowed"))
	if err := runtime.Handle(context.Background(), delivery, func(context.Context, engine.Delivery) error {
		return errPolicy
	}); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if delivery.deadLetterReason != "not allowed" {
		t.Fatalf("deadletter reason = %q, want %q", delivery.deadLetterReason, "not allowed")
	}
	if got, want := sink.types(), []engine.EventType{engine.EventRunning, engine.EventDeadLettered}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestRuntimeHandleDeadLetterFailureDoesNotEmitDeadLettered(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	runtime := newRuntimeForTest(t, sink)
	errDeadLetter := errors.New("deadletter failed")
	delivery := newDelivery("deadletter-failure", "lane-1")
	delivery.deadLetterErr = errDeadLetter
	err := runtime.Handle(context.Background(), delivery, func(context.Context, engine.Delivery) error {
		return actorlayer.PolicyError(errors.New("not allowed"))
	})
	if !errors.Is(err, errDeadLetter) {
		t.Fatalf("Handle() error = %v, want %v", err, errDeadLetter)
	}
	if got, want := sink.types(), []engine.EventType{engine.EventRunning}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestRuntimeHandleDeadLettersRetryExhaustion(t *testing.T) {
	t.Parallel()

	runtime := newRuntimeForTest(t, &recordingSink{})
	delivery := newDelivery("retry-exhausted", "lane-1")
	delivery.attempt = 3
	delivery.maxAttempts = 3
	if err := runtime.Handle(context.Background(), delivery, func(context.Context, engine.Delivery) error {
		return errors.New("temporary")
	}); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if !strings.Contains(delivery.deadLetterReason, "retry exhausted: temporary") {
		t.Fatalf("deadletter reason = %q, want retry exhausted", delivery.deadLetterReason)
	}
}

func TestRuntimeSerializesSameLane(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	runtime := newRuntimeForTest(t, sink)
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondEntered := make(chan struct{})
	handler := func(_ context.Context, delivery engine.Delivery) error {
		switch delivery.Envelope().ID {
		case "first":
			close(firstEntered)
			<-releaseFirst
		case "second":
			close(secondEntered)
		}
		return nil
	}

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- runtime.Handle(context.Background(), newDelivery("first", "same"), handler)
	}()
	waitForClosed(t, firstEntered, "first delivery entered")
	if got, want := sink.types(), []engine.EventType{engine.EventRunning}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events after first entered = %v, want %v", got, want)
	}

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- runtime.Handle(context.Background(), newDelivery("second", "same"), handler)
	}()

	select {
	case <-secondEntered:
		t.Fatal("second same-lane delivery entered before first completed")
	case <-time.After(20 * time.Millisecond):
	}
	if got, want := sink.types(), []engine.EventType{engine.EventRunning}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events while second queued = %v, want %v", got, want)
	}
	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Handle() error = %v", err)
	}
	waitForClosed(t, secondEntered, "second delivery entered")
	if err := <-secondDone; err != nil {
		t.Fatalf("second Handle() error = %v", err)
	}
}

func TestRuntimeAllowsDifferentLanesInParallel(t *testing.T) {
	t.Parallel()

	runtime := newRuntimeForTest(t, &recordingSink{})
	startedA := make(chan struct{})
	startedB := make(chan struct{})
	release := make(chan struct{})
	handler := func(_ context.Context, delivery engine.Delivery) error {
		switch delivery.Envelope().ID {
		case "a":
			close(startedA)
		case "b":
			close(startedB)
		}
		<-release
		return nil
	}

	doneA := make(chan error, 1)
	doneB := make(chan error, 1)
	go func() { doneA <- runtime.Handle(context.Background(), newDelivery("a", "lane-a"), handler) }()
	go func() { doneB <- runtime.Handle(context.Background(), newDelivery("b", "lane-b"), handler) }()
	waitForClosed(t, startedA, "lane-a delivery entered")
	waitForClosed(t, startedB, "lane-b delivery entered")
	close(release)
	if err := <-doneA; err != nil {
		t.Fatalf("lane-a Handle() error = %v", err)
	}
	if err := <-doneB; err != nil {
		t.Fatalf("lane-b Handle() error = %v", err)
	}
}

func TestRuntimeValidationPaths(t *testing.T) {
	t.Parallel()

	if _, err := engine.New(engine.Config{}); err == nil {
		t.Fatal("New() error = nil, want missing resolver error")
	}
	var nilRuntime *engine.Runtime
	if err := nilRuntime.Run(context.Background(), nil, func(context.Context, engine.Delivery) error { return nil }); err == nil {
		t.Fatal("nil Runtime.Run() error = nil, want error")
	}
	if err := nilRuntime.Handle(context.Background(), newDelivery("nil-runtime", "lane"), func(context.Context, engine.Delivery) error { return nil }); err == nil {
		t.Fatal("nil Runtime.Handle() error = nil, want error")
	}
	nilRuntime.EmitInProgress(context.Background(), newDelivery("nil-runtime-progress", "lane"))
	runtime := newRuntimeForTest(t, &recordingSink{})
	if err := runtime.Handle(context.Background(), nil, func(context.Context, engine.Delivery) error { return nil }); err != nil {
		t.Fatalf("Handle(nil delivery) error = %v, want nil", err)
	}
	if err := runtime.Handle(context.Background(), newDelivery("missing-handler", "lane"), nil); err == nil {
		t.Fatal("Handle(nil handler) error = nil, want error")
	}
	if err := runtime.Run(context.Background(), nil, func(context.Context, engine.Delivery) error { return nil }); err == nil {
		t.Fatal("Run(nil source) error = nil, want error")
	}
}

func TestRuntimeHandleRejectsInvalidEnvelopeBeforeExecution(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	runtime := newRuntimeForTest(t, sink)
	delivery := newDelivery("invalid-runtime", "lane")
	delivery.env.PayloadJSON = "{not-json"
	called := false
	err := runtime.Handle(context.Background(), delivery, func(context.Context, engine.Delivery) error {
		called = true
		return nil
	})
	if err == nil {
		t.Fatal("Handle() error = nil, want invalid envelope error")
	}
	var actorErr *actorlayer.ActorError
	if !errors.As(err, &actorErr) || actorErr.Kind != actorlayer.ErrorKindDecode {
		t.Fatalf("Handle() error = %v, want decode actor error", err)
	}
	if called {
		t.Fatal("handler was called for invalid envelope")
	}
	if delivery.acked || delivery.retryReason != "" || delivery.deadLetterReason != "" {
		t.Fatalf("delivery settled = ack:%v retry:%q deadletter:%q, want unsettled", delivery.acked, delivery.retryReason, delivery.deadLetterReason)
	}
	if got := sink.types(); len(got) != 0 {
		t.Fatalf("events = %v, want none", got)
	}
}

func TestRuntimeHandleCanceledContextDoesNotEmitRunning(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	runtime := newRuntimeForTest(t, sink)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := runtime.Handle(ctx, newDelivery("canceled", "lane"), func(context.Context, engine.Delivery) error {
		t.Fatal("handler was called for canceled context")
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Handle() error = %v, want context canceled", err)
	}
	if got := sink.types(); len(got) != 0 {
		t.Fatalf("events = %v, want none", got)
	}
}

func TestRuntimeEmitInProgressIncludesDeliveryMetadata(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	runtime := newRuntimeForTest(t, sink)
	runtime.EmitInProgress(context.Background(), newDelivery("progress", "lane"))
	events := sink.snapshot()
	if len(events) != 1 {
		t.Fatalf("events length = %d, want 1", len(events))
	}
	event := events[0]
	if event.Type != engine.EventInProgress {
		t.Fatalf("event type = %q, want %q", event.Type, engine.EventInProgress)
	}
	if event.EnvelopeID != "progress" || event.LaneKey != "lane" || event.To.Key != "lane" {
		t.Fatalf("event metadata = %#v, want progress/lane", event)
	}
}

func TestDispatchRuntimeResolvesActors(t *testing.T) {
	t.Parallel()

	registry := dispatch.NewMemoryRegistry()
	actor := &recordingActor{address: "session:*"}
	if err := registry.Register(actor); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	runtime, err := engine.NewDispatchRuntime(engine.RuntimeConfig{
		Registry:  registry,
		AddressOf: func(env engine.Envelope) (string, error) { return env.To.String() },
		Retry: engine.RetryPolicy{
			IsRetryable: actorlayer.IsRetryableError,
			Backoff:     func(int) time.Duration { return time.Millisecond },
		},
	})
	if err != nil {
		t.Fatalf("NewDispatchRuntime() error = %v", err)
	}
	if err := runtime.Handle(context.Background(), newDelivery("dispatch", "one")); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if actor.calls != 1 {
		t.Fatalf("actor calls = %d, want 1", actor.calls)
	}
}

func TestDispatchRuntimeRejectsInvalidEnvelopeBeforeActor(t *testing.T) {
	t.Parallel()

	registry := dispatch.NewMemoryRegistry()
	actor := &recordingActor{address: "session:*"}
	if err := registry.Register(actor); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	runtime, err := engine.NewDispatchRuntime(engine.RuntimeConfig{
		Registry:  registry,
		AddressOf: func(env engine.Envelope) (string, error) { return env.To.String() },
		Retry: engine.RetryPolicy{
			IsRetryable: actorlayer.IsRetryableError,
			Backoff:     func(int) time.Duration { return time.Millisecond },
		},
	})
	if err != nil {
		t.Fatalf("NewDispatchRuntime() error = %v", err)
	}
	delivery := newDelivery("invalid-dispatch", "one")
	delivery.env.PayloadJSON = "{not-json"
	err = runtime.Handle(context.Background(), delivery)
	if err == nil {
		t.Fatal("Handle() error = nil, want invalid envelope error")
	}
	var actorErr *actorlayer.ActorError
	if !errors.As(err, &actorErr) || actorErr.Kind != actorlayer.ErrorKindDecode {
		t.Fatalf("Handle() error = %v, want decode actor error", err)
	}
	if actor.calls != 0 {
		t.Fatalf("actor calls = %d, want 0", actor.calls)
	}
	if delivery.acked {
		t.Fatal("delivery was acked, want unsettled invalid envelope")
	}
}

type recordingActor struct {
	address string
	calls   int
}

func (a *recordingActor) Address() string { return a.address }

func (a *recordingActor) Handle(context.Context, actorlayer.Envelope) error {
	a.calls++
	return nil
}

func newRuntimeForTest(t *testing.T, sink engine.EventSink) *engine.Runtime {
	t.Helper()
	runtime, err := engine.New(engine.Config{
		Resolver: laneResolver{},
		Sink:     sink,
		Retry: engine.RetryPolicy{
			IsRetryable: actorlayer.IsRetryableError,
			Backoff:     func(int) time.Duration { return 7 * time.Millisecond },
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return runtime
}

func newDelivery(id, lane string) *testDelivery {
	return &testDelivery{
		env: engine.Envelope{
			ID:          id,
			Namespace:   "test.command",
			Kind:        "message",
			From:        engine.ActorAddress{Target: "test", Key: "source"},
			To:          engine.ActorAddress{Target: "session", Key: lane},
			PayloadJSON: `{"ok":true}`,
		},
	}
}

func waitForClosed(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}
