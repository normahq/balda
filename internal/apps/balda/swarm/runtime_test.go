package swarm

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/pkg/actorlayer"
	"github.com/normahq/balda/pkg/actorlayer/dispatch"
	actorengine "github.com/normahq/balda/pkg/actorlayer/engine"
)

type testActor struct {
	address string
	err     error
	calls   int
	run     func(context.Context, actorlayer.Envelope) error
}

func (a *testActor) Address() string { return a.address }
func (a *testActor) Handle(ctx context.Context, env actorlayer.Envelope) error {
	a.calls++
	if a.run != nil {
		return a.run(ctx, env)
	}
	return a.err
}

type testDelivery struct {
	env           actorlayer.Envelope
	numDelivered  int
	maxDeliveries int
	inProgress    func(context.Context) error
	ack           func(context.Context) error
	retry         func(context.Context, time.Duration, string) error
	deadletter    func(context.Context, string) error
}

func (m testDelivery) Envelope() actorengine.Envelope { return m.env }
func (m testDelivery) InProgress(ctx context.Context) error {
	if m.inProgress != nil {
		return m.inProgress(ctx)
	}
	return nil
}
func (m testDelivery) Attempt() int {
	if m.numDelivered <= 0 {
		return 1
	}
	return m.numDelivered
}
func (m testDelivery) MaxAttempts() int { return m.maxDeliveries }
func (m testDelivery) Ack(ctx context.Context) error {
	if m.ack != nil {
		return m.ack(ctx)
	}
	return nil
}

func (m testDelivery) Retry(ctx context.Context, delay time.Duration, reason string) error {
	if m.retry != nil {
		return m.retry(ctx, delay, reason)
	}
	return nil
}

func (m testDelivery) DeadLetter(ctx context.Context, reason string) error {
	if m.deadletter != nil {
		return m.deadletter(ctx, reason)
	}
	return nil
}

type recordingCommandBus struct {
	events   []string
	runCalls int
	mu       sync.Mutex
}

func (b *recordingCommandBus) PublishEvent(_ context.Context, subject string, _ actorlayer.Envelope) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, subject)
	return nil
}
func (b *recordingCommandBus) Run(ctx context.Context, _ actorengine.Handler) error {
	b.runCalls++
	<-ctx.Done()
	return ctx.Err()
}

func (b *recordingCommandBus) hasEvent(subject string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, event := range b.events {
		if event == subject {
			return true
		}
	}
	return false
}

func (b *recordingCommandBus) eventsSnapshot() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.events...)
}

func newTestRegistry(t *testing.T, actors ...dispatch.Actor) dispatch.Registry {
	t.Helper()
	registry := dispatch.NewMemoryRegistry()
	for _, actor := range actors {
		if err := registry.Register(actor); err != nil {
			t.Fatalf("register actors error = %v", err)
		}
	}
	return registry
}

func TestRuntime_HandleCommandDispatchesActor(t *testing.T) {
	bus := &recordingCommandBus{}
	actor := &testActor{address: actorlayer.WildcardAddress(ActorTypeSession)}
	registry := newTestRegistry(t, actor)
	runtime := newRuntimeForTest(bus, registry)
	if err := handleRuntimeDelivery(runtime, context.Background(), testDelivery{env: runtimeTestEnvelope("ok", actorlayer.ActorAddress{Target: ActorTypeSession, Key: "s-1"})}); err != nil {
		t.Fatalf("handleDelivery() error = %v", err)
	}
	if actor.calls != 1 {
		t.Fatalf("actor calls = %d, want 1", actor.calls)
	}
}

func TestRuntime_HandleCommandDispatchesActorWithNormalizedAddress(t *testing.T) {
	bus := &recordingCommandBus{}
	actor := &testActor{address: "  SESSION:S-1  "}
	registry := newTestRegistry(t, actor)
	runtime := newRuntimeForTest(bus, registry)
	if err := handleRuntimeDelivery(runtime, context.Background(), testDelivery{env: runtimeTestEnvelope("normalized", actorlayer.ActorAddress{Target: ActorTypeSession, Key: "s-1"})}); err != nil {
		t.Fatalf("handleDelivery() error = %v", err)
	}
	if actor.calls != 1 {
		t.Fatalf("actor calls = %d, want 1", actor.calls)
	}
}

func TestRuntimeAddressOf(t *testing.T) {
	tests := []struct {
		name     string
		env      actorlayer.Envelope
		haveAddr string
		wantErr  string
	}{
		{
			name:    "empty address",
			env:     runtimeTestEnvelope("empty-address", actorlayer.ActorAddress{}),
			wantErr: "actor target is required",
		},
		{
			name:     "known actor",
			env:      runtimeTestEnvelope("known", actorlayer.ActorAddress{Target: ActorTypeSession, Key: "s-1"}),
			haveAddr: "session:s-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAddr, err := runtimeAddressOf(tt.env)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("runtimeAddressOf() error = nil, want contains %q", tt.wantErr)
				}
				if got, want := err.Error(), tt.wantErr; !strings.Contains(got, want) {
					t.Fatalf("runtimeAddressOf() error = %q, want contains %q", got, want)
				}
				return
			}
			if err != nil {
				t.Fatalf("runtimeAddressOf() error = %v, want nil", err)
			}
			if gotAddr != tt.haveAddr {
				t.Fatalf("runtimeAddressOf() address = %q, want %q", gotAddr, tt.haveAddr)
			}
		})
	}
}

func TestActorLaneKeyFromEnvelopeUsesQualifiedDeliveryKey(t *testing.T) {
	env := actorlayer.Envelope{
		Namespace: NamespaceAgentResult,
		TaskID:    "task-1",
		To:        actorlayer.ActorAddress{Target: ActorTypeDelivery, Key: "telegram:9001:77"},
	}

	if got := actorLaneKeyFromEnvelope(env); got != "delivery:telegram:9001:77" {
		t.Fatalf("actorLaneKeyFromEnvelope() = %q, want %q", got, "delivery:telegram:9001:77")
	}
}

func TestRuntime_UnknownActorDeadLettersMessage(t *testing.T) {
	registry := newTestRegistry(t)
	runtime := newRuntimeForTest(&recordingCommandBus{}, registry)
	var deadletterReason string
	var retried bool
	err := handleRuntimeDelivery(runtime, context.Background(), testDelivery{
		env: runtimeTestEnvelope("unknown", actorlayer.ActorAddress{Target: ActorTypeSession, Key: "s-1"}),
		deadletter: func(_ context.Context, reason string) error {
			deadletterReason = reason
			return nil
		},
		retry: func(context.Context, time.Duration, string) error {
			retried = true
			return nil
		},
	})
	if err != nil {
		t.Fatalf("handleDelivery() error = %v", err)
	}
	if deadletterReason == "" {
		t.Fatal("DeadLetter() was not called")
	}
	if got := deadletterReason; !strings.Contains(got, actorengine.ErrActorNotFound.Error()) {
		t.Fatalf("deadletter reason = %q, want to contain %q", got, actorengine.ErrActorNotFound.Error())
	}
	if retried {
		t.Fatal("Retry() was called for unknown actor")
	}
}

func TestRuntime_ActorErrorRequestsRetry(t *testing.T) {
	actor := &testActor{address: actorlayer.WildcardAddress(ActorTypeSession), err: actorlayer.TransientError(fmt.Errorf("temporary"))}
	registry := newTestRegistry(t, actor)
	runtime := newRuntimeForTest(&recordingCommandBus{}, registry)
	var called bool
	err := handleRuntimeDelivery(runtime, context.Background(), testDelivery{
		env: runtimeTestEnvelope("retry", actorlayer.ActorAddress{Target: ActorTypeSession, Key: "s-1"}),
		retry: func(_ context.Context, _ time.Duration, _ string) error {
			called = true
			return nil
		},
	})
	if err != nil {
		t.Fatalf("handleDelivery() error = %v", err)
	}
	if !called {
		t.Fatal("Retry() was not called")
	}
}

func TestRuntime_RetryExhaustionMarksTaskDeadlettered(t *testing.T) {
	ctx := context.Background()
	provider, err := baldastate.NewSQLiteProvider(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	tasks, err := NewTaskService(taskServiceParams{StateProvider: provider, Bus: &recordingCommandBus{}})
	if err != nil {
		t.Fatalf("NewTaskService() error = %v", err)
	}
	_, err = tasks.Create(ctx, baldastate.SwarmTaskRecord{
		ID:        "task-retry",
		SessionID: "s-1",
		Objective: "retry",
		Status:    baldastate.SwarmTaskStatusRunning,
	}, "test", nil)
	if err != nil {
		t.Fatalf("Create task: %v", err)
	}
	actor := &testActor{address: actorlayer.WildcardAddress(ActorTypeSession), err: actorlayer.TransientError(fmt.Errorf("temporary"))}
	registry := newTestRegistry(t, actor)
	runtime := newRuntimeForTest(&recordingCommandBus{}, registry)
	runtime.tasks = tasks
	env := runtimeTestEnvelope("retry-exhausted", actorlayer.ActorAddress{Target: ActorTypeSession, Key: "s-1"})
	env.TaskID = "task-retry"
	var deadletterCalled bool
	err = handleRuntimeDelivery(runtime, ctx, testDelivery{
		env:           env,
		numDelivered:  5,
		maxDeliveries: 5,
		deadletter: func(context.Context, string) error {
			deadletterCalled = true
			return nil
		},
	})
	if err != nil {
		t.Fatalf("handleDelivery() error = %v", err)
	}
	if !deadletterCalled {
		t.Fatal("DeadLetter() was not called")
	}
	task, ok, err := tasks.Get(ctx, "task-retry")
	if err != nil {
		t.Fatalf("Get task: %v", err)
	}
	if !ok || task.Status != baldastate.SwarmTaskStatusDeadLettered {
		t.Fatalf("task = %+v found=%v, want deadlettered", task, ok)
	}
}

func TestRuntime_LongRunningCommandSendsInProgressHeartbeat(t *testing.T) {
	bus := &recordingCommandBus{}
	started := make(chan struct{})
	release := make(chan struct{})
	actor := &testActor{
		address: actorlayer.WildcardAddress(ActorTypeSession),
		run: func(ctx context.Context, _ actorlayer.Envelope) error {
			close(started)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	registry := newTestRegistry(t, actor)
	runtime := newRuntimeForTest(bus, registry)
	runtime.heartbeatTick = 2 * time.Millisecond
	var inProgressCalls atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- handleRuntimeDelivery(runtime, context.Background(), testDelivery{
			env: runtimeTestEnvelope("long-running", actorlayer.ActorAddress{Target: ActorTypeSession, Key: "s-1"}),
			inProgress: func(context.Context) error {
				inProgressCalls.Add(1)
				return nil
			},
		})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for long-running actor start")
	}
	deadline := time.After(time.Second)
	for inProgressCalls.Load() == 0 || !bus.hasEvent(SubjectEventCommandInProgress) {
		select {
		case <-deadline:
			t.Fatalf("in-progress heartbeat not observed: calls=%d events=%v", inProgressCalls.Load(), bus.eventsSnapshot())
		case <-time.After(5 * time.Millisecond):
		}
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("handleDelivery() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for long-running command completion")
	}
}

func newRuntimeForTest(bus *recordingCommandBus, registry dispatch.Registry) *ActorHost {
	rt := &ActorHost{source: bus, events: bus, heartbeatTick: heartbeatInterval}
	engine, err := actorengine.NewDispatchRuntime(actorengine.RuntimeConfig{
		Registry:  registry,
		AddressOf: runtimeAddressOf,
		LaneKey:   actorLaneKeyFromEnvelope,
		Sink:      rt,
		Retry: actorengine.RetryPolicy{
			IsRetryable:    actorlayer.IsRetryableError,
			Backoff:        actorlayer.RetryDelay,
			RetryExhausted: retryExhaustedDelivery,
		},
	})
	if err != nil {
		panic(err)
	}
	rt.engine = engine
	return rt
}

func handleRuntimeDelivery(runtime *ActorHost, ctx context.Context, delivery actorengine.Delivery) error {
	executionCtx, stop, prepared := runtime.prepareDelivery(ctx, delivery)
	defer stop()
	if runtime.engine == nil {
		return nil
	}
	return runtime.engine.Handle(executionCtx, prepared)
}

func runtimeTestEnvelope(id string, to actorlayer.ActorAddress) actorlayer.Envelope {
	return actorlayer.Envelope{ID: id, Namespace: NamespaceHumanInbound, Kind: KindMessage, From: actorlayer.ActorAddress{Target: "test", Key: "source"}, To: to, SessionID: to.Key, PayloadJSON: `{"ok":true}`}
}
