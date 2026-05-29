package swarm

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/norma/actorlayer/dispatch"
	actorengine "github.com/normahq/norma/actorlayer/engine"
)

type testActor struct {
	address string
	err     error
	calls   int
	run     func(context.Context, Envelope) error
}

func (a *testActor) Address() string { return a.address }
func (a *testActor) Handle(ctx context.Context, env Envelope) error {
	a.calls++
	if a.run != nil {
		return a.run(ctx, env)
	}
	return a.err
}

type testCommandMessage struct {
	env           Envelope
	numDelivered  int
	maxDeliveries int
	inProgress    func(context.Context) error
	ack           func(context.Context) error
	retry         func(context.Context, time.Duration, string) error
	deadletter    func(context.Context, string) error
}

func (m testCommandMessage) Envelope() Envelope { return m.env }
func (m testCommandMessage) Subject() string    { return SubjectForEnvelope(m.env) }
func (m testCommandMessage) InProgress(ctx context.Context) error {
	if m.inProgress != nil {
		return m.inProgress(ctx)
	}
	return nil
}
func (m testCommandMessage) DeliveryAttempt() int {
	if m.numDelivered <= 0 {
		return 1
	}
	return m.numDelivered
}
func (m testCommandMessage) MaxDeliveries() int { return m.maxDeliveries }
func (m testCommandMessage) Ack(ctx context.Context) error {
	if m.ack != nil {
		return m.ack(ctx)
	}
	return nil
}

func (m testCommandMessage) Retry(ctx context.Context, delay time.Duration, reason string) error {
	if m.retry != nil {
		return m.retry(ctx, delay, reason)
	}
	return nil
}

func (m testCommandMessage) DeadLetter(ctx context.Context, reason string) error {
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

func (b *recordingCommandBus) PublishEvent(_ context.Context, subject string, _ Envelope) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, subject)
	return nil
}
func (b *recordingCommandBus) RunCommandConsumer(ctx context.Context, _ CommandHandler) error {
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

func newTestRegistry(t *testing.T, actors ...Actor) dispatch.Registry {
	t.Helper()
	registry, err := registerActors(actors)
	if err != nil {
		t.Fatalf("register actors error = %v", err)
	}
	return registry
}

func TestRuntimeStartDisabledDoesNotRunConsumer(t *testing.T) {
	bus := &recordingCommandBus{}
	runtime := &Runtime{bus: bus, enabled: false}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if bus.runCalls != 0 {
		t.Fatalf("RunCommandConsumer calls = %d, want 0", bus.runCalls)
	}
}

func TestRuntime_HandleCommandDispatchesActor(t *testing.T) {
	bus := &recordingCommandBus{}
	actor := &testActor{address: WildcardAddress(ActorTypeSession)}
	registry := newTestRegistry(t, actor)
	runtime := newRuntimeForTest(bus, registry)
	if err := runtime.HandleCommand(context.Background(), testCommandMessage{env: runtimeTestEnvelope("ok", ActorAddress{Target: ActorTypeSession, Key: "s-1"})}); err != nil {
		t.Fatalf("HandleCommand() error = %v", err)
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
	if err := runtime.HandleCommand(context.Background(), testCommandMessage{env: runtimeTestEnvelope("normalized", ActorAddress{Target: ActorTypeSession, Key: "s-1"})}); err != nil {
		t.Fatalf("HandleCommand() error = %v", err)
	}
	if actor.calls != 1 {
		t.Fatalf("actor calls = %d, want 1", actor.calls)
	}
}

func TestRuntimeAddressOf(t *testing.T) {
	tests := []struct {
		name     string
		env      any
		haveAddr string
		wantErr  string
	}{
		{
			name:    "type error",
			env:     struct{ v string }{v: "not-an-envelope"},
			wantErr: "unexpected delivery envelope type",
		},
		{
			name:    "empty address",
			env:     runtimeTestEnvelope("empty-address", ActorAddress{}),
			wantErr: "actor target is required",
		},
		{
			name:     "known actor",
			env:      runtimeTestEnvelope("known", ActorAddress{Target: ActorTypeSession, Key: "s-1"}),
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

func TestRuntime_UnknownActorDeadLettersMessage(t *testing.T) {
	registry := newTestRegistry(t)
	runtime := newRuntimeForTest(&recordingCommandBus{}, registry)
	var deadletterReason string
	var retried bool
	err := runtime.HandleCommand(context.Background(), testCommandMessage{
		env: runtimeTestEnvelope("unknown", ActorAddress{Target: ActorTypeSession, Key: "s-1"}),
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
		t.Fatalf("HandleCommand() error = %v", err)
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
	actor := &testActor{address: WildcardAddress(ActorTypeSession), err: TransientError(fmt.Errorf("temporary"))}
	registry := newTestRegistry(t, actor)
	runtime := newRuntimeForTest(&recordingCommandBus{}, registry)
	var called bool
	err := runtime.HandleCommand(context.Background(), testCommandMessage{
		env: runtimeTestEnvelope("retry", ActorAddress{Target: ActorTypeSession, Key: "s-1"}),
		retry: func(_ context.Context, _ time.Duration, _ string) error {
			called = true
			return nil
		},
	})
	if err != nil {
		t.Fatalf("HandleCommand() error = %v", err)
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
	actor := &testActor{address: WildcardAddress(ActorTypeSession), err: TransientError(fmt.Errorf("temporary"))}
	registry := newTestRegistry(t, actor)
	runtime := newRuntimeForTest(&recordingCommandBus{}, registry)
	runtime.tasks = tasks
	env := runtimeTestEnvelope("retry-exhausted", ActorAddress{Target: ActorTypeSession, Key: "s-1"})
	env.TaskID = "task-retry"
	var deadletterCalled bool
	err = runtime.HandleCommand(ctx, testCommandMessage{
		env:           env,
		numDelivered:  5,
		maxDeliveries: 5,
		deadletter: func(context.Context, string) error {
			deadletterCalled = true
			return nil
		},
	})
	if err != nil {
		t.Fatalf("HandleCommand() error = %v", err)
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
		address: WildcardAddress(ActorTypeSession),
		run: func(ctx context.Context, _ Envelope) error {
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
		done <- runtime.HandleCommand(context.Background(), testCommandMessage{
			env: runtimeTestEnvelope("long-running", ActorAddress{Target: ActorTypeSession, Key: "s-1"}),
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
			t.Fatalf("HandleCommand() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for long-running command completion")
	}
}

func TestRuntime_LaneStatusTracksActiveLanes(t *testing.T) {
	bus := &recordingCommandBus{}
	started := make(chan struct{})
	release := make(chan struct{})
	actor := &testActor{
		address: WildcardAddress(ActorTypeSession),
		run: func(ctx context.Context, _ Envelope) error {
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
	env := runtimeTestEnvelope("lane-track", ActorAddress{Target: ActorTypeSession, Key: "s-1"})
	env.TaskID = "task-1"

	done := make(chan error, 1)
	go func() {
		done <- runtime.HandleCommand(context.Background(), testCommandMessage{env: env})
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for actor start")
	}
	status := runtime.LaneStatus()
	if status.Active != 1 {
		t.Fatalf("LaneStatus().Active = %d, want 1", status.Active)
	}
	if len(status.Keys) != 1 || status.Keys[0] != "task:task-1" {
		t.Fatalf("LaneStatus().Keys = %v, want [task:task-1]", status.Keys)
	}

	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("HandleCommand() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for completion")
	}

	status = runtime.LaneStatus()
	if status.Active != 0 || len(status.Keys) != 0 {
		t.Fatalf("LaneStatus() after completion = %+v, want zero", status)
	}
}

func TestIsRetryableRuntimeError(t *testing.T) {
	t.Parallel()

	t.Run("resolve error is non-retryable", func(t *testing.T) {
		t.Parallel()
		err := &actorengine.ResolveError{Address: "session:missing"}
		if got := isRetryableRuntimeError(err); got {
			t.Fatalf("isRetryableRuntimeError() = true, want false")
		}
	})

	t.Run("wrapped resolve error is non-retryable", func(t *testing.T) {
		t.Parallel()
		err := fmt.Errorf("dispatch failed: %w", &actorengine.ResolveError{Address: "session:wrapped"})
		if got := isRetryableRuntimeError(err); got {
			t.Fatalf("isRetryableRuntimeError() = true, want false")
		}
	})

	t.Run("wrapped canonical actor not found is non-retryable", func(t *testing.T) {
		t.Parallel()
		err := fmt.Errorf("lookup failed: %w", actorengine.ErrActorNotFound)
		if got := isRetryableRuntimeError(err); got {
			t.Fatalf("isRetryableRuntimeError() = true, want false")
		}
	})

	t.Run("other errors stay classified by actor errors", func(t *testing.T) {
		t.Parallel()
		err := fmt.Errorf("%w", PermanentError(errors.New("persist failed")))
		if got := isRetryableRuntimeError(err); got {
			t.Fatalf("isRetryableRuntimeError() = true, want false")
		}
	})
}

func newRuntimeForTest(bus RuntimeBus, registry dispatch.Registry) *Runtime {
	rt := &Runtime{bus: bus, heartbeatTick: heartbeatInterval}
	engine, err := actorengine.NewDispatchRuntime(actorengine.RuntimeConfig{
		Registry:  registry,
		AddressOf: runtimeAddressOf,
		LaneKey:   actorLaneKeyFromEnvelope,
		Retry: actorengine.RetryPolicy{
			IsRetryable: isRetryableRuntimeError,
			Backoff:     nextRetryDelay,
			RetryExhausted: func(delivery actorengine.Delivery) bool {
				wrapped, ok := delivery.(*runtimeDelivery)
				if !ok {
					return false
				}
				return retryExhaustedCommand(wrapped.cmd)
			},
		},
	})
	if err != nil {
		panic(err)
	}
	rt.engine = engine
	return rt
}

func runtimeTestEnvelope(id string, to ActorAddress) Envelope {
	return Envelope{ID: id, Namespace: NamespaceHumanInbound, Kind: KindMessage, From: ActorAddress{Target: "test", Key: "source"}, To: to, SessionID: to.Key, PayloadJSON: `{"ok":true}`}
}
