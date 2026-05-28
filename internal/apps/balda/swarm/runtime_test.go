package swarm

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	baldastate "github.com/normahq/balda/internal/apps/balda/state"
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
	registry := NewRegistry()
	if err := registry.Register(actor); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	runtime := newRuntimeForTest(bus, registry)
	if err := runtime.HandleCommand(context.Background(), testCommandMessage{env: runtimeTestEnvelope("ok", ActorAddress{Target: ActorTypeSession, Key: "s-1"})}); err != nil {
		t.Fatalf("HandleCommand() error = %v", err)
	}
	if actor.calls != 1 {
		t.Fatalf("actor calls = %d, want 1", actor.calls)
	}
}

func TestRuntime_UnknownActorDeadLettersMessage(t *testing.T) {
	runtime := newRuntimeForTest(&recordingCommandBus{}, NewRegistry())
	var deadletterReason string
	err := runtime.HandleCommand(context.Background(), testCommandMessage{
		env: runtimeTestEnvelope("unknown", ActorAddress{Target: ActorTypeSession, Key: "s-1"}),
		deadletter: func(_ context.Context, reason string) error {
			deadletterReason = reason
			return nil
		},
	})
	if err != nil {
		t.Fatalf("HandleCommand() error = %v", err)
	}
	if deadletterReason == "" {
		t.Fatal("DeadLetter() was not called")
	}
}

func TestRuntime_ActorErrorRequestsRetry(t *testing.T) {
	actor := &testActor{address: WildcardAddress(ActorTypeSession), err: TransientError(fmt.Errorf("temporary"))}
	registry := NewRegistry()
	if err := registry.Register(actor); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
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
	registry := NewRegistry()
	if err := registry.Register(actor); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
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
	registry := NewRegistry()
	if err := registry.Register(actor); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
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

func newRuntimeForTest(bus RuntimeBus, registry ActorRegistry) *Runtime {
	rt := &Runtime{bus: bus, registry: registry, heartbeatTick: heartbeatInterval}
	resolver, ok := registry.(EngineResolver)
	if !ok {
		resolver = runtimeResolver{registry: registry}
	}
	engine, err := newRuntimeEngine(runtimeEngineConfig{
		Resolver:       resolver,
		EventSink:      runtimeEngineEventSink{bus: bus},
		DeadLetterTask: rt.deadletterTask,
		IsRetryable:    isRetryableRuntimeError,
		ComputeBackoff: nextRetryDelay,
		RetryExhausted: retryExhaustedCommand,
	})
	if err != nil {
		panic(err)
	}
	rt.engine = engine
	return rt
}

type runtimeResolver struct {
	registry ActorRegistry
}

func (r runtimeResolver) Resolve(address string) (Actor, bool) {
	return r.registry.Resolve(address)
}

func (runtimeResolver) LaneKey(env Envelope) string {
	return actorLaneKey(env)
}

func runtimeTestEnvelope(id string, to ActorAddress) Envelope {
	return Envelope{ID: id, Namespace: NamespaceHumanInbound, Kind: KindMessage, From: ActorAddress{Target: "test", Key: "source"}, To: to, SessionID: to.Key, PayloadJSON: `{"ok":true}`}
}
