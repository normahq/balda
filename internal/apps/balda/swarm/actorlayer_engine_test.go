package swarm

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type engineTestResolver struct {
	resolve func(string) (Actor, bool)
	laneKey func(Envelope) string
}

func (r engineTestResolver) Resolve(address string) (Actor, bool) {
	return r.resolve(address)
}

func (r engineTestResolver) LaneKey(env Envelope) string {
	return r.laneKey(env)
}

type engineActor struct {
	address string
	err     error
	run     func(context.Context, Envelope) error
}

func (a *engineActor) Address() string { return a.address }

func (a *engineActor) Handle(ctx context.Context, env Envelope) error {
	if a.run != nil {
		return a.run(ctx, env)
	}
	return a.err
}

type engineTestDelivery struct {
	env          Envelope
	attempt      int
	maxDeliver   int
	acked        atomic.Int32
	retried      atomic.Int32
	deadlettered atomic.Int32
	lastRetry    time.Duration
	lastReason   string
}

func (d *engineTestDelivery) Envelope() Envelope { return d.env }
func (d *engineTestDelivery) Subject() string    { return SubjectForEnvelope(d.env) }
func (d *engineTestDelivery) InProgress(context.Context) error {
	return nil
}
func (d *engineTestDelivery) DeliveryAttempt() int {
	if d.attempt <= 0 {
		return 1
	}
	return d.attempt
}
func (d *engineTestDelivery) MaxDeliveries() int {
	return d.maxDeliver
}
func (d *engineTestDelivery) Ack(context.Context) error {
	d.acked.Add(1)
	return nil
}
func (d *engineTestDelivery) Retry(_ context.Context, delay time.Duration, reason string) error {
	d.lastRetry = delay
	d.lastReason = reason
	d.retried.Add(1)
	return nil
}
func (d *engineTestDelivery) DeadLetter(_ context.Context, reason string) error {
	d.lastReason = reason
	d.deadlettered.Add(1)
	return nil
}

func TestActorLaneKeyMapsSessionTaskAndAgent(t *testing.T) {
	tests := []struct {
		name string
		env  Envelope
		want string
	}{
		{name: "session", env: Envelope{Namespace: NamespaceWebhookInbound, SessionID: "s-1", To: ActorAddress{Target: ActorTypeTask, Key: "task-1"}}, want: "session:s-1"},
		{name: "task control", env: Envelope{Namespace: NamespaceTaskControl, TaskID: "task-1", To: ActorAddress{Target: ActorTypeTask, Key: "task-1"}}, want: "task:task-1"},
		{name: "agent task lane", env: Envelope{Namespace: NamespaceAgentCommand, TaskID: "task-1", To: ActorAddress{Target: ActorTypeAgent, Key: "executor"}}, want: "task:task-1"},
		{name: "agent result task lane", env: Envelope{Namespace: NamespaceAgentResult, TaskID: "task-1", To: ActorAddress{Target: ActorTypeTask, Key: "task-1"}}, want: "task:task-1"},
		{name: "agent result delivery lane", env: Envelope{Namespace: NamespaceAgentResult, TaskID: "task-1", To: ActorAddress{Target: ActorTypeDelivery, Key: "tg-9001"}}, want: "delivery:tg-9001"},
		{name: "webhook task lane", env: Envelope{Namespace: NamespaceWebhookInbound, SessionID: "s-1", TaskID: "task-1", To: ActorAddress{Target: ActorTypeTask, Key: "task-1"}}, want: "task:task-1"},
		{name: "schedule task lane", env: Envelope{Namespace: NamespaceScheduleInbound, SessionID: "s-1", TaskID: "task-1", To: ActorAddress{Target: ActorTypeTask, Key: "task-1"}}, want: "task:task-1"},
		{name: "agent fallback", env: Envelope{Namespace: NamespaceAgentCommand, To: ActorAddress{Target: ActorTypeAgent, Key: "executor"}}, want: "agent:executor"},
		{name: "fallback", env: Envelope{Namespace: NamespaceTelemetry, To: ActorAddress{Target: ActorTypeDelivery, Key: "tg"}}, want: "delivery:tg"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := actorLaneKey(tt.env); got != tt.want {
				t.Fatalf("actorLaneKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRuntimeEngineSerializesSameLane(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	actor := &engineActor{
		address: WildcardAddress(ActorTypeSession),
		run: func(_ context.Context, env Envelope) error {
			started <- env.ID
			if env.ID == "first" {
				<-release
			}
			return nil
		},
	}
	engine, err := newRuntimeEngine(runtimeEngineConfig{
		Resolver: engineTestResolver{
			resolve: func(string) (Actor, bool) { return actor, true },
			laneKey: actorLaneKey,
		},
		IsRetryable: func(error) bool { return true },
	})
	if err != nil {
		t.Fatalf("newRuntimeEngine() error = %v", err)
	}
	done := make(chan error, 2)
	go func() {
		done <- engine.HandleDelivery(context.Background(), &engineTestDelivery{
			env: Envelope{ID: "first", Namespace: NamespaceAgentCommand, Kind: KindGoal, From: ActorAddress{Target: ActorTypeTask, Key: "task-a"}, To: ActorAddress{Target: ActorTypeAgent, Key: AgentNameExecutor}, TaskID: "task-a", PayloadJSON: `{}`},
		})
	}()
	waitSignal(t, started, "first delivery")
	go func() {
		done <- engine.HandleDelivery(context.Background(), &engineTestDelivery{
			env: Envelope{ID: "second", Namespace: NamespaceAgentCommand, Kind: KindGoal, From: ActorAddress{Target: ActorTypeTask, Key: "task-a"}, To: ActorAddress{Target: ActorTypeAgent, Key: AgentNameExecutor}, TaskID: "task-a", PayloadJSON: `{}`},
		})
	}()
	select {
	case got := <-started:
		t.Fatalf("same lane started %q before release", got)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	waitSignal(t, started, "second delivery")
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("HandleDelivery() error = %v", err)
		}
	}
}

func TestRuntimeEngineRunsDifferentLanesConcurrently(t *testing.T) {
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	release := make(chan struct{})
	actor := &engineActor{
		address: WildcardAddress(ActorTypeTask),
		run: func(_ context.Context, _ Envelope) error {
			current := inFlight.Add(1)
			for {
				seen := maxInFlight.Load()
				if current <= seen || maxInFlight.CompareAndSwap(seen, current) {
					break
				}
			}
			<-release
			inFlight.Add(-1)
			return nil
		},
	}
	engine, err := newRuntimeEngine(runtimeEngineConfig{
		Resolver: engineTestResolver{
			resolve: func(string) (Actor, bool) { return actor, true },
			laneKey: actorLaneKey,
		},
		IsRetryable: func(error) bool { return true },
	})
	if err != nil {
		t.Fatalf("newRuntimeEngine() error = %v", err)
	}
	done := make(chan error, 2)
	go func() {
		done <- engine.HandleDelivery(context.Background(), &engineTestDelivery{
			env: Envelope{ID: "a", Namespace: NamespaceAgentCommand, Kind: KindGoal, From: ActorAddress{Target: ActorTypeTask, Key: "task-a"}, To: ActorAddress{Target: ActorTypeTask, Key: "task-a"}, TaskID: "task-a", PayloadJSON: `{}`},
		})
	}()
	go func() {
		done <- engine.HandleDelivery(context.Background(), &engineTestDelivery{
			env: Envelope{ID: "b", Namespace: NamespaceAgentCommand, Kind: KindGoal, From: ActorAddress{Target: ActorTypeTask, Key: "task-b"}, To: ActorAddress{Target: ActorTypeTask, Key: "task-b"}, TaskID: "task-b", PayloadJSON: `{}`},
		})
	}()
	deadline := time.After(time.Second)
	for maxInFlight.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("different lanes never overlapped: max_in_flight=%d", maxInFlight.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}
	close(release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("HandleDelivery() error = %v", err)
		}
	}
}

func TestRuntimeEngineClassifiesAckRetryAndDeadLetter(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		actor := &engineActor{address: WildcardAddress(ActorTypeSession)}
		delivery := &engineTestDelivery{
			env: Envelope{ID: "ok", Namespace: NamespaceHumanInbound, Kind: KindMessage, From: ActorAddress{Target: "test", Key: "u"}, To: ActorAddress{Target: ActorTypeSession, Key: "s-1"}, SessionID: "s-1", PayloadJSON: `{}`},
		}
		engine, err := newRuntimeEngine(runtimeEngineConfig{
			Resolver: engineTestResolver{
				resolve: func(string) (Actor, bool) { return actor, true },
				laneKey: actorLaneKey,
			},
			IsRetryable: func(error) bool { return true },
		})
		if err != nil {
			t.Fatalf("newRuntimeEngine() error = %v", err)
		}
		if err := engine.HandleDelivery(context.Background(), delivery); err != nil {
			t.Fatalf("HandleDelivery() error = %v", err)
		}
		if delivery.acked.Load() != 1 || delivery.retried.Load() != 0 || delivery.deadlettered.Load() != 0 {
			t.Fatalf("settlement counts = ack:%d retry:%d dlq:%d, want 1/0/0", delivery.acked.Load(), delivery.retried.Load(), delivery.deadlettered.Load())
		}
	})

	t.Run("retryable", func(t *testing.T) {
		actor := &engineActor{
			address: WildcardAddress(ActorTypeSession),
			err:     TransientError(errors.New("temporary")),
		}
		delivery := &engineTestDelivery{
			env: Envelope{ID: "retry", Namespace: NamespaceHumanInbound, Kind: KindMessage, From: ActorAddress{Target: "test", Key: "u"}, To: ActorAddress{Target: ActorTypeSession, Key: "s-1"}, SessionID: "s-1", PayloadJSON: `{}`},
		}
		engine, err := newRuntimeEngine(runtimeEngineConfig{
			Resolver: engineTestResolver{
				resolve: func(string) (Actor, bool) { return actor, true },
				laneKey: actorLaneKey,
			},
			IsRetryable:    isRetryableRuntimeError,
			ComputeBackoff: func(int) time.Duration { return 25 * time.Millisecond },
		})
		if err != nil {
			t.Fatalf("newRuntimeEngine() error = %v", err)
		}
		if err := engine.HandleDelivery(context.Background(), delivery); err != nil {
			t.Fatalf("HandleDelivery() error = %v", err)
		}
		if delivery.retried.Load() != 1 || delivery.acked.Load() != 0 || delivery.deadlettered.Load() != 0 {
			t.Fatalf("settlement counts = ack:%d retry:%d dlq:%d, want 0/1/0", delivery.acked.Load(), delivery.retried.Load(), delivery.deadlettered.Load())
		}
	})

	t.Run("permanent", func(t *testing.T) {
		actor := &engineActor{
			address: WildcardAddress(ActorTypeSession),
			err:     PermanentError(errors.New("denied")),
		}
		delivery := &engineTestDelivery{
			env: Envelope{ID: "dlq", Namespace: NamespaceHumanInbound, Kind: KindMessage, From: ActorAddress{Target: "test", Key: "u"}, To: ActorAddress{Target: ActorTypeSession, Key: "s-1"}, SessionID: "s-1", PayloadJSON: `{}`},
		}
		engine, err := newRuntimeEngine(runtimeEngineConfig{
			Resolver: engineTestResolver{
				resolve: func(string) (Actor, bool) { return actor, true },
				laneKey: actorLaneKey,
			},
			IsRetryable: isRetryableRuntimeError,
		})
		if err != nil {
			t.Fatalf("newRuntimeEngine() error = %v", err)
		}
		if err := engine.HandleDelivery(context.Background(), delivery); err != nil {
			t.Fatalf("HandleDelivery() error = %v", err)
		}
		if delivery.deadlettered.Load() != 1 || delivery.acked.Load() != 0 || delivery.retried.Load() != 0 {
			t.Fatalf("settlement counts = ack:%d retry:%d dlq:%d, want 0/0/1", delivery.acked.Load(), delivery.retried.Load(), delivery.deadlettered.Load())
		}
	})
}

func TestRuntimeEngineRetryExhaustionDeadLetters(t *testing.T) {
	actor := &engineActor{
		address: WildcardAddress(ActorTypeSession),
		err:     TransientError(errors.New("temporary")),
	}
	delivery := &engineTestDelivery{
		env:        Envelope{ID: "exhausted", Namespace: NamespaceHumanInbound, Kind: KindMessage, From: ActorAddress{Target: "test", Key: "u"}, To: ActorAddress{Target: ActorTypeSession, Key: "s-1"}, SessionID: "s-1", PayloadJSON: `{}`},
		attempt:    5,
		maxDeliver: 5,
	}
	var mu sync.Mutex
	var reasons []string
	engine, err := newRuntimeEngine(runtimeEngineConfig{
		Resolver: engineTestResolver{
			resolve: func(string) (Actor, bool) { return actor, true },
			laneKey: actorLaneKey,
		},
		IsRetryable: isRetryableRuntimeError,
		DeadLetterTask: func(_ context.Context, _ Envelope, reason string) {
			mu.Lock()
			defer mu.Unlock()
			reasons = append(reasons, reason)
		},
		RetryExhausted: func(CommandMessage) bool { return true },
	})
	if err != nil {
		t.Fatalf("newRuntimeEngine() error = %v", err)
	}
	if err := engine.HandleDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("HandleDelivery() error = %v", err)
	}
	if delivery.deadlettered.Load() != 1 {
		t.Fatalf("DeadLetter() calls = %d, want 1", delivery.deadlettered.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reasons) != 1 {
		t.Fatalf("deadletter task reasons = %d, want 1", len(reasons))
	}
}

func waitSignal(t *testing.T, ch <-chan string, name string) string {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
		return ""
	}
}
