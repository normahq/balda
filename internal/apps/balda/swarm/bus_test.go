package swarm

import (
	"context"
	"testing"
	"time"
)

const subjectTestTaskID = "task-1"

func TestNoopEventBus_SubscribeIsNoop(t *testing.T) {
	bus := NewNoopEventBus("sqlite")
	sub, err := bus.Subscribe(context.Background(), SubjectWakeupMailbox, func(context.Context, string, Envelope) error {
		t.Fatal("noop bus should not call handler")
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("Unsubscribe() error = %v", err)
	}
	if status := bus.Status(); status.Mode != "sqlite" || status.Running {
		t.Fatalf("Status() = %+v, want sqlite stopped", status)
	}
}

func TestSubjectForEnvelope_UsesStableFamilies(t *testing.T) {
	tests := []struct {
		name string
		env  Envelope
		want string
	}{
		{name: "session", env: subjectTestEnvelope(ActorAddress{Target: ActorTypeSession, Key: "tg-1.2"}), want: SubjectCommandSession},
		{name: "task", env: subjectTestEnvelope(ActorAddress{Target: ActorTypeTask, Key: subjectTestTaskID}), want: SubjectCommandTask},
		{name: "agent", env: subjectTestEnvelope(ActorAddress{Target: ActorTypeAgent, Key: "planner"}), want: SubjectCommandAgent},
		{name: "delivery", env: subjectTestEnvelope(ActorAddress{Target: ActorTypeDelivery, Key: "tg-1"}), want: SubjectCommandDelivery},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SubjectForEnvelope(tt.env); got != tt.want {
				t.Fatalf("SubjectForEnvelope() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEnvelopeHeaders_UseIdentityHeaders(t *testing.T) {
	env := subjectTestEnvelope(ActorAddress{Target: ActorTypeTask, Key: subjectTestTaskID})
	env.TaskID = subjectTestTaskID
	env.CorrelationID = "corr-1"
	env.Priority = 80
	headers := EnvelopeHeaders(env)
	if headers[HeaderEnvelopeID] != env.ID {
		t.Fatalf("%s = %q, want %q", HeaderEnvelopeID, headers[HeaderEnvelopeID], env.ID)
	}
	if headers[HeaderTaskID] != "task-1" {
		t.Fatalf("%s = %q, want task-1", HeaderTaskID, headers[HeaderTaskID])
	}
	if headers[HeaderMailbox] != "task:task-1" {
		t.Fatalf("%s = %q, want task:task-1", HeaderMailbox, headers[HeaderMailbox])
	}
	if headers[HeaderPriority] != "80" {
		t.Fatalf("%s = %q, want 80", HeaderPriority, headers[HeaderPriority])
	}
}

func TestNoopEventBus_RequestFails(t *testing.T) {
	_, err := NewNoopEventBus("sqlite").Request(context.Background(), "subject", subjectTestEnvelope(ActorAddress{Target: ActorTypeTask, Key: subjectTestTaskID}), time.Millisecond)
	if err == nil {
		t.Fatal("Request() error = nil, want unsupported")
	}
}

func subjectTestEnvelope(to ActorAddress) Envelope {
	return Envelope{
		ID:          "env-1",
		Namespace:   NamespaceHumanInbound,
		Kind:        KindMessage,
		From:        ActorAddress{Target: "test", Key: "source"},
		To:          to,
		SessionID:   "session-1",
		PayloadJSON: `{"ok":true}`,
	}
}
