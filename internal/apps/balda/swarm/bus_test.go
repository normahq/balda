package swarm

import (
	"strings"
	"testing"
	"time"
)

const subjectTestTaskID = "task-1"

func TestSubjectForEnvelope_UsesStableCommandSubjects(t *testing.T) {
	tests := []struct {
		name string
		env  Envelope
		want string
	}{
		{name: "session", env: subjectTestEnvelope(ActorAddress{Target: ActorTypeSession, Key: "tg-1.2"}), want: SubjectCommandSession},
		{name: "task", env: subjectTestEnvelope(ActorAddress{Target: ActorTypeTask, Key: subjectTestTaskID}), want: SubjectCommandTask},
		{name: "agent", env: subjectTestEnvelope(ActorAddress{Target: ActorTypeAgent, Key: "planner"}), want: SubjectCommandAgent},
		{name: "delivery", env: subjectTestEnvelope(ActorAddress{Target: ActorTypeDelivery, Key: "tg-1"}), want: SubjectCommandDelivery},
		{name: "memory", env: subjectTestEnvelope(ActorAddress{Target: ActorTypeMemory, Key: "global"}), want: SubjectCommandMemory},
		{name: "control", env: controlTestEnvelope(), want: SubjectCommandControl},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SubjectForEnvelope(tt.env); got != tt.want {
				t.Fatalf("SubjectForEnvelope() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCommandSubjects_UseCommandNamespacePrefix(t *testing.T) {
	t.Parallel()

	commandSubjects := []string{
		SubjectCommandSession,
		SubjectCommandTask,
		SubjectCommandAgent,
		SubjectCommandDelivery,
		SubjectCommandMemory,
		SubjectCommandControl,
		SubjectCommandAll,
	}
	for _, subject := range commandSubjects {
		if !strings.HasPrefix(subject, "balda.v1.cmd") {
			t.Fatalf("command subject %q must start with balda.v1.cmd", subject)
		}
	}
}

func TestEnvelopeHeaders_UseJetStreamIdentityHeaders(t *testing.T) {
	env := subjectTestEnvelope(ActorAddress{Target: ActorTypeTask, Key: subjectTestTaskID})
	env.TaskID = subjectTestTaskID
	env.CorrelationID = "corr-1"
	env.Priority = 80
	headers := EnvelopeHeaders(env)
	if headers[HeaderEnvelopeID] != env.ID {
		t.Fatalf("%s = %q, want %q", HeaderEnvelopeID, headers[HeaderEnvelopeID], env.ID)
	}
	if headers[HeaderTaskID] != subjectTestTaskID {
		t.Fatalf("%s = %q, want %s", HeaderTaskID, headers[HeaderTaskID], subjectTestTaskID)
	}
	if headers[HeaderActorKey] != subjectTestTaskID {
		t.Fatalf("%s = %q, want %s", HeaderActorKey, headers[HeaderActorKey], subjectTestTaskID)
	}
	if headers[HeaderPriority] != "80" {
		t.Fatalf("%s = %q, want 80", HeaderPriority, headers[HeaderPriority])
	}
}

func TestRetryExhausted(t *testing.T) {
	t.Run("non-positive max attempts", func(t *testing.T) {
		if got := RetryExhausted(1, 0); got {
			t.Fatalf("RetryExhausted(1, 0) = %v, want false", got)
		}
		if got := RetryExhausted(10, -1); got {
			t.Fatalf("RetryExhausted(10, -1) = %v, want false", got)
		}
	})

	t.Run("attempt threshold behavior", func(t *testing.T) {
		if got := RetryExhausted(2, 3); got {
			t.Fatalf("RetryExhausted(2, 3) = %v, want false", got)
		}
		if got := RetryExhausted(3, 3); !got {
			t.Fatalf("RetryExhausted(3, 3) = %v, want true", got)
		}
	})
}

func TestRetryDelay_AppliesExponentialBackoffWithJitter(t *testing.T) {
	t.Parallel()

	low := RetryDelay(0)
	baseDelay := time.Second
	if low < baseDelay || low > baseDelay+(baseDelay/4) {
		t.Fatalf("RetryDelay(0) = %s, want in [%s, %s]", low, baseDelay, baseDelay+(baseDelay/4))
	}

	high := RetryDelay(16)
	maxDelay := time.Minute
	if high < maxDelay || high > maxDelay+(maxDelay/4) {
		t.Fatalf("RetryDelay(16) = %s, want in [%s, %s]", high, maxDelay, maxDelay+(maxDelay/4))
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

func controlTestEnvelope() Envelope {
	env := subjectTestEnvelope(ActorAddress{Target: ActorTypeTask, Key: subjectTestTaskID})
	env.Namespace = NamespaceTaskControl
	return env
}
