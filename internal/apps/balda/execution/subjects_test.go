package execution

import (
	"strings"
	"testing"

	"github.com/normahq/balda/pkg/actorlayer"
)

const subjectTestJobID = "task-1"

func TestSubjectForEnvelope_UsesStableCommandSubjects(t *testing.T) {
	tests := []struct {
		name string
		env  actorlayer.Envelope
		want string
	}{
		{name: "session", env: subjectTestEnvelope(actorlayer.ActorAddress{Target: ActorTypeSession, Key: "tg-1.2"}), want: SubjectCommandSession},
		{name: "task", env: subjectTestEnvelope(actorlayer.ActorAddress{Target: ActorTypeJob, Key: subjectTestJobID}), want: SubjectCommandJob},
		{name: "goal", env: subjectTestEnvelope(actorlayer.ActorAddress{Target: ActorTypeGoal, Key: "goal-1"}), want: SubjectCommandGoal},
		{name: "delivery", env: subjectTestEnvelope(actorlayer.ActorAddress{Target: ActorTypeDelivery, Key: "tg-1"}), want: SubjectCommandDelivery},
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
		SubjectCommandJob,
		SubjectCommandGoal,
		SubjectCommandDelivery,
		SubjectCommandControl,
		SubjectCommandAll,
	}
	for _, subject := range commandSubjects {
		if !strings.HasPrefix(subject, "balda.v1.cmd") {
			t.Fatalf("command subject %q must start with balda.v1.cmd", subject)
		}
	}
}

func TestEnvelopeHeaders_UseEnvelopeIdentityHeaders(t *testing.T) {
	env := subjectTestEnvelope(actorlayer.ActorAddress{Target: ActorTypeJob, Key: subjectTestJobID})
	env.CorrelationID = "corr-1"
	env.Priority = 80
	headers := EnvelopeHeaders(env)
	if headers[HeaderEnvelopeID] != env.ID {
		t.Fatalf("%s = %q, want %q", HeaderEnvelopeID, headers[HeaderEnvelopeID], env.ID)
	}
	if headers[HeaderActorKey] != subjectTestJobID {
		t.Fatalf("%s = %q, want %s", HeaderActorKey, headers[HeaderActorKey], subjectTestJobID)
	}
	if headers[HeaderPriority] != "80" {
		t.Fatalf("%s = %q, want 80", HeaderPriority, headers[HeaderPriority])
	}
}

func subjectTestEnvelope(to actorlayer.ActorAddress) actorlayer.Envelope {
	return actorlayer.Envelope{
		ID:          "env-1",
		Namespace:   NamespaceHumanInbound,
		Kind:        KindMessage,
		From:        actorlayer.ActorAddress{Target: "test", Key: "source"},
		To:          to,
		SessionID:   "session-1",
		PayloadJSON: `{"ok":true}`,
	}
}

func controlTestEnvelope() actorlayer.Envelope {
	env := subjectTestEnvelope(actorlayer.ActorAddress{Target: ActorTypeJob, Key: subjectTestJobID})
	env.Namespace = NamespaceJobControl
	return env
}
