package swarm

import "testing"

func TestActorKeyMapsSessionTaskAndAgent(t *testing.T) {
	tests := []struct {
		name string
		env  Envelope
		want string
	}{
		{name: "session", env: Envelope{Namespace: NamespaceWebhookInbound, SessionID: "s-1", To: ActorAddress{Target: ActorTypeTask, Key: "task-1"}}, want: "session:s-1"},
		{name: "task control", env: Envelope{Namespace: NamespaceTaskControl, TaskID: "task-1", To: ActorAddress{Target: ActorTypeTask, Key: "task-1"}}, want: "task:task-1"},
		{name: "agent", env: Envelope{Namespace: NamespaceAgentCommand, To: ActorAddress{Target: ActorTypeAgent, Key: "executor"}}, want: "agent:executor"},
		{name: "fallback", env: Envelope{Namespace: NamespaceTelemetry, To: ActorAddress{Target: ActorTypeDelivery, Key: "tg"}}, want: "delivery:tg"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := actorKey(tt.env); got != tt.want {
				t.Fatalf("actorKey() = %q, want %q", got, tt.want)
			}
		})
	}
}
