package swarm

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/normahq/balda/internal/apps/balda/memory"
)

func TestMemoryActorRejectsUnsupportedNamespace(t *testing.T) {
	t.Parallel()

	actor := NewMemoryActor()
	err := actor.Handle(context.Background(), memoryEnvelopeForTest(NamespaceHumanInbound, KindMessage, `{"operation":"task_summary"}`))
	if err == nil {
		t.Fatal("Handle() error = nil, want policy error")
	}
	if ClassifyError(err) != ErrorKindPolicy {
		t.Fatalf("ClassifyError() = %q, want %q", ClassifyError(err), ErrorKindPolicy)
	}
}

func TestMemoryActorSupportsV1Operations(t *testing.T) {
	t.Parallel()

	actor := NewMemoryActor()
	for _, op := range []string{memoryOpTaskSummary, memoryOpSessionSummary, memoryOpFactExtract, memoryOpContextPack} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			payload := fmt.Sprintf(`{"operation":%q,"scope":"default","task_id":"task-1","session_id":"session-1","content":"done"}`, op)
			if err := actor.Handle(context.Background(), memoryEnvelopeForTest(NamespaceMemorySync, op, payload)); err != nil {
				t.Fatalf("Handle(%s) error = %v, want nil", op, err)
			}
		})
	}
}

func TestMemoryActorUnknownOperationNoops(t *testing.T) {
	t.Parallel()

	actor := NewMemoryActor()
	err := actor.Handle(context.Background(), memoryEnvelopeForTest(NamespaceMemorySync, "memory_tick", `{"operation":"future_op"}`))
	if err != nil {
		t.Fatalf("Handle() error = %v, want nil noop", err)
	}
}

func TestMemoryActorInvalidPayloadIsPermanent(t *testing.T) {
	t.Parallel()

	actor := NewMemoryActor()
	err := actor.Handle(context.Background(), memoryEnvelopeForTest(NamespaceMemorySync, memoryOpTaskSummary, `{`))
	if err == nil {
		t.Fatal("Handle() error = nil, want decode error")
	}
	if ClassifyError(err) != ErrorKindDecode {
		t.Fatalf("ClassifyError() = %q, want %q", ClassifyError(err), ErrorKindDecode)
	}
	var actorErr *ActorError
	if !errors.As(err, &actorErr) || actorErr == nil || actorErr.Err == nil {
		t.Fatalf("err = %v, want wrapped actor error", err)
	}
}

func TestMemoryActorFactExtractWritesFacts(t *testing.T) {
	t.Parallel()

	store := memory.NewStore(t.TempDir(), true)
	actor := NewMemoryActorWithStore(store)
	payload := `{"operation":"fact_extract","scope":"default","task_id":"task-1","session_id":"session-1","content":"fact: Balda uses JetStream\n- actor lanes are serialized\n\nKeep docs updated"}`
	if err := actor.Handle(context.Background(), memoryEnvelopeForTest(NamespaceMemorySync, memoryOpFactExtract, payload)); err != nil {
		t.Fatalf("Handle(fact_extract) error = %v, want nil", err)
	}

	got, err := store.ReadMemory(context.Background())
	if err != nil {
		t.Fatalf("ReadMemory() error = %v", err)
	}
	lines := make([]string, 0, 4)
	for _, line := range strings.Split(strings.TrimSpace(got), "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			lines = append(lines, trimmed)
		}
	}
	want := []string{
		"Balda uses JetStream",
		"actor lanes are serialized",
		"Keep docs updated",
	}
	if !slices.Equal(lines, want) {
		t.Fatalf("memory facts = %#v, want %#v", lines, want)
	}
	if path := store.MemoryPath(); !strings.HasSuffix(path, memory.MemoryFileName) {
		t.Fatalf("MemoryPath() = %q, want suffix %q", path, memory.MemoryFileName)
	}
}

func TestMemoryActorFactExtractNoopsWhenMemoryDisabled(t *testing.T) {
	t.Parallel()

	store := memory.NewStore(t.TempDir(), false)
	actor := NewMemoryActorWithStore(store)
	payload := `{"operation":"fact_extract","content":"fact: noop"}`
	if err := actor.Handle(context.Background(), memoryEnvelopeForTest(NamespaceMemorySync, memoryOpFactExtract, payload)); err != nil {
		t.Fatalf("Handle(fact_extract disabled) error = %v, want nil", err)
	}
	got, err := store.ReadMemory(context.Background())
	if err != nil {
		t.Fatalf("ReadMemory() error = %v", err)
	}
	if strings.TrimSpace(got) != "" {
		t.Fatalf("memory content = %q, want empty when disabled", got)
	}
}

func memoryEnvelopeForTest(namespace, kind, payload string) Envelope {
	return Envelope{
		ID:          "mem-1",
		Namespace:   namespace,
		Kind:        kind,
		From:        ActorAddress{Target: ActorTypeTask, Key: "task-1"},
		To:          ActorAddress{Target: ActorTypeMemory, Key: "global"},
		SessionID:   "session-1",
		TaskID:      "task-1",
		PayloadJSON: payload,
	}
}
