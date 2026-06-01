package swarm

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/normahq/balda/internal/apps/balda/memory"
)

type memoryActor struct {
	memoryStore *memory.Store
}

const (
	memoryOpTaskSummary    = "task_summary"
	memoryOpSessionSummary = "session_summary"
	memoryOpFactExtract    = "fact_extract"
	memoryOpContextPack    = "context_pack"
)

type memorySyncPayload struct {
	Operation string `json:"operation,omitempty"`
	Scope     string `json:"scope,omitempty"`
	TaskID    string `json:"task_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	Content   string `json:"content,omitempty"`
}

func (memoryActor) Address() string {
	return WildcardAddress(ActorTypeMemory)
}

func (a memoryActor) Handle(ctx context.Context, envelope any) error {
	env, err := assertEnvelope(envelope)
	if err != nil {
		return err
	}
	if env.Namespace != NamespaceMemorySync {
		return PolicyError(fmt.Errorf("memory actor does not support %q/%q yet", env.Namespace, env.Kind))
	}
	if strings.TrimSpace(env.PayloadJSON) == "" {
		return nil
	}
	var payload memorySyncPayload
	if err := json.Unmarshal([]byte(env.PayloadJSON), &payload); err != nil {
		return DecodeError(fmt.Errorf("decode memory sync payload: %w", err))
	}
	operation := strings.ToLower(strings.TrimSpace(payload.Operation))
	if operation == "" {
		operation = strings.ToLower(strings.TrimSpace(env.Kind))
	}
	switch operation {
	case "", memoryOpTaskSummary, memoryOpSessionSummary, memoryOpFactExtract, memoryOpContextPack:
		return a.handleOperation(ctx, operation, payload)
	default:
		// Unknown operations are treated as noop to keep memory sync idempotent.
		return nil
	}
}

func (a memoryActor) handleOperation(ctx context.Context, operation string, payload memorySyncPayload) error {
	switch operation {
	case memoryOpTaskSummary, memoryOpSessionSummary, memoryOpContextPack:
		return a.handleSummaryOperation(ctx, operation, payload)
	case memoryOpFactExtract:
		return a.handleFactExtract(ctx, payload)
	default:
		return nil
	}
}

func (a memoryActor) handleSummaryOperation(ctx context.Context, operation string, payload memorySyncPayload) error {
	if a.memoryStore == nil || !a.memoryStore.MemoryEnabled() {
		return nil
	}
	content := strings.TrimSpace(payload.Content)
	if content == "" {
		return nil
	}
	meta := make([]string, 0, 3)
	if value := strings.TrimSpace(payload.Scope); value != "" {
		meta = append(meta, "scope="+value)
	}
	if value := strings.TrimSpace(payload.TaskID); value != "" {
		meta = append(meta, "task_id="+value)
	}
	if value := strings.TrimSpace(payload.SessionID); value != "" {
		meta = append(meta, "session_id="+value)
	}
	prefix := strings.TrimSpace(operation)
	if prefix == "" {
		prefix = "memory"
	}
	entry := prefix + ": " + content
	if len(meta) > 0 {
		entry = prefix + " (" + strings.Join(meta, ", ") + "): " + content
	}
	if err := a.memoryStore.Remember(ctx, entry); err != nil {
		return TransientError(fmt.Errorf("remember %s entry: %w", operation, err))
	}
	return nil
}

func (a memoryActor) handleFactExtract(ctx context.Context, payload memorySyncPayload) error {
	if a.memoryStore == nil || !a.memoryStore.MemoryEnabled() {
		return nil
	}
	trimmed := strings.TrimSpace(payload.Content)
	if trimmed == "" {
		return nil
	}
	lines := strings.Split(trimmed, "\n")
	facts := make([]string, 0, len(lines))
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		line = strings.TrimLeft(line, "-*• \t")
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		lower := strings.ToLower(line)
		if strings.HasPrefix(lower, "fact:") {
			line = strings.TrimSpace(line[len("fact:"):])
		}
		if line == "" {
			continue
		}
		facts = append(facts, line)
	}
	if len(facts) == 0 {
		facts = []string{trimmed}
	}
	for _, fact := range facts {
		if err := a.memoryStore.Remember(ctx, fact); err != nil {
			return TransientError(fmt.Errorf("remember extracted fact: %w", err))
		}
	}
	return nil
}
