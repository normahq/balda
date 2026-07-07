package handlers

import (
	"context"
	"fmt"
	"strings"

	"github.com/normahq/balda/internal/apps/balda/memory"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"google.golang.org/adk/v2/runner"
)

func prepareMemoryRunOptions(ctx context.Context, store *memory.Store, ts *baldasession.TopicSession) ([]runner.RunOption, error) {
	if store == nil || !store.MemoryEnabled() || ts == nil {
		return nil, nil
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot balda memory: %w", err)
	}
	seenVersion := int64(0)
	value, ok, err := ts.RuntimeStateValue(ctx, memory.MemoryVersionStateKey)
	if err != nil {
		return nil, fmt.Errorf("read balda memory version: %w", err)
	}
	if ok {
		seenVersion = memory.VersionFromState(value)
	}
	if snapshot.Version <= seenVersion {
		return nil, nil
	}
	stateDelta := map[string]any{
		memory.MemoryStateKey:        strings.TrimSpace(snapshot.Content),
		memory.MemoryVersionStateKey: memory.VersionStateValue(snapshot.Version),
	}
	return []runner.RunOption{runner.WithStateDelta(stateDelta)}, nil
}
