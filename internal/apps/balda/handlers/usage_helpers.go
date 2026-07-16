package handlers

import (
	"context"

	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/usageview"
	"google.golang.org/genai"
)

type usageSnapshot = usageview.Snapshot

type usageStateReader interface {
	RuntimeStateValue(ctx context.Context, locator baldasession.SessionLocator, key string) (any, bool, error)
}

func loadUsageSnapshot(ctx context.Context, sessions usageStateReader, locator baldasession.SessionLocator) (usageSnapshot, bool, error) {
	if sessions == nil {
		return usageSnapshot{}, false, nil
	}
	value, ok, err := sessions.RuntimeStateValue(ctx, locator, usageview.UsageStateKey)
	if err != nil || !ok {
		return usageSnapshot{}, false, err
	}
	raw, ok := value.(map[string]any)
	if !ok {
		return usageSnapshot{}, false, nil
	}
	return usageview.SnapshotFromMap(raw)
}

func renderUsageSnapshot(snapshot usageSnapshot) string {
	return usageview.RenderSnapshot(snapshot)
}

func usageSnapshotFromMetadata(meta *genai.GenerateContentResponseUsageMetadata) (usageSnapshot, bool) {
	return usageview.SnapshotFromMetadata(meta)
}

func usageSnapshotFromMap(raw map[string]any) (usageSnapshot, bool, error) {
	return usageview.SnapshotFromMap(raw)
}

const usageStateKey = usageview.UsageStateKey
