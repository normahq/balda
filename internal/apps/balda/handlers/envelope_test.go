package handlers

import (
	"context"
	"strings"
	"testing"
)

func TestResolveEnvelopeTarget_AliasOwner(t *testing.T) {
	t.Parallel()

	target, err := resolveEnvelopeTarget(
		context.Background(),
		newOwnerStoreForTest(t, 101, 9001),
		envelopeTarget{Target: " alias ", Key: " owner "},
	)
	if err != nil {
		t.Fatalf("resolveEnvelopeTarget() error = %v", err)
	}
	if got, want := target.Locator.SessionID, "tg-9001-0"; got != want {
		t.Fatalf("session_id = %q, want %q", got, want)
	}
	if got, want := target.UserID, "tg-101"; got != want {
		t.Fatalf("user_id = %q, want %q", got, want)
	}
	if got := target.TopicID; got != 0 {
		t.Fatalf("topic_id = %d, want 0", got)
	}
}

func TestResolveEnvelopeTarget_RejectsUnknownAlias(t *testing.T) {
	t.Parallel()

	_, err := resolveEnvelopeTarget(
		context.Background(),
		newOwnerStoreForTest(t, 101, 9001),
		envelopeTarget{Target: "alias", Key: "vasya"},
	)
	if err == nil {
		t.Fatal("resolveEnvelopeTarget() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), `unsupported alias target "vasya"`) {
		t.Fatalf("resolveEnvelopeTarget() error = %v", err)
	}
}
