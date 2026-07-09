package handlers

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/normahq/balda/internal/apps/balda/actors"
	"github.com/normahq/balda/internal/apps/balda/auth"
	baldaslack "github.com/normahq/balda/internal/apps/balda/channel/slack"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	"github.com/rs/zerolog"
)

func TestVerifySlackSignature(t *testing.T) {
	t.Parallel()

	secret := "test-secret"
	body := []byte(`{"type":"event_callback"}`)
	now := time.Unix(1712345678, 0)
	timestamp := "1712345678"
	base := slackSignatureVersion + ":" + timestamp + ":" + string(body)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(base))
	signature := slackSignatureVersion + "=" + hex.EncodeToString(mac.Sum(nil))

	if err := verifySlackSignature(secret, timestamp, signature, body, now); err != nil {
		t.Fatalf("verifySlackSignature() error = %v", err)
	}
}

func TestVerifySlackSignatureRejectsMismatch(t *testing.T) {
	t.Parallel()

	err := verifySlackSignature("test-secret", "1712345678", "v0=bad", []byte("body"), time.Unix(1712345678, 0))
	if err == nil {
		t.Fatal("verifySlackSignature() error = nil, want mismatch")
	}
	if !strings.Contains(err.Error(), "signature mismatch") {
		t.Fatalf("verifySlackSignature() error = %v, want mismatch marker", err)
	}
}

func TestStripSlackBotMentions(t *testing.T) {
	t.Parallel()

	got := stripSlackBotMentions("<@U123> <@U123> run tests", "U123")
	if got != "run tests" {
		t.Fatalf("stripSlackBotMentions() = %q, want run tests", got)
	}
}

func TestSlackHandlerHandleMessagePublishesDirectSessionTurn(t *testing.T) {
	stateStore := &fakeOwnerKVStore{}
	ownerStore, err := auth.NewOwnerStore(stateStore)
	if err != nil {
		t.Fatalf("NewOwnerStore() error = %v", err)
	}
	if _, err := ownerStore.RegisterOwnerSubject("slack:T123:U456"); err != nil {
		t.Fatalf("RegisterOwnerSubject() error = %v", err)
	}

	locator := baldaslack.NewDMLocator("T123", "D123")
	ts := newBaldaTopicSession(t, locator.SessionID)
	setUnexportedField(t, ts, "userID", "slack:T123:U456")
	setUnexportedField(t, ts, "agentSessionID", "agent-session-1")
	sessionManager := newBaldaSessionManagerWithSession(t, locator, ts)
	dispatcher := &recordingHandlerCommandBus{}
	handler := &SlackHandler{
		ownerStore:      ownerStore,
		sessionManager:  sessionManager,
		actorDispatcher: dispatcher,
		logger:          zerolog.Nop(),
	}

	handler.handleMessage(context.Background(), locator, "T123", "U456", "1712345678.1234", "hello", true, true)

	var envFound bool
	var envPayload actors.SessionTurnPayload
	for _, env := range dispatcher.commands {
		if env.To.Target != baldaexecution.ActorTypeSession {
			continue
		}
		if got, want := env.DedupeKey, "slack:1712345678.1234"; got != want {
			t.Fatalf("dedupe_key = %q, want %q", got, want)
		}
		if err := json.Unmarshal([]byte(env.PayloadJSON), &envPayload); err != nil {
			t.Fatalf("decode session turn payload: %v", err)
		}
		envFound = true
		break
	}
	if !envFound {
		t.Fatalf("session command not found in published commands: %+v", dispatcher.commands)
	}
	if envPayload.Source != "slack" || !envPayload.Deliver {
		t.Fatalf("session turn payload = %+v, want slack deliver=true", envPayload)
	}
	if got, want := envPayload.MessageID, slackMessageID("1712345678.1234"); got != want {
		t.Fatalf("payload message_id = %d, want %d", got, want)
	}
	if got, want := envPayload.DedupeKey, "slack:1712345678.1234"; got != want {
		t.Fatalf("payload dedupe_key = %q, want %q", got, want)
	}
	if got, want := envPayload.UserID, "slack:T123:U456"; got != want {
		t.Fatalf("payload user_id = %q, want %q", got, want)
	}
}
