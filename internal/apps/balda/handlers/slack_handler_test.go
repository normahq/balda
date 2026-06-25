package handlers

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"
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
