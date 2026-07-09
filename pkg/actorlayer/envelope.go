package actorlayer

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// ActorAddress identifies an actor target and concrete key.
//
// String renders addresses as lowercase target plus the original trimmed key,
// separated by a colon. Both fields are required for concrete delivery
// addresses.
type ActorAddress struct {
	Target string `json:"target"`
	Key    string `json:"key"`
}

// SystemAddress returns an address in the reserved "system" target.
func SystemAddress(key string) ActorAddress {
	return ActorAddress{Target: "system", Key: key}
}

// WildcardAddress returns the normalized registry address for all keys in a
// target, such as "session:*".
func WildcardAddress(target string) string {
	return strings.ToLower(strings.TrimSpace(target)) + ":*"
}

// String returns the normalized full actor address.
func (a ActorAddress) String() (string, error) {
	target := strings.ToLower(strings.TrimSpace(a.Target))
	key := strings.TrimSpace(a.Key)
	if target == "" {
		return "", fmt.Errorf("actor target is required")
	}
	if key == "" {
		return "", fmt.Errorf("actor key is required")
	}
	return target + ":" + key, nil
}

// Envelope is the durable actor transport unit.
//
// Attempt is zero-based delivery state persisted in the envelope. Runtime
// Delivery.Attempt reports the current delivery attempt as a one-based value
// for retry policy and event metadata.
type Envelope struct {
	ID            string            `json:"id"`
	Namespace     string            `json:"namespace"`
	Kind          string            `json:"kind"`
	From          ActorAddress      `json:"from"`
	To            ActorAddress      `json:"to"`
	SessionID     string            `json:"session_id,omitempty"`
	CorrelationID string            `json:"correlation_id,omitempty"`
	CausationID   string            `json:"causation_id,omitempty"`
	Priority      int               `json:"priority,omitempty"`
	DedupeKey     string            `json:"dedupe_key,omitempty"`
	Attempt       int               `json:"attempt,omitempty"`
	MaxAttempts   int               `json:"max_attempts,omitempty"`
	NotBefore     time.Time         `json:"not_before,omitempty"`
	ExpiresAt     time.Time         `json:"expires_at,omitempty"`
	PayloadJSON   string            `json:"payload_json"`
	Meta          map[string]string `json:"meta,omitempty"`
	ReportTo      *ActorAddress     `json:"report_to,omitempty"`
}

// Validate verifies the envelope fields required by actorlayer runtimes and
// transports.
func (e Envelope) Validate() error {
	if strings.TrimSpace(e.ID) == "" {
		return fmt.Errorf("envelope id is required")
	}
	if strings.TrimSpace(e.Namespace) == "" {
		return fmt.Errorf("envelope namespace is required")
	}
	if strings.TrimSpace(e.Kind) == "" {
		return fmt.Errorf("envelope kind is required")
	}
	if _, err := e.From.String(); err != nil {
		return fmt.Errorf("envelope from: %w", err)
	}
	if _, err := e.To.String(); err != nil {
		return fmt.Errorf("envelope to: %w", err)
	}
	if strings.TrimSpace(e.PayloadJSON) == "" {
		return fmt.Errorf("envelope payload_json is required")
	}
	if !json.Valid([]byte(strings.TrimSpace(e.PayloadJSON))) {
		return fmt.Errorf("envelope payload_json must be valid json")
	}
	if e.ReportTo != nil {
		if _, err := e.ReportTo.String(); err != nil {
			return fmt.Errorf("envelope report_to: %w", err)
		}
	}
	return nil
}

// EncodeEnvelope validates and marshals an envelope as JSON.
func EncodeEnvelope(e Envelope) (string, error) {
	if err := e.Validate(); err != nil {
		return "", fmt.Errorf("encode envelope: %w", err)
	}
	data, err := json.Marshal(e)
	if err != nil {
		return "", fmt.Errorf("encode envelope: %w", err)
	}
	return string(data), nil
}

// DecodeEnvelope unmarshals and validates an envelope JSON string.
//
// JSON and envelope validation errors are wrapped as DecodeError so runtimes can
// classify malformed deliveries as non-retryable.
func DecodeEnvelope(raw string) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &env); err != nil {
		return Envelope{}, DecodeError(fmt.Errorf("decode envelope: %w", err))
	}
	if err := env.Validate(); err != nil {
		return Envelope{}, DecodeError(err)
	}
	return env, nil
}

// MarshalPayload marshals a typed actor payload for Envelope.PayloadJSON.
func MarshalPayload(payload any) (string, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal actor payload: %w", err)
	}
	return string(data), nil
}

// UnmarshalPayload unmarshals Envelope.PayloadJSON into dst.
//
// Invalid payloads and nil destinations are wrapped as DecodeError.
func UnmarshalPayload(raw string, dst any) error {
	if dst == nil {
		return DecodeError(fmt.Errorf("payload destination is required"))
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), dst); err != nil {
		return DecodeError(fmt.Errorf("unmarshal actor payload: %w", err))
	}
	return nil
}

// DedupeKeyOrID returns the explicit dedupe key, or the envelope ID when no
// dedupe key is set.
func DedupeKeyOrID(env Envelope) string {
	if trimmed := strings.TrimSpace(env.DedupeKey); trimmed != "" {
		return trimmed
	}
	return strings.TrimSpace(env.ID)
}
