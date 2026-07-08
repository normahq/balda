package actorlayer

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type ActorAddress struct {
	Target string `json:"target"`
	Key    string `json:"key"`
}

func SystemAddress(key string) ActorAddress {
	return ActorAddress{Target: "system", Key: key}
}

func WildcardAddress(target string) string {
	return strings.ToLower(strings.TrimSpace(target)) + ":*"
}

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
type Envelope struct {
	ID            string            `json:"id"`
	Namespace     string            `json:"namespace"`
	Kind          string            `json:"kind"`
	From          ActorAddress      `json:"from"`
	To            ActorAddress      `json:"to"`
	SessionID     string            `json:"session_id,omitempty"`
	TaskID        string            `json:"task_id,omitempty"`
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

func MarshalPayload(payload any) (string, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal actor payload: %w", err)
	}
	return string(data), nil
}

func UnmarshalPayload(raw string, dst any) error {
	if dst == nil {
		return DecodeError(fmt.Errorf("payload destination is required"))
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), dst); err != nil {
		return DecodeError(fmt.Errorf("unmarshal actor payload: %w", err))
	}
	return nil
}

func DedupeKeyOrID(env Envelope) string {
	if trimmed := strings.TrimSpace(env.DedupeKey); trimmed != "" {
		return trimmed
	}
	return strings.TrimSpace(env.ID)
}
