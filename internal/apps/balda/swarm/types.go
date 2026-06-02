// Package swarm contains Balda's durable actor runtime primitives.
package swarm

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	ActorTypeSystem     = "system"
	ActorTypeSession    = "session"
	ActorTypeTask       = "task"
	ActorTypeGoalkeeper = "goalkeeper"
	ActorTypeGoal       = ActorTypeGoalkeeper
	ActorTypeDelivery   = "delivery"

	NamespaceHumanInbound      = "human.inbound"
	NamespaceWebhookInbound    = "webhook.inbound"
	NamespaceScheduleInbound   = "schedule.inbound"
	NamespaceAgentResult       = "agent.result"
	NamespaceGoalkeeperCommand = "goalkeeper.command"
	NamespaceGoalCommand       = NamespaceGoalkeeperCommand
	NamespaceTaskControl       = "task.control"
	NamespaceTelemetry         = "telemetry"

	KindMessage       = "message"
	KindWebhookEvent  = "webhook_event"
	KindScheduledTask = "scheduled_task"
	KindGoal          = "goal"
	KindCancel        = "cancel"
)

type ActorAddress struct {
	Target string `json:"target"`
	Key    string `json:"key"`
}

func SystemAddress(key string) ActorAddress {
	return ActorAddress{Target: ActorTypeSystem, Key: key}
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
	return nil
}

func EncodeEnvelope(e Envelope) (string, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return "", fmt.Errorf("encode envelope: %w", err)
	}
	return string(data), nil
}

func DecodeEnvelope(raw string) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &env); err != nil {
		return Envelope{}, fmt.Errorf("decode envelope: %w", err)
	}
	if err := env.Validate(); err != nil {
		return Envelope{}, err
	}
	return env, nil
}
