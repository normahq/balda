package natsbus

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/rs/zerolog"
)

func TestWithDeliveryKeyAddsFieldForDeliveryActor(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	env := swarm.Envelope{To: swarm.ActorAddress{Target: swarm.ActorTypeDelivery, Key: "delivery:user-1"}}
	withDeliveryKey(logger.Info(), env).Msg("test")

	var payload map[string]any
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal log: %v", err)
	}
	if payload["delivery_key"] != "delivery:user-1" {
		t.Fatalf("delivery_key = %v, want delivery:user-1", payload["delivery_key"])
	}
}

func TestWithDeliveryKeySkipsNonDeliveryActor(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	env := swarm.Envelope{To: swarm.ActorAddress{Target: swarm.ActorTypeTask, Key: "task:123"}}
	withDeliveryKey(logger.Info(), env).Msg("test")

	var payload map[string]any
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal log: %v", err)
	}
	if _, ok := payload["delivery_key"]; ok {
		t.Fatalf("delivery_key present for non-delivery actor: %+v", payload)
	}
}
