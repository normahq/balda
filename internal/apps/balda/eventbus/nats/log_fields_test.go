package natsbus

import (
	"bytes"
	"encoding/json"
	"testing"

	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	"github.com/normahq/balda/pkg/actorlayer"
	"github.com/rs/zerolog"
)

func TestWithDeliveryKeyAddsFieldForDeliveryActor(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	env := actorlayer.Envelope{To: actorlayer.ActorAddress{Target: baldaexecution.ActorTypeDelivery, Key: "telegram:user-1"}}
	withDeliveryKey(logger.Info(), env).Msg("test")

	var payload map[string]any
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal log: %v", err)
	}
	if payload["delivery_key"] != "telegram:user-1" {
		t.Fatalf("delivery_key = %v, want telegram:user-1", payload["delivery_key"])
	}
}

func TestWithDeliveryKeySkipsNonDeliveryActor(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	env := actorlayer.Envelope{To: actorlayer.ActorAddress{Target: baldaexecution.ActorTypeJob, Key: "task:123"}}
	withDeliveryKey(logger.Info(), env).Msg("test")

	var payload map[string]any
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal log: %v", err)
	}
	if _, ok := payload["delivery_key"]; ok {
		t.Fatalf("delivery_key present for non-delivery actor: %+v", payload)
	}
}
