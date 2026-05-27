package natsbus

import (
	"strings"

	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/rs/zerolog"
)

func withDeliveryKey(evt *zerolog.Event, env swarm.Envelope) *zerolog.Event {
	if evt == nil {
		return nil
	}
	if !strings.EqualFold(strings.TrimSpace(env.To.Target), swarm.ActorTypeDelivery) {
		return evt
	}
	return evt.Str("delivery_key", strings.TrimSpace(env.To.Key))
}
