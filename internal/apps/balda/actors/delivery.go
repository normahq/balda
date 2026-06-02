package actors

import (
	"encoding/json"
	"fmt"
	"strings"

	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/swarm"
)

func DeliveryEnvelope(
	taskID string,
	from swarm.ActorAddress,
	locator baldasession.SessionLocator,
	text string,
	dedupeSuffix string,
) (swarm.Envelope, error) {
	message := strings.TrimSpace(text)
	if message == "" {
		return swarm.Envelope{}, fmt.Errorf("delivery text is required")
	}
	payload := DeliveryPayload{
		TaskID:  strings.TrimSpace(taskID),
		Locator: locator,
		Text:    message,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return swarm.Envelope{}, fmt.Errorf("encode delivery payload: %w", err)
	}
	dedupeKey := strings.TrimSpace(taskID)
	if dedupeKey == "" {
		dedupeKey = "delivery:" + shortTaskHash(strings.Join([]string{
			strings.TrimSpace(locator.SessionID),
			strings.TrimSpace(locator.AddressKey),
			message,
		}, "|"))
	}
	if suffix := strings.TrimSpace(dedupeSuffix); suffix != "" {
		dedupeKey += ":delivery:" + suffix
	}
	return swarm.Envelope{
		ID:            dedupeKey,
		Namespace:     swarm.NamespaceAgentResult,
		Kind:          taskPayloadKindDelivery,
		From:          from,
		To:            swarm.ActorAddress{Target: swarm.ActorTypeDelivery, Key: firstNonEmpty(locator.AddressKey, locator.SessionID, "telegram")},
		SessionID:     locator.SessionID,
		TaskID:        strings.TrimSpace(taskID),
		CorrelationID: strings.TrimSpace(taskID),
		Priority:      70,
		DedupeKey:     dedupeKey,
		PayloadJSON:   string(data),
	}, nil
}
