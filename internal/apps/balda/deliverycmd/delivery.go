package deliverycmd

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/normahq/balda/pkg/actorlayer"
)

const taskPayloadKindDelivery = "delivery"

type Payload struct {
	TaskID  string                      `json:"task_id,omitempty"`
	Locator baldasession.SessionLocator `json:"locator"`
	Profile Profile                     `json:"profile,omitempty,omitzero"`
	Mode    Mode                        `json:"mode"`
	Text    string                      `json:"text,omitempty"`
	DraftID int                         `json:"draft_id,omitempty"`
	Action  string                      `json:"action,omitempty"`
}

type Mode string

type Profile = deliveryfmt.Profile

const (
	ModeAgentReply Mode = "agent_reply"
	ModePlain      Mode = "plain"
	ModeMarkdown   Mode = "markdown"
	ModeDraftPlain Mode = "draft_plain"
	ModeChatAction Mode = "chat_action"
)

func AgentReplyEnvelope(
	taskID string,
	from actorlayer.ActorAddress,
	locator baldasession.SessionLocator,
	text string,
	dedupeSuffix string,
) (actorlayer.Envelope, error) {
	return AgentReplyEnvelopeWithProfile(taskID, from, locator, Profile{}, text, dedupeSuffix)
}

func AgentReplyEnvelopeWithProfile(
	taskID string,
	from actorlayer.ActorAddress,
	locator baldasession.SessionLocator,
	profile Profile,
	text string,
	dedupeSuffix string,
) (actorlayer.Envelope, error) {
	return envelope(taskID, from, Payload{
		TaskID:  strings.TrimSpace(taskID),
		Locator: locator,
		Profile: normalizeProfile(profile),
		Mode:    ModeAgentReply,
		Text:    strings.TrimSpace(text),
	}, dedupeSuffix)
}

func PlainEnvelope(
	taskID string,
	from actorlayer.ActorAddress,
	locator baldasession.SessionLocator,
	text string,
	dedupeSuffix string,
) (actorlayer.Envelope, error) {
	return envelope(taskID, from, Payload{
		TaskID:  strings.TrimSpace(taskID),
		Locator: locator,
		Mode:    ModePlain,
		Text:    strings.TrimSpace(text),
	}, dedupeSuffix)
}

func MarkdownEnvelope(
	taskID string,
	from actorlayer.ActorAddress,
	locator baldasession.SessionLocator,
	text string,
	dedupeSuffix string,
) (actorlayer.Envelope, error) {
	return MarkdownEnvelopeWithProfile(taskID, from, locator, Profile{}, text, dedupeSuffix)
}

func MarkdownEnvelopeWithProfile(
	taskID string,
	from actorlayer.ActorAddress,
	locator baldasession.SessionLocator,
	profile Profile,
	text string,
	dedupeSuffix string,
) (actorlayer.Envelope, error) {
	return envelope(taskID, from, Payload{
		TaskID:  strings.TrimSpace(taskID),
		Locator: locator,
		Profile: normalizeProfile(profile),
		Mode:    ModeMarkdown,
		Text:    strings.TrimSpace(text),
	}, dedupeSuffix)
}

func DraftPlainEnvelope(
	taskID string,
	from actorlayer.ActorAddress,
	locator baldasession.SessionLocator,
	draftID int,
	text string,
) (actorlayer.Envelope, error) {
	return envelope(taskID, from, Payload{
		TaskID:  strings.TrimSpace(taskID),
		Locator: locator,
		Mode:    ModeDraftPlain,
		Text:    strings.TrimSpace(text),
		DraftID: draftID,
	}, "")
}

func ChatActionEnvelope(
	taskID string,
	from actorlayer.ActorAddress,
	locator baldasession.SessionLocator,
	action string,
) (actorlayer.Envelope, error) {
	return envelope(taskID, from, Payload{
		TaskID:  strings.TrimSpace(taskID),
		Locator: locator,
		Mode:    ModeChatAction,
		Action:  strings.TrimSpace(action),
	}, "")
}

func normalizeProfile(profile Profile) Profile {
	return deliveryfmt.NormalizeProfile(profile)
}

func Validate(payload Payload) error {
	switch payload.Mode {
	case ModeAgentReply, ModePlain, ModeMarkdown, ModeDraftPlain:
		if strings.TrimSpace(payload.Text) == "" {
			return fmt.Errorf("delivery text is required")
		}
	case ModeChatAction:
		if strings.TrimSpace(payload.Action) == "" {
			return fmt.Errorf("delivery action is required")
		}
	default:
		return fmt.Errorf("unsupported delivery mode %q", payload.Mode)
	}
	if payload.Mode == ModeDraftPlain && payload.DraftID <= 0 {
		return fmt.Errorf("draft id is required")
	}
	return nil
}

func envelope(
	taskID string,
	from actorlayer.ActorAddress,
	payload Payload,
	dedupeSuffix string,
) (actorlayer.Envelope, error) {
	if strings.TrimSpace(payload.Locator.ChannelType) == "" || strings.TrimSpace(payload.Locator.AddressKey) == "" || strings.TrimSpace(payload.Locator.SessionID) == "" {
		return actorlayer.Envelope{}, fmt.Errorf("delivery locator is required")
	}
	if err := Validate(payload); err != nil {
		return actorlayer.Envelope{}, err
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return actorlayer.Envelope{}, fmt.Errorf("encode delivery payload: %w", err)
	}

	dedupeKey := deliveryDedupeKey(taskID, payload.Mode, dedupeSuffix)
	return actorlayer.Envelope{
		ID:            dedupeKey,
		Namespace:     swarm.NamespaceAgentResult,
		Kind:          taskPayloadKindDelivery,
		From:          from,
		To:            actorlayer.ActorAddress{Target: swarm.ActorTypeDelivery, Key: payload.Locator.DeliveryActorKey()},
		SessionID:     payload.Locator.SessionID,
		TaskID:        strings.TrimSpace(taskID),
		CorrelationID: strings.TrimSpace(taskID),
		Priority:      70,
		DedupeKey:     dedupeKey,
		PayloadJSON:   string(data),
	}, nil
}

func deliveryDedupeKey(taskID string, mode Mode, dedupeSuffix string) string {
	trimmedTaskID := strings.TrimSpace(taskID)
	if trimmedTaskID == "" {
		id := "delivery:" + strings.ToLower(string(mode)) + ":" + uuid.NewString()
		if suffix := strings.TrimSpace(dedupeSuffix); suffix != "" {
			return id + ":" + suffix
		}
		return id
	}
	if suffix := strings.TrimSpace(dedupeSuffix); suffix != "" {
		return trimmedTaskID + ":delivery:" + suffix
	}
	return trimmedTaskID + ":delivery:" + strings.ToLower(string(mode)) + ":" + uuid.NewString()
}
