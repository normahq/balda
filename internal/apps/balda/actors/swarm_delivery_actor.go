package actors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	baldachannel "github.com/normahq/balda/internal/apps/balda/channel"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/normahq/balda/pkg/actorlayer"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

const deliveryPendingRetryAfter = 30 * time.Second

type taskDeliveryActor struct {
	channel *baldachannel.Router
	tasks   *swarm.TaskService
	logger  zerolog.Logger
}

type taskDeliveryActorParams struct {
	fx.In

	Channel     *baldachannel.Router
	TaskService *swarm.TaskService
	Logger      zerolog.Logger
}

func (a *taskDeliveryActor) Address() string {
	return actorlayer.WildcardAddress(swarm.ActorTypeDelivery)
}

func (a *taskDeliveryActor) Handle(ctx context.Context, env actorlayer.Envelope) error {
	if strings.TrimSpace(env.Kind) != taskPayloadKindDelivery {
		return actorlayer.PolicyError(fmt.Errorf("unsupported delivery kind %q", env.Kind))
	}
	var payload DeliveryPayload
	if err := json.Unmarshal([]byte(env.PayloadJSON), &payload); err != nil {
		return actorlayer.PermanentError(fmt.Errorf("decode task delivery payload: %w", err))
	}
	if a.channel == nil {
		return actorlayer.TransientError(fmt.Errorf("channel router is required"))
	}
	if err := validateDeliveryPayload(payload); err != nil {
		return actorlayer.PermanentError(err)
	}
	durable := deliveryModeIsDurable(payload.Mode)
	deliveryKey := strings.TrimSpace(env.DedupeKey)
	if deliveryKey == "" {
		deliveryKey = strings.TrimSpace(env.ID)
	}
	if deliveryKey == "" {
		deliveryKey = "delivery:" + shortTaskHash(env.PayloadJSON)
	}

	sum := sha256.Sum256([]byte(strings.TrimSpace(env.PayloadJSON)))
	payloadHash := hex.EncodeToString(sum[:])
	if durable && a.tasks != nil {
		record, created, err := a.tasks.ReserveDelivery(ctx, baldastate.SwarmDeliveryRecord{
			ID:          uuid.NewString(),
			DeliveryKey: deliveryKey,
			TaskID:      payload.TaskID,
			SessionID:   payload.Locator.SessionID,
			Channel:     firstNonEmpty(payload.Locator.ChannelType, "telegram"),
			AddressKey:  firstNonEmpty(payload.Locator.AddressKey, payload.Locator.SessionID),
			Kind:        env.Kind,
			PayloadJSON: env.PayloadJSON,
			PayloadHash: payloadHash,
			Status:      baldastate.SwarmDeliveryStatusPending,
		})
		if err != nil {
			return actorlayer.TransientError(err)
		}
		if record.PayloadHash != "" && record.PayloadHash != payloadHash {
			return actorlayer.PermanentError(fmt.Errorf("delivery key %q already reserved for different payload", deliveryKey))
		}
		if record.Status == baldastate.SwarmDeliveryStatusSent {
			return nil
		}
		if !created && !deliveryReadyForAttempt(record) {
			if record.Status == baldastate.SwarmDeliveryStatusSending {
				return actorlayer.TransientError(fmt.Errorf("delivery %q has ambiguous sending status; automatic resend is disabled; last updated at %s", deliveryKey, record.UpdatedAt.Format(time.RFC3339)))
			}
			return actorlayer.TransientError(fmt.Errorf("delivery %q is already %s; last updated at %s", deliveryKey, record.Status, record.UpdatedAt.Format(time.RFC3339)))
		}
		if err := a.tasks.MarkDeliverySending(ctx, deliveryKey); err != nil {
			return actorlayer.TransientError(err)
		}
	}
	providerMessageID, err := a.dispatchDelivery(ctx, payload)
	if err != nil {
		if durable && a.tasks != nil {
			_ = a.tasks.MarkDeliveryFailed(ctx, deliveryKey, err.Error())
			if strings.TrimSpace(payload.TaskID) != "" {
				if appendErr := a.tasks.AppendEvent(ctx, payload.TaskID, swarm.TaskEventDeliveryFailed, "delivery.actor", env.ID, map[string]any{
					"text":   strings.TrimSpace(payload.Text),
					"action": strings.TrimSpace(payload.Action),
					"mode":   payload.Mode,
					"reason": err.Error(),
				}); appendErr != nil {
					a.logger.Warn().Err(appendErr).Str("task_id", payload.TaskID).Msg("failed to record task delivery failure event")
				}
			}
		}
		return actorlayer.ExternalDeliveryError(err)
	}
	if durable && a.tasks != nil {
		if err := a.tasks.MarkDeliverySent(ctx, deliveryKey, providerMessageID); err != nil {
			return actorlayer.TransientError(err)
		}
	}
	if durable && a.tasks != nil && strings.TrimSpace(payload.TaskID) != "" {
		if err := a.tasks.AppendEvent(ctx, payload.TaskID, swarm.TaskEventDeliverySent, "delivery.actor", env.ID, map[string]any{
			"text": strings.TrimSpace(payload.Text),
			"mode": payload.Mode,
		}); err != nil {
			a.logger.Warn().Err(err).Str("task_id", payload.TaskID).Msg("failed to record task delivery event")
		}
	}
	return nil
}

func (a *taskDeliveryActor) dispatchDelivery(ctx context.Context, payload DeliveryPayload) (string, error) {
	switch payload.Mode {
	case DeliveryModeAgentReply:
		return a.channel.SendAgentReplyWithProviderMessageIDAndProfile(ctx, payload.Locator, payload.Profile, payload.Text)
	case DeliveryModePlain:
		return "", a.channel.SendPlain(ctx, payload.Locator, payload.Text)
	case DeliveryModeMarkdown:
		return "", a.channel.SendMarkdownWithProfile(ctx, payload.Locator, payload.Profile, payload.Text)
	case DeliveryModeDraftPlain:
		return "", a.channel.SendDraftPlain(ctx, payload.Locator, payload.DraftID, payload.Text)
	case DeliveryModeChatAction:
		return "", a.channel.SendTyping(ctx, payload.Locator)
	default:
		return "", fmt.Errorf("unsupported delivery mode %q", payload.Mode)
	}
}

func deliveryModeIsDurable(mode DeliveryMode) bool {
	switch mode {
	case DeliveryModeAgentReply, DeliveryModePlain, DeliveryModeMarkdown:
		return true
	default:
		return false
	}
}

func deliveryReadyForAttempt(record baldastate.SwarmDeliveryRecord) bool {
	switch record.Status {
	case baldastate.SwarmDeliveryStatusSent:
		return false
	case baldastate.SwarmDeliveryStatusSending:
		// A crash after Telegram accepted the message but before MarkDeliverySent
		// leaves this state ambiguous. Never auto-resend it.
		return false
	case baldastate.SwarmDeliveryStatusFailed:
		return true
	case baldastate.SwarmDeliveryStatusPending:
		if record.UpdatedAt.IsZero() {
			return true
		}
		return time.Since(record.UpdatedAt) >= deliveryPendingRetryAfter
	default:
		return true
	}
}
