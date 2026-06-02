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
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

const deliveryPendingRetryAfter = 30 * time.Second

type taskDeliveryActor struct {
	channel *baldatelegram.Adapter
	tasks   *swarm.TaskService
	logger  zerolog.Logger
}

type taskDeliveryActorParams struct {
	fx.In

	Channel     *baldatelegram.Adapter
	TaskService *swarm.TaskService
	Logger      zerolog.Logger
}

func (a *taskDeliveryActor) Address() string {
	return swarm.WildcardAddress(swarm.ActorTypeDelivery)
}

func (a *taskDeliveryActor) Handle(ctx context.Context, envelope any) error {
	env, err := swarm.AssertEnvelope(envelope)
	if err != nil {
		return err
	}
	if strings.TrimSpace(env.Kind) != taskPayloadKindDelivery {
		return swarm.PolicyError(fmt.Errorf("unsupported delivery kind %q", env.Kind))
	}
	var payload DeliveryPayload
	if err := json.Unmarshal([]byte(env.PayloadJSON), &payload); err != nil {
		return swarm.PermanentError(fmt.Errorf("decode task delivery payload: %w", err))
	}
	text := strings.TrimSpace(payload.Text)
	if text == "" {
		return nil
	}
	if a.channel == nil {
		return swarm.TransientError(fmt.Errorf("telegram channel adapter is required"))
	}
	deliveryKey := strings.TrimSpace(env.DedupeKey)
	if deliveryKey == "" {
		deliveryKey = strings.TrimSpace(env.ID)
	}
	if deliveryKey == "" {
		deliveryKey = "delivery:" + shortTaskHash(env.PayloadJSON)
	}

	sum := sha256.Sum256([]byte(strings.TrimSpace(env.PayloadJSON)))
	payloadHash := hex.EncodeToString(sum[:])
	if a.tasks != nil {
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
			return swarm.TransientError(err)
		}
		if record.PayloadHash != "" && record.PayloadHash != payloadHash {
			return swarm.PermanentError(fmt.Errorf("delivery key %q already reserved for different payload", deliveryKey))
		}
		if record.Status == baldastate.SwarmDeliveryStatusSent {
			return nil
		}
		if !created && !deliveryReadyForAttempt(record) {
			if record.Status == baldastate.SwarmDeliveryStatusSending {
				return swarm.TransientError(fmt.Errorf("delivery %q has ambiguous sending status; automatic resend is disabled; last updated at %s", deliveryKey, record.UpdatedAt.Format(time.RFC3339)))
			}
			return swarm.TransientError(fmt.Errorf("delivery %q is already %s; last updated at %s", deliveryKey, record.Status, record.UpdatedAt.Format(time.RFC3339)))
		}
		if err := a.tasks.MarkDeliverySending(ctx, deliveryKey); err != nil {
			return swarm.TransientError(err)
		}
	}
	providerMessageID, err := a.channel.SendAgentReplyWithProviderMessageID(ctx, payload.Locator, text)
	if err != nil {
		if a.tasks != nil {
			_ = a.tasks.MarkDeliveryFailed(ctx, deliveryKey, err.Error())
			if strings.TrimSpace(payload.TaskID) != "" {
				if appendErr := a.tasks.AppendEvent(ctx, payload.TaskID, swarm.TaskEventDeliveryFailed, "delivery.actor", env.ID, map[string]any{
					"text":   text,
					"reason": err.Error(),
				}); appendErr != nil {
					a.logger.Warn().Err(appendErr).Str("task_id", payload.TaskID).Msg("failed to record task delivery failure event")
				}
			}
		}
		return swarm.ExternalDeliveryError(err)
	}
	if a.tasks != nil {
		if err := a.tasks.MarkDeliverySent(ctx, deliveryKey, providerMessageID); err != nil {
			return swarm.TransientError(err)
		}
	}
	if a.tasks != nil && strings.TrimSpace(payload.TaskID) != "" {
		if err := a.tasks.AppendEvent(ctx, payload.TaskID, swarm.TaskEventDeliverySent, "delivery.actor", env.ID, map[string]any{
			"text": text,
		}); err != nil {
			a.logger.Warn().Err(err).Str("task_id", payload.TaskID).Msg("failed to record task delivery event")
		}
	}
	return nil
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
