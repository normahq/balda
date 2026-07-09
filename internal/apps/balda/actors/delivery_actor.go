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
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	baldajobs "github.com/normahq/balda/internal/apps/balda/jobs"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/balda/pkg/actorlayer"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

const deliveryPendingRetryAfter = 30 * time.Second

type jobDeliveryActor struct {
	channel *baldachannel.Router
	tasks   *baldajobs.JobService
	logger  zerolog.Logger
}

type jobDeliveryActorParams struct {
	fx.In

	Channel    *baldachannel.Router
	JobService *baldajobs.JobService
	Logger     zerolog.Logger
}

func (a *jobDeliveryActor) Address() string {
	return actorlayer.WildcardAddress(baldaexecution.ActorTypeDelivery)
}

func (a *jobDeliveryActor) Handle(ctx context.Context, env actorlayer.Envelope) error {
	if strings.TrimSpace(env.Kind) != jobPayloadKindDelivery {
		return actorlayer.PolicyError(fmt.Errorf("unsupported delivery kind %q", env.Kind))
	}
	var payload DeliveryPayload
	if err := json.Unmarshal([]byte(env.PayloadJSON), &payload); err != nil {
		return actorlayer.PermanentError(fmt.Errorf("decode job delivery payload: %w", err))
	}
	if a.channel == nil {
		return actorlayer.TransientError(fmt.Errorf("channel router is required"))
	}
	if err := validateDeliveryPayload(payload); err != nil {
		return actorlayer.PermanentError(err)
	}
	envelopeJobID := strings.TrimSpace(baldaexecution.EnvelopeJobID(env))
	payloadJobID := strings.TrimSpace(payload.JobID)
	switch {
	case envelopeJobID == "" && payloadJobID == "":
	case envelopeJobID == "":
		return actorlayer.PolicyError(fmt.Errorf("delivery envelope job scope is required when payload job id is set"))
	case payloadJobID == "":
		return actorlayer.PolicyError(fmt.Errorf("delivery payload job id is required when envelope job scope is set"))
	case envelopeJobID != payloadJobID:
		return actorlayer.PolicyError(fmt.Errorf("delivery job scope mismatch: envelope=%q payload=%q", envelopeJobID, payloadJobID))
	}
	durable := deliveryRequiresOutbox(payload)
	deliveryKey := strings.TrimSpace(env.DedupeKey)
	if deliveryKey == "" {
		deliveryKey = strings.TrimSpace(env.ID)
	}
	if deliveryKey == "" {
		deliveryKey = "delivery:" + shortJobHash(env.PayloadJSON)
	}

	sum := sha256.Sum256([]byte(strings.TrimSpace(env.PayloadJSON)))
	payloadHash := hex.EncodeToString(sum[:])
	if durable && a.tasks != nil {
		record, created, err := a.tasks.ReserveDelivery(ctx, baldastate.DeliveryRecord{
			ID:          uuid.NewString(),
			DeliveryKey: deliveryKey,
			JobID:       payload.JobID,
			SessionID:   payload.Locator.SessionID,
			Channel:     firstNonEmpty(payload.Locator.ChannelType, "telegram"),
			AddressKey:  firstNonEmpty(payload.Locator.AddressKey, payload.Locator.SessionID),
			Kind:        env.Kind,
			PayloadJSON: env.PayloadJSON,
			PayloadHash: payloadHash,
			Status:      baldastate.DeliveryStatusPending,
		})
		if err != nil {
			return actorlayer.TransientError(err)
		}
		if record.PayloadHash != "" && record.PayloadHash != payloadHash {
			return actorlayer.PermanentError(fmt.Errorf("delivery key %q already reserved for different payload", deliveryKey))
		}
		if record.Status == baldastate.DeliveryStatusSent {
			return nil
		}
		if !created && !deliveryReadyForAttempt(record) {
			if record.Status == baldastate.DeliveryStatusSending {
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
			if strings.TrimSpace(payload.JobID) != "" {
				if appendErr := a.tasks.AppendEvent(ctx, payload.JobID, baldajobs.TaskEventDeliveryFailed, "delivery.actor", env.ID, map[string]any{
					"text":   strings.TrimSpace(payload.Text),
					"action": strings.TrimSpace(payload.Action),
					"mode":   payload.Mode,
					"reason": err.Error(),
				}); appendErr != nil {
					a.logger.Warn().Err(appendErr).Str("job_id", payload.JobID).Msg("failed to record job delivery failure event")
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
	if durable && a.tasks != nil && strings.TrimSpace(payload.JobID) != "" {
		if err := a.tasks.AppendEvent(ctx, payload.JobID, baldajobs.TaskEventDeliverySent, "delivery.actor", env.ID, map[string]any{
			"text": strings.TrimSpace(payload.Text),
			"mode": payload.Mode,
		}); err != nil {
			a.logger.Warn().Err(err).Str("job_id", payload.JobID).Msg("failed to record job delivery event")
		}
	}
	return nil
}

func (a *jobDeliveryActor) dispatchDelivery(ctx context.Context, payload DeliveryPayload) (string, error) {
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
	case DeliveryModeProgress:
		if payload.Progress == nil {
			return "", fmt.Errorf("progress payload is required")
		}
		return "", a.channel.SendProgress(ctx, payload.Locator, *payload.Progress)
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

func deliveryReadyForAttempt(record baldastate.DeliveryRecord) bool {
	switch record.Status {
	case baldastate.DeliveryStatusSent:
		return false
	case baldastate.DeliveryStatusSending:
		// A crash after Telegram accepted the message but before MarkDeliverySent
		// leaves this state ambiguous. Never auto-resend it.
		return false
	case baldastate.DeliveryStatusFailed:
		return true
	case baldastate.DeliveryStatusPending:
		if record.UpdatedAt.IsZero() {
			return true
		}
		return time.Since(record.UpdatedAt) >= deliveryPendingRetryAfter
	default:
		return true
	}
}

func deliveryRequiresOutbox(payload DeliveryPayload) bool {
	if !deliveryModeIsDurable(payload.Mode) {
		return false
	}
	switch payload.Settlement {
	case deliverycmd.SettlementBypass:
		return false
	case deliverycmd.SettlementOutbox:
		return true
	case "", deliverycmd.SettlementAuto:
		return strings.TrimSpace(payload.JobID) != ""
	default:
		return strings.TrimSpace(payload.JobID) != ""
	}
}
