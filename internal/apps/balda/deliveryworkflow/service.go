package deliveryworkflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/baldaworks/go-actorlayer"
	"github.com/google/uuid"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/actorcmd"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	baldajobs "github.com/normahq/balda/internal/apps/balda/jobs"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/rs/zerolog"
)

type Lifecycle interface {
	ReserveDelivery(ctx context.Context, record baldastate.DeliveryRecord) (baldastate.DeliveryRecord, bool, error)
	MarkDeliverySending(ctx context.Context, deliveryKey string) error
	MarkDeliveryFailed(ctx context.Context, deliveryKey string, reason string) error
	MarkDeliverySent(ctx context.Context, deliveryKey string, providerMessageID string) error
	AppendEvent(ctx context.Context, jobID string, eventType string, actor string, messageID string, payload any) error
}

type Sender interface {
	SendAgentReplyWithProviderMessageIDAndProfile(ctx context.Context, locator deliverycmd.Locator, profile deliverycmd.Profile, text string) (string, error)
	SendPlain(ctx context.Context, locator deliverycmd.Locator, text string) error
	SendMarkdownWithProfile(ctx context.Context, locator deliverycmd.Locator, profile deliverycmd.Profile, text string) error
	SendDraftPlain(ctx context.Context, locator deliverycmd.Locator, draftID int, text string) error
	SendTyping(ctx context.Context, locator deliverycmd.Locator) error
	SendProgress(ctx context.Context, locator deliverycmd.Locator, progress deliverycmd.Progress) error
}

type Service struct {
	dispatcher Dispatcher
	outbox     DeliveryStore
	events     JobEvents
	logger     zerolog.Logger
}

func New(dispatcher Dispatcher, outbox DeliveryStore, events JobEvents, logger zerolog.Logger) *Service {
	return &Service{dispatcher: dispatcher, outbox: outbox, events: events, logger: logger}
}

func (s *Service) Handle(ctx context.Context, env actorlayer.Envelope, payload deliverycmd.Payload) error {
	if s.dispatcher == nil {
		return actorlayer.TransientError(fmt.Errorf("delivery dispatcher is required"))
	}
	if err := deliverycmd.Validate(payload); err != nil {
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
	durable := RequiresOutbox(payload)
	deliveryKey := strings.TrimSpace(env.DedupeKey)
	if deliveryKey == "" {
		deliveryKey = strings.TrimSpace(env.ID)
	}
	if deliveryKey == "" {
		deliveryKey = "delivery:" + shortJobHash(env.Payload.String())
	}
	sum := sha256.Sum256(env.Payload.Data)
	payloadHash := hex.EncodeToString(sum[:])
	if durable && s.outbox != nil {
		record, created, err := s.outbox.ReserveDelivery(ctx, baldastate.DeliveryRecord{
			ID:          uuid.NewString(),
			DeliveryKey: deliveryKey,
			JobID:       payload.JobID,
			SessionID:   payload.Locator.SessionID,
			Channel:     firstNonEmpty(payload.Locator.ChannelType, "telegram"),
			AddressKey:  firstNonEmpty(payload.Locator.AddressKey, payload.Locator.SessionID),
			Kind:        env.Kind,
			Payload:     strings.TrimSpace(env.Payload.String()),
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
		if !created && !ReadyForAttempt(record) {
			if record.Status == baldastate.DeliveryStatusSending {
				return actorlayer.TransientError(fmt.Errorf("delivery %q has ambiguous sending status; automatic resend is disabled; last updated at %s", deliveryKey, record.UpdatedAt.Format(time.RFC3339)))
			}
			return actorlayer.TransientError(fmt.Errorf("delivery %q is already %s; last updated at %s", deliveryKey, record.Status, record.UpdatedAt.Format(time.RFC3339)))
		}
		if err := s.outbox.MarkDeliverySending(ctx, deliveryKey); err != nil {
			return actorlayer.TransientError(err)
		}
	}
	if payload.Progress != nil && payload.Progress.Kind == deliverycmd.ProgressThinking {
		s.logger.Debug().
			Str("session_id", payload.Locator.SessionID).
			Bool("visible", payload.Progress.Visible).
			Bool("policy_thinking", payload.Progress.Policy.Thinking).
			Int("text_char_count", len(strings.TrimSpace(payload.Progress.Text))).
			Int("sequence", payload.Progress.Sequence).
			Msg("dispatching thinking progress delivery")
	}
	providerMessageID, err := s.dispatcher.Dispatch(ctx, payload)
	if err != nil {
		if durable && s.outbox != nil {
			_ = s.outbox.MarkDeliveryFailed(ctx, deliveryKey, err.Error())
			if strings.TrimSpace(payload.JobID) != "" {
				if s.events != nil {
					if appendErr := s.events.AppendEvent(ctx, payload.JobID, baldajobs.JobEventDeliveryFailed, "delivery.actor", env.ID, map[string]any{
						"text":   strings.TrimSpace(payload.Text),
						"action": strings.TrimSpace(payload.Action),
						"mode":   payload.Mode,
						"reason": err.Error(),
					}); appendErr != nil {
						s.logger.Warn().Err(appendErr).Str("job_id", payload.JobID).Msg("failed to record job delivery failure event")
					}
				}
			}
		}
		return actorlayer.ExternalDeliveryError(err)
	}
	if durable && s.outbox != nil {
		if err := s.outbox.MarkDeliverySent(ctx, deliveryKey, providerMessageID); err != nil {
			return actorlayer.TransientError(err)
		}
	}
	if durable && s.events != nil && strings.TrimSpace(payload.JobID) != "" {
		if err := s.events.AppendEvent(ctx, payload.JobID, baldajobs.JobEventDeliverySent, "delivery.actor", env.ID, map[string]any{
			"text": strings.TrimSpace(payload.Text),
			"mode": payload.Mode,
		}); err != nil {
			s.logger.Warn().Err(err).Str("job_id", payload.JobID).Msg("failed to record job delivery event")
		}
	}
	return nil
}

func RequiresOutbox(payload deliverycmd.Payload) bool {
	switch payload.Mode {
	case deliverycmd.ModeAgentReply, deliverycmd.ModePlain, deliverycmd.ModeMarkdown:
	default:
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

func ReadyForAttempt(record baldastate.DeliveryRecord) bool {
	switch record.Status {
	case baldastate.DeliveryStatusSent:
		return false
	case baldastate.DeliveryStatusSending:
		return false
	case baldastate.DeliveryStatusFailed:
		return true
	case baldastate.DeliveryStatusPending:
		if record.UpdatedAt.IsZero() {
			return true
		}
		return time.Since(record.UpdatedAt) >= 30*time.Second
	default:
		return true
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func shortJobHash(input string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(input)))
	return hex.EncodeToString(sum[:])[:12]
}
