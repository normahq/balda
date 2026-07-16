package questions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/baldaworks/go-actorlayer"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/actorcmd"
	"github.com/normahq/balda/internal/apps/balda/questioncmd"
	"github.com/rs/zerolog"
	"go.uber.org/fx"
)

type DeliveryBindingProjector struct {
	consumer actortransport.EventConsumer
	service  *Service
	logger   zerolog.Logger

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type deliveryBindingProjectorParams struct {
	fx.In

	Consumer actortransport.EventConsumer `optional:"true"`
	Service  *Service
	Logger   zerolog.Logger
}

type deliverySentEventPayload struct {
	Provider          string            `json:"provider,omitempty"`
	ConversationKey   string            `json:"conversation_key,omitempty"`
	ProviderMessageID string            `json:"provider_message_id,omitempty"`
	ReplyHandle       string            `json:"reply_handle,omitempty"`
	ControlHandle     string            `json:"control_handle,omitempty"`
	Refs              map[string]string `json:"refs,omitempty"`
}

func NewDeliveryBindingProjector(params deliveryBindingProjectorParams) *DeliveryBindingProjector {
	return &DeliveryBindingProjector{
		consumer: params.Consumer,
		service:  params.Service,
		logger:   params.Logger.With().Str("component", "balda.questions.delivery_binding").Logger(),
	}
}

func (p *DeliveryBindingProjector) Start(ctx context.Context) error {
	if p == nil || p.consumer == nil || p.service == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cancel != nil {
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		if err := p.consumer.RunEventConsumer(runCtx, p.Project); err != nil && !errors.Is(err, context.Canceled) {
			p.logger.Error().Err(err).Msg("delivery binding projector stopped")
		}
	}()
	return nil
}

func (p *DeliveryBindingProjector) Stop(ctx context.Context) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	cancel := p.cancel
	p.cancel = nil
	p.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.wg.Wait()
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *DeliveryBindingProjector) Project(ctx context.Context, subject string, env actorlayer.Envelope) error {
	if p == nil || p.service == nil || strings.TrimSpace(subject) != baldaexecution.SubjectEventDeliverySent {
		return nil
	}
	var payload deliverySentEventPayload
	if err := json.Unmarshal(env.Payload.Data, &payload); err != nil {
		return nil
	}
	questionID := strings.TrimSpace(payload.Refs["question_id"])
	if questionID == "" || strings.TrimSpace(payload.ProviderMessageID) == "" {
		return nil
	}
	if err := p.service.BindDelivery(ctx, questionID, questioncmd.DeliveryRef{
		Provider:          strings.TrimSpace(payload.Provider),
		ConversationKey:   strings.TrimSpace(payload.ConversationKey),
		ProviderMessageID: strings.TrimSpace(payload.ProviderMessageID),
		ReplyHandle:       strings.TrimSpace(payload.ReplyHandle),
		ControlHandle:     firstNonEmptyBinding(strings.TrimSpace(payload.ControlHandle), strings.TrimSpace(payload.Refs["question_control_handle"])),
	}); err != nil {
		return fmt.Errorf("bind question delivery ref for %q: %w", questionID, err)
	}
	return nil
}

func firstNonEmptyBinding(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
