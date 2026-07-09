package execution

import (
	"context"
	"time"

	"github.com/normahq/balda/pkg/actorlayer"
	actorengine "github.com/normahq/balda/pkg/actorlayer/engine"
)

type runtimeDelivery struct {
	delivery     actorengine.Delivery
	onDeadLetter func(reason string)
}

type envelopeContextKey struct{}

func withEnvelopeContext(ctx context.Context, env actorlayer.Envelope) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, envelopeContextKey{}, env)
}

func (r *ActorHost) prepareDelivery(ctx context.Context, delivery actorengine.Delivery) (context.Context, func(), actorengine.Delivery) {
	if r == nil || delivery == nil {
		return ctx, func() {}, delivery
	}
	env := delivery.Envelope()
	wrapped := &runtimeDelivery{
		delivery: delivery,
		onDeadLetter: func(reason string) {
			r.deadletterJob(ctx, env, reason)
		},
	}
	heartbeatCtx, stop := r.startHeartbeat(ctx, env, wrapped)
	return heartbeatCtx, stop, wrapped
}

func (d *runtimeDelivery) Envelope() actorengine.Envelope { return d.delivery.Envelope() }
func (d *runtimeDelivery) Attempt() int                   { return d.delivery.Attempt() }
func (d *runtimeDelivery) MaxAttempts() int               { return d.delivery.MaxAttempts() }
func (d *runtimeDelivery) InProgress(ctx context.Context) error {
	return d.delivery.InProgress(ctx)
}
func (d *runtimeDelivery) Ack(ctx context.Context) error { return d.delivery.Ack(ctx) }
func (d *runtimeDelivery) Retry(ctx context.Context, delay time.Duration, reason string) error {
	return d.delivery.Retry(ctx, delay, reason)
}
func (d *runtimeDelivery) DeadLetter(ctx context.Context, reason string) error {
	if d.onDeadLetter != nil {
		d.onDeadLetter(reason)
	}
	return d.delivery.DeadLetter(ctx, reason)
}
