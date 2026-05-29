package swarm

import (
	"context"
	"fmt"
	"time"

	actorengine "github.com/normahq/norma/actorlayer/engine"
)

type runtimeResolver struct {
	registry ActorRegistry
}

func (r runtimeResolver) LaneKey(delivery actorengine.Delivery) string {
	if delivery == nil {
		return unknownLaneKey
	}
	env, ok := delivery.Envelope().(Envelope)
	if !ok {
		return unknownLaneKey
	}
	return actorLaneKey(env)
}

type runtimeDelivery struct {
	cmd          CommandMessage
	onDeadLetter func(reason string)
}

func (d *runtimeDelivery) Envelope() any { return d.cmd.Envelope() }
func (d *runtimeDelivery) Attempt() int  { return d.cmd.DeliveryAttempt() }
func (d *runtimeDelivery) MaxAttempts() int {
	return d.cmd.MaxDeliveries()
}
func (d *runtimeDelivery) InProgress(ctx context.Context) error {
	return d.cmd.InProgress(ctx)
}
func (d *runtimeDelivery) Ack(ctx context.Context) error {
	return d.cmd.Ack(ctx)
}
func (d *runtimeDelivery) Retry(ctx context.Context, delay time.Duration, reason string) error {
	return d.cmd.Retry(ctx, delay, reason)
}
func (d *runtimeDelivery) DeadLetter(ctx context.Context, reason string) error {
	if d.onDeadLetter != nil {
		d.onDeadLetter(reason)
	}
	return d.cmd.DeadLetter(ctx, reason)
}

func (r *Runtime) handleDelivery(ctx context.Context, delivery actorengine.Delivery) error {
	env, ok := delivery.Envelope().(Envelope)
	if !ok {
		return DecodeError(fmt.Errorf("unexpected delivery envelope type %T", delivery.Envelope()))
	}
	to, err := env.To.String()
	if err != nil {
		return DecodeError(err)
	}
	actor, found := r.registry.Resolve(to)
	if !found {
		return PermanentError(fmt.Errorf("actor not found: %s", to))
	}
	return actor.Handle(ctx, env)
}
