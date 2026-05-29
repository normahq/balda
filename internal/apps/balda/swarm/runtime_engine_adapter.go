package swarm

import (
	"context"
	"fmt"
	"time"
)

type runtimeDelivery struct {
	cmd          CommandMessage
	onDeadLetter func(reason string)
}

func runtimeAddressOf(envelope any, registry *Registry) (string, error) {
	if registry == nil {
		return "", PermanentError(fmt.Errorf("actor registry is required"))
	}
	env, ok := envelope.(Envelope)
	if !ok {
		return "", DecodeError(fmt.Errorf("unexpected delivery envelope type %T", envelope))
	}
	to, err := env.To.String()
	if err != nil {
		return "", DecodeError(err)
	}
	if to == "" {
		return "", DecodeError(fmt.Errorf("empty actor address"))
	}
	dispatchRegistry := registry.DispatchRegistry()
	if dispatchRegistry == nil {
		return "", DecodeError(fmt.Errorf("actor registry is not configured"))
	}
	if _, found := dispatchRegistry.Resolve(to); !found {
		return "", PermanentError(fmt.Errorf("actor not found: %s", to))
	}
	return to, nil
}

func actorLaneKeyFromEnvelope(envelope any) string {
	env, ok := envelope.(Envelope)
	if !ok {
		return "unknown"
	}
	return actorLaneKey(env)
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
