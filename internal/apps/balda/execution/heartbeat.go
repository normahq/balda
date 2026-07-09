package execution

import (
	"context"
	"strings"
	"time"

	"github.com/normahq/balda/pkg/actorlayer"
	actorengine "github.com/normahq/balda/pkg/actorlayer/engine"
)

const heartbeatInterval = 30 * time.Second

func (r *ActorHost) startHeartbeat(ctx context.Context, env actorlayer.Envelope, delivery actorengine.Delivery) (context.Context, func()) {
	if r == nil || r.engine == nil || delivery == nil {
		return ctx, func() {}
	}
	heartbeatCtx := withEnvelopeContext(ctx, env)
	child, cancel := context.WithCancel(heartbeatCtx)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(r.heartbeatTickInterval())
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := delivery.InProgress(child); err != nil {
					r.logger.Warn().Err(err).Str("envelope_id", env.ID).Msg("failed to send actor delivery in-progress")
				}
				r.Publish(child, actorengine.Event{Type: actorengine.EventInProgress, Attempt: delivery.Attempt(), MaxAttempts: delivery.MaxAttempts()})
			case <-child.Done():
				return
			}
		}
	}()
	return child, cancel
}

func (r *ActorHost) Publish(ctx context.Context, event actorengine.Event) {
	if r == nil || event.Type != actorengine.EventInProgress {
		return
	}
	if ctx == nil {
		return
	}
	env, ok := ctx.Value(envelopeContextKey{}).(actorlayer.Envelope)
	if !ok {
		return
	}
	if r.events == nil {
		return
	}
	inProgressEnv := env
	inProgressEnv.ID = strings.TrimSpace(env.ID) + ":event:in_progress"
	inProgressEnv.Namespace = NamespaceTelemetry
	inProgressEnv.Kind = "command_event"
	inProgressEnv.DedupeKey = inProgressEnv.ID
	if strings.TrimSpace(inProgressEnv.PayloadJSON) == "" {
		inProgressEnv.PayloadJSON = `{"ok":true}`
	}
	if err := r.events.PublishEvent(ctx, SubjectEventCommandInProgress, inProgressEnv); err != nil {
		r.logger.Warn().Err(err).Str("envelope_id", env.ID).Msg("failed to publish command in-progress event")
	}
}

func (r *ActorHost) heartbeatTickInterval() time.Duration {
	if r == nil || r.heartbeatTick <= 0 {
		return heartbeatInterval
	}
	return r.heartbeatTick
}
