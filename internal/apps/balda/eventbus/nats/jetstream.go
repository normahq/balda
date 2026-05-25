package natsbus

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	gnats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/normahq/balda/internal/apps/balda/swarm"
)

const (
	streamCommands = "BALDA_COMMANDS"
	streamEvents   = "BALDA_EVENTS"
	streamDLQ      = "BALDA_DLQ"
	streamControl  = "BALDA_CONTROL"
)

type JetStreamMailbox struct {
	js      jetstream.JetStream
	cfg     resolvedConfig
	pending sync.Map
}

func (m *JetStreamMailbox) PublishCommand(ctx context.Context, env swarm.Envelope) error {
	if m == nil || m.js == nil {
		return nil
	}
	subject := swarm.SubjectForEnvelope(env)
	msg, err := messageFromEnvelope(subject, env)
	if err != nil {
		return err
	}
	_, err = m.js.PublishMsg(ctx, msg, jetstream.WithMsgID(swarm.DedupeKeyOrID(env)))
	if err != nil {
		return fmt.Errorf("publish jetstream command %q: %w", subject, err)
	}
	return nil
}

func (m *JetStreamMailbox) ConsumeCommands(ctx context.Context, actorGroup string, handler swarm.EventHandler) error {
	if m == nil || m.js == nil || handler == nil {
		return nil
	}
	consumer, err := m.js.CreateOrUpdateConsumer(ctx, streamCommands, jetstream.ConsumerConfig{
		Durable:       durableName(actorGroup),
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       consumerAckWait(m.cfg),
		MaxAckPending: m.cfg.NATS.Consumers.Commands.MaxAckPending,
		FilterSubject: swarm.SubjectCommandAll,
	})
	if err != nil {
		return fmt.Errorf("create jetstream command consumer: %w", err)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		batch, err := consumer.Fetch(m.cfg.NATS.Consumers.Commands.FetchBatch, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			continue
		}
		for msg := range batch.Messages() {
			env, err := swarm.DecodeEnvelope(string(msg.Data()))
			if err != nil {
				_ = msg.TermWithReason(err.Error())
				continue
			}
			ref := MessageRefFromJetStreamMsg(msg)
			if ref.MessageID == "" {
				ref.MessageID = env.ID
			}
			m.pending.Store(ref.MessageID, msg)
			if err := handler(ctx, msg.Subject(), env); err != nil {
				_ = m.settleError(ctx, ref, err)
				m.pending.Delete(ref.MessageID)
				continue
			}
			_ = m.Ack(ctx, ref)
			m.pending.Delete(ref.MessageID)
		}
	}
}

func (m *JetStreamMailbox) Ack(ctx context.Context, msg swarm.MessageRef) error {
	jsMsg, ok := m.pending.Load(msg.MessageID)
	if !ok {
		return nil
	}
	return jsMsg.(jetstream.Msg).DoubleAck(ctx)
}

func (m *JetStreamMailbox) Retry(_ context.Context, msg swarm.MessageRef, delay time.Duration, _ string) error {
	jsMsg, ok := m.pending.Load(msg.MessageID)
	if !ok {
		return nil
	}
	return jsMsg.(jetstream.Msg).NakWithDelay(delay)
}

func (m *JetStreamMailbox) DeadLetter(ctx context.Context, msg swarm.MessageRef, reason string) error {
	jsMsg, ok := m.pending.Load(msg.MessageID)
	if !ok {
		return nil
	}
	env, err := swarm.DecodeEnvelope(string(jsMsg.(jetstream.Msg).Data()))
	if err == nil {
		dlq, msgErr := newDLQMessage(env, reason)
		if msgErr == nil {
			_, _ = m.js.PublishMsg(ctx, dlq, jetstream.WithMsgID(swarm.DedupeKeyOrID(env)+":dlq"))
		}
	}
	return jsMsg.(jetstream.Msg).TermWithReason(reason)
}

func (m *JetStreamMailbox) settleError(ctx context.Context, ref swarm.MessageRef, err error) error {
	switch swarm.ClassifyError(err) {
	case swarm.ErrorKindDuplicate:
		return m.Ack(ctx, ref)
	case swarm.ErrorKindAuth, swarm.ErrorKindPolicy, swarm.ErrorKindPermanent:
		return m.DeadLetter(ctx, ref, err.Error())
	default:
		return m.Retry(ctx, ref, time.Second, err.Error())
	}
}

func ensureStreams(ctx context.Context, js jetstream.JetStream, cfg resolvedConfig) error {
	if js == nil {
		return fmt.Errorf("jetstream is required")
	}
	streams := []jetstream.StreamConfig{
		streamConfig(streamCommands, []string{swarm.SubjectCommandAll}, jetstream.WorkQueuePolicy, cfg.StreamSpec.Commands),
		streamConfig(streamEvents, []string{swarm.SubjectEventAll}, jetstream.LimitsPolicy, cfg.StreamSpec.Events),
		streamConfig(streamDLQ, []string{swarm.SubjectDLQ}, jetstream.LimitsPolicy, cfg.StreamSpec.DLQ),
		streamConfig(streamControl, []string{swarm.SubjectControlAll}, jetstream.WorkQueuePolicy, cfg.StreamSpec.Control),
	}
	for _, stream := range streams {
		if _, err := js.CreateOrUpdateStream(ctx, stream); err != nil {
			return fmt.Errorf("create or update stream %s: %w", stream.Name, err)
		}
	}
	return nil
}

func streamConfig(name string, subjects []string, retention jetstream.RetentionPolicy, spec streamSpec) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:       name,
		Subjects:   subjects,
		Retention:  retention,
		Storage:    jetstream.FileStorage,
		MaxAge:     spec.MaxAge,
		MaxBytes:   spec.MaxBytes,
		MaxMsgSize: spec.MaxMsgSize,
		Discard:    discardPolicy(spec.Discard),
		Replicas:   1,
	}
}

func discardPolicy(raw string) jetstream.DiscardPolicy {
	if raw == "new" {
		return jetstream.DiscardNew
	}
	return jetstream.DiscardOld
}

func MessageRefFromJetStreamMsg(msg jetstream.Msg) swarm.MessageRef {
	if msg == nil {
		return swarm.MessageRef{Source: "jetstream"}
	}
	id := msg.Headers().Get(swarm.HeaderEnvelopeID)
	return swarm.MessageRef{Source: "jetstream", Subject: msg.Subject(), MessageID: id}
}

func newDLQMessage(env swarm.Envelope, reason string) (*gnats.Msg, error) {
	msg, err := messageFromEnvelope(swarm.SubjectDLQ, env)
	if err != nil {
		return nil, err
	}
	msg.Header.Set("Balda-DLQ-Reason", reason)
	return msg, nil
}

func durableName(actorGroup string) string {
	trimmed := strings.TrimSpace(actorGroup)
	if trimmed == "" {
		trimmed = "balda-workers"
	}
	replacer := strings.NewReplacer(".", "-", "*", "all", ">", "all", " ", "-", "/", "-")
	return replacer.Replace(trimmed)
}

func consumerAckWait(cfg resolvedConfig) time.Duration {
	ackWait, err := parseDuration(cfg.NATS.Consumers.Commands.AckWait)
	if err != nil {
		return 5 * time.Minute
	}
	return ackWait
}
