package natsbus

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	gnats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/normahq/balda/internal/apps/balda/swarm"
)

const commandSettlementTimeout = 5 * time.Second

type commandMessage struct {
	subject       string
	env           swarm.Envelope
	msg           jetstream.Msg
	numDelivered  int
	maxDeliveries int
}

func (m commandMessage) Envelope() swarm.Envelope { return m.env }
func (m commandMessage) Subject() string          { return m.subject }
func (m commandMessage) InProgress(context.Context) error {
	return m.msg.InProgress()
}
func (m commandMessage) DeliveryAttempt() int { return m.numDelivered }
func (m commandMessage) MaxDeliveries() int   { return m.maxDeliveries }

func (b *Bus) RunCommandConsumer(ctx context.Context, handler swarm.CommandHandler) error {
	if b == nil || b.consumer == nil {
		return fmt.Errorf("jetstream command consumer is required")
	}
	workers := make(chan struct{}, b.commandWorkerLimit())
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		batch, err := b.consumer.Fetch(b.cfg.Swarm.Commands.FetchBatch, jetstream.FetchMaxWait(b.cfg.FetchWait))
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		for msg := range batch.Messages() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case workers <- struct{}{}:
			}
			wg.Add(1)
			go func(msg jetstream.Msg) {
				defer wg.Done()
				defer func() { <-workers }()
				if err := b.handleMessage(ctx, msg, handler); err != nil {
					b.logger.Warn().Err(err).Str("subject", msg.Subject()).Msg("failed to settle jetstream command")
				}
			}(msg)
		}
	}
}

func (b *Bus) commandWorkerLimit() int {
	if b == nil {
		return 1
	}
	switch {
	case b.cfg.Swarm.Commands.MaxAckPending > 0:
		return b.cfg.Swarm.Commands.MaxAckPending
	case b.cfg.Swarm.Commands.FetchBatch > 0:
		return b.cfg.Swarm.Commands.FetchBatch
	default:
		return 1
	}
}

func (b *Bus) handleMessage(ctx context.Context, msg jetstream.Msg, handler swarm.CommandHandler) error {
	env, err := swarm.DecodeEnvelope(string(msg.Data()))
	if err != nil {
		settleCtx, settleCancel := settlementContext(ctx)
		defer settleCancel()
		_ = b.publishRawDLQ(settleCtx, msg, "decode failed: "+err.Error())
		_ = msg.TermWithReason("decode failed: " + err.Error())
		return err
	}
	numDelivered := messageDeliveryAttempt(msg)
	env.Attempt = numDelivered - 1
	cmd := commandMessage{subject: msg.Subject(), env: env, msg: msg, numDelivered: numDelivered, maxDeliveries: b.cfg.Swarm.Commands.MaxDeliver}
	b.publishCommandEventBestEffort(ctx, swarm.SubjectEventCommandRunning, env, "running", "")
	err = handler(ctx, cmd)
	settleCtx, settleCancel := settlementContext(ctx)
	defer settleCancel()
	if err == nil {
		if err := msg.DoubleAck(settleCtx); err != nil {
			return err
		}
		if err := b.PublishEvent(settleCtx, swarm.SubjectEventCommandAcked, commandEventEnvelope(env, nil, "acked", "")); err != nil {
			b.logger.Warn().
				Err(err).
				Str("envelope_id", env.ID).
				Msg("failed to publish command acked event")
		}
		return nil
	}
	if isRetryable(err) {
		if retryExhausted(numDelivered, b.cfg.Swarm.Commands.MaxDeliver) {
			reason := "retry exhausted: " + err.Error()
			if err := b.publishDLQ(settleCtx, env, reason, false); err != nil {
				return err
			}
			if err := msg.TermWithReason(reason); err != nil {
				return err
			}
			b.publishCommandEventBestEffort(settleCtx, swarm.SubjectEventCommandDeadLettered, env, "deadlettered", reason)
			return nil
		}
		delay := computeBackoff(env.Attempt)
		if settleErr := msg.NakWithDelay(delay); settleErr != nil {
			return settleErr
		}
		if eventErr := b.PublishEvent(settleCtx, swarm.SubjectEventCommandRetrying, commandEventEnvelope(env, nil, "retrying", err.Error())); eventErr != nil {
			b.logger.Warn().
				Err(eventErr).
				Str("envelope_id", env.ID).
				Msg("failed to publish command retrying event")
		}
		return nil
	}
	if dlqErr := b.publishDLQ(settleCtx, env, err.Error(), false); dlqErr != nil {
		return dlqErr
	}
	if err := msg.TermWithReason(err.Error()); err != nil {
		return err
	}
	b.publishCommandEventBestEffort(settleCtx, swarm.SubjectEventCommandDeadLettered, env, "deadlettered", err.Error())
	return nil
}

func settlementContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), commandSettlementTimeout)
}

func (b *Bus) publishCommandEventBestEffort(ctx context.Context, subject string, env swarm.Envelope, status string, reason string) {
	if err := b.PublishEvent(ctx, subject, commandEventEnvelope(env, nil, status, reason)); err != nil {
		b.logger.Warn().
			Err(err).
			Str("envelope_id", env.ID).
			Str("event_status", status).
			Msg("failed to publish command lifecycle event")
	}
}

func (b *Bus) RunEventConsumer(ctx context.Context, handler swarm.EventHandler) error {
	if b == nil || b.eventConsumer == nil {
		return fmt.Errorf("jetstream event projector consumer is required")
	}
	if handler == nil {
		return fmt.Errorf("event handler is required")
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		batch, err := b.eventConsumer.Fetch(b.cfg.Swarm.Commands.FetchBatch, jetstream.FetchMaxWait(b.cfg.FetchWait))
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		for msg := range batch.Messages() {
			if err := b.handleEventMessage(ctx, msg, handler); err != nil {
				b.logger.Warn().Err(err).Str("subject", msg.Subject()).Msg("failed to settle jetstream event")
			}
		}
	}
}

func (b *Bus) handleEventMessage(ctx context.Context, msg jetstream.Msg, handler swarm.EventHandler) error {
	env, err := swarm.DecodeEnvelope(string(msg.Data()))
	if err != nil {
		_ = b.publishRawDLQ(ctx, msg, "decode failed: "+err.Error())
		_ = msg.TermWithReason("decode failed: " + err.Error())
		return err
	}
	if err := handler(ctx, msg.Subject(), env); err != nil {
		numDelivered := messageDeliveryAttempt(msg)
		if isRetryable(err) && !retryExhausted(numDelivered, b.cfg.Swarm.Commands.MaxDeliver) {
			return msg.NakWithDelay(computeBackoff(numDelivered - 1))
		}
		reason := "event projection failed: " + err.Error()
		_ = b.publishDLQ(ctx, env, reason, false)
		return msg.TermWithReason(reason)
	}
	return msg.DoubleAck(ctx)
}

func messageDeliveryAttempt(msg jetstream.Msg) int {
	if md, err := msg.Metadata(); err == nil && md.NumDelivered > 0 {
		return int(md.NumDelivered)
	}
	return 1
}

func ensureStreams(ctx context.Context, js jetstream.JetStream, cfg resolvedConfig) error {
	if js == nil {
		return fmt.Errorf("jetstream is required")
	}
	streams := []jetstream.StreamConfig{
		streamConfig(cfg.Swarm.Commands.Stream, []string{swarm.SubjectCommandAll}, jetstream.WorkQueuePolicy, cfg.Commands),
		streamConfig(cfg.Swarm.Events.Stream, []string{swarm.SubjectEventAll}, jetstream.LimitsPolicy, cfg.Events),
		streamConfig(cfg.Swarm.DLQ.Stream, []string{swarm.SubjectDLQAll}, jetstream.LimitsPolicy, cfg.DLQ),
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

func newDLQMessage(env swarm.Envelope, reason string) (*gnats.Msg, error) {
	msg, err := messageFromEnvelope(swarm.SubjectDLQCommand, env)
	if err != nil {
		return nil, err
	}
	msg.Header.Set("Balda-DLQ-Reason", reason)
	return msg, nil
}

func (b *Bus) publishRawDLQ(ctx context.Context, source jetstream.Msg, reason string) error {
	headers := make(map[string][]string, len(source.Headers()))
	for key, values := range source.Headers() {
		headers[key] = append([]string(nil), values...)
	}
	payload := map[string]any{
		"subject": source.Subject(),
		"reason":  reason,
		"headers": headers,
		"payload": string(source.Data()),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	env := swarm.Envelope{
		ID:          "poison-" + uuid.NewString(),
		Namespace:   swarm.NamespaceTelemetry,
		Kind:        "poison_message",
		From:        swarm.SystemAddress("jetstream"),
		To:          swarm.SystemAddress("dlq"),
		PayloadJSON: string(data),
	}
	msg, err := messageFromEnvelope(swarm.SubjectDLQCommand, env)
	if err != nil {
		return err
	}
	msg.Header.Set("Balda-DLQ-Reason", reason)
	_, err = b.js.PublishMsg(ctx, msg, jetstream.WithExpectStream(b.cfg.Swarm.DLQ.Stream), jetstream.WithMsgID(env.ID))
	if err != nil {
		return fmt.Errorf("publish raw jetstream dlq: %w", err)
	}
	return nil
}

func isRetryable(err error) bool {
	switch swarm.ClassifyError(err) {
	case swarm.ErrorKindDuplicate, swarm.ErrorKindAuth, swarm.ErrorKindPolicy, swarm.ErrorKindPermanent:
		return false
	default:
		return true
	}
}

func retryExhausted(numDelivered int, maxDeliver int) bool {
	return maxDeliver > 0 && numDelivered >= maxDeliver
}

func computeBackoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	delay := time.Second
	for range attempt {
		delay *= 2
		if delay >= time.Minute {
			return time.Minute
		}
	}
	return delay
}

func commandEventEnvelope(env swarm.Envelope, result *swarm.CommandPublishResult, status string, reason string) swarm.Envelope {
	payload := map[string]any{
		"envelope_id": env.ID,
		"task_id":     env.TaskID,
		"session_id":  env.SessionID,
		"namespace":   env.Namespace,
		"status":      status,
	}
	if result != nil {
		payload["stream"] = result.Stream
		payload["sequence"] = result.Sequence
		payload["subject"] = result.Subject
		payload["msg_id"] = result.MsgID
		payload["duplicate"] = result.Duplicate
	}
	if strings.TrimSpace(reason) != "" {
		payload["reason"] = reason
	}
	data, _ := json.Marshal(payload)
	out := env
	out.ID = strings.TrimSpace(env.ID) + ":event:" + strings.TrimSpace(status)
	out.Namespace = swarm.NamespaceTelemetry
	out.Kind = "command_event"
	out.PayloadJSON = string(data)
	out.DedupeKey = out.ID
	if out.Meta == nil {
		out.Meta = map[string]string{}
	}
	out.Meta["event_type"] = "command." + strings.TrimSpace(status)
	if out.From.Target == "" {
		out.From = swarm.SystemAddress("jetstream")
	}
	if out.To.Target == "" {
		out.To = swarm.SystemAddress("jetstream")
	}
	return out
}
