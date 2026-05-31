package natsbus

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	actorengine "github.com/normahq/norma/pkg/actorlayer/engine"
	"github.com/rs/zerolog"
)

const commandSettlementTimeout = 5 * time.Second
const unknownDecodeTarget = "unknown"

const (
	dlqMetaErrorClass   = "dlq_error_class"
	dlqMetaSourceStream = "dlq_source_stream"
	dlqMetaSourceCns    = "dlq_source_consumer"
	dlqMetaSourceSubj   = "dlq_source_subject"
	dlqMetaDelivered    = "dlq_num_delivered"
)

type commandMessage struct {
	subject       string
	env           swarm.Envelope
	msg           jetstream.Msg
	numDelivered  int
	maxDeliveries int
	bus           *Bus

	mu      sync.Mutex
	settled bool
}

func (m *commandMessage) Envelope() any { return m.env }
func (m *commandMessage) InProgress(context.Context) error {
	return m.msg.InProgress()
}
func (m *commandMessage) Attempt() int     { return m.numDelivered }
func (m *commandMessage) MaxAttempts() int { return m.maxDeliveries }

func (m *commandMessage) Ack(ctx context.Context) error {
	return m.settle(func() error {
		settleCtx, settleCancel := settlementContext(ctx)
		defer settleCancel()
		if err := m.msg.DoubleAck(settleCtx); err != nil {
			return err
		}
		if err := m.bus.PublishEvent(settleCtx, swarm.SubjectEventCommandAcked, commandEventEnvelope(m.env, nil, "acked", "", nil)); err != nil {
			m.bus.logger.Warn().
				Err(err).
				Str("envelope_id", m.env.ID).
				Msg("failed to publish command acked event")
		}
		commandLogEnvelope(commandLogEvent(m.bus.logger.Info(), m.msg), m.env).Msg("command handled and acknowledged")
		return nil
	})
}

func (m *commandMessage) Retry(ctx context.Context, delay time.Duration, reason string) error {
	return m.settle(func() error {
		settleCtx, settleCancel := settlementContext(ctx)
		defer settleCancel()
		if err := m.msg.NakWithDelay(delay); err != nil {
			return err
		}
		eventExtras := map[string]any{
			"retry_delay_ms":  delay.Milliseconds(),
			"next_attempt_at": time.Now().UTC().Add(delay).Format(time.RFC3339Nano),
		}
		if err := m.bus.PublishEvent(settleCtx, swarm.SubjectEventCommandRetrying, commandEventEnvelope(m.env, nil, "retrying", reason, eventExtras)); err != nil {
			m.bus.logger.Warn().
				Err(err).
				Str("envelope_id", m.env.ID).
				Msg("failed to publish command retrying event")
		}
		commandLogEnvelope(commandLogEvent(m.bus.logger.Warn(), m.msg), m.env).Str("retry_reason", reason).Dur("retry_delay", delay).Msg("command failed with retryable error")
		return nil
	})
}

func (m *commandMessage) DeadLetter(ctx context.Context, reason string) error {
	return m.settle(func() error {
		settleCtx, settleCancel := settlementContext(ctx)
		defer settleCancel()
		if err := m.bus.publishDLQ(settleCtx, m.env, reason, false); err != nil {
			return err
		}
		if err := m.msg.TermWithReason(reason); err != nil {
			return err
		}
		m.bus.publishCommandEventBestEffort(settleCtx, swarm.SubjectEventCommandDeadLettered, m.env, "deadlettered", reason)
		commandLogEnvelope(commandLogEvent(m.bus.logger.Warn(), m.msg), m.env).Str("reason", reason).Msg("command deadlettered")
		return nil
	})
}

func (m *commandMessage) isSettled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.settled
}

func (m *commandMessage) settle(fn func() error) error {
	m.mu.Lock()
	if m.settled {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()
	if err := fn(); err != nil {
		return err
	}
	m.mu.Lock()
	m.settled = true
	m.mu.Unlock()
	return nil
}

func (b *Bus) Run(ctx context.Context, handler actorengine.Handler) error {
	if b == nil || b.consumer == nil {
		return fmt.Errorf("actor delivery consumer is required")
	}
	workerLimit := b.commandWorkerLimit()
	workers := make(chan struct{}, workerLimit)
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		available := workerLimit - len(workers)
		if available <= 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		fetchSize := b.cfg.Swarm.Commands.FetchBatch
		if fetchSize <= 0 {
			fetchSize = 1
		}
		if fetchSize > available {
			fetchSize = available
		}
		batch, err := b.consumer.Fetch(fetchSize, jetstream.FetchMaxWait(b.cfg.FetchWait))
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
					b.logger.Warn().Err(err).Str("subject", msg.Subject()).Msg("failed to settle command")
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
	case b.cfg.Swarm.Commands.FetchBatch > 0:
		// Keep local in-memory fan-out bounded to the pull batch size.
		// Transport max_ack_pending stays the transport limit.
		return b.cfg.Swarm.Commands.FetchBatch
	case b.cfg.Swarm.Commands.MaxAckPending > 0:
		return b.cfg.Swarm.Commands.MaxAckPending
	default:
		return 1
	}
}

func (b *Bus) handleMessage(ctx context.Context, msg jetstream.Msg, handler actorengine.Handler) error {
	env, err := swarm.DecodeEnvelope(string(msg.Data()))
	if err != nil {
		id := strings.TrimSpace(msg.Headers().Get(swarm.HeaderEnvelopeID))
		if id == "" {
			id = "poison-" + uuid.NewString()
		}
		namespace := strings.TrimSpace(msg.Headers().Get(swarm.HeaderNamespace))
		if namespace == "" {
			namespace = swarm.NamespaceTelemetry
		}
		toTarget, toKey := unknownDecodeTarget, unknownDecodeTarget
		if strings.HasPrefix(msg.Subject(), "balda.v1.cmd.") {
			toTarget = strings.TrimPrefix(msg.Subject(), "balda.v1.cmd.")
			toKey = strings.TrimSpace(msg.Headers().Get(swarm.HeaderActorKey))
			if toKey == "" {
				toKey = unknownDecodeTarget
			}
		}
		payload, _ := json.Marshal(map[string]any{
			"subject": msg.Subject(),
			"reason":  "decode failed: " + err.Error(),
			"payload": string(msg.Data()),
		})
		decodeFailureEnv := swarm.Envelope{
			ID:          id,
			Namespace:   namespace,
			Kind:        "decode_failed",
			From:        swarm.SystemAddress("transport"),
			To:          swarm.ActorAddress{Target: toTarget, Key: toKey},
			SessionID:   strings.TrimSpace(msg.Headers().Get(swarm.HeaderSessionID)),
			TaskID:      strings.TrimSpace(msg.Headers().Get(swarm.HeaderTaskID)),
			PayloadJSON: string(payload),
		}
		settleCtx, settleCancel := settlementContext(ctx)
		defer settleCancel()
		_ = b.publishRawDLQ(settleCtx, msg, "decode failed: "+err.Error())
		b.publishCommandEventBestEffort(settleCtx, swarm.SubjectEventCommandDecodeFailed, decodeFailureEnv, "decode_failed", err.Error())
		_ = msg.TermWithReason("decode failed: " + err.Error())
		commandLogEvent(b.logger.Warn(), msg).Err(err).Msg("failed to decode command envelope; moved to dlq")
		return err
	}
	numDelivered := messageDeliveryAttempt(msg)
	env.Attempt = numDelivered - 1
	cmd := &commandMessage{
		subject:       msg.Subject(),
		env:           env,
		msg:           msg,
		numDelivered:  numDelivered,
		maxDeliveries: b.cfg.Swarm.Commands.MaxDeliver,
		bus:           b,
	}
	b.publishCommandEventBestEffort(ctx, swarm.SubjectEventCommandRunning, env, "running", "")
	commandLogEnvelope(commandLogEvent(b.logger.Debug(), msg), env).Msg("command running")
	err = handler(ctx, cmd)
	if cmd.isSettled() {
		return nil
	}
	settleCtx, settleCancel := settlementContext(ctx)
	defer settleCancel()
	if err == nil {
		return cmd.Ack(settleCtx)
	}
	if swarm.IsRetryableError(err) {
		if swarm.RetryExhausted(numDelivered, b.cfg.Swarm.Commands.MaxDeliver) {
			reason := "retry exhausted: " + err.Error()
			cmd.env = decorateDLQEnvelope(cmd.env, reason, swarm.ClassifyError(err), b.cfg.Swarm.Commands.Stream, b.cfg.Swarm.Commands.Consumer, msg.Subject(), numDelivered)
			return cmd.DeadLetter(settleCtx, reason)
		}
		delay := swarm.RetryDelay(env.Attempt)
		return cmd.Retry(settleCtx, delay, err.Error())
	}
	cmd.env = decorateDLQEnvelope(cmd.env, err.Error(), swarm.ClassifyError(err), b.cfg.Swarm.Commands.Stream, b.cfg.Swarm.Commands.Consumer, msg.Subject(), numDelivered)
	return cmd.DeadLetter(settleCtx, err.Error())
}

func settlementContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), commandSettlementTimeout)
}

func (b *Bus) publishCommandEventBestEffort(ctx context.Context, subject string, env swarm.Envelope, status string, reason string) {
	if err := b.PublishEvent(ctx, subject, commandEventEnvelope(env, nil, status, reason, nil)); err != nil {
		b.logger.Warn().
			Err(err).
			Str("envelope_id", env.ID).
			Str("event_status", status).
			Msg("failed to publish command lifecycle event")
	}
}

func (b *Bus) RunEventConsumer(ctx context.Context, handler swarm.EventHandler) error {
	if b == nil || b.eventConsumer == nil {
		return fmt.Errorf("event projector consumer is required")
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
				b.logger.Warn().Err(err).Str("subject", msg.Subject()).Msg("failed to settle event")
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
		if swarm.IsRetryableError(err) && !swarm.RetryExhausted(numDelivered, b.cfg.Swarm.Commands.MaxDeliver) {
			return msg.NakWithDelay(swarm.RetryDelay(numDelivered - 1))
		}
		reason := "event projection failed: " + err.Error()
		dlqEnv := decorateDLQEnvelope(env, reason, swarm.ClassifyError(err), b.cfg.Swarm.Events.Stream, swarm.DefaultEventProjectorConsumer, msg.Subject(), numDelivered)
		_ = b.publishDLQ(ctx, dlqEnv, reason, false)
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
		return fmt.Errorf("runtime transport is required")
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
	discard := jetstream.DiscardOld
	if spec.Discard == "new" {
		discard = jetstream.DiscardNew
	}
	return jetstream.StreamConfig{
		Name:       name,
		Subjects:   subjects,
		Retention:  retention,
		Storage:    jetstream.FileStorage,
		MaxAge:     spec.MaxAge,
		MaxBytes:   spec.MaxBytes,
		MaxMsgSize: spec.MaxMsgSize,
		Discard:    discard,
		Replicas:   1,
	}
}

func (b *Bus) publishRawDLQ(ctx context.Context, source jetstream.Msg, reason string) error {
	headers := make(map[string][]string, len(source.Headers()))
	for key, values := range source.Headers() {
		headers[key] = append([]string(nil), values...)
	}
	payload := map[string]any{
		"subject":     source.Subject(),
		"reason":      reason,
		"headers":     headers,
		"payload":     string(source.Data()),
		"error_class": swarm.ErrorKindDecode,
	}
	if md, err := source.Metadata(); err == nil {
		payload["source_stream"] = strings.TrimSpace(md.Stream)
		payload["source_consumer"] = strings.TrimSpace(md.Consumer)
		payload["num_delivered"] = int(md.NumDelivered)
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	env := swarm.Envelope{
		ID:          "poison-" + uuid.NewString(),
		Namespace:   swarm.NamespaceTelemetry,
		Kind:        "poison_message",
		From:        swarm.SystemAddress("transport"),
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
		return fmt.Errorf("publish raw dlq: %w", err)
	}
	return nil
}

func commandEventEnvelope(env swarm.Envelope, result *swarm.DispatchReceipt, status string, reason string, extra map[string]any) swarm.Envelope {
	payload := map[string]any{
		"envelope_id":    env.ID,
		"task_id":        env.TaskID,
		"session_id":     env.SessionID,
		"namespace":      env.Namespace,
		"status":         status,
		"correlation_id": env.CorrelationID,
		"causation_id":   env.CausationID,
		"actor_key":      strings.TrimSpace(env.To.Key),
	}
	if strings.EqualFold(strings.TrimSpace(env.To.Target), swarm.ActorTypeDelivery) {
		payload["delivery_key"] = strings.TrimSpace(env.To.Key)
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
	for key, value := range extra {
		if strings.TrimSpace(key) == "" {
			continue
		}
		payload[key] = value
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
		out.From = swarm.SystemAddress("transport")
	}
	if out.To.Target == "" {
		out.To = swarm.SystemAddress("transport")
	}
	return out
}

func decorateDLQEnvelope(env swarm.Envelope, reason string, class swarm.ErrorKind, stream string, consumer string, subject string, numDelivered int) swarm.Envelope {
	out := env
	if out.Meta == nil {
		out.Meta = map[string]string{}
	}
	if class != "" {
		out.Meta[dlqMetaErrorClass] = string(class)
	}
	if trimmed := strings.TrimSpace(stream); trimmed != "" {
		out.Meta[dlqMetaSourceStream] = trimmed
	}
	if trimmed := strings.TrimSpace(consumer); trimmed != "" {
		out.Meta[dlqMetaSourceCns] = trimmed
	}
	if trimmed := strings.TrimSpace(subject); trimmed != "" {
		out.Meta[dlqMetaSourceSubj] = trimmed
	}
	if numDelivered > 0 {
		out.Meta[dlqMetaDelivered] = strconv.Itoa(numDelivered)
	}
	if trimmed := strings.TrimSpace(reason); trimmed != "" && strings.TrimSpace(out.Meta["reason"]) == "" {
		out.Meta["reason"] = trimmed
	}
	return out
}

func commandLogEvent(evt *zerolog.Event, msg jetstream.Msg) *zerolog.Event {
	evt = evt.
		Str("subject", msg.Subject()).
		Int("delivery_attempt", messageDeliveryAttempt(msg))
	if md, err := msg.Metadata(); err == nil {
		evt = evt.
			Str("stream", md.Stream).
			Uint64("stream_sequence", md.Sequence.Stream).
			Uint64("consumer_sequence", md.Sequence.Consumer)
	}
	return evt
}

func commandLogEnvelope(evt *zerolog.Event, env swarm.Envelope) *zerolog.Event {
	to, _ := env.To.String()
	evt = evt.
		Str("envelope_id", strings.TrimSpace(env.ID)).
		Str("namespace", strings.TrimSpace(env.Namespace)).
		Str("task_id", strings.TrimSpace(env.TaskID)).
		Str("session_id", strings.TrimSpace(env.SessionID)).
		Str("correlation_id", strings.TrimSpace(env.CorrelationID)).
		Str("causation_id", strings.TrimSpace(env.CausationID)).
		Str("actor_key", strings.TrimSpace(env.To.Key)).
		Str("to", strings.TrimSpace(to))
	return withDeliveryKey(evt, env)
}
