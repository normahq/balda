package swarm

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"go.uber.org/fx"
)

const (
	DefaultClaimLimit    = 8
	DefaultLeaseDuration = 30 * time.Second
)

type MailboxService struct {
	store     baldastate.SwarmStore
	bus       WakeBus
	cfg       Config
	metrics   *ShadowMetrics
	collectMu sync.Mutex
	collect   *CollectBuffer
}

type mailboxServiceParams struct {
	fx.In

	StateProvider baldastate.Provider
	Bus           WakeBus
	Config        Config
	Metrics       *ShadowMetrics
}

func NewMailboxService(params mailboxServiceParams) (*MailboxService, error) {
	if params.StateProvider == nil {
		return nil, fmt.Errorf("balda state provider is required")
	}
	if params.Bus == nil {
		return nil, fmt.Errorf("swarm wake bus is required")
	}
	return &MailboxService{
		store:   params.StateProvider.Swarm(),
		bus:     params.Bus,
		cfg:     params.Config,
		metrics: params.Metrics,
	}, nil
}

func (s *MailboxService) Enabled() bool {
	return s != nil && s.cfg.MailboxEnabled()
}

func (s *MailboxService) GlobalMailboxEnabled() bool {
	return s != nil && s.cfg.GlobalMailboxEnabled()
}

func (s *MailboxService) WebhookMailboxEnabled() bool {
	return s != nil && s.cfg.WebhookMailboxEnabled()
}

func (s *MailboxService) SchedulerMailboxEnabled() bool {
	return s != nil && s.cfg.SchedulerMailboxEnabled()
}

func (s *MailboxService) ShadowEnabled() bool {
	return s != nil && s.cfg.ShadowEnabled()
}

func (s *MailboxService) ShadowRuntimeEnabled() bool {
	return s != nil && s.cfg.ShadowRuntimeEnabled()
}

func (s *MailboxService) WebhookShadowEnabled() bool {
	return s != nil && s.cfg.WebhookShadowEnabled()
}

func (s *MailboxService) SchedulerShadowEnabled() bool {
	return s != nil && s.cfg.SchedulerShadowEnabled()
}

type SubmittedMessage struct {
	MessageID     string
	MailboxID     string
	QueuePosition int
	Published     bool
}

func (s *MailboxService) Publish(ctx context.Context, env Envelope) (SubmittedMessage, error) {
	return s.publish(ctx, env, publishOptions{})
}

type publishOptions struct {
	skipCollect bool
}

func (s *MailboxService) publish(ctx context.Context, env Envelope, opts publishOptions) (SubmittedMessage, error) {
	if !s.Enabled() {
		return SubmittedMessage{}, fmt.Errorf("swarm mailbox runtime is disabled")
	}
	if strings.TrimSpace(env.ID) == "" {
		env.ID = uuid.NewString()
	}
	if err := env.Validate(); err != nil {
		return SubmittedMessage{}, err
	}
	mailbox, err := env.To.MailboxID()
	if err != nil {
		return SubmittedMessage{}, err
	}
	policy := s.cfg.Queue.PolicyFor(env.Namespace)
	env = applyDefaultPriority(env, policy)
	if policy.Mode == QueueModeCollect && !opts.skipCollect {
		if canCollect(env) {
			s.collectBuffer(policy.Debounce).Add(env)
			return SubmittedMessage{MessageID: env.ID, MailboxID: mailbox, Published: false}, nil
		}
	}
	if policy.Mode == QueueModeInterrupt {
		if err := s.cancelInterrupted(ctx, env); err != nil {
			return SubmittedMessage{}, err
		}
		env = withQueueMode(env, QueueModeInterrupt)
	}
	pendingBefore, env, err := s.enforceCap(ctx, mailbox, env, policy)
	if err != nil {
		return SubmittedMessage{}, err
	}
	record, err := envelopeToRecord(env, mailbox)
	if err != nil {
		return SubmittedMessage{}, err
	}
	result, err := s.store.Publish(ctx, record)
	if err != nil {
		return SubmittedMessage{}, err
	}
	if result.Published {
		if err := s.Notify(ctx, env.To); err != nil {
			return SubmittedMessage{}, err
		}
	}
	return SubmittedMessage{
		MessageID:     env.ID,
		MailboxID:     mailbox,
		QueuePosition: queuePosition(pendingBefore),
		Published:     result.Published,
	}, nil
}

func (s *MailboxService) PublishShadow(ctx context.Context, env Envelope) (SubmittedMessage, error) {
	if !s.ShadowRuntimeEnabled() {
		return SubmittedMessage{}, nil
	}
	if strings.TrimSpace(env.ID) == "" {
		env.ID = uuid.NewString()
	}
	if err := env.Validate(); err != nil {
		return SubmittedMessage{}, err
	}
	mailbox, err := env.To.MailboxID()
	if err != nil {
		return SubmittedMessage{}, err
	}
	record, err := envelopeToRecord(env, mailbox)
	if err != nil {
		return SubmittedMessage{}, err
	}
	record.Status = baldastate.SwarmMessageStatusShadow
	result, err := s.store.Publish(ctx, record)
	if err != nil {
		return SubmittedMessage{}, err
	}
	s.metrics.RecordEnvelope()
	if strings.TrimSpace(env.SessionID) == "" {
		s.metrics.RecordMissingSession()
	}
	if !result.Published {
		s.metrics.RecordDedupeHit()
	}
	return SubmittedMessage{MessageID: env.ID, MailboxID: mailbox, Published: result.Published}, nil
}

func (s *MailboxService) PublishBatch(ctx context.Context, envs []Envelope) error {
	if !s.Enabled() {
		return fmt.Errorf("swarm mailbox runtime is disabled")
	}
	for _, env := range envs {
		if _, err := s.Publish(ctx, env); err != nil {
			return err
		}
	}
	return nil
}

func (s *MailboxService) Claim(ctx context.Context, mailbox string, owner string, limit int, lease time.Duration) ([]Envelope, error) {
	if !s.Enabled() {
		return nil, nil
	}
	records, err := s.store.Claim(ctx, mailbox, owner, limit, lease)
	if err != nil {
		return nil, err
	}
	envs := make([]Envelope, 0, len(records))
	for _, record := range records {
		env, err := recordToEnvelope(record)
		if err != nil {
			return nil, err
		}
		envs = append(envs, env)
	}
	return envs, nil
}

func (s *MailboxService) Ack(ctx context.Context, mailbox string, messageID string) error {
	return s.store.Ack(ctx, mailbox, messageID)
}

func (s *MailboxService) Retry(ctx context.Context, mailbox string, messageID string, next time.Time, reason string) error {
	return s.store.Retry(ctx, mailbox, messageID, next, reason)
}

func (s *MailboxService) DeadLetter(ctx context.Context, mailbox string, messageID string, reason string) error {
	return s.store.DeadLetter(ctx, mailbox, messageID, reason)
}

func (s *MailboxService) CancelByTask(ctx context.Context, taskID string, reason string) (int, error) {
	return s.store.CancelByTask(ctx, taskID, reason)
}

func (s *MailboxService) CancelBySession(ctx context.Context, sessionID string, reason string) (int, error) {
	return s.store.CancelBySession(ctx, sessionID, reason)
}

func (s *MailboxService) Recover(ctx context.Context, now time.Time) (baldastate.SwarmRecoveryResult, error) {
	return s.store.Recover(ctx, now)
}

func (s *MailboxService) ListReadyMailboxes(ctx context.Context, limit int) ([]string, error) {
	return s.store.ListReadyMailboxes(ctx, limit)
}

func (s *MailboxService) Notify(ctx context.Context, addr ActorAddress) error {
	if !s.Enabled() {
		return nil
	}
	return s.bus.Publish(ctx, addr)
}

func (s *MailboxService) RecordShadowDispatch() {
	s.metrics.RecordDispatch()
}

func (s *MailboxService) ShadowMetricsSnapshot() map[string]uint64 {
	return s.metrics.Snapshot()
}

func (s *MailboxService) collectBuffer(debounce time.Duration) *CollectBuffer {
	s.collectMu.Lock()
	defer s.collectMu.Unlock()
	if s.collect == nil {
		s.collect = NewCollectBuffer(debounce, func(envs []Envelope) error {
			return s.flushCollected(envs)
		})
	}
	return s.collect
}

func (s *MailboxService) flushCollected(envs []Envelope) error {
	for _, env := range mergeCollected(envs) {
		if _, err := s.publish(context.Background(), env, publishOptions{skipCollect: true}); err != nil {
			return err
		}
	}
	return nil
}

func (s *MailboxService) cancelInterrupted(ctx context.Context, env Envelope) error {
	reason := "interrupted by newer queue message"
	if taskID := strings.TrimSpace(env.TaskID); taskID != "" {
		if _, err := s.store.CancelByTask(ctx, taskID, reason); err != nil {
			return err
		}
	}
	if sessionID := strings.TrimSpace(env.SessionID); sessionID != "" {
		if _, err := s.store.CancelBySession(ctx, sessionID, reason); err != nil {
			return err
		}
	}
	return nil
}

func (s *MailboxService) enforceCap(
	ctx context.Context,
	mailbox string,
	env Envelope,
	policy QueuePolicy,
) (int, Envelope, error) {
	if policy.Cap <= 0 {
		return 0, env, nil
	}
	pending, err := s.store.PendingCount(ctx, mailbox)
	if err != nil {
		return 0, Envelope{}, err
	}
	overflow := pending - policy.Cap + 1
	if overflow <= 0 {
		return pending, env, nil
	}
	switch policy.Drop {
	case QueueDropNew:
		return pending, Envelope{}, queueFull(mailbox, policy.Cap)
	case QueueDropOld:
		_, err := s.store.CancelDroppable(ctx, mailbox, overflow, "queue cap reached")
		return pending, env, err
	case QueueDropSummarize:
		return s.summarizeForCap(ctx, mailbox, pending, overflow, env)
	default:
		return pending, Envelope{}, fmt.Errorf("unsupported queue drop policy %q", policy.Drop)
	}
}

func (s *MailboxService) summarizeForCap(
	ctx context.Context,
	mailbox string,
	pending int,
	overflow int,
	env Envelope,
) (int, Envelope, error) {
	dropped, err := s.store.CancelDroppable(ctx, mailbox, overflow, "queue cap summarized")
	if err != nil {
		return pending, Envelope{}, err
	}
	if len(dropped) == 0 {
		return pending, Envelope{}, queueFull(mailbox, pending)
	}
	droppedEnvs, err := recordsToEnvelopes(dropped)
	if err != nil {
		return pending, Envelope{}, err
	}
	summary, ok := summarizeSessionEnvelopes(append(droppedEnvs, env), "summarized")
	if !ok {
		return pending, env, nil
	}
	return pending, summary, nil
}

func mergeCollected(envs []Envelope) []Envelope {
	if len(envs) <= 1 {
		return envs
	}
	summary, ok := summarizeSessionEnvelopes(envs, "collected")
	if !ok {
		return envs
	}
	return []Envelope{summary}
}

func canCollect(env Envelope) bool {
	return strings.EqualFold(strings.TrimSpace(env.To.Target), ActorTypeSession)
}

func queuePosition(pendingBefore int) int {
	if pendingBefore <= 0 {
		return 0
	}
	return pendingBefore
}

func envelopeToRecord(env Envelope, mailbox string) (baldastate.SwarmMessageRecord, error) {
	from, err := env.From.String()
	if err != nil {
		return baldastate.SwarmMessageRecord{}, err
	}
	to, err := env.To.String()
	if err != nil {
		return baldastate.SwarmMessageRecord{}, err
	}
	metaJSON := ""
	if len(env.Meta) > 0 {
		data, err := json.Marshal(env.Meta)
		if err != nil {
			return baldastate.SwarmMessageRecord{}, fmt.Errorf("encode envelope meta: %w", err)
		}
		metaJSON = string(data)
	}
	return baldastate.SwarmMessageRecord{
		ID:            strings.TrimSpace(env.ID),
		Mailbox:       mailbox,
		Namespace:     strings.TrimSpace(env.Namespace),
		Kind:          strings.TrimSpace(env.Kind),
		FromAddr:      from,
		ToAddr:        to,
		SessionID:     strings.TrimSpace(env.SessionID),
		TaskID:        strings.TrimSpace(env.TaskID),
		CorrelationID: strings.TrimSpace(env.CorrelationID),
		CausationID:   strings.TrimSpace(env.CausationID),
		Priority:      env.Priority,
		DedupeKey:     strings.TrimSpace(env.DedupeKey),
		MaxAttempts:   env.MaxAttempts,
		NotBefore:     env.NotBefore,
		ExpiresAt:     env.ExpiresAt,
		PayloadJSON:   strings.TrimSpace(env.PayloadJSON),
		MetaJSON:      metaJSON,
	}, nil
}

func recordToEnvelope(record baldastate.SwarmMessageRecord) (Envelope, error) {
	from, err := parseActorAddress(record.FromAddr)
	if err != nil {
		return Envelope{}, fmt.Errorf("parse from address: %w", err)
	}
	to, err := parseActorAddress(record.ToAddr)
	if err != nil {
		return Envelope{}, fmt.Errorf("parse to address: %w", err)
	}
	meta := map[string]string(nil)
	if strings.TrimSpace(record.MetaJSON) != "" {
		if err := json.Unmarshal([]byte(record.MetaJSON), &meta); err != nil {
			return Envelope{}, fmt.Errorf("decode envelope meta: %w", err)
		}
	}
	return Envelope{
		ID:            record.ID,
		Namespace:     record.Namespace,
		Kind:          record.Kind,
		From:          from,
		To:            to,
		SessionID:     record.SessionID,
		TaskID:        record.TaskID,
		CorrelationID: record.CorrelationID,
		CausationID:   record.CausationID,
		Priority:      record.Priority,
		DedupeKey:     record.DedupeKey,
		Attempt:       record.Attempt,
		MaxAttempts:   record.MaxAttempts,
		NotBefore:     record.NotBefore,
		ExpiresAt:     record.ExpiresAt,
		PayloadJSON:   record.PayloadJSON,
		Meta:          meta,
	}, nil
}

func parseActorAddress(raw string) (ActorAddress, error) {
	trimmed := strings.TrimSpace(raw)
	idx := strings.Index(trimmed, ":")
	if idx <= 0 || idx == len(trimmed)-1 {
		return ActorAddress{}, fmt.Errorf("invalid actor address %q", raw)
	}
	return ActorAddress{Target: trimmed[:idx], Key: trimmed[idx+1:]}, nil
}
