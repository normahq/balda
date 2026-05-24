package swarm

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
)

const (
	queueMetaMode           = "queue_mode"
	queueMetaCollectedCount = "queue_collected_count"
	queueMetaSummaryReason  = "queue_summary_reason"
)

var ErrQueueFull = errors.New("swarm mailbox queue full")

type QueueFullError struct {
	Mailbox string
	Cap     int
}

func (e *QueueFullError) Error() string {
	return fmt.Sprintf("%s: mailbox %q reached cap %d", ErrQueueFull, e.Mailbox, e.Cap)
}

func (e *QueueFullError) Unwrap() error {
	return ErrQueueFull
}

type CollectBuffer struct {
	mu       sync.Mutex
	debounce time.Duration
	entries  map[string]*collectEntry
	flush    func([]Envelope) error
}

type collectEntry struct {
	envs  []Envelope
	timer *time.Timer
}

func NewCollectBuffer(debounce time.Duration, flush func([]Envelope) error) *CollectBuffer {
	if debounce <= 0 {
		debounce = time.Duration(defaultQueueDebounceMS) * time.Millisecond
	}
	return &CollectBuffer{
		debounce: debounce,
		entries:  make(map[string]*collectEntry),
		flush:    flush,
	}
}

func (b *CollectBuffer) Add(env Envelope) {
	if b == nil || b.flush == nil {
		return
	}
	key := collectKey(env)
	b.mu.Lock()
	entry := b.entries[key]
	if entry == nil {
		entry = &collectEntry{}
		b.entries[key] = entry
	}
	entry.envs = append(entry.envs, env)
	if entry.timer != nil {
		entry.timer.Stop()
	}
	entry.timer = time.AfterFunc(b.debounce, func() {
		_ = b.flushKey(key)
	})
	b.mu.Unlock()
}

func (b *CollectBuffer) FlushAll() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	keys := make([]string, 0, len(b.entries))
	for key := range b.entries {
		keys = append(keys, key)
	}
	b.mu.Unlock()

	for _, key := range keys {
		if err := b.flushKey(key); err != nil {
			return err
		}
	}
	return nil
}

func (b *CollectBuffer) flushKey(key string) error {
	b.mu.Lock()
	entry := b.entries[key]
	if entry == nil {
		b.mu.Unlock()
		return nil
	}
	delete(b.entries, key)
	if entry.timer != nil {
		entry.timer.Stop()
	}
	envs := append([]Envelope(nil), entry.envs...)
	b.mu.Unlock()

	return b.flush(envs)
}

func collectKey(env Envelope) string {
	to, _ := env.To.String()
	return strings.Join([]string{
		strings.TrimSpace(to),
		strings.TrimSpace(env.Namespace),
		strings.TrimSpace(env.Kind),
		strings.TrimSpace(env.SessionID),
		strings.TrimSpace(env.TaskID),
	}, "\x00")
}

func queueFull(mailbox string, limit int) error {
	return &QueueFullError{Mailbox: mailbox, Cap: limit}
}

func withQueueMode(env Envelope, mode string) Envelope {
	env.Meta = cloneMeta(env.Meta)
	env.Meta[queueMetaMode] = mode
	return env
}

func queueMode(env Envelope) string {
	if env.Meta == nil {
		return ""
	}
	return strings.TrimSpace(env.Meta[queueMetaMode])
}

func QueueModeOf(env Envelope) string {
	return queueMode(env)
}

func cloneMeta(in map[string]string) map[string]string {
	out := make(map[string]string, len(in)+1)
	for key, value := range in {
		out[key] = value
	}
	return out
}

func applyDefaultPriority(env Envelope, policy QueuePolicy) Envelope {
	if env.Priority == 0 {
		env.Priority = policy.Priority
	}
	return env
}

func summarizeSessionEnvelopes(envs []Envelope, reason string) (Envelope, bool) {
	if len(envs) == 0 {
		return Envelope{}, false
	}
	if !strings.EqualFold(strings.TrimSpace(envs[0].To.Target), ActorTypeSession) {
		return Envelope{}, false
	}
	payloads := make([]sessionQueuePayload, 0, len(envs))
	for _, env := range envs {
		var payload sessionQueuePayload
		if err := json.Unmarshal([]byte(env.PayloadJSON), &payload); err != nil {
			return Envelope{}, false
		}
		payloads = append(payloads, payload)
	}

	merged := payloads[0]
	merged.Text = queueSummaryText(payloads, reason)
	merged.MessageID = 0
	merged.DedupeKey = ""
	merged.Source = "queue"
	merged.Deliver = anyPayloadDelivers(payloads)

	data, err := json.Marshal(merged)
	if err != nil {
		return Envelope{}, false
	}
	out := envs[0]
	out.ID = uuid.NewString()
	out.From = SystemAddress("queue")
	out.Priority = maxEnvelopePriority(envs)
	out.DedupeKey = ""
	out.CausationID = envs[0].ID
	out.PayloadJSON = string(data)
	out.Meta = cloneMeta(out.Meta)
	out.Meta[queueMetaCollectedCount] = fmt.Sprintf("%d", len(envs))
	out.Meta[queueMetaSummaryReason] = reason
	return out, out.Validate() == nil
}

func queueSummaryText(payloads []sessionQueuePayload, reason string) string {
	var b strings.Builder
	if strings.TrimSpace(reason) == "" {
		reason = "collected"
	}
	fmt.Fprintf(&b, "Queue %s %d message(s):", reason, len(payloads))
	for idx, payload := range payloads {
		text := strings.TrimSpace(payload.Text)
		if text == "" {
			text = "(empty message)"
		}
		fmt.Fprintf(&b, "\n\n%d. %s", idx+1, text)
	}
	return b.String()
}

func anyPayloadDelivers(payloads []sessionQueuePayload) bool {
	for _, payload := range payloads {
		if payload.Deliver {
			return true
		}
	}
	return false
}

func maxEnvelopePriority(envs []Envelope) int {
	maxPriority := 0
	for _, env := range envs {
		if env.Priority > maxPriority {
			maxPriority = env.Priority
		}
	}
	return maxPriority
}

func recordsToEnvelopes(records []baldastate.SwarmMessageRecord) ([]Envelope, error) {
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

type sessionQueuePayload struct {
	Text           string         `json:"text"`
	Locator        map[string]any `json:"locator"`
	UserID         string         `json:"user_id,omitempty"`
	AgentSessionID string         `json:"agent_session_id,omitempty"`
	MessageID      int            `json:"message_id,omitempty"`
	TopicID        int            `json:"topic_id,omitempty"`
	ProgressPolicy map[string]any `json:"progress_policy,omitempty"`
	Deliver        bool           `json:"deliver"`
	Source         string         `json:"source,omitempty"`
	DedupeKey      string         `json:"dedupe_key,omitempty"`
}
