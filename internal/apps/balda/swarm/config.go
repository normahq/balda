package swarm

import (
	"fmt"
	"strings"
	"time"
)

const (
	ModeLegacy  = "legacy"
	ModeShadow  = "shadow"
	ModeMailbox = "mailbox"

	QueueModeFollowup  = "followup"
	QueueModeCollect   = "collect"
	QueueModeInterrupt = "interrupt"

	QueueDropSummarize = "summarize"
	QueueDropOld       = "old"
	QueueDropNew       = "new"

	defaultQueueDebounceMS = 500
	defaultQueueCap        = 20
)

type Config struct {
	Enabled       bool
	Mode          string
	WebhookMode   string
	SchedulerMode string
	Shadow        ShadowConfig
	Queue         QueueConfig
}

type ShadowConfig struct {
	Enabled bool
}

type QueueConfig struct {
	DefaultMode string
	DebounceMS  int
	Cap         int
	Drop        string
	ByNamespace map[string]string
}

type QueuePolicy struct {
	Mode     string
	Drop     string
	Debounce time.Duration
	Cap      int
	Priority int
}

func (c Config) MailboxEnabled() bool {
	return c.Enabled && (modeIs(c.Mode, ModeMailbox) || modeIs(c.WebhookMode, ModeMailbox) || modeIs(c.SchedulerMode, ModeMailbox))
}

func (c Config) GlobalMailboxEnabled() bool {
	return c.Enabled && modeIs(c.Mode, ModeMailbox)
}

func (c Config) WebhookMailboxEnabled() bool {
	return c.Enabled && modeIs(c.WebhookMode, ModeMailbox)
}

func (c Config) SchedulerMailboxEnabled() bool {
	return c.Enabled && modeIs(c.SchedulerMode, ModeMailbox)
}

func (c Config) ShadowEnabled() bool {
	return c.Enabled && c.Shadow.Enabled && modeIs(c.Mode, ModeShadow)
}

func (c Config) ShadowRuntimeEnabled() bool {
	return c.Enabled && c.Shadow.Enabled && (modeIs(c.Mode, ModeShadow) || modeIs(c.WebhookMode, ModeShadow) || modeIs(c.SchedulerMode, ModeShadow))
}

func (c Config) WebhookShadowEnabled() bool {
	return c.Enabled && c.Shadow.Enabled && modeIs(c.WebhookMode, ModeShadow)
}

func (c Config) SchedulerShadowEnabled() bool {
	return c.Enabled && c.Shadow.Enabled && modeIs(c.SchedulerMode, ModeShadow)
}

func (c Config) Normalized() (Config, error) {
	var err error
	if c.Mode, err = normalizeMode(c.Mode); err != nil {
		return Config{}, err
	}
	if c.WebhookMode, err = normalizeMode(c.WebhookMode); err != nil {
		return Config{}, err
	}
	if c.SchedulerMode, err = normalizeMode(c.SchedulerMode); err != nil {
		return Config{}, err
	}
	if c.Queue, err = c.Queue.Normalized(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func (c QueueConfig) PolicyFor(namespace string) QueuePolicy {
	normalized, err := c.Normalized()
	if err != nil {
		normalized = defaultQueueConfig()
	}
	ns := strings.TrimSpace(namespace)
	mode := normalized.DefaultMode
	if override := strings.TrimSpace(normalized.ByNamespace[ns]); override != "" {
		mode = override
	}
	return QueuePolicy{
		Mode:     mode,
		Drop:     normalized.Drop,
		Debounce: time.Duration(normalized.DebounceMS) * time.Millisecond,
		Cap:      normalized.Cap,
		Priority: defaultPriorityForNamespace(ns),
	}
}

func (c QueueConfig) Normalized() (QueueConfig, error) {
	defaults := defaultQueueConfig()
	out := QueueConfig{
		DefaultMode: strings.TrimSpace(c.DefaultMode),
		DebounceMS:  c.DebounceMS,
		Cap:         c.Cap,
		Drop:        strings.TrimSpace(c.Drop),
		ByNamespace: make(map[string]string, len(defaults.ByNamespace)+len(c.ByNamespace)),
	}
	if out.DefaultMode == "" {
		out.DefaultMode = defaults.DefaultMode
	}
	mode, err := normalizeQueueMode(out.DefaultMode)
	if err != nil {
		return QueueConfig{}, err
	}
	out.DefaultMode = mode

	if out.DebounceMS <= 0 {
		out.DebounceMS = defaults.DebounceMS
	}
	if out.Cap <= 0 {
		out.Cap = defaults.Cap
	}
	if out.Drop == "" {
		out.Drop = defaults.Drop
	}
	drop, err := normalizeQueueDrop(out.Drop)
	if err != nil {
		return QueueConfig{}, err
	}
	out.Drop = drop

	for namespace, defaultMode := range defaults.ByNamespace {
		out.ByNamespace[namespace] = defaultMode
	}
	for namespace, rawMode := range c.ByNamespace {
		ns := strings.TrimSpace(namespace)
		if ns == "" {
			return QueueConfig{}, fmt.Errorf("queue namespace override key is required")
		}
		mode, err := normalizeQueueMode(rawMode)
		if err != nil {
			return QueueConfig{}, fmt.Errorf("invalid queue mode for namespace %q: %w", ns, err)
		}
		out.ByNamespace[ns] = mode
	}
	return out, nil
}

func modeIs(raw string, want string) bool {
	return modeOrDefault(raw) == want
}

func normalizeMode(raw string) (string, error) {
	mode := modeOrDefault(raw)
	switch mode {
	case ModeLegacy, ModeShadow, ModeMailbox:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid swarm mode %q: supported values are %q, %q, and %q", mode, ModeLegacy, ModeShadow, ModeMailbox)
	}
}

func normalizeQueueMode(raw string) (string, error) {
	mode := strings.ToLower(strings.TrimSpace(raw))
	switch mode {
	case QueueModeFollowup, QueueModeCollect, QueueModeInterrupt:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid queue mode %q: supported values are %q, %q, and %q", raw, QueueModeFollowup, QueueModeCollect, QueueModeInterrupt)
	}
}

func normalizeQueueDrop(raw string) (string, error) {
	drop := strings.ToLower(strings.TrimSpace(raw))
	switch drop {
	case QueueDropSummarize, QueueDropOld, QueueDropNew:
		return drop, nil
	default:
		return "", fmt.Errorf("invalid queue drop policy %q: supported values are %q, %q, and %q", raw, QueueDropSummarize, QueueDropOld, QueueDropNew)
	}
}

func modeOrDefault(raw string) string {
	mode := strings.ToLower(strings.TrimSpace(raw))
	if mode == "" {
		return ModeShadow
	}
	return mode
}

func defaultQueueConfig() QueueConfig {
	return QueueConfig{
		DefaultMode: QueueModeFollowup,
		DebounceMS:  defaultQueueDebounceMS,
		Cap:         defaultQueueCap,
		Drop:        QueueDropSummarize,
		ByNamespace: map[string]string{
			NamespaceTaskControl:     QueueModeInterrupt,
			NamespaceWebhookInbound:  QueueModeFollowup,
			NamespaceScheduleInbound: QueueModeCollect,
			NamespaceMemorySync:      QueueModeCollect,
		},
	}
}

func defaultPriorityForNamespace(namespace string) int {
	switch strings.TrimSpace(namespace) {
	case NamespaceTaskControl:
		return 100
	case NamespaceHumanInbound:
		return 90
	case NamespaceWebhookInbound:
		return 80
	case NamespaceAgentResult:
		return 70
	case NamespaceScheduleInbound:
		return 50
	case NamespaceAgentCommand:
		return 30
	case NamespaceMemorySync:
		return 10
	case NamespaceTelemetry:
		return 0
	default:
		return 0
	}
}
