package swarm

import (
	"fmt"
	"strings"
)

const (
	QueueModeFollowup  = "followup"
	QueueModeCollect   = "collect"
	QueueModeInterrupt = "interrupt"

	QueueDropSummarize = "summarize"
	QueueDropOld       = "old"
	QueueDropNew       = "new"

	defaultQueueDebounceMS = 500
	defaultQueueCap        = 20
)

const (
	DefaultCommandStream          = "BALDA_COMMANDS"
	DefaultCommandConsumer        = "BALDA_WORKER_COMMANDS"
	DefaultEventStream            = "BALDA_EVENTS"
	DefaultEventProjectorConsumer = "BALDA_EVENT_PROJECTOR"
	DefaultDLQStream              = "BALDA_DLQ"
)

type Config struct {
	Enabled  bool
	Commands CommandConfig
	Events   EventStreamConfig
	DLQ      DLQConfig
	Queue    QueueConfig
	Agents   map[string]AgentSpec
}

type CommandConfig struct {
	Stream        string
	Consumer      string
	AckWait       string
	MaxDeliver    int
	MaxAckPending int
	FetchBatch    int
	FetchWait     string
}

type EventStreamConfig struct {
	Stream string
}

type DLQConfig struct {
	Stream string
}

type QueueConfig struct {
	DefaultMode string
	DebounceMS  int
	Cap         int
	Drop        string
	ByNamespace map[string]string
}

func (c Config) RuntimeEnabled() bool {
	return c.Enabled
}

func (c Config) Normalized() (Config, error) {
	var err error
	c.Commands = c.Commands.Normalized()
	c.Events = c.Events.Normalized()
	c.DLQ = c.DLQ.Normalized()
	if c.Queue, err = c.Queue.Normalized(); err != nil {
		return Config{}, err
	}
	agents, err := NormalizeAgentSpecs(c.Agents)
	if err != nil {
		return Config{}, err
	}
	c.Agents = make(map[string]AgentSpec, len(agents))
	for _, spec := range agents {
		c.Agents[spec.Name] = spec
	}
	return c, nil
}

func (c CommandConfig) Normalized() CommandConfig {
	out := c
	if strings.TrimSpace(out.Stream) == "" {
		out.Stream = DefaultCommandStream
	}
	if strings.TrimSpace(out.Consumer) == "" {
		out.Consumer = DefaultCommandConsumer
	}
	if strings.TrimSpace(out.AckWait) == "" {
		out.AckWait = "5m"
	}
	if out.MaxDeliver <= 0 {
		out.MaxDeliver = 5
	}
	if out.MaxAckPending <= 0 {
		out.MaxAckPending = 64
	}
	if out.FetchBatch <= 0 {
		out.FetchBatch = 16
	}
	if out.FetchBatch > out.MaxAckPending {
		out.FetchBatch = out.MaxAckPending
	}
	if strings.TrimSpace(out.FetchWait) == "" {
		out.FetchWait = "1s"
	}
	return out
}

func (c EventStreamConfig) Normalized() EventStreamConfig {
	if strings.TrimSpace(c.Stream) == "" {
		c.Stream = DefaultEventStream
	}
	return c
}

func (c DLQConfig) Normalized() DLQConfig {
	if strings.TrimSpace(c.Stream) == "" {
		c.Stream = DefaultDLQStream
	}
	return c
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
