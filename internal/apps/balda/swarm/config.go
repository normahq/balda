package swarm

import (
	"strings"
)

const QueueModeInterrupt = "interrupt"

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

func (c Config) RuntimeEnabled() bool {
	return c.Enabled
}

func (c Config) Normalized() (Config, error) {
	c.Commands = c.Commands.Normalized()
	c.Events = c.Events.Normalized()
	c.DLQ = c.DLQ.Normalized()
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
