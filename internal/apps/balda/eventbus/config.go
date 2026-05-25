package eventbus

import (
	"fmt"
	"strings"
)

const (
	ModeSQLite        = "sqlite"
	ModeNATSCore      = "nats_core"
	ModeNATSJetStream = "nats_jetstream"
)

const (
	defaultNATSHost      = "127.0.0.1"
	defaultNATSPort      = -1
	defaultNATSStoreDir  = ".balda/nats"
	defaultNATSMaxMemory = "256mb"
	defaultNATSMaxStore  = "2gb"
)

// Config controls Balda's internal event bus transport.
type Config struct {
	Mode string     `mapstructure:"mode"`
	NATS NATSConfig `mapstructure:"nats"`
}

// NATSConfig controls embedded or external NATS transport settings.
type NATSConfig struct {
	Embedded         bool              `mapstructure:"embedded"`
	URLs             []string          `mapstructure:"urls"`
	Host             string            `mapstructure:"host"`
	Port             int               `mapstructure:"port"`
	JetStream        bool              `mapstructure:"jetstream"`
	StoreDir         string            `mapstructure:"store_dir"`
	MaxMemory        string            `mapstructure:"max_memory"`
	MaxStore         string            `mapstructure:"max_store"`
	SyncAlways       bool              `mapstructure:"sync_always"`
	ExposeMonitoring bool              `mapstructure:"expose_monitoring"`
	Streams          NATSStreamsConfig `mapstructure:"streams"`
	Consumers        NATSConsumers     `mapstructure:"consumers"`
}

// NATSStreamsConfig controls JetStream stream limits.
type NATSStreamsConfig struct {
	Commands NATSStreamConfig `mapstructure:"commands"`
	Events   NATSStreamConfig `mapstructure:"events"`
	DLQ      NATSStreamConfig `mapstructure:"dlq"`
	Control  NATSStreamConfig `mapstructure:"control"`
}

// NATSStreamConfig controls one JetStream stream.
type NATSStreamConfig struct {
	MaxAge     string `mapstructure:"max_age"`
	MaxBytes   string `mapstructure:"max_bytes"`
	MaxMsgSize string `mapstructure:"max_msg_size"`
	Discard    string `mapstructure:"discard"`
}

// NATSConsumers controls JetStream pull consumer defaults.
type NATSConsumers struct {
	Commands NATSConsumerConfig `mapstructure:"commands"`
	Delivery NATSConsumerConfig `mapstructure:"delivery"`
}

// NATSConsumerConfig controls a pull consumer class.
type NATSConsumerConfig struct {
	FetchBatch    int    `mapstructure:"fetch_batch"`
	MaxAckPending int    `mapstructure:"max_ack_pending"`
	AckWait       string `mapstructure:"ack_wait"`
}

func (c Config) Normalized() (Config, error) {
	mode := strings.ToLower(strings.TrimSpace(c.Mode))
	if mode == "" {
		mode = ModeNATSCore
	}
	switch mode {
	case ModeSQLite, ModeNATSCore, ModeNATSJetStream:
		c.Mode = mode
	default:
		return Config{}, fmt.Errorf("invalid event_bus.mode %q: supported values are %q, %q, and %q", c.Mode, ModeSQLite, ModeNATSCore, ModeNATSJetStream)
	}

	c.NATS = c.NATS.normalized()
	if c.Mode != ModeSQLite && !c.NATS.Embedded && len(c.NATS.URLs) == 0 {
		return Config{}, fmt.Errorf("event_bus.nats.urls is required when embedded=false")
	}
	return c, nil
}

func (c Config) UsesNATS() bool {
	mode := strings.ToLower(strings.TrimSpace(c.Mode))
	return mode == ModeNATSCore || mode == ModeNATSJetStream
}

func (c Config) UsesJetStream() bool {
	return strings.EqualFold(strings.TrimSpace(c.Mode), ModeNATSJetStream)
}

func (c NATSConfig) normalized() NATSConfig {
	out := c
	if !out.Embedded && len(out.URLs) == 0 {
		// Keep local-first behavior as the default when the key is omitted.
		out.Embedded = true
	}
	if strings.TrimSpace(out.Host) == "" {
		out.Host = defaultNATSHost
	}
	if out.Port == 0 {
		out.Port = defaultNATSPort
	}
	if strings.TrimSpace(out.StoreDir) == "" {
		out.StoreDir = defaultNATSStoreDir
	}
	if strings.TrimSpace(out.MaxMemory) == "" {
		out.MaxMemory = defaultNATSMaxMemory
	}
	if strings.TrimSpace(out.MaxStore) == "" {
		out.MaxStore = defaultNATSMaxStore
	}
	// JetStream is enabled by default so nats_core can expose a passive JS server
	// and nats_jetstream does not require a second config switch.
	out.JetStream = true
	out.Streams = out.Streams.normalized()
	out.Consumers = out.Consumers.normalized()
	return out
}

func (c NATSStreamsConfig) normalized() NATSStreamsConfig {
	return NATSStreamsConfig{
		Commands: withStreamDefaults(c.Commands, "168h", "1gb", "1mb", "new"),
		Events:   withStreamDefaults(c.Events, "336h", "512mb", "1mb", "old"),
		DLQ:      withStreamDefaults(c.DLQ, "720h", "256mb", "1mb", "old"),
		Control:  withStreamDefaults(c.Control, "168h", "128mb", "256kb", "new"),
	}
}

func withStreamDefaults(c NATSStreamConfig, maxAge string, maxBytes string, maxMsgSize string, discard string) NATSStreamConfig {
	if strings.TrimSpace(c.MaxAge) == "" {
		c.MaxAge = maxAge
	}
	if strings.TrimSpace(c.MaxBytes) == "" {
		c.MaxBytes = maxBytes
	}
	if strings.TrimSpace(c.MaxMsgSize) == "" {
		c.MaxMsgSize = maxMsgSize
	}
	if strings.TrimSpace(c.Discard) == "" {
		c.Discard = discard
	}
	c.Discard = strings.ToLower(strings.TrimSpace(c.Discard))
	return c
}

func (c NATSConsumers) normalized() NATSConsumers {
	return NATSConsumers{
		Commands: withConsumerDefaults(c.Commands, 16, 64, "5m"),
		Delivery: withConsumerDefaults(c.Delivery, 8, 16, "5m"),
	}
}

func withConsumerDefaults(c NATSConsumerConfig, fetchBatch int, maxAckPending int, ackWait string) NATSConsumerConfig {
	if c.FetchBatch <= 0 {
		c.FetchBatch = fetchBatch
	}
	if c.MaxAckPending <= 0 {
		c.MaxAckPending = maxAckPending
	}
	if strings.TrimSpace(c.AckWait) == "" {
		c.AckWait = ackWait
	}
	return c
}
