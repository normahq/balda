package eventbus

import (
	"fmt"
	"strings"
)

const (
	defaultNATSHost      = "127.0.0.1"
	defaultNATSPort      = -1
	defaultNATSStoreDir  = ".balda/nats"
	defaultNATSMaxMemory = "256mb"
	defaultNATSMaxStore  = "2gb"
)

// Config controls Balda's built-in command/event runtime.
type Config struct {
	Embedded         bool     `mapstructure:"embedded"`
	URLs             []string `mapstructure:"urls"`
	Host             string   `mapstructure:"host"`
	Port             int      `mapstructure:"port"`
	JetStream        bool     `mapstructure:"jetstream"`
	StoreDir         string   `mapstructure:"store_dir"`
	MaxMemory        string   `mapstructure:"max_memory"`
	MaxStore         string   `mapstructure:"max_store"`
	SyncAlways       bool     `mapstructure:"sync_always"`
	ExposeMonitoring bool     `mapstructure:"expose_monitoring"`
}

// Normalized applies safe localhost built-in runtime defaults.
func (c Config) Normalized() (Config, error) {
	out := c
	if !out.Embedded && len(out.URLs) == 0 {
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
	out.JetStream = true
	if !out.Embedded && len(out.URLs) == 0 {
		return Config{}, fmt.Errorf("balda.nats.urls is required when embedded=false")
	}
	return out, nil
}
