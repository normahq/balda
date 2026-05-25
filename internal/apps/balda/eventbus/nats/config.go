package natsbus

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	baldaeventbus "github.com/normahq/balda/internal/apps/balda/eventbus"
)

type resolvedConfig struct {
	baldaeventbus.Config
	StoreDir   string
	MaxMemory  int64
	MaxStore   int64
	StreamSpec streamSpecSet
}

type streamSpecSet struct {
	Commands streamSpec
	Events   streamSpec
	DLQ      streamSpec
	Control  streamSpec
}

type streamSpec struct {
	MaxAge     time.Duration
	MaxBytes   int64
	MaxMsgSize int32
	Discard    string
}

func resolveConfig(cfg baldaeventbus.Config, workingDir string) (resolvedConfig, error) {
	normalized, err := cfg.Normalized()
	if err != nil {
		return resolvedConfig{}, err
	}
	out := resolvedConfig{Config: normalized}
	out.StoreDir = strings.TrimSpace(normalized.NATS.StoreDir)
	if out.StoreDir != "" && !filepath.IsAbs(out.StoreDir) {
		out.StoreDir = filepath.Join(workingDir, out.StoreDir)
	}
	out.MaxMemory, err = parseBytes(normalized.NATS.MaxMemory)
	if err != nil {
		return resolvedConfig{}, fmt.Errorf("event_bus.nats.max_memory: %w", err)
	}
	out.MaxStore, err = parseBytes(normalized.NATS.MaxStore)
	if err != nil {
		return resolvedConfig{}, fmt.Errorf("event_bus.nats.max_store: %w", err)
	}
	out.StreamSpec.Commands, err = resolveStreamSpec(normalized.NATS.Streams.Commands)
	if err != nil {
		return resolvedConfig{}, fmt.Errorf("event_bus.nats.streams.commands: %w", err)
	}
	out.StreamSpec.Events, err = resolveStreamSpec(normalized.NATS.Streams.Events)
	if err != nil {
		return resolvedConfig{}, fmt.Errorf("event_bus.nats.streams.events: %w", err)
	}
	out.StreamSpec.DLQ, err = resolveStreamSpec(normalized.NATS.Streams.DLQ)
	if err != nil {
		return resolvedConfig{}, fmt.Errorf("event_bus.nats.streams.dlq: %w", err)
	}
	out.StreamSpec.Control, err = resolveStreamSpec(normalized.NATS.Streams.Control)
	if err != nil {
		return resolvedConfig{}, fmt.Errorf("event_bus.nats.streams.control: %w", err)
	}
	return out, nil
}

func resolveStreamSpec(cfg baldaeventbus.NATSStreamConfig) (streamSpec, error) {
	maxAge, err := parseDuration(strings.TrimSpace(cfg.MaxAge))
	if err != nil {
		return streamSpec{}, fmt.Errorf("max_age: %w", err)
	}
	maxBytes, err := parseBytes(cfg.MaxBytes)
	if err != nil {
		return streamSpec{}, fmt.Errorf("max_bytes: %w", err)
	}
	maxMsgSize, err := parseBytes(cfg.MaxMsgSize)
	if err != nil {
		return streamSpec{}, fmt.Errorf("max_msg_size: %w", err)
	}
	if maxMsgSize > int64(^uint32(0)>>1) {
		return streamSpec{}, fmt.Errorf("max_msg_size exceeds int32: %d", maxMsgSize)
	}
	return streamSpec{
		MaxAge:     maxAge,
		MaxBytes:   maxBytes,
		MaxMsgSize: int32(maxMsgSize),
		Discard:    strings.ToLower(strings.TrimSpace(cfg.Discard)),
	}, nil
}

func parseDuration(raw string) (time.Duration, error) {
	trimmed := strings.ToLower(strings.TrimSpace(raw))
	if strings.HasSuffix(trimmed, "d") {
		value := strings.TrimSpace(strings.TrimSuffix(trimmed, "d"))
		days, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q: %w", raw, err)
		}
		return time.Duration(days * float64(24*time.Hour)), nil
	}
	return time.ParseDuration(trimmed)
}

func parseBytes(raw string) (int64, error) {
	trimmed := strings.ToLower(strings.TrimSpace(raw))
	if trimmed == "" {
		return 0, fmt.Errorf("value is required")
	}
	multipliers := []struct {
		suffix string
		value  int64
	}{
		{"gib", 1024 * 1024 * 1024},
		{"gb", 1000 * 1000 * 1000},
		{"mib", 1024 * 1024},
		{"mb", 1000 * 1000},
		{"kib", 1024},
		{"kb", 1000},
		{"b", 1},
	}
	for _, item := range multipliers {
		if strings.HasSuffix(trimmed, item.suffix) {
			value := strings.TrimSpace(strings.TrimSuffix(trimmed, item.suffix))
			if value == "" {
				return 0, fmt.Errorf("invalid byte value %q", raw)
			}
			parsed, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid byte value %q: %w", raw, err)
			}
			if parsed < 0 {
				return 0, fmt.Errorf("byte value must be non-negative")
			}
			return int64(parsed * float64(item.value)), nil
		}
	}
	parsed, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte value %q: %w", raw, err)
	}
	if parsed < 0 {
		return 0, fmt.Errorf("byte value must be non-negative")
	}
	return parsed, nil
}
