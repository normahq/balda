// Package logging provides application-wide logging configuration.
package logging

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	LevelTrace = "trace"
	LevelDebug = "debug"
	LevelInfo  = "info"
	LevelWarn  = "warn"
	LevelError = "error"
)

// Init initializes the global logger for zerolog and configures slog for third-party libraries.
func Init(setters ...OptOptionsSetter) error {
	opts := NewOptions(setters...)
	if err := opts.Validate(); err != nil {
		return fmt.Errorf("validate logging options: %w", err)
	}
	levelName, zlLevel, slogLevel, err := resolveLevels(opts.level)
	if err != nil {
		return fmt.Errorf("resolve logging level: %w", err)
	}
	opts.level = levelName

	// 1. Configure zerolog (Primary for the project)
	zerolog.SetGlobalLevel(zlLevel)

	var zl zerolog.Logger
	if !opts.json {
		zl = zerolog.New(zerolog.ConsoleWriter{
			Out:        os.Stderr,
			TimeFormat: time.RFC3339,
		})
	} else {
		zl = zerolog.New(os.Stderr)
	}

	zl = zl.With().Timestamp().Logger()
	log.Logger = zl
	zerolog.DefaultContextLogger = &log.Logger

	// 2. Configure slog (Only for third-party libraries)
	var slogHandler slog.Handler
	if !opts.json {
		slogHandler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: slogLevel,
		})
	} else {
		slogHandler = slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
			Level: slogLevel,
		})
	}

	// Set as default so third-party libs using slog will use this configuration.
	slog.SetDefault(slog.New(slogHandler))

	return nil
}

func resolveLevels(levelRaw string) (string, zerolog.Level, slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(levelRaw)) {
	case "", LevelInfo:
		return LevelInfo, zerolog.InfoLevel, slog.LevelInfo, nil
	case LevelTrace:
		return LevelTrace, zerolog.TraceLevel, slog.LevelDebug - 4, nil
	case LevelDebug:
		return LevelDebug, zerolog.DebugLevel, slog.LevelDebug, nil
	case LevelWarn, "warning":
		return LevelWarn, zerolog.WarnLevel, slog.LevelWarn, nil
	case LevelError:
		return LevelError, zerolog.ErrorLevel, slog.LevelError, nil
	default:
		return "", zerolog.NoLevel, slog.LevelInfo, fmt.Errorf("unsupported level %q (allowed: trace, debug, info, warn, error)", levelRaw)
	}
}
