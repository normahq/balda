package main

import (
	"testing"

	baldaapp "github.com/normahq/balda/internal/apps/balda"
	"github.com/normahq/balda/internal/logging"
	"github.com/rs/zerolog"
)

func TestResolveBaldaLoggingSettings(t *testing.T) {
	tests := []struct {
		name      string
		cfg       baldaapp.LoggerConfig
		debugFlag bool
		traceFlag bool
		wantLevel string
		wantJSON  bool
	}{
		{
			name:      "defaults to info when config level empty",
			cfg:       baldaapp.LoggerConfig{Pretty: true},
			wantLevel: logging.LevelInfo,
			wantJSON:  false,
		},
		{
			name:      "uses config level when flags disabled",
			cfg:       baldaapp.LoggerConfig{Level: " debug ", Pretty: true},
			wantLevel: logging.LevelDebug,
			wantJSON:  false,
		},
		{
			name:      "debug flag overrides config level",
			cfg:       baldaapp.LoggerConfig{Level: logging.LevelWarn, Pretty: false},
			debugFlag: true,
			wantLevel: logging.LevelDebug,
			wantJSON:  true,
		},
		{
			name:      "trace flag overrides debug and config",
			cfg:       baldaapp.LoggerConfig{Level: logging.LevelWarn, Pretty: true},
			debugFlag: true,
			traceFlag: true,
			wantLevel: logging.LevelTrace,
			wantJSON:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveBaldaLoggingSettings(tc.cfg, tc.debugFlag, tc.traceFlag)
			if got.level != tc.wantLevel {
				t.Fatalf("level = %q, want %q", got.level, tc.wantLevel)
			}
			if got.json != tc.wantJSON {
				t.Fatalf("json = %t, want %t", got.json, tc.wantJSON)
			}
		})
	}
}

func TestApplyBaldaLogging_DebugFlagOverridesConfig(t *testing.T) {
	restore := setBaldaLogFlagsForTest(t, true, false)
	defer restore()

	if err := applyBaldaLogging(baldaapp.LoggerConfig{Level: logging.LevelError, Pretty: true}); err != nil {
		t.Fatalf("applyBaldaLogging(): %v", err)
	}
	if got := zerolog.GlobalLevel(); got != zerolog.DebugLevel {
		t.Fatalf("GlobalLevel() = %v, want %v", got, zerolog.DebugLevel)
	}
}

func TestApplyBaldaLogging_TraceFlagOverridesDebug(t *testing.T) {
	restore := setBaldaLogFlagsForTest(t, true, true)
	defer restore()

	if err := applyBaldaLogging(baldaapp.LoggerConfig{Level: logging.LevelError, Pretty: true}); err != nil {
		t.Fatalf("applyBaldaLogging(): %v", err)
	}
	if got := zerolog.GlobalLevel(); got != zerolog.TraceLevel {
		t.Fatalf("GlobalLevel() = %v, want %v", got, zerolog.TraceLevel)
	}
}

func setBaldaLogFlagsForTest(t *testing.T, debugFlag, traceFlag bool) func() {
	t.Helper()
	prevDebug, prevTrace := debug, trace
	debug, trace = debugFlag, traceFlag
	return func() {
		debug, trace = prevDebug, prevTrace
	}
}
