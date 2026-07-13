package main

import (
	"errors"
	"fmt"
	"io/fs"
	buildinfo "runtime/debug"
	"strings"

	"github.com/joho/godotenv"
	"github.com/normahq/balda/internal/apps/balda/shutdown"
	"github.com/normahq/balda/internal/logging"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	configDir string
	debug     bool
	trace     bool
	profile   string
	version   = "dev"
	commit    = "unknown"
	date      = "unknown"
	executeFn = Execute
)

// Execute runs the balda root command.
func Execute() error {
	cmd, err := newRootCommand()
	if err != nil {
		return err
	}
	executed, err := cmd.ExecuteC()
	return normalizeExecuteError(executed, err)
}

type unprintedCLIError struct {
	err error
}

func (e *unprintedCLIError) Error() string {
	if e == nil || e.err == nil {
		return ""
	}
	return e.err.Error()
}

func (e *unprintedCLIError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func normalizeExecuteError(cmd *cobra.Command, err error) error {
	if err == nil {
		return nil
	}
	if !isStartCommand(cmd) {
		return err
	}
	if shutdown.IsExpected(err) {
		return nil
	}
	return &unprintedCLIError{err: err}
}

func isStartCommand(cmd *cobra.Command) bool {
	if cmd == nil {
		return false
	}
	switch strings.TrimSpace(cmd.Name()) {
	case "start", "validate", "preflight", "doctor":
		return true
	default:
		return false
	}
}

func newRootCommand() (*cobra.Command, error) {
	cobra.OnInitialize(initDotEnv)

	resolvedVersion := strings.TrimSpace(version)
	if resolvedVersion == "" || resolvedVersion == "dev" {
		if info, ok := buildinfo.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
			resolvedVersion = info.Main.Version
		}
	}
	if resolvedVersion == "" {
		resolvedVersion = "dev"
	}

	resolvedCommit := strings.TrimSpace(commit)
	if resolvedCommit == "" {
		resolvedCommit = "unknown"
	}
	resolvedDate := strings.TrimSpace(date)
	if resolvedDate == "" {
		resolvedDate = "unknown"
	}

	cmd := &cobra.Command{
		Use:     "balda",
		Short:   "balda is a standalone Telegram control plane",
		Version: fmt.Sprintf("balda %s (commit %s, built %s)", resolvedVersion, resolvedCommit, resolvedDate),
		PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
			logLevel := logging.LevelInfo
			if debug {
				logLevel = logging.LevelDebug
			}
			if trace {
				logLevel = logging.LevelTrace
			}
			return logging.Init(logging.WithLevel(logLevel))
		},
	}
	cmd.SetVersionTemplate("{{.Version}}\n")

	cmd.PersistentFlags().StringVar(&configDir, "config-dir", "", "extra config root directory (highest priority)")
	cmd.PersistentFlags().BoolVar(&debug, "debug", false, "enable debug logging")
	cmd.PersistentFlags().BoolVar(&trace, "trace", false, "enable trace logging (overrides --debug)")
	cmd.PersistentFlags().StringVar(&profile, "profile", "", "config profile name")

	if err := viper.BindPFlag("config_dir", cmd.PersistentFlags().Lookup("config-dir")); err != nil {
		return nil, fmt.Errorf("bind config-dir flag: %w", err)
	}
	if err := viper.BindPFlag("profile", cmd.PersistentFlags().Lookup("profile")); err != nil {
		return nil, fmt.Errorf("bind profile flag: %w", err)
	}

	cmd.AddCommand(startCommand())
	cmd.AddCommand(validateCommand())
	cmd.AddCommand(preflightCommand())
	cmd.AddCommand(doctorCommand())
	cmd.AddCommand(initCommand())
	cmd.AddCommand(evalFixturesCommand())
	return cmd, nil
}

func initDotEnv() {
	if err := godotenv.Load(); err != nil && !errors.Is(err, fs.ErrNotExist) {
		log.Warn().Err(err).Msg("failed to load .env file")
	}
}
