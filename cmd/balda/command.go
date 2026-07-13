package main

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/normahq/balda/internal/apps/balda"
	"github.com/normahq/balda/internal/apps/balda/paths"
	"github.com/normahq/balda/internal/apps/balda/shutdown"
	"github.com/normahq/runtime/v2/appconfig"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

//go:embed balda.yaml
var defaultBaldaConfig []byte

const shutdownTimeout = 10 * time.Second

type baldaConfigDocument struct {
	Runtime appconfig.RuntimeConfig `mapstructure:"runtime"`
	Balda   balda.BaldaConfig       `mapstructure:"balda"`
}

type preparedBaldaCommand struct {
	workingDir      string
	doc             baldaConfigDocument
	baldaCfg        balda.Config
	runtimeLoadOpts appconfig.RuntimeLoadOptions
	ownerToken      string
}

var (
	validateBaldaApplicationFn = validateBaldaApplication
	preflightBaldaRuntimeFn    = preflightBaldaRuntime
)

func startCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "start",
		Short:         "Start Telegram Balda bot",
		Long:          "Start the Telegram Balda bot server.",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			prepared, err := prepareBaldaCommand(cmd.Context())
			if err != nil {
				return err
			}

			app := balda.App(
				prepared.baldaCfg,
				prepared.doc.Runtime,
				prepared.ownerToken,
				prepared.runtimeLoadOpts,
				defaultBaldaConfig,
			)

			ctx, cancel := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			if err := app.Start(ctx); err != nil {
				return fmt.Errorf("starting Balda app: %w", err)
			}

			logBaldaStartup(ctx, prepared.baldaCfg.Balda.Telegram.Token)

			<-ctx.Done()
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer shutdownCancel()
			if err := app.Stop(shutdownCtx); err != nil {
				if shutdown.IsExpected(err) {
					return nil
				}
				return fmt.Errorf("stopping Balda app: %w", err)
			}

			return nil
		},
	}

	return cmd
}

func validateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "validate",
		Short:         "Validate Balda configuration",
		Long:          "Load and validate Balda configuration without starting the provider runtime.",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			prepared, err := prepareBaldaCommand(cmd.Context())
			if err != nil {
				return err
			}
			if err := validateBaldaApplicationFn(prepared); err != nil {
				return err
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "balda validate: ok")
			return nil
		},
	}
	return cmd
}

func preflightCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "preflight",
		Short:         "Validate Balda configuration and provider runtime readiness",
		Long:          "Load Balda configuration, validate app wiring, start the configured provider runtime, and stop it cleanly.",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			prepared, err := prepareBaldaCommand(cmd.Context())
			if err != nil {
				return err
			}
			if err := validateBaldaApplicationFn(prepared); err != nil {
				return err
			}
			if err := preflightBaldaRuntimeFn(cmd.Context(), prepared); err != nil {
				return err
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "balda preflight: ok")
			return nil
		},
	}
	return cmd
}

func doctorCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "doctor",
		Short:         "Run full Balda operator checks",
		Long:          "Load Balda configuration, validate app wiring, preflight the configured provider runtime, and report overall readiness.",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			prepared, err := prepareBaldaCommand(cmd.Context())
			if err != nil {
				return err
			}
			if err := validateBaldaApplicationFn(prepared); err != nil {
				return err
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "doctor: validate ok")
			if err := preflightBaldaRuntimeFn(cmd.Context(), prepared); err != nil {
				return err
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "doctor: preflight ok")
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "balda doctor: ok")
			return nil
		},
	}
	return cmd
}

func prepareBaldaCommand(ctx context.Context) (preparedBaldaCommand, error) {
	workingDir, err := os.Getwd()
	if err != nil {
		return preparedBaldaCommand{}, fmt.Errorf("getting working directory: %w", err)
	}

	runtimeLoadOpts := appconfig.RuntimeLoadOptions{
		WorkingDir: workingDir,
		ConfigDir:  viper.GetString("config_dir"),
		Profile:    viper.GetString("profile"),
	}

	var doc baldaConfigDocument
	_, err = appconfig.LoadConfigDocument(
		runtimeLoadOpts,
		appconfig.AppLoadOptions{
			AppName:            "balda",
			DefaultsYAML:       defaultBaldaConfig,
			UseDotConfigAppDir: true,
		},
		&doc,
	)
	if err != nil {
		return preparedBaldaCommand{}, err
	}
	if err := applyBaldaLogging(doc.Balda.Logger); err != nil {
		return preparedBaldaCommand{}, fmt.Errorf("configure balda logging: %w", err)
	}

	baldaCfg := balda.Config{Balda: doc.Balda}
	if err := validateBaldaChannelConfiguration(workingDir, baldaCfg); err != nil {
		return preparedBaldaCommand{}, err
	}

	stateDir, err := paths.ResolveStateDir(workingDir, baldaCfg.Balda.StateDir)
	if err != nil {
		return preparedBaldaCommand{}, fmt.Errorf("resolve balda state_dir: %w", err)
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return preparedBaldaCommand{}, fmt.Errorf("create balda state dir: %w", err)
	}

	dbPath := paths.StateDBPath(stateDir)
	ownerToken, err := loadOrCreateBaldaOwnerToken(ctx, dbPath)
	if err != nil {
		return preparedBaldaCommand{}, fmt.Errorf("bootstrap balda owner token: %w", err)
	}

	return preparedBaldaCommand{
		workingDir:      workingDir,
		doc:             doc,
		baldaCfg:        baldaCfg,
		runtimeLoadOpts: runtimeLoadOpts,
		ownerToken:      ownerToken,
	}, nil
}

func validateBaldaChannelConfiguration(workingDir string, cfg balda.Config) error {
	if cfg.Balda.Telegram.Token == "" && !cfg.Balda.Zulip.Webhook.Enabled && !cfg.Balda.Slack.Enabled {
		return fmt.Errorf("at least one channel is required.\nFor Telegram:\n  - Environment: BALDA_TELEGRAM_TOKEN=<token>\n  - CWD .env: %s with BALDA_TELEGRAM_TOKEN=<token>\nFor Zulip: set balda.zulip.webhook.enabled=true (or BALDA_ZULIP_WEBHOOK_ENABLED=true)\nFor Slack: set balda.slack.enabled=true (or BALDA_SLACK_ENABLED=true)", filepath.Join(workingDir, ".env"))
	}
	return nil
}

func validateBaldaApplication(prepared preparedBaldaCommand) error {
	if err := fx.ValidateApp(
		balda.Module(
			prepared.baldaCfg,
			prepared.doc.Runtime,
			prepared.ownerToken,
			prepared.runtimeLoadOpts,
			defaultBaldaConfig,
		),
	); err != nil {
		return fmt.Errorf("validate Balda app: %w", err)
	}
	return nil
}

func preflightBaldaRuntime(ctx context.Context, prepared preparedBaldaCommand) error {
	if err := balda.PreflightRuntime(ctx, prepared.baldaCfg, prepared.doc.Runtime, prepared.runtimeLoadOpts); err != nil {
		return fmt.Errorf("preflight Balda runtime: %w", err)
	}
	return nil
}
