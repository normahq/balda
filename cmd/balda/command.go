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
	"github.com/normahq/balda/internal/apps/balda/shutdown"
	"github.com/normahq/norma/pkg/runtime/appconfig"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

//go:embed balda.yaml
var defaultBaldaConfig []byte

const shutdownTimeout = 10 * time.Second

type baldaConfigDocument struct {
	Runtime appconfig.RuntimeConfig `mapstructure:"runtime"`
	Balda   balda.BaldaConfig       `mapstructure:"balda"`
}

func startCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "start",
		Short:        "Start Telegram Balda bot",
		Long:         "Start the Telegram Balda bot server.",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			workingDir, err := os.Getwd()
			if err != nil {
				return fmt.Errorf("getting working directory: %w", err)
			}

			var doc baldaConfigDocument
			_, err = appconfig.LoadConfigDocument(
				appconfig.RuntimeLoadOptions{
					WorkingDir: workingDir,
					ConfigDir:  viper.GetString("config_dir"),
					Profile:    viper.GetString("profile"),
				},
				appconfig.AppLoadOptions{
					AppName:            "balda",
					DefaultsYAML:       defaultBaldaConfig,
					UseDotConfigAppDir: true,
				},
				&doc,
			)
			if err != nil {
				return err
			}
			if err := applyBaldaLogging(doc.Balda.Logger); err != nil {
				return fmt.Errorf("configure balda logging: %w", err)
			}

			baldaCfg := balda.Config{Balda: doc.Balda}

			if baldaCfg.Balda.Telegram.Token == "" {
				return fmt.Errorf("telegram token is required\nSet it via:\n  - Environment: BALDA_TELEGRAM_TOKEN=<token>\n  - CWD .env: %s with BALDA_TELEGRAM_TOKEN=<token>\n  - App config: balda.telegram.token in .config/balda/config.yaml\n  - Profile override: profiles.<name>.balda.telegram.token in the same file", filepath.Join(workingDir, ".env"))
			}

			stateDir, err := resolveBaldaStateDir(workingDir, baldaCfg.Balda.StateDir)
			if err != nil {
				return fmt.Errorf("resolve balda state_dir: %w", err)
			}
			if err := os.MkdirAll(stateDir, 0o700); err != nil {
				return fmt.Errorf("create balda state dir: %w", err)
			}

			dbPath := baldaStateDBPath(stateDir)
			ownerToken, err := loadOrCreateBaldaOwnerToken(context.Background(), dbPath)
			if err != nil {
				return fmt.Errorf("bootstrap balda owner token: %w", err)
			}

			runtimeLoadOpts := appconfig.RuntimeLoadOptions{
				WorkingDir: workingDir,
				ConfigDir:  viper.GetString("config_dir"),
				Profile:    viper.GetString("profile"),
			}

			app := balda.App(baldaCfg, doc.Runtime, ownerToken, runtimeLoadOpts, defaultBaldaConfig)

			ctx, cancel := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			if err := app.Start(ctx); err != nil {
				return fmt.Errorf("starting Balda app: %w", err)
			}

			logBaldaStartup(ctx, baldaCfg.Balda.Telegram.Token)

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
