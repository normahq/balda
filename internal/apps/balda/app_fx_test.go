package balda

import (
	"context"
	"testing"

	runtimeconfig "github.com/normahq/norma/pkg/runtime/appconfig"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func TestValidateApp(t *testing.T) {
	ctx := context.Background()
	workingDir := t.TempDir()
	runGitForBalda(t, ctx, workingDir, "init")

	cfg := Config{
		Balda: BaldaConfig{
			Telegram: TelegramConfig{
				Token: "test-token",
			},
			WorkingDir: workingDir,
			StateDir:   ".config/balda",
			Workspace: WorkspaceConfig{
				Mode: string(WorkspaceModeAuto),
			},
		},
	}

	err := fx.ValidateApp(
		Module(
			cfg,
			runtimeconfig.RuntimeConfig{},
			"test-owner-token",
			runtimeconfig.RuntimeLoadOptions{WorkingDir: workingDir},
			nil,
		),
	)

	require.NoError(t, err)
}

func TestValidateApp_InvalidTelegramFormattingModeFails(t *testing.T) {
	ctx := context.Background()
	workingDir := t.TempDir()
	runGitForBalda(t, ctx, workingDir, "init")

	cfg := Config{
		Balda: BaldaConfig{
			Telegram: TelegramConfig{
				Token:          "test-token",
				FormattingMode: "markdown",
			},
			WorkingDir: workingDir,
			StateDir:   ".config/balda",
			Workspace: WorkspaceConfig{
				Mode: string(WorkspaceModeAuto),
			},
		},
	}

	err := fx.ValidateApp(
		Module(
			cfg,
			runtimeconfig.RuntimeConfig{},
			"test-owner-token",
			runtimeconfig.RuntimeLoadOptions{WorkingDir: workingDir},
			nil,
		),
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid balda.telegram.formatting_mode")
}
