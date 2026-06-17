package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	baldaapp "github.com/normahq/balda/internal/apps/balda"
	"github.com/normahq/norma/pkg/runtime/appconfig"
	"github.com/spf13/cobra"
)

type baldaTestConfigDocument struct {
	Runtime appconfig.RuntimeConfig `mapstructure:"runtime"`
	Balda   baldaapp.BaldaConfig    `mapstructure:"balda"`
}

const testBaldaDefaultProfile = "default"

func TestLoadConfigDocument_AppliesProfileBaldaOverrides(t *testing.T) {
	workingDir := t.TempDir()
	t.Setenv("BALDA_TELEGRAM_WEBHOOK_ENABLED", "true")
	t.Setenv("BALDA_TELEGRAM_PLAN_UPDATES", "true")
	t.Setenv("BALDA_MEMORY_ENABLED", "false")

	if err := writeFile(filepath.Join(workingDir, ".config", "balda", "config.yaml"), `runtime:
  providers:
    balda_agent:
      type: opencode_acp
      opencode_acp:
        model: opencode/big-pickle
profiles:
  default:
    balda:
      provider: balda_agent
      global_instruction: from profile
balda:
  telegram:
    plan_updates: false
`); err != nil {
		t.Fatalf("write balda config: %v", err)
	}

	var doc baldaTestConfigDocument
	selectedProfile, err := appconfig.LoadConfigDocument(
		appconfig.RuntimeLoadOptions{WorkingDir: workingDir, Profile: testBaldaDefaultProfile},
		appconfig.AppLoadOptions{
			AppName:            "balda",
			DefaultsYAML:       defaultBaldaConfig,
			UseDotConfigAppDir: true,
		},
		&doc,
	)
	if err != nil {
		t.Fatalf("LoadConfigDocument: %v", err)
	}
	if selectedProfile != testBaldaDefaultProfile {
		t.Fatalf("profile = %q, want %s", selectedProfile, testBaldaDefaultProfile)
	}

	baldaCfg := baldaapp.Config{Balda: doc.Balda}

	if baldaCfg.Balda.Provider != "balda_agent" {
		t.Fatalf("provider = %q, want balda_agent", baldaCfg.Balda.Provider)
	}
	if baldaCfg.Balda.GlobalInstruction != "from profile" {
		t.Fatalf("global_instruction = %q, want from profile", baldaCfg.Balda.GlobalInstruction)
	}
	if !baldaCfg.Balda.Telegram.Webhook.Enabled {
		t.Fatal("webhook.enabled = false, want true from env override")
	}
	if !baldaCfg.Balda.Telegram.PlanUpdates {
		t.Fatal("plan_updates = false, want true from env override")
	}
	if baldaCfg.Balda.Memory.Enabled {
		t.Fatal("memory.enabled = true, want false from env override")
	}
}

func TestLoadConfigDocument_AppliesZulipEnvOverrides(t *testing.T) {
	workingDir := t.TempDir()
	t.Setenv("BALDA_ZULIP_BOT_EMAIL", "bot@example.com")
	t.Setenv("BALDA_ZULIP_API_KEY", "zulip-api-key")
	t.Setenv("BALDA_ZULIP_SERVER_URL", "https://zulip.example.com")
	t.Setenv("BALDA_ZULIP_WEBHOOK_TOKEN", "zulip-webhook-token")
	t.Setenv("BALDA_ZULIP_ALLOWED_OWNERS", "owner@example.com,second@example.com")
	t.Setenv("BALDA_ZULIP_WEBHOOK_ENABLED", "true")
	t.Setenv("BALDA_ZULIP_WEBHOOK_LISTEN_ADDR", "127.0.0.1:19090")
	t.Setenv("BALDA_ZULIP_WEBHOOK_PATH", "/custom/zulip")

	if err := writeFile(filepath.Join(workingDir, ".config", "balda", "config.yaml"), `runtime:
  providers:
    balda_agent:
      type: opencode_acp
      opencode_acp:
        model: opencode/big-pickle
balda:
  provider: balda_agent
`); err != nil {
		t.Fatalf("write balda config: %v", err)
	}

	var doc baldaTestConfigDocument
	_, err := appconfig.LoadConfigDocument(
		appconfig.RuntimeLoadOptions{WorkingDir: workingDir},
		appconfig.AppLoadOptions{
			AppName:            "balda",
			DefaultsYAML:       defaultBaldaConfig,
			UseDotConfigAppDir: true,
		},
		&doc,
	)
	if err != nil {
		t.Fatalf("LoadConfigDocument: %v", err)
	}

	if doc.Balda.Zulip.BotEmail != "bot@example.com" {
		t.Fatalf("zulip.bot_email = %q, want bot@example.com", doc.Balda.Zulip.BotEmail)
	}
	if doc.Balda.Zulip.APIKey != "zulip-api-key" {
		t.Fatalf("zulip.api_key = %q, want zulip-api-key", doc.Balda.Zulip.APIKey)
	}
	if doc.Balda.Zulip.ServerURL != "https://zulip.example.com" {
		t.Fatalf("zulip.server_url = %q, want https://zulip.example.com", doc.Balda.Zulip.ServerURL)
	}
	if doc.Balda.Zulip.WebhookToken != "zulip-webhook-token" {
		t.Fatalf("zulip.webhook_token = %q, want zulip-webhook-token", doc.Balda.Zulip.WebhookToken)
	}
	wantOwners := []string{"owner@example.com", "second@example.com"}
	if got := doc.Balda.Zulip.AllowedOwners; strings.Join(got, ",") != strings.Join(wantOwners, ",") {
		t.Fatalf("zulip.allowed_owners = %#v, want %#v", got, wantOwners)
	}
	if !doc.Balda.Zulip.Webhook.Enabled {
		t.Fatal("zulip.webhook.enabled = false, want true")
	}
	if doc.Balda.Zulip.Webhook.ListenAddr != "127.0.0.1:19090" {
		t.Fatalf("zulip.webhook.listen_addr = %q, want 127.0.0.1:19090", doc.Balda.Zulip.Webhook.ListenAddr)
	}
	if doc.Balda.Zulip.Webhook.Path != "/custom/zulip" {
		t.Fatalf("zulip.webhook.path = %q, want /custom/zulip", doc.Balda.Zulip.Webhook.Path)
	}
}

func TestLoadConfigDocument_ImplicitDefaultProfileDoesNotRequireProfilesDefault(t *testing.T) {
	workingDir := t.TempDir()

	if err := writeFile(filepath.Join(workingDir, ".config", "balda", "config.yaml"), `runtime:
  providers:
    balda_agent:
      type: opencode_acp
      opencode_acp:
        model: opencode/big-pickle
profiles:
  codex:
    balda:
      provider: codex
balda:
  provider: balda_agent
`); err != nil {
		t.Fatalf("write balda config: %v", err)
	}

	var doc baldaTestConfigDocument
	selectedProfile, err := appconfig.LoadConfigDocument(
		appconfig.RuntimeLoadOptions{WorkingDir: workingDir},
		appconfig.AppLoadOptions{
			AppName:            "balda",
			DefaultsYAML:       defaultBaldaConfig,
			UseDotConfigAppDir: true,
		},
		&doc,
	)
	if err != nil {
		t.Fatalf("LoadConfigDocument: %v", err)
	}
	if selectedProfile != testBaldaDefaultProfile {
		t.Fatalf("profile = %q, want %s", selectedProfile, testBaldaDefaultProfile)
	}
	if doc.Balda.Provider != "balda_agent" {
		t.Fatalf("provider = %q, want balda_agent", doc.Balda.Provider)
	}
	if !doc.Balda.Memory.Enabled {
		t.Fatal("memory.enabled = false, want true from defaults")
	}
	if doc.Balda.Sessions.Persistence != "sqlite" {
		t.Fatalf("sessions.persistence = %q, want sqlite from defaults", doc.Balda.Sessions.Persistence)
	}
	if !doc.Balda.NATS.Embedded {
		t.Fatal("nats.embedded = false, want true from defaults")
	}
	if doc.Balda.Swarm.Commands.Stream != "" {
		t.Fatalf("swarm.commands.stream = %q, want omitted from defaults YAML", doc.Balda.Swarm.Commands.Stream)
	}
	if doc.Balda.Swarm.Commands.Consumer != "" {
		t.Fatalf("swarm.commands.consumer = %q, want omitted from defaults YAML", doc.Balda.Swarm.Commands.Consumer)
	}
}

func TestLoadConfigDocument_LoadsCodexReasoningEffort(t *testing.T) {
	workingDir := t.TempDir()

	if err := writeFile(filepath.Join(workingDir, ".config", "balda", "config.yaml"), `runtime:
  providers:
    codex:
      type: codex_acp
      codex_acp:
        model: gpt-5-codex
        reasoning_effort: high
balda:
  provider: codex
`); err != nil {
		t.Fatalf("write balda config: %v", err)
	}

	var doc baldaTestConfigDocument
	_, err := appconfig.LoadConfigDocument(
		appconfig.RuntimeLoadOptions{WorkingDir: workingDir},
		appconfig.AppLoadOptions{
			AppName:            "balda",
			DefaultsYAML:       defaultBaldaConfig,
			UseDotConfigAppDir: true,
		},
		&doc,
	)
	if err != nil {
		t.Fatalf("LoadConfigDocument: %v", err)
	}

	providerCfg, ok := doc.Runtime.Providers["codex"]
	if !ok {
		t.Fatal("runtime.providers.codex missing after config load")
	}
	if providerCfg.CodexACP == nil {
		t.Fatal("runtime.providers.codex.codex_acp missing after config load")
	}
	if providerCfg.CodexACP.ReasoningEffort != "high" {
		t.Fatalf("runtime.providers.codex.codex_acp.reasoning_effort = %q, want high", providerCfg.CodexACP.ReasoningEffort)
	}
}

func TestDefaultBaldaConfig_DocumentsCurrentTemplateWording(t *testing.T) {
	body := string(defaultBaldaConfig)
	for _, want := range []string{
		"# /goal worker-validator loop iteration cap.",
		"# Required built-in command/event runtime.",
		"# Use the built-in `balda` server ID for Balda workspace/state tools.",
		"# Tasks are upserted by id on boot; missing ids are deleted from state.db.",
		"swarm: {}",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("defaultBaldaConfig missing current template wording %q", want)
		}
	}
}

func TestLoadConfigDocument_ExplicitMissingProfileFails(t *testing.T) {
	workingDir := t.TempDir()

	if err := writeFile(filepath.Join(workingDir, ".config", "balda", "config.yaml"), `runtime:
  providers:
    balda_agent:
      type: opencode_acp
      opencode_acp:
        model: opencode/big-pickle
profiles:
  codex:
    balda:
      provider: codex
balda:
  provider: balda_agent
`); err != nil {
		t.Fatalf("write balda config: %v", err)
	}

	var doc baldaTestConfigDocument
	_, err := appconfig.LoadConfigDocument(
		appconfig.RuntimeLoadOptions{WorkingDir: workingDir, Profile: testBaldaDefaultProfile},
		appconfig.AppLoadOptions{
			AppName:            "balda",
			DefaultsYAML:       defaultBaldaConfig,
			UseDotConfigAppDir: true,
		},
		&doc,
	)
	if err == nil {
		t.Fatal("expected error for missing explicit profile")
	}
	if got, want := err.Error(), `top-level profile "default" not found`; got != want {
		t.Fatalf("error = %q, want %q", got, want)
	}
}

func TestNewRootCommand_RegistersCommandsAndFlags(t *testing.T) {
	t.Setenv("GOOGLE_API_KEY", "test-google-api-key")

	cmd, err := newRootCommand()
	if err != nil {
		t.Fatalf("newRootCommand: %v", err)
	}

	if _, _, err := cmd.Find([]string{"start"}); err != nil {
		t.Fatalf("start command missing: %v", err)
	}
	if _, _, err := cmd.Find([]string{"serve"}); err == nil {
		t.Fatal("serve command must not be registered")
	}
	if _, _, err := cmd.Find([]string{"init"}); err != nil {
		t.Fatalf("init command missing: %v", err)
	}

	for _, name := range []string{"config-dir", "profile", "debug", "trace"} {
		if cmd.PersistentFlags().Lookup(name) == nil {
			t.Fatalf("missing persistent flag %q", name)
		}
	}
}

func TestNewRootCommand_VersionFlag(t *testing.T) {
	cmd, err := newRootCommand()
	if err != nil {
		t.Fatalf("newRootCommand: %v", err)
	}

	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--version"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute(--version): %v", err)
	}

	got := out.String()
	if !strings.HasPrefix(got, "balda ") {
		t.Fatalf("version output = %q, want balda prefix", got)
	}
	if !strings.Contains(got, "commit ") {
		t.Fatalf("version output = %q, want commit metadata", got)
	}
	if !strings.Contains(got, "built ") {
		t.Fatalf("version output = %q, want build date metadata", got)
	}
}

func TestStartCommandSilencesRuntimeErrors(t *testing.T) {
	cmd := startCommand()
	if !cmd.SilenceUsage {
		t.Fatal("startCommand().SilenceUsage = false, want true")
	}
	if !cmd.SilenceErrors {
		t.Fatal("startCommand().SilenceErrors = false, want true")
	}
}

func TestNormalizeExecuteError(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		if err := normalizeExecuteError(nil, nil); err != nil {
			t.Fatalf("normalizeExecuteError(nil, nil) = %v, want nil", err)
		}
	})

	t.Run("start command expected cancel", func(t *testing.T) {
		err := normalizeExecuteError(&cobra.Command{Use: "start"}, context.Canceled)
		if err != nil {
			t.Fatalf("normalizeExecuteError(start, context.Canceled) = %v, want nil", err)
		}
	})

	t.Run("start command wrapped expected cancel", func(t *testing.T) {
		input := errors.New("context canceled")
		err := normalizeExecuteError(&cobra.Command{Use: "start"}, input)
		if err != nil {
			t.Fatalf("normalizeExecuteError(start, wrapped expected cancel) = %v, want nil", err)
		}
	})

	t.Run("start command unexpected error", func(t *testing.T) {
		input := errors.New("boom")
		err := normalizeExecuteError(&cobra.Command{Use: "start"}, input)
		var got *unprintedCLIError
		if !errors.As(err, &got) {
			t.Fatalf("normalizeExecuteError(start, boom) = %T, want *unprintedCLIError", err)
		}
		if !errors.Is(err, input) {
			t.Fatalf("normalizeExecuteError(start, boom) = %v, want wrapped %v", err, input)
		}
	})

	t.Run("non-start command keeps error", func(t *testing.T) {
		input := errors.New("boom")
		err := normalizeExecuteError(&cobra.Command{Use: "init"}, input)
		if !errors.Is(err, input) {
			t.Fatalf("normalizeExecuteError(init, boom) = %v, want %v", err, input)
		}
		var got *unprintedCLIError
		if errors.As(err, &got) {
			t.Fatalf("normalizeExecuteError(init, boom) = %T, do not want *unprintedCLIError", err)
		}
	})
}

func writeFile(path, content string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(content), 0o600)
}
