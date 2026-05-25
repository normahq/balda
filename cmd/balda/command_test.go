package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	baldaapp "github.com/normahq/balda/internal/apps/balda"
	"github.com/normahq/norma/pkg/runtime/appconfig"
)

type baldaTestConfigDocument struct {
	Runtime appconfig.RuntimeConfig `mapstructure:"runtime"`
	Balda   baldaapp.BaldaConfig    `mapstructure:"balda"`
}

const (
	testBaldaDefaultProfile  = "default"
	testBaldaSwarmModeShadow = "shadow"
)

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
	if doc.Balda.Swarm.Mode != testBaldaSwarmModeShadow {
		t.Fatalf("swarm.mode = %q, want shadow from defaults", doc.Balda.Swarm.Mode)
	}
	if doc.Balda.Swarm.WebhookMode != testBaldaSwarmModeShadow {
		t.Fatalf("swarm.webhook_mode = %q, want shadow from defaults", doc.Balda.Swarm.WebhookMode)
	}
	if doc.Balda.Swarm.SchedulerMode != testBaldaSwarmModeShadow {
		t.Fatalf("swarm.scheduler_mode = %q, want shadow from defaults", doc.Balda.Swarm.SchedulerMode)
	}
	if doc.Balda.EventBus.Mode != "nats_core" {
		t.Fatalf("event_bus.mode = %q, want nats_core from defaults", doc.Balda.EventBus.Mode)
	}
	if !doc.Balda.EventBus.NATS.Embedded {
		t.Fatal("event_bus.nats.embedded = false, want true from defaults")
	}
	if doc.Balda.EventBus.NATS.StoreDir != ".balda/nats" {
		t.Fatalf("event_bus.nats.store_dir = %q, want .balda/nats from defaults", doc.Balda.EventBus.NATS.StoreDir)
	}
	if !doc.Balda.Swarm.Shadow.Enabled {
		t.Fatal("swarm.shadow.enabled = false, want true from defaults")
	}
	if got := doc.Balda.Swarm.Agents["planner"].Role; got != "Plan work and split into subtasks" {
		t.Fatalf("swarm.agents.planner.role = %q, want default planner role", got)
	}
	if got := strings.Join(doc.Balda.Swarm.Agents["executor"].Tools, ","); got != "workspace,shell,mcp" {
		t.Fatalf("swarm.agents.executor.tools = %q, want workspace,shell,mcp", got)
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

func TestStartCommandSilencesUsageForRuntimeErrors(t *testing.T) {
	cmd := startCommand()
	if !cmd.SilenceUsage {
		t.Fatal("startCommand().SilenceUsage = false, want true")
	}
}

func writeFile(path, content string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(content), 0o600)
}
