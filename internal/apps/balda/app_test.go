package balda

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/normahq/balda/internal/apps/balda/handlers"
	"github.com/normahq/balda/internal/apps/balda/paths"
	"github.com/normahq/balda/internal/apps/balda/swarm"
	"github.com/normahq/balda/internal/git"
	"github.com/normahq/norma/pkg/runtime/agentconfig"
	runtimeconfig "github.com/normahq/norma/pkg/runtime/appconfig"
)

const (
	testWorkspaceBaseBranchSourceConfig = "config"
	testWorkspaceBaseBranchSourceHead   = "head"
)

var (
	_ = os.Getwd
	_ = filepath.Clean
	_ = paths.ConfigPath
)

func TestIsExpectedBotRunShutdown(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "context canceled",
			err:  context.Canceled,
			want: true,
		},
		{
			name: "wrapped context canceled",
			err:  fmt.Errorf("shutdown: %w", context.Canceled),
			want: true,
		},
		{
			name: "other error",
			err:  context.DeadlineExceeded,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isExpectedBotRunShutdown(tt.err); got != tt.want {
				t.Fatalf("isExpectedBotRunShutdown(%v) = %t, want %t", tt.err, got, tt.want)
			}
		})
	}
}

func TestOpenBaldaStateProviderUsesStateDB(t *testing.T) {
	stateDir := t.TempDir()

	provider, err := openBaldaStateProvider(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("openBaldaStateProvider() error = %v", err)
	}
	defer func() { _ = provider.Close() }()

	if _, err := os.Stat(paths.StateDBPath(stateDir)); err != nil {
		t.Fatalf("stat state db: %v", err)
	}
	if _, err := os.Stat(filepath.Join(stateDir, "balda.db")); !os.IsNotExist(err) {
		t.Fatalf("old balda.db stat error = %v, want not exist", err)
	}
}

func TestOpenBaldaStateProviderIgnoresOldDBPath(t *testing.T) {
	stateDir := t.TempDir()
	oldPath := filepath.Join(stateDir, "balda.db")
	if err := os.WriteFile(oldPath, []byte("legacy"), 0o600); err != nil {
		t.Fatalf("write old db path: %v", err)
	}

	provider, err := openBaldaStateProvider(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("openBaldaStateProvider() error = %v", err)
	}
	defer func() { _ = provider.Close() }()

	if _, err := os.Stat(paths.StateDBPath(stateDir)); err != nil {
		t.Fatalf("stat state db: %v", err)
	}
	content, err := os.ReadFile(oldPath)
	if err != nil {
		t.Fatalf("read old db path: %v", err)
	}
	if string(content) != "legacy" {
		t.Fatalf("old db path content = %q, want unchanged", string(content))
	}
}

func TestValidateBaldaMCPConfiguration_RejectsUnsupportedBuiltInServerReferences(t *testing.T) {
	cfg := Config{
		Balda: BaldaConfig{
			MCPServers: []string{"balda.config"},
		},
	}
	normaCfg := runtimeconfig.RuntimeConfig{
		Providers: map[string]agentconfig.Config{
			"root": {MCPServers: []string{"balda.workspace"}},
		},
	}

	err := validateBaldaMCPConfiguration(cfg, normaCfg, "/tmp/work/.config/balda/config.yaml")
	if err == nil {
		t.Fatal("validateBaldaMCPConfiguration() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), `balda.mcp_servers[0] references unsupported built-in config MCP server "balda.config"; edit the balda config file directly at "/tmp/work/.config/balda/config.yaml"`) {
		t.Fatalf("unexpected balda.mcp_servers validation error: %v", err)
	}
	if !strings.Contains(err.Error(), `runtime.providers.root.mcp_servers[0] references unsupported built-in MCP server "balda.workspace"; use "balda"`) {
		t.Fatalf("unexpected runtime.providers validation error: %v", err)
	}
}

func TestValidateBaldaMCPConfiguration_RejectsReservedCustomServerIDs(t *testing.T) {
	normaCfg := runtimeconfig.RuntimeConfig{
		Providers: map[string]agentconfig.Config{
			"root": {},
		},
		MCPServers: map[string]agentconfig.MCPServerConfig{
			"balda":          {Type: agentconfig.MCPServerTypeHTTP, URL: "http://example.com/mcp"},
			"runtime.config": {Type: agentconfig.MCPServerTypeHTTP, URL: "http://example.com/state"},
		},
	}

	err := validateBaldaMCPConfiguration(Config{}, normaCfg, "/tmp/work/.config/balda/config.yaml")
	if err == nil {
		t.Fatal("validateBaldaMCPConfiguration() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "runtime.mcp_servers.balda is reserved for the built-in balda MCP server") {
		t.Fatalf("missing reserved balda error: %v", err)
	}
	if !strings.Contains(err.Error(), `runtime.mcp_servers.runtime.config conflicts with unsupported built-in config MCP server ID "runtime.config"; edit the balda config file directly at "/tmp/work/.config/balda/config.yaml"`) {
		t.Fatalf("missing unsupported built-in server conflict error: %v", err)
	}
}

func TestValidateTelegramFormattingMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "default empty", in: "", want: "markdownv2"},
		{name: "trimmed markdownv2", in: "  MARKDOWNV2 ", want: "markdownv2"},
		{name: "html", in: "html", want: "html"},
		{name: "none", in: "none", want: "none"},
		{name: "invalid", in: "markdown", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := validateTelegramFormattingMode(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("validateTelegramFormattingMode(%q) error = nil, want non-nil", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateTelegramFormattingMode(%q) error = %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("validateTelegramFormattingMode(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestValidateSessionPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "default empty", in: "", want: sessionPersistenceSQLite},
		{name: "trimmed memory", in: "  MEMORY ", want: sessionPersistenceMemory},
		{name: "sqlite", in: "sqlite", want: sessionPersistenceSQLite},
		{name: "invalid", in: "gorm", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := validateSessionPersistence(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("validateSessionPersistence(%q) error = nil, want non-nil", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateSessionPersistence(%q) error = %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("validateSessionPersistence(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestValidateUnsupportedRuntimeConfig(t *testing.T) {
	t.Parallel()

	err := validateRemovedRuntimeConfig(BaldaConfig{
		RemovedEventBus: map[string]any{"mode": "sqlite"},
		Swarm:           SwarmConfig{RemovedMode: "shadow"},
		Webhooks:        WebhooksConfig{RemovedMode: "mailbox"},
		Scheduler:       SchedulerConfig{RemovedMode: "mailbox"},
	})
	if err == nil {
		t.Fatal("validateRemovedRuntimeConfig() error = nil, want unsupported-config error")
	}
	want := []string{
		"balda.event_bus is no longer supported",
		"balda.swarm.mode is no longer supported",
		"balda.webhooks.mode is no longer supported",
		"balda.scheduler.mode is no longer supported",
	}
	for _, marker := range want {
		if !strings.Contains(err.Error(), marker) {
			t.Fatalf("validateRemovedRuntimeConfig() error = %q, want marker %q", err.Error(), marker)
		}
	}
	if strings.Contains(err.Error(), "legacy mode configuration") {
		t.Fatalf("validateRemovedRuntimeConfig() error = %q, want current unsupported-config wording", err.Error())
	}
	if strings.Contains(err.Error(), "invalid removed runtime configuration") {
		t.Fatalf("validateRemovedRuntimeConfig() error = %q, still contains removed-runtime summary wording", err.Error())
	}
	if strings.Contains(err.Error(), "configure balda.nats for JetStream") {
		t.Fatalf("validateRemovedRuntimeConfig() error = %q, still contains transport-specific event_bus guidance", err.Error())
	}
	if !strings.Contains(err.Error(), "invalid unsupported runtime configuration:") {
		t.Fatalf("validateRemovedRuntimeConfig() error = %q, want unsupported-runtime summary wording", err.Error())
	}
	if !strings.Contains(err.Error(), "balda.event_bus is no longer supported; use balda.nats built-in runtime settings") {
		t.Fatalf("validateRemovedRuntimeConfig() error = %q, want simplified event_bus guidance", err.Error())
	}
}

func TestValidateUnsupportedRuntimeConfig_AllowsCurrentConfig(t *testing.T) {
	t.Parallel()

	if err := validateRemovedRuntimeConfig(BaldaConfig{}); err != nil {
		t.Fatalf("validateRemovedRuntimeConfig() error = %v, want nil", err)
	}
}

func TestBuildScheduledTaskSchedulerConfig(t *testing.T) {
	t.Parallel()

	cfg := BaldaConfig{
		Scheduler: SchedulerConfig{
			Tasks: []ScheduledTaskConfig{
				{
					ID:   " nightly ",
					Cron: " */15 * * * * ",
					Envelope: ScheduledTaskEnvelopeConfig{
						Target:  " alias ",
						Key:     " owner ",
						Content: " summarize ",
						ReportTo: &ScheduledTaskEnvelopeTargetConfig{
							Target: " alias ",
							Key:    " owner ",
						},
					},
				},
			},
		},
	}

	got := buildScheduledTaskSchedulerConfig(cfg)
	want := handlers.ScheduledTaskSchedulerConfig{
		Tasks: []handlers.ConfiguredScheduledTask{
			{
				ID:      "nightly",
				Cron:    "*/15 * * * *",
				Target:  "alias",
				Key:     "owner",
				Content: "summarize",
				ReportTo: &handlers.ConfiguredScheduledTaskTarget{
					Target: "alias",
					Key:    "owner",
				},
			},
		},
	}

	if len(got.Tasks) != len(want.Tasks) {
		t.Fatalf("len(tasks) = %d, want %d", len(got.Tasks), len(want.Tasks))
	}
	if !reflect.DeepEqual(got.Tasks[0], want.Tasks[0]) {
		t.Fatalf("task mismatch: got %+v want %+v", got.Tasks[0], want.Tasks[0])
	}
}

func TestValidateSchedulerConfigRejectsUnsupportedJobsKey(t *testing.T) {
	t.Parallel()

	err := validateSchedulerConfig(SchedulerConfig{
		RemovedJobs: []any{map[string]any{"id": "legacy"}},
	})
	if err == nil {
		t.Fatal("validateSchedulerConfig() error = nil, want unsupported jobs key error")
	}
	if !strings.Contains(err.Error(), "balda.scheduler.jobs is no longer supported") {
		t.Fatalf("validateSchedulerConfig() error = %v, want unsupported jobs key guidance", err)
	}
}

func TestValidateRuntimeConfigLint_AllowsAlwaysOnSwarmConfig(t *testing.T) {
	t.Parallel()

	if err := validateRuntimeConfigLint(swarm.Config{
		Commands: swarm.CommandConfig{
			Stream:   "BALDA_COMMANDS",
			Consumer: "BALDA_WORKER_COMMANDS",
		},
		Events: swarm.EventStreamConfig{Stream: "BALDA_EVENTS"},
		DLQ:    swarm.DLQConfig{Stream: "BALDA_DLQ"},
	}, handlers.InboundWebhookConfig{}); err != nil {
		t.Fatalf("validateRuntimeConfigLint() error = %v, want nil", err)
	}
}

func TestValidateUnsupportedRuntimeConfig_AvoidsTransportSpecificModeGuidance(t *testing.T) {
	t.Parallel()

	err := validateRemovedRuntimeConfig(BaldaConfig{
		Webhooks:  WebhooksConfig{RemovedMode: true},
		Scheduler: SchedulerConfig{RemovedMode: true},
	})
	if err == nil {
		t.Fatal("validateRemovedRuntimeConfig() error = nil, want unsupported mode guidance")
	}
	got := err.Error()
	for _, needle := range []string{
		"webhooks publish JetStream commands only",
		"scheduler publishes JetStream commands only",
	} {
		if strings.Contains(got, needle) {
			t.Fatalf("validateRemovedRuntimeConfig() error = %q, still contains transport-specific guidance %q", got, needle)
		}
	}
	for _, needle := range []string{
		"balda.webhooks.mode is no longer supported; webhooks use the always-on runtime",
		"balda.scheduler.mode is no longer supported; scheduling uses the always-on runtime",
	} {
		if !strings.Contains(got, needle) {
			t.Fatalf("validateRemovedRuntimeConfig() error = %q, want marker %q", got, needle)
		}
	}
}

func TestValidateRuntimeConfigLint_RejectsInvalidAndDuplicateJetStreamNames(t *testing.T) {
	t.Parallel()

	err := validateRuntimeConfigLint(swarm.Config{
		Commands: swarm.CommandConfig{
			Stream:   "BALDA COMMANDS",
			Consumer: "BALDA_EVENT_PROJECTOR",
		},
		Events: swarm.EventStreamConfig{Stream: "BALDA_EVENTS"},
		DLQ:    swarm.DLQConfig{Stream: "BALDA_EVENTS"},
	}, handlers.InboundWebhookConfig{})
	if err == nil {
		t.Fatal("validateRuntimeConfigLint() error = nil, want non-nil")
	}
	markers := []string{
		`balda.swarm.commands.stream must match "^[A-Za-z0-9_-]+$"`,
		"balda.swarm.commands.stream, balda.swarm.events.stream, and balda.swarm.dlq.stream must be distinct",
		"balda.swarm.commands.consumer must differ from balda.swarm.events.consumer",
	}
	for _, marker := range markers {
		if !strings.Contains(err.Error(), marker) {
			t.Fatalf("validateRuntimeConfigLint() error = %v, want marker %q", err, marker)
		}
	}
}

func TestValidateRuntimeConfigLint_RejectsPublicWebhookWithoutRouteAuth(t *testing.T) {
	t.Parallel()

	err := validateRuntimeConfigLint(swarm.Config{
		Commands: swarm.CommandConfig{
			Stream:   "BALDA_COMMANDS",
			Consumer: "BALDA_WORKER_COMMANDS",
		},
		Events: swarm.EventStreamConfig{Stream: "BALDA_EVENTS"},
		DLQ:    swarm.DLQConfig{Stream: "BALDA_DLQ"},
	}, handlers.InboundWebhookConfig{
		Enabled:    true,
		ListenAddr: "0.0.0.0:8090",
		Routes: map[string]handlers.InboundWebhookRouteConfig{
			"release": {
				Auth: handlers.InboundWebhookRouteAuthConfig{Type: "none"},
			},
		},
	})
	if err == nil {
		t.Fatal("validateRuntimeConfigLint() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "must configure auth.type=header") {
		t.Fatalf("validateRuntimeConfigLint() error = %v, want auth marker", err)
	}
}

func TestValidateRuntimeConfigLint_AllowsLoopbackWebhookWithoutRouteAuth(t *testing.T) {
	t.Parallel()

	err := validateRuntimeConfigLint(swarm.Config{
		Commands: swarm.CommandConfig{
			Stream:   "BALDA_COMMANDS",
			Consumer: "BALDA_WORKER_COMMANDS",
		},
		Events: swarm.EventStreamConfig{Stream: "BALDA_EVENTS"},
		DLQ:    swarm.DLQConfig{Stream: "BALDA_DLQ"},
	}, handlers.InboundWebhookConfig{
		Enabled:    true,
		ListenAddr: "127.0.0.1:8090",
		Routes: map[string]handlers.InboundWebhookRouteConfig{
			"release": {
				Auth: handlers.InboundWebhookRouteAuthConfig{Type: "none"},
			},
		},
	})
	if err != nil {
		t.Fatalf("validateRuntimeConfigLint() error = %v, want nil", err)
	}
}

func TestBuildInboundWebhookConfig(t *testing.T) {
	t.Parallel()

	cfg := BaldaConfig{
		Webhooks: WebhooksConfig{
			Enabled:    true,
			ListenAddr: " 127.0.0.1:8091 ",
			Routes: map[string]WebhookRouteConfig{
				" webhook1 ": {
					Path:           " webhook1 ",
					PromptTemplate: " {{.RawBody}} ",
					Envelope: WebhookRouteEnvelopeConfig{
						Target: " alias ",
						Key:    " owner ",
						Mode:   " task ",
						ReportTo: &WebhookRouteEnvelopeTargetConfig{
							Target: " alias ",
							Key:    " owner ",
						},
					},
					Auth: WebhookRouteAuthConfig{
						Type:   " header ",
						Header: " X-Test-Auth ",
						Value:  " s3cr3t ",
					},
					Dedupe: WebhookRouteDedupeConfig{
						Source: " header ",
						Header: " X-Event-ID ",
					},
				},
			},
		},
	}

	got := buildInboundWebhookConfig(cfg)
	want := handlers.InboundWebhookConfig{
		Enabled:    true,
		ListenAddr: "127.0.0.1:8091",
		Routes: map[string]handlers.InboundWebhookRouteConfig{
			"webhook1": {
				Path:           "webhook1",
				PromptTemplate: "{{.RawBody}}",
				Envelope: handlers.InboundWebhookRouteEnvelopeConfig{
					Target: "alias",
					Key:    "owner",
					Mode:   "task",
					ReportTo: &handlers.InboundWebhookRouteTargetConfig{
						Target: "alias",
						Key:    "owner",
					},
				},
				Auth: handlers.InboundWebhookRouteAuthConfig{
					Type:   "header",
					Header: "X-Test-Auth",
					Value:  "s3cr3t",
				},
				Dedupe: handlers.InboundWebhookRouteDedupeConfig{
					Source: "header",
					Header: "X-Event-ID",
				},
			},
		},
	}

	if got.Enabled != want.Enabled {
		t.Fatalf("Enabled = %t, want %t", got.Enabled, want.Enabled)
	}
	if got.ListenAddr != want.ListenAddr {
		t.Fatalf("ListenAddr = %q, want %q", got.ListenAddr, want.ListenAddr)
	}
	if len(got.Routes) != len(want.Routes) {
		t.Fatalf("len(routes) = %d, want %d", len(got.Routes), len(want.Routes))
	}
	if !reflect.DeepEqual(got.Routes["webhook1"], want.Routes["webhook1"]) {
		t.Fatalf("route mismatch: got %+v want %+v", got.Routes["webhook1"], want.Routes["webhook1"])
	}
}

func TestBuildInboundWebhookConfig_UsesSecretEnvAuthValue(t *testing.T) {
	t.Setenv("BALDA_WEBHOOK_AUTH_VALUE", "from-env")
	cfg := BaldaConfig{
		Webhooks: WebhooksConfig{
			Enabled: true,
			Routes: map[string]WebhookRouteConfig{
				"webhook1": {
					Path:           "/webhook1",
					PromptTemplate: "{{.RawBody}}",
					Auth: WebhookRouteAuthConfig{
						Type:      "header",
						Header:    "X-Auth",
						SecretEnv: "BALDA_WEBHOOK_AUTH_VALUE",
					},
				},
			},
		},
	}

	got := buildInboundWebhookConfig(cfg)
	route := got.Routes["webhook1"]
	if route.Auth.Value != "from-env" {
		t.Fatalf("auth value = %q, want from-env", route.Auth.Value)
	}
}

func TestResolveWorkspaceBaseBranch_ConfigPreferredWhenValid(t *testing.T) {
	ctx := context.Background()
	repoDir := t.TempDir()
	initGitRepoForBalda(t, ctx, repoDir)

	runGitForBalda(t, ctx, repoDir, "branch", "main")

	branch, source, err := resolveWorkspaceBaseBranch(ctx, repoDir, "main", true)
	if err != nil {
		t.Fatalf("resolveWorkspaceBaseBranch returned error: %v", err)
	}
	if branch != "main" {
		t.Fatalf("branch = %q, want main", branch)
	}
	if source != testWorkspaceBaseBranchSourceConfig {
		t.Fatalf("source = %q, want %s", source, testWorkspaceBaseBranchSourceConfig)
	}
}

func TestResolveWorkspaceBaseBranch_FallbackToHeadWhenConfiguredMissing(t *testing.T) {
	ctx := context.Background()
	repoDir := t.TempDir()
	initGitRepoForBalda(t, ctx, repoDir)
	runGitForBalda(t, ctx, repoDir, "checkout", "-b", "trunk")

	branch, source, err := resolveWorkspaceBaseBranch(ctx, repoDir, "missing-branch", true)
	if err != nil {
		t.Fatalf("resolveWorkspaceBaseBranch returned error: %v", err)
	}
	if branch != "trunk" {
		t.Fatalf("branch = %q, want trunk", branch)
	}
	if source != testWorkspaceBaseBranchSourceHead {
		t.Fatalf("source = %q, want %s", source, testWorkspaceBaseBranchSourceHead)
	}
}

func TestResolveWorkspaceBaseBranch_EnabledRequiresResolvableBranch(t *testing.T) {
	ctx := context.Background()
	workingDir := t.TempDir()

	if _, _, err := resolveWorkspaceBaseBranch(ctx, workingDir, "", true); err == nil {
		t.Fatal("resolveWorkspaceBaseBranch returned nil error for non-git workspace-enabled config")
	}
}

func TestResolveWorkspaceEnabledForApp_AutoDisablesWhenBaseBranchUnresolvable(t *testing.T) {
	ctx := context.Background()
	repoDir := t.TempDir()
	runGitForBalda(t, ctx, repoDir, "init")

	mode, enabled, err := resolveWorkspaceEnabledForApp(ctx, string(WorkspaceModeAuto), repoDir, "", git.Available)
	if err != nil {
		t.Fatalf("resolveWorkspaceEnabledForApp returned error: %v", err)
	}
	if mode != WorkspaceModeAuto {
		t.Fatalf("mode = %q, want %q", mode, WorkspaceModeAuto)
	}
	if enabled {
		t.Fatal("enabled = true, want false for unborn HEAD in auto mode")
	}
}

func TestResolveWorkspaceEnabledForApp_OnRemainsEnabledForGitRepo(t *testing.T) {
	ctx := context.Background()
	repoDir := t.TempDir()
	runGitForBalda(t, ctx, repoDir, "init")

	mode, enabled, err := resolveWorkspaceEnabledForApp(ctx, string(WorkspaceModeOn), repoDir, "", git.Available)
	if err != nil {
		t.Fatalf("resolveWorkspaceEnabledForApp returned error: %v", err)
	}
	if mode != WorkspaceModeOn {
		t.Fatalf("mode = %q, want %q", mode, WorkspaceModeOn)
	}
	if !enabled {
		t.Fatal("enabled = false, want true")
	}
}

func TestResolveWorkspaceSessionsDir_DefaultWhenEmpty(t *testing.T) {
	got, err := resolveWorkspaceSessionsDir("")
	if err != nil {
		t.Fatalf("resolveWorkspaceSessionsDir() error = %v", err)
	}
	if got != "sessions" {
		t.Fatalf("resolveWorkspaceSessionsDir() = %q, want sessions", got)
	}
}

func TestResolveWorkspaceSessionsDir_RejectsInvalidPath(t *testing.T) {
	if _, err := resolveWorkspaceSessionsDir("custom/sessions"); err == nil {
		t.Fatal("resolveWorkspaceSessionsDir() error = nil, want non-nil")
	}
}

func TestResolveWorkspaceSessionsDir_AcceptsIdentifier(t *testing.T) {
	got, err := resolveWorkspaceSessionsDir("balda_sessions")
	if err != nil {
		t.Fatalf("resolveWorkspaceSessionsDir() error = %v", err)
	}
	if got != "balda_sessions" {
		t.Fatalf("resolveWorkspaceSessionsDir() = %q, want balda_sessions", got)
	}
}

func initGitRepoForBalda(t *testing.T, ctx context.Context, dir string) {
	t.Helper()
	runGitForBalda(t, ctx, dir, "init")
	runGitForBalda(t, ctx, dir, "config", "user.name", "Norma Test")
	runGitForBalda(t, ctx, dir, "config", "user.email", "norma-test@example.com")
	if err := os.WriteFile(filepath.Join(dir, "seed.txt"), []byte("seed\n"), 0o600); err != nil {
		t.Fatalf("write seed file: %v", err)
	}
	runGitForBalda(t, ctx, dir, "add", "seed.txt")
	runGitForBalda(t, ctx, dir, "commit", "-m", "chore: seed")
}

func runGitForBalda(t *testing.T, ctx context.Context, dir string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}
