package balda

import (
	"context"
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
}

func TestValidateBaldaMCPConfiguration_AllowsCurrentBuiltInServerUsage(t *testing.T) {
	normaCfg := runtimeconfig.RuntimeConfig{
		Providers: map[string]agentconfig.Config{
			"root": {MCPServers: []string{"balda"}},
		},
		MCPServers: map[string]agentconfig.MCPServerConfig{
			"custom": {Type: agentconfig.MCPServerTypeHTTP, URL: "http://example.com/mcp"},
		},
	}

	if err := validateBaldaMCPConfiguration(normaCfg); err != nil {
		t.Fatalf("validateBaldaMCPConfiguration() error = %v, want nil", err)
	}
}

func TestValidateBaldaMCPConfiguration_RejectsReservedCustomServerIDs(t *testing.T) {
	normaCfg := runtimeconfig.RuntimeConfig{
		Providers: map[string]agentconfig.Config{
			"root": {},
		},
		MCPServers: map[string]agentconfig.MCPServerConfig{
			"balda": {Type: agentconfig.MCPServerTypeHTTP, URL: "http://example.com/mcp"},
		},
	}

	err := validateBaldaMCPConfiguration(normaCfg)
	if err == nil {
		t.Fatal("validateBaldaMCPConfiguration() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "runtime.mcp_servers.balda is reserved for the built-in balda MCP server") {
		t.Fatalf("missing reserved balda error: %v", err)
	}
}

func TestNormalizedZulipAllowedOwnersTrimsEmptyEntries(t *testing.T) {
	t.Parallel()

	got := normalizedZulipAllowedOwners([]string{" owner@example.com ", "", " second@example.com"})
	want := []string{"owner@example.com", "second@example.com"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normalizedZulipAllowedOwners() = %#v, want %#v", got, want)
	}
}

func TestNormalizedSlackAllowedOwnersTrimsEmptyEntries(t *testing.T) {
	t.Parallel()

	got := normalizedSlackAllowedOwners([]string{" slack:T123:U1 ", "", " slack:T123:U2 "})
	want := []string{"slack:T123:U1", "slack:T123:U2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normalizedSlackAllowedOwners() = %#v, want %#v", got, want)
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

func TestValidateRuntimeConfigLint_RejectsInvalidAndDuplicateRuntimeNames(t *testing.T) {
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

func TestValidateZulipConfigRequiresWebhookAuthAndReplyCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cfg       ZulipConfig
		wantError string
	}{
		{
			name:      "disabled",
			cfg:       ZulipConfig{},
			wantError: "",
		},
		{
			name: "missing token",
			cfg: ZulipConfig{
				ServerURL: "https://zulip.example.com",
				BotEmail:  "bot@example.com",
				APIKey:    "key",
				Webhook:   ZulipWebhookConfig{Enabled: true},
			},
			wantError: "webhook_token",
		},
		{
			name: "missing reply credentials",
			cfg: ZulipConfig{
				WebhookToken: "token",
				Webhook:      ZulipWebhookConfig{Enabled: true},
			},
			wantError: "server_url",
		},
		{
			name: "invalid webhook path",
			cfg: ZulipConfig{
				ServerURL:    "https://zulip.example.com",
				BotEmail:     "bot@example.com",
				APIKey:       "key",
				WebhookToken: "token",
				Webhook: ZulipWebhookConfig{
					Enabled: true,
					Path:    "zulip/webhook",
				},
			},
			wantError: "webhook.path",
		},
		{
			name: "valid",
			cfg: ZulipConfig{
				ServerURL:    "https://zulip.example.com",
				BotEmail:     "bot@example.com",
				APIKey:       "key",
				WebhookToken: "token",
				Webhook:      ZulipWebhookConfig{Enabled: true},
			},
			wantError: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateZulipConfig(tt.cfg)
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("validateZulipConfig() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateZulipConfig() error = nil, want marker %q", tt.wantError)
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("validateZulipConfig() error = %v, want marker %q", err, tt.wantError)
			}
		})
	}
}

func TestValidateSlackConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cfg       SlackConfig
		wantError string
	}{
		{name: "disabled", cfg: SlackConfig{}, wantError: ""},
		{name: "missing bot token", cfg: SlackConfig{Enabled: true, SigningSecret: "secret"}, wantError: "bot_token"},
		{name: "missing signing secret", cfg: SlackConfig{Enabled: true, BotToken: "xoxb-token"}, wantError: "signing_secret"},
		{
			name: "invalid events path",
			cfg: SlackConfig{
				Enabled:       true,
				BotToken:      "xoxb-token",
				SigningSecret: "secret",
				EventsPath:    "slack/events",
			},
			wantError: "events_path",
		},
		{
			name: "invalid commands path",
			cfg: SlackConfig{
				Enabled:       true,
				BotToken:      "xoxb-token",
				SigningSecret: "secret",
				CommandsPath:  "slack/commands",
			},
			wantError: "commands_path",
		},
		{
			name: "valid",
			cfg: SlackConfig{
				Enabled:       true,
				BotToken:      "xoxb-token",
				SigningSecret: "secret",
			},
			wantError: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateSlackConfig(tt.cfg)
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("validateSlackConfig() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateSlackConfig() error = nil, want marker %q", tt.wantError)
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("validateSlackConfig() error = %v, want marker %q", err, tt.wantError)
			}
		})
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
	t.Setenv("GIT_CEILING_DIRECTORIES", filepath.Dir(workingDir))

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
	runGitForBalda(t, ctx, dir, "config", "user.name", "Balda Test")
	runGitForBalda(t, ctx, dir, "config", "user.email", "balda-test@example.com")
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
