package balda

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/normahq/balda/internal/apps/balda/handlers"
	"github.com/normahq/balda/internal/apps/balda/paths"
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
		t.Fatalf("legacy balda.db stat error = %v, want not exist", err)
	}
}

func TestOpenBaldaStateProviderIgnoresLegacyOnlyDB(t *testing.T) {
	stateDir := t.TempDir()
	legacyPath := filepath.Join(stateDir, "balda.db")
	if err := os.WriteFile(legacyPath, []byte("legacy"), 0o600); err != nil {
		t.Fatalf("write legacy db: %v", err)
	}

	provider, err := openBaldaStateProvider(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("openBaldaStateProvider() error = %v", err)
	}
	defer func() { _ = provider.Close() }()

	if _, err := os.Stat(paths.StateDBPath(stateDir)); err != nil {
		t.Fatalf("stat state db: %v", err)
	}
	content, err := os.ReadFile(legacyPath)
	if err != nil {
		t.Fatalf("read legacy db: %v", err)
	}
	if string(content) != "legacy" {
		t.Fatalf("legacy db content = %q, want unchanged", string(content))
	}
}

func TestValidateBaldaMCPConfiguration_RejectsRemovedBuiltInServerReferences(t *testing.T) {
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
	if !strings.Contains(err.Error(), `balda.mcp_servers[0] references removed built-in config MCP server "balda.config"; edit the balda config file directly at "/tmp/work/.config/balda/config.yaml"`) {
		t.Fatalf("unexpected balda.mcp_servers validation error: %v", err)
	}
	if !strings.Contains(err.Error(), `runtime.providers.root.mcp_servers[0] references removed built-in MCP server "balda.workspace"; use "balda"`) {
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
	if !strings.Contains(err.Error(), `runtime.mcp_servers.runtime.config conflicts with removed built-in config MCP server ID "runtime.config"; edit the balda config file directly at "/tmp/work/.config/balda/config.yaml"`) {
		t.Fatalf("missing removed built-in server conflict error: %v", err)
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

func TestBuildJobSchedulerConfig(t *testing.T) {
	t.Parallel()

	cfg := BaldaConfig{
		Scheduler: SchedulerConfig{
			Jobs: []ScheduledJobConfig{
				{
					ID:     " nightly ",
					Cron:   " */15 * * * * ",
					Prompt: " summarize ",
				},
			},
		},
	}

	got := buildJobSchedulerConfig(cfg)
	want := handlers.JobSchedulerConfig{
		Jobs: []handlers.ConfiguredScheduledJob{
			{
				ID:     "nightly",
				Cron:   "*/15 * * * *",
				Prompt: "summarize",
			},
		},
	}

	if len(got.Jobs) != len(want.Jobs) {
		t.Fatalf("len(jobs) = %d, want %d", len(got.Jobs), len(want.Jobs))
	}
	if got.Jobs[0] != want.Jobs[0] {
		t.Fatalf("job mismatch: got %+v want %+v", got.Jobs[0], want.Jobs[0])
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
	if got.Routes["webhook1"] != want.Routes["webhook1"] {
		t.Fatalf("route mismatch: got %+v want %+v", got.Routes["webhook1"], want.Routes["webhook1"])
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
