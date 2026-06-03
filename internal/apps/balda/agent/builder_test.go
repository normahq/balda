package agent

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/normahq/balda/internal/apps/balda/memory"
	"github.com/normahq/norma/pkg/runtime/agentconfig"
	"github.com/normahq/norma/pkg/runtime/agentfactory"
	runtimeconfig "github.com/normahq/norma/pkg/runtime/appconfig"
	"github.com/normahq/norma/pkg/runtime/mcpregistry"
	"github.com/normahq/norma/pkg/runtime/sessionstate"
	adksession "google.golang.org/adk/session"
)

func TestMergeMCPServerIDsWithBase(t *testing.T) {
	explicit := []string{" custom.one ", "balda", "", "custom.one", "custom.two"}
	extra := []string{"balda.extra", "custom.two", " "}
	got := mergeMCPServerIDsWithBase([]string{"balda"}, explicit, extra)
	want := []string{"balda", "custom.one", "custom.two", "balda.extra"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mergeMCPServerIDsWithBase(%#v, %#v, %#v) = %#v, want %#v", []string{"balda"}, explicit, extra, got, want)
	}
}

func TestBuildBaldaInstruction_IncludesGlobalAndAgentInstruction(t *testing.T) {
	t.Parallel()

	builder := &Builder{
		normaCfg: runtimeconfig.RuntimeConfig{
			Providers: map[string]agentconfig.Config{
				"alpha": {
					SystemInstructions: "norma instruction",
				},
			},
		},
		baldaGlobalInstruction: "balda instruction",
	}

	got := builder.buildBaldaInstruction(
		"tg-1-2",
		"telegram",
		"alpha",
		"norma/balda/tg-1-2",
		"/tmp/work",
		"main",
	)

	wantSnippet := "Global instruction:\nbalda instruction\n\nInstruction:\nnorma instruction"
	if !strings.Contains(got, wantSnippet) {
		t.Fatalf("buildBaldaInstruction() missing snippet %q in output:\n%s", wantSnippet, got)
	}
}

func TestBuildBaldaInstruction_OmitsInstructionSectionsWhenEmpty(t *testing.T) {
	t.Parallel()

	builder := &Builder{}
	got := builder.buildBaldaInstruction(
		"tg-1-2",
		"telegram",
		"alpha",
		"norma/balda/tg-1-2",
		"/tmp/work",
		"main",
	)

	if strings.Contains(got, "Global instruction:") || strings.Contains(got, "Instruction:") {
		t.Fatalf("buildBaldaInstruction() unexpectedly contained instruction block:\n%s", got)
	}
}

func TestBuildBaldaInstruction_IncludesMemoryPlaceholdersWhenEnabled(t *testing.T) {
	t.Parallel()

	builder := &Builder{
		memoryStore: memory.NewStore(t.TempDir(), true),
	}
	got := builder.buildBaldaInstruction(
		"tg-1-2",
		"telegram",
		"alpha",
		"norma/balda/tg-1-2",
		"/tmp/work",
		"main",
	)

	for _, snippet := range []string{
		"Memory guidance:",
		"balda.memory.remember",
		"explicitly asks you to remember/save",
		"future sessions after start/restore",
		"MEMORY.md session-start facts:\n{balda_memory?}",
	} {
		if !strings.Contains(got, snippet) {
			t.Fatalf("buildBaldaInstruction() missing snippet %q in output:\n%s", snippet, got)
		}
	}
}

func TestBuildBaldaInstruction_ExcludesMemoryWhenDisabled(t *testing.T) {
	t.Parallel()

	builder := &Builder{
		memoryStore: memory.NewStore(t.TempDir(), false),
	}
	got := builder.buildBaldaInstruction(
		"tg-1-2",
		"telegram",
		"alpha",
		"norma/balda/tg-1-2",
		"/tmp/work",
		"main",
	)

	for _, forbidden := range []string{
		"Memory guidance:",
		"balda.memory.remember",
		"MEMORY.md session-start facts:",
		"{balda_memory?}",
	} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("buildBaldaInstruction() contained %q with memory disabled:\n%s", forbidden, got)
		}
	}
}

func TestBuildBaldaInstruction_IncludesGitWorkspaceContext(t *testing.T) {
	t.Parallel()

	builder := &Builder{
		workspaceEnabled:    true,
		workspaceBaseBranch: "main",
		workingDir:          "/repo",
	}

	got := builder.buildBaldaInstruction(
		"tg-1-2",
		"telegram",
		"alpha",
		"norma/balda/tg-1-2",
		"/tmp/work",
		"develop",
	)

	wantSnippets := []string{
		"You are communicating with the Balda bot owner or an authorized collaborator.",
		"Workspace settings:",
		"This session belongs to channel type: telegram.",
		"Mode: git-worktree",
		"Path: /tmp/work",
		"Config path: /repo/.config/balda/config.yaml",
		"Base branch: main",
		"Session branch: norma/balda/tg-1-2",
		"Main repo branch at start: develop",
		"Git workspace guidance:",
		"When calling `balda.workspace.import` or `balda.workspace.export`, pass `session_id` equal to the current Session ID shown above.",
	}
	for _, snippet := range wantSnippets {
		if !strings.Contains(got, snippet) {
			t.Fatalf("buildBaldaInstruction() missing snippet %q in output:\n%s", snippet, got)
		}
	}
	if strings.Contains(got, "ask one short clarifying question") {
		t.Fatalf("buildBaldaInstruction() unexpectedly included clarification mandate:\n%s", got)
	}
}

func TestBuildBaldaInstruction_IncludesDirectModeSettingsWhenWorkspaceDisabled(t *testing.T) {
	t.Parallel()

	builder := &Builder{workspaceEnabled: false, workingDir: "/repo"}
	got := builder.buildBaldaInstruction(
		"tg-1-2",
		"telegram",
		"alpha",
		"norma/balda/tg-1-2",
		"/tmp/work",
		"main",
	)

	wantSnippets := []string{
		"You are communicating with the Balda bot owner or an authorized collaborator.",
		"Workspace settings:",
		"This session belongs to channel type: telegram.",
		"Mode: direct",
		"Path: /tmp/work",
		"Config path: /repo/.config/balda/config.yaml",
		"Base branch: n/a",
		"Git workspace tooling: disabled",
	}
	for _, snippet := range wantSnippets {
		if !strings.Contains(got, snippet) {
			t.Fatalf("buildBaldaInstruction() missing snippet %q in output:\n%s", snippet, got)
		}
	}

	if strings.Contains(got, "Git workspace guidance:") {
		t.Fatalf("buildBaldaInstruction() unexpectedly included git guidance in direct mode:\n%s", got)
	}
	if strings.Contains(got, "Available namespaces:") {
		t.Fatalf("buildBaldaInstruction() unexpectedly included generic MCP namespace docs:\n%s", got)
	}
}

func TestBuildBaldaInstruction_IncludesFormattingGuidance_DefaultMarkdownV2(t *testing.T) {
	t.Parallel()

	builder := &Builder{}
	got := builder.buildBaldaInstruction(
		"tg-1-2",
		"telegram",
		"alpha",
		"norma/balda/tg-1-2",
		"/tmp/work",
		"main",
	)

	wantSnippets := []string{
		"Telegram formatting mode: `markdownv2`.",
		"Write normal Markdown or plain text. Balda converts it to Telegram MarkdownV2; use Markdown blank lines or lists for structure, and do not pre-escape Telegram MarkdownV2 reserved characters.",
		"Example output: **Build:** success. Run `balda start`.",
	}
	for _, snippet := range wantSnippets {
		if !strings.Contains(got, snippet) {
			t.Fatalf("buildBaldaInstruction() missing snippet %q in output:\n%s", snippet, got)
		}
	}
	if strings.Contains(got, "core.telegram.org/bots/api#formatting-options") {
		t.Fatalf("buildBaldaInstruction() unexpectedly contains docs URL:\n%s", got)
	}
}

func TestBuildBaldaInstruction_IncludesDirectConversationPolicyWithoutGoalSteering(t *testing.T) {
	t.Parallel()

	builder := &Builder{}
	got := builder.buildBaldaInstruction(
		"tg-1-2",
		"telegram",
		"alpha",
		"norma/balda/tg-1-2",
		"/tmp/work",
		"main",
	)

	wantSnippets := []string{
		"Treat ordinary user messages as ordinary conversation and reply to their actual content.",
		"Use the user's language by default unless they clearly ask for another language.",
		"Do not redirect the user to a command when a direct answer is appropriate.",
	}
	for _, snippet := range wantSnippets {
		if !strings.Contains(got, snippet) {
			t.Fatalf("buildBaldaInstruction() missing snippet %q in output:\n%s", snippet, got)
		}
	}
}

func TestBuildBaldaInstruction_IncludesFormattingGuidance_HTML(t *testing.T) {
	t.Parallel()

	builder := &Builder{telegramFormattingMode: "html"}
	got := builder.buildBaldaInstruction(
		"tg-1-2",
		"telegram",
		"alpha",
		"norma/balda/tg-1-2",
		"/tmp/work",
		"main",
	)

	wantSnippets := []string{
		"Telegram formatting mode: `html`.",
		"Use Telegram HTML parse mode. Supported tags: b/strong, i/em, u/ins, s/strike/del, tg-spoiler or span class=\"tg-spoiler\", a href, code, pre with nested code class=\"language-...\", blockquote expandable, tg-emoji emoji-id, tg-time unix/format. Balda escapes unsafe raw <, >, & while preserving supported Telegram HTML tags.",
		"Example output: <b>Build:</b> success. Run <code>balda start</code>.",
	}
	for _, snippet := range wantSnippets {
		if !strings.Contains(got, snippet) {
			t.Fatalf("buildBaldaInstruction() missing snippet %q in output:\n%s", snippet, got)
		}
	}
}

func TestBuildRootRuntimeInstruction_UsesPerSessionPlaceholders(t *testing.T) {
	t.Parallel()

	builder := &Builder{
		workspaceEnabled:    true,
		workspaceBaseBranch: "main",
		workingDir:          "/repo",
	}

	got := builder.buildRootRuntimeInstruction("alpha", "/tmp/work")

	for _, snippet := range []string{
		"ID: {balda_session_id}",
		"Path: {cwd}",
		"Session branch: {balda_session_branch}",
		"Main repo branch at start: {balda_repo_branch_at_start}",
	} {
		if !strings.Contains(got, snippet) {
			t.Fatalf("buildRootRuntimeInstruction() missing snippet %q in output:\n%s", snippet, got)
		}
	}
	for _, forbidden := range []string{"balda-runtime", "norma/balda/balda-runtime"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("buildRootRuntimeInstruction() unexpectedly contained %q:\n%s", forbidden, got)
		}
	}
}

func TestCreateRuntimeSession_IncludesCanonicalCWDState(t *testing.T) {
	t.Parallel()

	providers := map[string]agentconfig.Config{
		"alpha": {Type: "llm"},
	}
	builder := &Builder{
		factory:  agentfactory.New(providers, mcpregistry.New(nil)),
		normaCfg: runtimeconfig.RuntimeConfig{Providers: providers},
	}
	runtime := &BuiltRuntime{
		SessionSvc: adksession.InMemoryService(),
		AppName:    "norma-balda",
	}

	workspaceDir := t.TempDir()
	sess, err := builder.CreateRuntimeSession(context.Background(), runtime, "alpha", "user-1", "s-1", workspaceDir, RuntimeSessionContext{
		BaldaSessionID: "tg-1-2",
		SessionBranch:  "norma/balda/tg-1-2",
	})
	if err != nil {
		t.Fatalf("CreateRuntimeSession() error = %v", err)
	}
	gotCWD, err := sess.State().Get(sessionstate.CWDKey)
	if err != nil {
		t.Fatalf("session state get %q error = %v", sessionstate.CWDKey, err)
	}
	if gotCWD != workspaceDir {
		t.Fatalf("session state %q = %v, want %q", sessionstate.CWDKey, gotCWD, workspaceDir)
	}
	gotBaldaSessionID, err := sess.State().Get(BaldaSessionIDStateKey)
	if err != nil {
		t.Fatalf("session state get %q error = %v", BaldaSessionIDStateKey, err)
	}
	if gotBaldaSessionID != "tg-1-2" {
		t.Fatalf("session state %q = %v, want %q", BaldaSessionIDStateKey, gotBaldaSessionID, "tg-1-2")
	}
	gotSessionBranch, err := sess.State().Get(BaldaSessionBranchStateKey)
	if err != nil {
		t.Fatalf("session state get %q error = %v", BaldaSessionBranchStateKey, err)
	}
	if gotSessionBranch != "norma/balda/tg-1-2" {
		t.Fatalf("session state %q = %v, want %q", BaldaSessionBranchStateKey, gotSessionBranch, "norma/balda/tg-1-2")
	}
	gotRepoBranch, err := sess.State().Get(BaldaRepoBranchAtStartStateKey)
	if err != nil {
		t.Fatalf("session state get %q error = %v", BaldaRepoBranchAtStartStateKey, err)
	}
	if gotRepoBranch != workspaceBranchNA {
		t.Fatalf("session state %q = %v, want %q", BaldaRepoBranchAtStartStateKey, gotRepoBranch, workspaceBranchNA)
	}
}

func TestCreateRuntimeSession_IncludesMemorySnapshotState(t *testing.T) {
	t.Parallel()

	sess := createRuntimeSessionWithMemory(t, true)
	gotMemory, err := sess.State().Get(memory.MemoryStateKey)
	if err != nil {
		t.Fatalf("session state get %q error = %v", memory.MemoryStateKey, err)
	}
	if gotMemory != "remember this" {
		t.Fatalf("session state %q = %v, want remember this", memory.MemoryStateKey, gotMemory)
	}
}

func TestCreateRuntimeSession_MemoryDisabledLeavesSnapshotEmpty(t *testing.T) {
	t.Parallel()

	sess := createRuntimeSessionWithMemory(t, false)
	gotMemory, err := sess.State().Get(memory.MemoryStateKey)
	if err != nil {
		t.Fatalf("session state get %q error = %v", memory.MemoryStateKey, err)
	}
	if gotMemory != "" {
		t.Fatalf("session state %q = %v, want empty", memory.MemoryStateKey, gotMemory)
	}
}

func createRuntimeSessionWithMemory(t *testing.T, memoryEnabled bool) adksession.Session {
	t.Helper()

	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, memory.MemoryFileName), []byte("remember this\n"), 0o600); err != nil {
		t.Fatalf("write memory: %v", err)
	}
	providers := map[string]agentconfig.Config{
		"alpha": {Type: "llm"},
	}
	builder := &Builder{
		factory:     agentfactory.New(providers, mcpregistry.New(nil)),
		normaCfg:    runtimeconfig.RuntimeConfig{Providers: providers},
		memoryStore: memory.NewStore(stateDir, memoryEnabled),
	}
	runtime := &BuiltRuntime{
		SessionSvc: adksession.InMemoryService(),
		AppName:    "norma-balda",
	}

	sess, err := builder.CreateRuntimeSession(context.Background(), runtime, "alpha", "user-1", "s-1", t.TempDir(), RuntimeSessionContext{
		BaldaSessionID: "tg-1-2",
	})
	if err != nil {
		t.Fatalf("CreateRuntimeSession() error = %v", err)
	}
	return sess
}

func TestCreateRuntimeSession_InvalidCWD_FailsBeforeCreate(t *testing.T) {
	t.Parallel()

	providers := map[string]agentconfig.Config{
		"alpha": {Type: agentconfig.AgentTypeGenericACP},
	}
	builder := &Builder{
		factory:  agentfactory.New(providers, mcpregistry.New(nil)),
		normaCfg: runtimeconfig.RuntimeConfig{Providers: providers},
	}
	runtime := &BuiltRuntime{
		SessionSvc: adksession.InMemoryService(),
		AppName:    "norma-balda",
	}

	_, err := builder.CreateRuntimeSession(context.Background(), runtime, "alpha", "user-1", "s-1", t.TempDir()+"/missing", RuntimeSessionContext{
		BaldaSessionID: "tg-1-2",
	})
	if err == nil {
		t.Fatal("CreateRuntimeSession() error = nil, want invalid cwd error")
	}
	if !strings.Contains(err.Error(), "stat session cwd") {
		t.Fatalf("CreateRuntimeSession() error = %q, want stat session cwd", err)
	}
}

func TestCreateRuntimeSession_RequiresBaldaSessionIDState(t *testing.T) {
	t.Parallel()

	providers := map[string]agentconfig.Config{
		"alpha": {Type: "llm"},
	}
	builder := &Builder{
		factory:  agentfactory.New(providers, mcpregistry.New(nil)),
		normaCfg: runtimeconfig.RuntimeConfig{Providers: providers},
	}
	runtime := &BuiltRuntime{
		SessionSvc: adksession.InMemoryService(),
		AppName:    "norma-balda",
	}

	_, err := builder.CreateRuntimeSession(context.Background(), runtime, "alpha", "user-1", "s-1", t.TempDir(), RuntimeSessionContext{})
	if err == nil {
		t.Fatal("CreateRuntimeSession() error = nil, want missing balda session id error")
	}
	if !strings.Contains(err.Error(), "balda session id is required") {
		t.Fatalf("CreateRuntimeSession() error = %q, want missing balda session id", err)
	}
}

func TestCurrentRepoBranch_Fallbacks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		builder Builder
		want    string
	}{
		{
			name:    "workspace_disabled",
			builder: Builder{workspaceEnabled: false},
			want:    "n/a",
		},
		{
			name:    "missing_working_dir",
			builder: Builder{workspaceEnabled: true},
			want:    "unknown",
		},
		{
			name:    "non_git_working_dir",
			builder: Builder{workspaceEnabled: true, workingDir: mustMakeExternalTempDirForAgentTest(t)},
			want:    "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.builder.currentRepoBranch(context.Background()); got != tt.want {
				t.Fatalf("currentRepoBranch() = %q, want %q", got, tt.want)
			}
		})
	}
}

func mustMakeExternalTempDirForAgentTest(t *testing.T) string {
	t.Helper()

	dir, err := os.MkdirTemp("", "balda-agent-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})
	return dir
}
