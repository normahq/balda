package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/normahq/balda/internal/apps/balda/paths"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/normahq/norma/pkg/runtime/appconfig"
	"gopkg.in/yaml.v3"
)

const (
	testBaldaProviderCodex    = "codex"
	testBaldaProviderOpencode = "opencode"
	testBaldaProviderCopilot  = "copilot"
	testBaldaTokenMyToken     = "my-token"
)

func TestInitCommand_NonInteractiveAutoSelectsRootAndGeneratesDetectedAgents(t *testing.T) {
	workingDir := setWorkingDir(t)
	setDetectedBinaries(t, "codex", "opencode", "copilot", "gemini", "claude")
	setDetectedBaseBranch(t, "main", nil)
	setBaldaInitBotIdentityLoader(t, func(_ context.Context, token string) (botIdentity, error) {
		if strings.TrimSpace(token) == "" {
			return botIdentity{}, fmt.Errorf("missing token")
		}
		return botIdentity{username: "BaldaBot", name: "Balda"}, nil
	})
	setBaldaOwnerTokenGenerator(t, "owner-token-init")

	prevInput := baldaInitInput
	prevOutput := baldaInitOutput
	prevInteractive := baldaInitIsInteractive
	t.Cleanup(func() {
		baldaInitInput = prevInput
		baldaInitOutput = prevOutput
		baldaInitIsInteractive = prevInteractive
	})

	baldaInitInput = strings.NewReader("tg-token\n")
	baldaInitOutput = &bytes.Buffer{}
	baldaInitIsInteractive = func() bool { return false }

	cmd := initCommand()
	cmd.SetArgs([]string{})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("init command failed: %v", err)
	}

	assertBaldaInitArtifacts(t, workingDir)

	doc := mustReadBaldaDoc(t, workingDir)
	assertNoCLISection(t, doc)

	baldaSection, ok := toStringAnyMap(doc["balda"])
	if !ok {
		t.Fatal("balda section missing in generated config")
	}
	if got := baldaSection["provider"]; got != testBaldaProviderCodex {
		t.Fatalf("balda.provider = %#v, want %s", got, testBaldaProviderCodex)
	}
	telegramSection, ok := toStringAnyMap(baldaSection["telegram"])
	if !ok {
		t.Fatal("balda.telegram section missing in generated config")
	}
	if got := telegramSection["token"]; got != "" {
		t.Fatalf("balda.telegram.token = %#v, want empty string when stored in .env", got)
	}
	rawBaldaMCPServers, ok := baldaSection["mcp_servers"].([]any)
	if !ok {
		t.Fatalf("balda.mcp_servers type = %T, want []any", baldaSection["mcp_servers"])
	}
	if len(rawBaldaMCPServers) != 0 {
		t.Fatalf("balda.mcp_servers = %#v, want empty", rawBaldaMCPServers)
	}
	workspaceSection, ok := toStringAnyMap(baldaSection["workspace"])
	if !ok {
		t.Fatal("balda.workspace section missing in generated config")
	}
	if got := workspaceSection["base_branch"]; got != "main" {
		t.Fatalf("balda.workspace.base_branch = %#v, want main", got)
	}
	assertBaldaGlobalInstructionExample(t, baldaSection)

	runtimeSection, ok := toStringAnyMap(doc["runtime"])
	if !ok {
		t.Fatal("runtime section missing in generated config")
	}
	assertMapHasOnlyKeys(t, runtimeSection, []string{"providers", "mcp_servers"})
	providers, ok := toStringAnyMap(runtimeSection["providers"])
	if !ok {
		t.Fatal("runtime.providers missing in generated config")
	}
	mcpServers, ok := toStringAnyMap(runtimeSection["mcp_servers"])
	if !ok {
		t.Fatal("runtime.mcp_servers missing in generated config")
	}
	if len(mcpServers) != 0 {
		t.Fatalf("runtime.mcp_servers = %#v, want empty map", mcpServers)
	}
	assertMapHasOnlyKeys(t, providers, []string{"codex", "opencode", "copilot", "gemini", "claude_code", "pool"})
	assertAgentModel(t, providers, "codex", "codex_acp", baldaInitCodexModel)
	assertAgentModel(t, providers, "claude_code", "claude_code_acp", baldaInitClaudeCodeModel)

	poolMembers := readPoolMembers(t, providers)
	wantMembers := []string{"codex", "opencode", "copilot", "gemini", "claude_code"}
	if !reflect.DeepEqual(poolMembers, wantMembers) {
		t.Fatalf("pool.members = %#v, want %#v", poolMembers, wantMembers)
	}

	profiles, ok := toStringAnyMap(doc["profiles"])
	if !ok {
		t.Fatal("profiles section missing in generated config")
	}
	assertMapHasOnlyKeys(t, profiles, []string{"codex", "opencode", "copilot", "gemini", "claude_code"})
	assertProfileRoot(t, profiles, "codex", "codex")
	assertProfileRoot(t, profiles, "opencode", "opencode")
	assertProfileRoot(t, profiles, "copilot", "copilot")
	assertProfileRoot(t, profiles, "gemini", "gemini")
	assertProfileRoot(t, profiles, "claude_code", "claude_code")

	if _, ok := profiles["pool"]; ok {
		t.Fatal("profiles.pool must not be generated")
	}

	assertBaldaOwnerTokenStored(t, workingDir, "owner-token-init")
	assertDotEnvTokenValue(t, workingDir, "tg-token")

	out := baldaInitOutput.(*bytes.Buffer).String()
	if !strings.Contains(out, "start command: balda start") {
		t.Fatalf("init output missing start command: %q", out)
	}
	if !strings.Contains(out, "auth command: /start owner=owner-token-init") {
		t.Fatalf("init output missing auth command: %q", out)
	}
	if !strings.Contains(out, "auth link: https://t.me/BaldaBot?start=owner_owner-token-init") {
		t.Fatalf("init output missing auth link: %q", out)
	}
	if !strings.Contains(out, "telegram token stored in: "+filepath.Join(workingDir, ".env")) {
		t.Fatalf("init output missing token storage path: %q", out)
	}
}

func TestInitCommand_RejectsUnsupportedClaudecodeBinaryName(t *testing.T) {
	setDetectedBinaries(t, "claudecode")

	if _, _, err := buildBaldaInitDocument(t.TempDir()); err == nil {
		t.Fatal("buildBaldaInitDocument succeeded with only claudecode in PATH, want error")
	} else if !strings.Contains(err.Error(), "codex, opencode, copilot, gemini, claude") {
		t.Fatalf("buildBaldaInitDocument error = %v, want supported CLI list with claude", err)
	}
}

func TestInitCommand_InteractiveSelectionAndToken(t *testing.T) {
	workingDir := setWorkingDir(t)
	setDetectedBinaries(t, "codex", "opencode", "gemini")
	setDetectedBaseBranch(t, "", fmt.Errorf("not a git repo"))
	setBaldaInitBotIdentityLoader(t, func(_ context.Context, token string) (botIdentity, error) {
		if token != testBaldaTokenMyToken {
			return botIdentity{}, fmt.Errorf("invalid token")
		}
		return botIdentity{username: "BaldaBot"}, nil
	})
	setBaldaOwnerTokenGenerator(t, "owner-token-interactive")

	prevInput := baldaInitInput
	prevOutput := baldaInitOutput
	prevInteractive := baldaInitIsInteractive
	t.Cleanup(func() {
		baldaInitInput = prevInput
		baldaInitOutput = prevOutput
		baldaInitIsInteractive = prevInteractive
	})

	baldaInitInput = strings.NewReader("2\n" + testBaldaTokenMyToken + "\n2\n")
	baldaInitOutput = &bytes.Buffer{}
	baldaInitIsInteractive = func() bool { return true }

	cmd := initCommand()
	cmd.SetArgs([]string{})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("init command failed: %v", err)
	}

	doc := mustReadBaldaDoc(t, workingDir)
	baldaSection := mustMap(t, doc, "balda")
	if got := baldaSection["provider"]; got != testBaldaProviderOpencode {
		t.Fatalf("balda.provider = %#v, want %s", got, testBaldaProviderOpencode)
	}
	assertBaldaGlobalInstructionExample(t, baldaSection)
	telegramSection := mustMap(t, baldaSection, "telegram")
	if got := telegramSection["token"]; got != testBaldaTokenMyToken {
		t.Fatalf("balda.telegram.token = %#v, want %s", got, testBaldaTokenMyToken)
	}
	assertDotEnvTokenMissing(t, workingDir)
	profiles := mustMap(t, doc, "profiles")
	if _, ok := profiles["default"]; ok {
		t.Fatal("profiles.default must not be generated")
	}
	assertProfileRoot(t, profiles, "opencode", "opencode")
	assertBaldaOwnerTokenStored(t, workingDir, "owner-token-interactive")
}

func TestInitCommand_InteractiveDefaultPrioritizesCopilotBeforeGemini(t *testing.T) {
	workingDir := setWorkingDir(t)
	setDetectedBinaries(t, "copilot", "gemini")
	setDetectedBaseBranch(t, "", fmt.Errorf("not a git repo"))
	setBaldaInitBotIdentityLoader(t, func(_ context.Context, token string) (botIdentity, error) {
		if token != testBaldaTokenMyToken {
			return botIdentity{}, fmt.Errorf("invalid token")
		}
		return botIdentity{username: "BaldaBot"}, nil
	})

	prevInput := baldaInitInput
	prevOutput := baldaInitOutput
	prevInteractive := baldaInitIsInteractive
	t.Cleanup(func() {
		baldaInitInput = prevInput
		baldaInitOutput = prevOutput
		baldaInitIsInteractive = prevInteractive
	})

	baldaInitInput = strings.NewReader("\n" + testBaldaTokenMyToken + "\n\n")
	baldaInitOutput = &bytes.Buffer{}
	baldaInitIsInteractive = func() bool { return true }

	cmd := initCommand()
	cmd.SetArgs([]string{})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("init command failed: %v", err)
	}

	doc := mustReadBaldaDoc(t, workingDir)
	baldaSection := mustMap(t, doc, "balda")
	if got := baldaSection["provider"]; got != testBaldaProviderCopilot {
		t.Fatalf("balda.provider = %#v, want %s", got, testBaldaProviderCopilot)
	}
	telegramSection := mustMap(t, baldaSection, "telegram")
	if got := telegramSection["token"]; got != "" {
		t.Fatalf("balda.telegram.token = %#v, want empty string when stored in .env", got)
	}
	assertDotEnvTokenValue(t, workingDir, testBaldaTokenMyToken)
	assertBaldaGlobalInstructionExample(t, baldaSection)
}

func TestInitCommand_FailsWhenNoSupportedAgentCLIFound(t *testing.T) {
	_ = setWorkingDir(t)
	setDetectedBinaries(t)
	setDetectedBaseBranch(t, "", fmt.Errorf("not a git repo"))

	prevInput := baldaInitInput
	prevOutput := baldaInitOutput
	prevInteractive := baldaInitIsInteractive
	t.Cleanup(func() {
		baldaInitInput = prevInput
		baldaInitOutput = prevOutput
		baldaInitIsInteractive = prevInteractive
	})

	baldaInitInput = strings.NewReader("")
	baldaInitOutput = &bytes.Buffer{}
	baldaInitIsInteractive = func() bool { return false }

	cmd := initCommand()
	cmd.SetArgs([]string{})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when no supported agent CLI is detected")
	}
	if !strings.Contains(err.Error(), "no supported agent CLI detected") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInitCommand_FailsWhenConfigAlreadyExists(t *testing.T) {
	workingDir := setWorkingDir(t)
	setDetectedBinaries(t, "codex")
	setDetectedBaseBranch(t, "", fmt.Errorf("not a git repo"))

	configPath := filepath.Join(workingDir, baldaConfigRelDir, baldaConfigFileName)
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(configPath, []byte("balda:\n  provider: existing\n"), 0o600); err != nil {
		t.Fatalf("write existing config: %v", err)
	}

	cmd := initCommand()
	cmd.SetArgs([]string{})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when balda config already exists")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("unexpected error: %v", err)
	}

	gitignorePath := filepath.Join(workingDir, baldaConfigRelDir, ".gitignore")
	content, readErr := os.ReadFile(gitignorePath)
	if readErr != nil {
		t.Fatalf("read %s: %v", gitignorePath, readErr)
	}
	if got, want := string(content), baldaConfigGitignoreContent; got != want {
		t.Fatalf("%s content = %q, want %q", gitignorePath, got, want)
	}
}

func TestInitCommand_PreservesExistingConfigGitignore(t *testing.T) {
	workingDir := setWorkingDir(t)
	setDetectedBinaries(t, "codex")
	setDetectedBaseBranch(t, "main", nil)
	setBaldaInitBotIdentityLoader(t, func(_ context.Context, token string) (botIdentity, error) {
		if strings.TrimSpace(token) == "" {
			return botIdentity{}, fmt.Errorf("missing token")
		}
		return botIdentity{username: "BaldaBot", name: "Balda"}, nil
	})

	customGitignore := "# keep local state files for this repo\n*\n!.gitignore\n"
	gitignorePath := filepath.Join(workingDir, baldaConfigRelDir, ".gitignore")
	if err := os.MkdirAll(filepath.Dir(gitignorePath), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(gitignorePath, []byte(customGitignore), 0o600); err != nil {
		t.Fatalf("write existing .gitignore: %v", err)
	}

	prevInput := baldaInitInput
	prevOutput := baldaInitOutput
	prevInteractive := baldaInitIsInteractive
	t.Cleanup(func() {
		baldaInitInput = prevInput
		baldaInitOutput = prevOutput
		baldaInitIsInteractive = prevInteractive
	})

	baldaInitInput = strings.NewReader("tg-token\n")
	baldaInitOutput = &bytes.Buffer{}
	baldaInitIsInteractive = func() bool { return false }

	cmd := initCommand()
	cmd.SetArgs([]string{})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("init command failed: %v", err)
	}

	content, err := os.ReadFile(gitignorePath)
	if err != nil {
		t.Fatalf("read %s: %v", gitignorePath, err)
	}
	if got := string(content); got != customGitignore {
		t.Fatalf("%s content = %q, want %q", gitignorePath, got, customGitignore)
	}
}

func TestChooseBaldaProvider_NonInteractivePicksTopPriority(t *testing.T) {
	got, err := chooseBaldaProvider([]string{"codex", "opencode"}, strings.NewReader(""), &bytes.Buffer{}, false)
	if err != nil {
		t.Fatalf("chooseBaldaProvider: %v", err)
	}
	if got != "codex" {
		t.Fatalf("selected = %q, want codex", got)
	}
}

func TestChooseBaldaProvider_InteractiveSelectionByNumber(t *testing.T) {
	var out bytes.Buffer
	got, err := chooseBaldaProvider([]string{"alpha", "beta"}, strings.NewReader("2\n"), &out, true)
	if err != nil {
		t.Fatalf("chooseBaldaProvider: %v", err)
	}
	if got != "beta" {
		t.Fatalf("selected = %q, want beta", got)
	}
}

func TestChooseBaldaTelegramTokenStorage_NonInteractiveDefaultsEnv(t *testing.T) {
	got, err := chooseBaldaTelegramTokenStorage(strings.NewReader(""), &bytes.Buffer{}, false)
	if err != nil {
		t.Fatalf("chooseBaldaTelegramTokenStorage: %v", err)
	}
	if got != baldaTokenStorageEnv {
		t.Fatalf("storage = %q, want %q", got, baldaTokenStorageEnv)
	}
}

func TestChooseBaldaTelegramTokenStorage_InteractiveSelection(t *testing.T) {
	var out bytes.Buffer
	got, err := chooseBaldaTelegramTokenStorage(strings.NewReader("2\n"), &out, true)
	if err != nil {
		t.Fatalf("chooseBaldaTelegramTokenStorage: %v", err)
	}
	if got != baldaTokenStorageConfig {
		t.Fatalf("storage = %q, want %q", got, baldaTokenStorageConfig)
	}
}

func TestInitCommand_GeneratedConfigLoadableByBaldaLoader(t *testing.T) {
	workingDir := setWorkingDir(t)
	setDetectedBinaries(t, "codex")
	setDetectedBaseBranch(t, "main", nil)
	setBaldaInitBotIdentityLoader(t, func(_ context.Context, token string) (botIdentity, error) {
		if strings.TrimSpace(token) == "" {
			return botIdentity{}, fmt.Errorf("missing token")
		}
		return botIdentity{username: "BaldaBot"}, nil
	})

	prevInput := baldaInitInput
	prevOutput := baldaInitOutput
	prevInteractive := baldaInitIsInteractive
	t.Cleanup(func() {
		baldaInitInput = prevInput
		baldaInitOutput = prevOutput
		baldaInitIsInteractive = prevInteractive
	})
	baldaInitInput = strings.NewReader("tg-token\n")
	baldaInitOutput = &bytes.Buffer{}
	baldaInitIsInteractive = func() bool { return false }

	cmd := initCommand()
	cmd.SetArgs([]string{})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("init command failed: %v", err)
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
	if selectedProfile != "default" {
		t.Fatalf("selected profile = %q, want default", selectedProfile)
	}
	if got := doc.Balda.Provider; got != testBaldaProviderCodex {
		t.Fatalf("doc.Balda.Provider = %q, want %s", got, testBaldaProviderCodex)
	}
	if got := doc.Balda.Sessions.Persistence; got != "sqlite" {
		t.Fatalf("doc.Balda.Sessions.Persistence = %q, want sqlite", got)
	}
}

func TestInitCommand_FailsWhenTelegramTokenMissing(t *testing.T) {
	_ = setWorkingDir(t)
	setDetectedBinaries(t, "codex")
	setDetectedBaseBranch(t, "main", nil)
	setBaldaInitBotIdentityLoader(t, func(_ context.Context, _ string) (botIdentity, error) {
		return botIdentity{username: "BaldaBot"}, nil
	})

	prevInput := baldaInitInput
	prevOutput := baldaInitOutput
	prevInteractive := baldaInitIsInteractive
	t.Cleanup(func() {
		baldaInitInput = prevInput
		baldaInitOutput = prevOutput
		baldaInitIsInteractive = prevInteractive
	})

	baldaInitInput = strings.NewReader("")
	baldaInitOutput = &bytes.Buffer{}
	baldaInitIsInteractive = func() bool { return false }

	cmd := initCommand()
	cmd.SetArgs([]string{})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when telegram token is missing")
	}
	if !strings.Contains(err.Error(), "balda.telegram.token is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInitCommand_FailsWhenTelegramTokenValidationFails(t *testing.T) {
	_ = setWorkingDir(t)
	setDetectedBinaries(t, "codex")
	setDetectedBaseBranch(t, "main", nil)
	setBaldaInitBotIdentityLoader(t, func(_ context.Context, _ string) (botIdentity, error) {
		return botIdentity{}, fmt.Errorf("invalid bot token")
	})

	prevInput := baldaInitInput
	prevOutput := baldaInitOutput
	prevInteractive := baldaInitIsInteractive
	t.Cleanup(func() {
		baldaInitInput = prevInput
		baldaInitOutput = prevOutput
		baldaInitIsInteractive = prevInteractive
	})

	baldaInitInput = strings.NewReader("bad-token\n")
	baldaInitOutput = &bytes.Buffer{}
	baldaInitIsInteractive = func() bool { return false }

	cmd := initCommand()
	cmd.SetArgs([]string{})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected validation error for invalid telegram token")
	}
	if !strings.Contains(err.Error(), "validate balda.telegram.token") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInitCommand_NonInteractiveUpsertsExistingDotEnvToken(t *testing.T) {
	workingDir := setWorkingDir(t)
	setDetectedBinaries(t, "codex")
	setDetectedBaseBranch(t, "main", nil)
	setBaldaInitBotIdentityLoader(t, func(_ context.Context, token string) (botIdentity, error) {
		if token != "fresh-token" {
			return botIdentity{}, fmt.Errorf("invalid token")
		}
		return botIdentity{username: "BaldaBot"}, nil
	})

	if err := os.WriteFile(
		filepath.Join(workingDir, ".env"),
		[]byte("EXTRA=1\nBALDA_TELEGRAM_TOKEN=previous-token\nANOTHER=2\n"),
		0o600,
	); err != nil {
		t.Fatalf("write .env: %v", err)
	}

	prevInput := baldaInitInput
	prevOutput := baldaInitOutput
	prevInteractive := baldaInitIsInteractive
	t.Cleanup(func() {
		baldaInitInput = prevInput
		baldaInitOutput = prevOutput
		baldaInitIsInteractive = prevInteractive
	})

	baldaInitInput = strings.NewReader("fresh-token\n")
	baldaInitOutput = &bytes.Buffer{}
	baldaInitIsInteractive = func() bool { return false }

	cmd := initCommand()
	cmd.SetArgs([]string{})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("init command failed: %v", err)
	}

	content, err := os.ReadFile(filepath.Join(workingDir, ".env"))
	if err != nil {
		t.Fatalf("read .env: %v", err)
	}
	got := string(content)
	if !strings.Contains(got, "EXTRA=1") || !strings.Contains(got, "ANOTHER=2") {
		t.Fatalf("existing .env entries were not preserved: %q", got)
	}
	if strings.Count(got, "BALDA_TELEGRAM_TOKEN=") != 1 {
		t.Fatalf("expected single BALDA_TELEGRAM_TOKEN entry, got: %q", got)
	}
	if !strings.Contains(got, "BALDA_TELEGRAM_TOKEN=fresh-token") {
		t.Fatalf("updated token missing from .env: %q", got)
	}
}

func setWorkingDir(t *testing.T) string {
	t.Helper()
	workingDir := t.TempDir()
	prevWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("get wd: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(prevWD)
	})
	if err := os.Chdir(workingDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	return workingDir
}

func setDetectedBinaries(t *testing.T, binaries ...string) {
	t.Helper()
	prevLookPath := baldaInitLookPath
	t.Cleanup(func() {
		baldaInitLookPath = prevLookPath
	})

	present := make(map[string]struct{}, len(binaries))
	for _, name := range binaries {
		present[strings.TrimSpace(name)] = struct{}{}
	}
	baldaInitLookPath = func(file string) (string, error) {
		if _, ok := present[file]; ok {
			return "/usr/bin/" + file, nil
		}
		return "", fmt.Errorf("%s not found", file)
	}
}

func setDetectedBaseBranch(t *testing.T, branch string, branchErr error) {
	t.Helper()
	prev := baldaInitCurrentBranch
	t.Cleanup(func() {
		baldaInitCurrentBranch = prev
	})
	baldaInitCurrentBranch = func(string) (string, error) {
		return branch, branchErr
	}
}

func setBaldaInitBotIdentityLoader(t *testing.T, loader func(context.Context, string) (botIdentity, error)) {
	t.Helper()
	prev := baldaInitLoadBotIdentity
	t.Cleanup(func() {
		baldaInitLoadBotIdentity = prev
	})
	baldaInitLoadBotIdentity = loader
}

func setBaldaOwnerTokenGenerator(t *testing.T, token string) {
	t.Helper()
	prev := baldaGenerateOwnerToken
	t.Cleanup(func() {
		baldaGenerateOwnerToken = prev
	})
	baldaGenerateOwnerToken = func() (string, error) {
		return token, nil
	}
}

func mustReadBaldaDoc(t *testing.T, workingDir string) map[string]any {
	t.Helper()
	content, err := os.ReadFile(filepath.Join(workingDir, baldaConfigRelDir, baldaConfigFileName))
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(content, &doc); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}
	return doc
}

func assertBaldaInitArtifacts(t *testing.T, workingDir string) {
	t.Helper()

	gitignorePath := filepath.Join(workingDir, baldaConfigRelDir, ".gitignore")
	content, err := os.ReadFile(gitignorePath)
	if err != nil {
		t.Fatalf("read %s: %v", gitignorePath, err)
	}
	if got, want := string(content), baldaConfigGitignoreContent; got != want {
		t.Fatalf("%s content = %q, want %q", gitignorePath, got, want)
	}

	stateDir := filepath.Join(workingDir, baldaRuntimeStatePath)
	info, err := os.Stat(stateDir)
	if err != nil {
		t.Fatalf("stat %s: %v", stateDir, err)
	}
	if !info.IsDir() {
		t.Fatalf("%s is not a directory", stateDir)
	}

	dbPath := paths.StateDBPath(stateDir)
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("stat %s: %v", dbPath, err)
	}
}

func assertBaldaOwnerTokenStored(t *testing.T, workingDir string, want string) {
	t.Helper()

	dbPath := paths.StateDBPath(filepath.Join(workingDir, baldaRuntimeStatePath))
	provider, err := baldastate.NewSQLiteProvider(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("open provider: %v", err)
	}
	defer func() { _ = provider.Close() }()

	got, ok, err := provider.AppKV().Get(context.Background(), baldaOwnerAuthTokenKV)
	if err != nil {
		t.Fatalf("read owner token: %v", err)
	}
	if !ok {
		t.Fatal("owner auth token missing from state")
	}
	if got != want {
		t.Fatalf("owner auth token = %q, want %q", got, want)
	}
}

func assertDotEnvTokenValue(t *testing.T, workingDir, want string) {
	t.Helper()
	content, err := os.ReadFile(filepath.Join(workingDir, ".env"))
	if err != nil {
		t.Fatalf("read .env: %v", err)
	}
	if !strings.Contains(string(content), "BALDA_TELEGRAM_TOKEN="+want) {
		t.Fatalf(".env missing token value %q: %q", want, string(content))
	}
}

func assertDotEnvTokenMissing(t *testing.T, workingDir string) {
	t.Helper()
	content, err := os.ReadFile(filepath.Join(workingDir, ".env"))
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		t.Fatalf("read .env: %v", err)
	}
	if strings.Contains(string(content), "BALDA_TELEGRAM_TOKEN=") {
		t.Fatalf(".env unexpectedly contains BALDA_TELEGRAM_TOKEN: %q", string(content))
	}
}

func mustMap(t *testing.T, parent map[string]any, key string) map[string]any {
	t.Helper()
	raw, ok := parent[key]
	if !ok {
		t.Fatalf("missing key %q", key)
	}
	m, ok := toStringAnyMap(raw)
	if !ok {
		t.Fatalf("%s is not a map", key)
	}
	return m
}

func assertNoCLISection(t *testing.T, doc map[string]any) {
	t.Helper()
	if _, ok := doc["cli"]; ok {
		t.Fatal("top-level cli section must not be generated by balda init")
	}
	profiles, ok := toStringAnyMap(doc["profiles"])
	if !ok {
		t.Fatal("profiles section missing in generated config")
	}
	for profileName, raw := range profiles {
		profile, ok := toStringAnyMap(raw)
		if !ok {
			t.Fatalf("profiles.%s is not a map", profileName)
		}
		if _, hasCLI := profile["cli"]; hasCLI {
			t.Fatalf("profiles.%s.cli must not be generated", profileName)
		}
	}
}

func assertMapHasOnlyKeys(t *testing.T, m map[string]any, expected []string) {
	t.Helper()
	want := make(map[string]struct{}, len(expected))
	for _, key := range expected {
		want[key] = struct{}{}
	}
	if len(m) != len(expected) {
		t.Fatalf("map keys = %v, want %v", sortedKeys(m), expected)
	}
	for key := range m {
		if _, ok := want[key]; !ok {
			t.Fatalf("unexpected key %q in map; keys=%v", key, sortedKeys(m))
		}
	}
}

func assertAgentModel(t *testing.T, providers map[string]any, id, typeName, wantModel string) {
	t.Helper()
	agent := mustMap(t, providers, id)
	if got := agent["type"]; got != typeName {
		t.Fatalf("runtime.providers.%s.type = %#v, want %s", id, got, typeName)
	}
	typeBlock := mustMap(t, agent, typeName)
	if got := typeBlock["model"]; got != wantModel {
		t.Fatalf("runtime.providers.%s.%s.model = %#v, want %s", id, typeName, got, wantModel)
	}
}

func readPoolMembers(t *testing.T, providers map[string]any) []string {
	t.Helper()
	poolAgent := mustMap(t, providers, "pool")
	if got := poolAgent["type"]; got != "pool" {
		t.Fatalf("runtime.providers.pool.type = %#v, want pool", got)
	}
	poolCfg := mustMap(t, poolAgent, "pool")
	rawMembers, ok := poolCfg["members"].([]any)
	if !ok {
		t.Fatalf("runtime.providers.pool.pool.members type = %T, want []any", poolCfg["members"])
	}
	members := make([]string, 0, len(rawMembers))
	for _, raw := range rawMembers {
		member, ok := raw.(string)
		if !ok {
			t.Fatalf("pool member type = %T, want string", raw)
		}
		members = append(members, member)
	}
	return members
}

func assertProfileRoot(t *testing.T, profiles map[string]any, profileName, wantRoot string) {
	t.Helper()
	profile := mustMap(t, profiles, profileName)
	baldaProfile := mustMap(t, profile, "balda")
	if got := baldaProfile["provider"]; got != wantRoot {
		t.Fatalf("profiles.%s.balda.provider = %#v, want %s", profileName, got, wantRoot)
	}
}

func assertBaldaGlobalInstructionExample(t *testing.T, baldaSection map[string]any) {
	t.Helper()
	rawPrompt, ok := baldaSection["global_instruction"]
	if !ok {
		t.Fatalf("balda.global_instruction key is missing")
	}
	prompt, ok := rawPrompt.(string)
	if !ok {
		t.Fatalf("balda.global_instruction type = %T, want string", rawPrompt)
	}
	if strings.TrimSpace(prompt) == "" {
		t.Fatalf("balda.global_instruction is empty")
	}
	if prompt != baldaInitGlobalInstructionExample {
		t.Fatalf("balda.global_instruction = %q, want %q", prompt, baldaInitGlobalInstructionExample)
	}
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	for i := 0; i < len(keys)-1; i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	return keys
}
