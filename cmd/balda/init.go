package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/normahq/balda/internal/apps/balda/auth"
	"github.com/normahq/balda/internal/apps/balda/paths"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

const (
	baldaConfigFileName   = "config.yaml"
	baldaConfigRelDir     = ".config/balda"
	baldaRuntimeStatePath = ".config/balda"
	baldaDotEnvFileName   = ".env"
)

const baldaConfigGitignoreContent = "*\n!.gitignore\n"

const (
	baldaInitCodexModel      = "gpt-5.3-codex"
	baldaInitClaudeCodeModel = "claude-sonnet-4-6"
)

const baldaInitGlobalInstructionExample = "You are my balda agent.\nPrefer concise, actionable answers.\nUse balda.providers.start without a locator when you want a subagent in the current chat context.\nUse balda.workspace import/export instead of manual branch landing when workspace mode is enabled."

type baldaTokenStorageMode string

const (
	baldaTokenStorageEnv    baldaTokenStorageMode = "env"
	baldaTokenStorageConfig baldaTokenStorageMode = "config"
)

type baldaInitAgentTemplate struct {
	ID           string
	Type         string
	Model        string
	DetectBinary []string
}

var baldaInitAgentTemplates = []baldaInitAgentTemplate{
	{ID: "codex", Type: "codex_acp", Model: baldaInitCodexModel, DetectBinary: []string{"codex"}},
	{ID: "opencode", Type: "opencode_acp", Model: "opencode/big-pickle", DetectBinary: []string{"opencode"}},
	{ID: "copilot", Type: "copilot_acp", Model: "gpt-5-codex", DetectBinary: []string{"copilot"}},
	{ID: "gemini", Type: "gemini_acp", Model: "gemini-3-flash-preview", DetectBinary: []string{"gemini"}},
	{ID: "claude_code", Type: "claude_code_acp", Model: baldaInitClaudeCodeModel, DetectBinary: []string{"claude"}},
}

func initCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize balda config in the current repository",
		Long:  "Create .config/balda/config.yaml with balda defaults and autodetected runtime agents.",
		RunE: func(_ *cobra.Command, _ []string) error {
			workingDir, err := os.Getwd()
			if err != nil {
				return fmt.Errorf("getting working directory: %w", err)
			}

			baldaConfigDir := filepath.Join(workingDir, baldaConfigRelDir)
			if err := os.MkdirAll(baldaConfigDir, 0o700); err != nil {
				return fmt.Errorf("create balda config directory: %w", err)
			}
			gitignorePath := filepath.Join(baldaConfigDir, ".gitignore")
			if _, err := os.Stat(gitignorePath); err != nil {
				if !os.IsNotExist(err) {
					return fmt.Errorf("stat %s: %w", gitignorePath, err)
				}
				if err := os.WriteFile(gitignorePath, []byte(baldaConfigGitignoreContent), 0o600); err != nil {
					return fmt.Errorf("write %s: %w", gitignorePath, err)
				}
			}

			configPath := filepath.Join(baldaConfigDir, baldaConfigFileName)
			if _, err := os.Stat(configPath); err == nil {
				return fmt.Errorf("%s already exists", configPath)
			} else if !os.IsNotExist(err) {
				return fmt.Errorf("stat %s: %w", configPath, err)
			}

			doc, agentIDs, err := buildBaldaInitDocument(workingDir)
			if err != nil {
				return err
			}

			interactive := baldaInitIsInteractive()
			inputReader := bufio.NewReader(baldaInitInput)
			selectedBaldaProvider, err := chooseBaldaProvider(agentIDs, inputReader, baldaInitOutput, interactive)
			if err != nil {
				return err
			}

			if err := setBaldaProvider(doc, selectedBaldaProvider); err != nil {
				return err
			}
			if err := setBaldaGlobalInstructionExample(doc); err != nil {
				return err
			}
			telegramToken, bot, promptErr := promptBaldaTelegramToken(inputReader, baldaInitOutput, interactive)
			if promptErr != nil {
				return promptErr
			}
			tokenStorageMode, err := chooseBaldaTelegramTokenStorage(inputReader, baldaInitOutput, interactive)
			if err != nil {
				return err
			}
			storageTarget, err := storeBaldaTelegramToken(doc, workingDir, telegramToken, tokenStorageMode)
			if err != nil {
				return err
			}

			baldaSection, ok := toStringAnyMap(doc["balda"])
			if !ok {
				return fmt.Errorf("balda section is missing from generated config")
			}
			stateDirRaw := baldaRuntimeStatePath
			if raw, exists := baldaSection["state_dir"]; exists {
				stateDirRaw = strings.TrimSpace(fmt.Sprintf("%v", raw))
				if stateDirRaw == "" {
					return fmt.Errorf("balda.state_dir is required")
				}
			}
			stateDir, err := paths.ResolveStateDir(workingDir, stateDirRaw)
			if err != nil {
				return fmt.Errorf("resolve balda state_dir: %w", err)
			}
			if err := os.MkdirAll(stateDir, 0o700); err != nil {
				return fmt.Errorf("create Balda runtime state directory: %w", err)
			}
			dbPath := paths.StateDBPath(stateDir)

			ownerToken, err := loadOrCreateBaldaOwnerToken(context.Background(), dbPath)
			if err != nil {
				return fmt.Errorf("bootstrap balda owner token: %w", err)
			}

			content, err := yaml.Marshal(doc)
			if err != nil {
				return fmt.Errorf("marshal balda config: %w", err)
			}

			if err := os.WriteFile(configPath, content, 0o600); err != nil {
				return fmt.Errorf("write %s: %w", configPath, err)
			}

			_, _ = fmt.Fprintf(baldaInitOutput, "balda initialized successfully\n")
			_, _ = fmt.Fprintf(baldaInitOutput, "config: %s\n", configPath)
			_, _ = fmt.Fprintf(baldaInitOutput, "state db: %s\n", dbPath)
			_, _ = fmt.Fprintf(baldaInitOutput, "Balda provider: %s\n", selectedBaldaProvider)
			_, _ = fmt.Fprintf(baldaInitOutput, "telegram token stored in: %s\n", storageTarget)
			_, _ = fmt.Fprintf(baldaInitOutput, "start command: balda start\n")
			_, _ = fmt.Fprintf(baldaInitOutput, "auth command: %s\n", auth.BuildOwnerAuthCommand(ownerToken))
			_, _ = fmt.Fprintf(baldaInitOutput, "auth link: %s\n", auth.BuildOwnerAuthLink(bot.username, ownerToken))

			return nil
		},
	}

	return cmd
}
