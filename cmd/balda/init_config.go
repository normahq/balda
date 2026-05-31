package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/normahq/balda/internal/git"
	"gopkg.in/yaml.v3"
)

func buildBaldaInitDocument(workingDir string) (map[string]any, []string, error) {
	detectedAgents := make([]baldaInitAgentTemplate, 0, len(baldaInitAgentTemplates))
	for _, template := range baldaInitAgentTemplates {
		for _, binary := range template.DetectBinary {
			if _, err := baldaInitLookPath(binary); err == nil {
				detectedAgents = append(detectedAgents, template)
				break
			}
		}
	}
	if len(detectedAgents) == 0 {
		return nil, nil, fmt.Errorf(
			"no supported agent CLI detected in PATH; install at least one of: codex, opencode, copilot, gemini, claude",
		)
	}

	var baldaDefaults map[string]any
	if err := yaml.Unmarshal(defaultBaldaConfig, &baldaDefaults); err != nil {
		return nil, nil, fmt.Errorf("parse default balda config template: %w", err)
	}

	baldaSection, ok := toStringAnyMap(baldaDefaults["balda"])
	if !ok {
		return nil, nil, fmt.Errorf("default balda template is missing balda section")
	}
	if raw, exists := baldaSection["mcp_servers"]; !exists || raw == nil {
		baldaSection["mcp_servers"] = []any{}
	} else if _, ok := raw.([]any); !ok {
		if _, ok := raw.([]string); !ok {
			baldaSection["mcp_servers"] = []any{}
		}
	}
	baldaBaseBranch, err := baldaInitCurrentBranch(workingDir)
	if err != nil {
		baldaBaseBranch = ""
	}
	workspaceSection, ok := toStringAnyMap(baldaSection["workspace"])
	if !ok {
		return nil, nil, fmt.Errorf("balda.workspace section is missing from generated config")
	}
	workspaceSection["base_branch"] = strings.TrimSpace(baldaBaseBranch)
	baldaSection["workspace"] = workspaceSection

	agentIDs := make([]string, 0, len(detectedAgents))
	for _, detected := range detectedAgents {
		agentIDs = append(agentIDs, detected.ID)
	}
	profiles := make(map[string]any, len(agentIDs))
	for _, id := range agentIDs {
		profiles[id] = map[string]any{
			"balda": map[string]any{
				"provider": id,
			},
		}
	}

	doc := map[string]any{
		"runtime": map[string]any{
			"providers":   buildBaldaInitAgents(detectedAgents),
			"mcp_servers": map[string]any{},
		},
		"balda":    baldaSection,
		"profiles": profiles,
	}

	return doc, agentIDs, nil
}

func buildBaldaInitAgents(detected []baldaInitAgentTemplate) map[string]any {
	agents := make(map[string]any, len(detected)+1)
	poolMembers := make([]any, 0, len(detected))

	for _, agentTemplate := range detected {
		agentBlock := map[string]any{"type": agentTemplate.Type}
		typeConfig := map[string]any{}
		if strings.TrimSpace(agentTemplate.Model) != "" {
			typeConfig["model"] = agentTemplate.Model
		}
		agentBlock[agentTemplate.Type] = typeConfig
		agents[agentTemplate.ID] = agentBlock
		poolMembers = append(poolMembers, agentTemplate.ID)
	}

	agents["pool"] = map[string]any{
		"type": "pool",
		"pool": map[string]any{
			"members": poolMembers,
		},
	}

	return agents
}

func setBaldaTelegramToken(doc map[string]any, token string) error {
	baldaSection, ok := toStringAnyMap(doc["balda"])
	if !ok {
		return fmt.Errorf("balda section is missing from generated config")
	}
	telegramSection, ok := toStringAnyMap(baldaSection["telegram"])
	if !ok {
		return fmt.Errorf("balda.telegram section is missing from generated config")
	}
	telegramSection["token"] = token
	baldaSection["telegram"] = telegramSection
	doc["balda"] = baldaSection
	return nil
}

func toStringAnyMap(raw any) (map[string]any, bool) {
	switch v := raw.(type) {
	case map[string]any:
		return v, true
	case map[any]any:
		m := make(map[string]any, len(v))
		for key, value := range v {
			k, ok := key.(string)
			if !ok {
				return nil, false
			}
			m[k] = value
		}
		return m, true
	default:
		return nil, false
	}
}

var (
	baldaInitInput         io.Reader = os.Stdin
	baldaInitOutput        io.Writer = os.Stdout
	baldaInitIsInteractive           = func() bool {
		info, err := os.Stdin.Stat()
		if err != nil {
			return false
		}
		return (info.Mode() & os.ModeCharDevice) != 0
	}
	baldaInitLookPath      = exec.LookPath
	baldaInitCurrentBranch = func(workingDir string) (string, error) {
		return git.CurrentBranch(context.Background(), workingDir)
	}
	baldaInitLoadBotIdentity = loadBotIdentityFromToken
)
