package agent

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/template"

	"github.com/normahq/balda/internal/apps/balda/memory"
	"github.com/normahq/balda/internal/apps/balda/paths"
	"github.com/normahq/balda/internal/apps/balda/telegramfmt"
	"github.com/normahq/balda/internal/git"
	"github.com/normahq/norma/pkg/runtime/agentconfig"
	"github.com/normahq/norma/pkg/runtime/agentfactory"
	runtimeconfig "github.com/normahq/norma/pkg/runtime/appconfig"
	"github.com/normahq/norma/pkg/runtime/sessionstate"
	"go.uber.org/fx"
	"google.golang.org/adk/agent"
	"google.golang.org/adk/runner"
	adksession "google.golang.org/adk/session"
)

//go:embed system_instruction.gotmpl
var baldaInstructionTmpl string

const (
	workspaceBranchUnknown = "unknown"
	workspaceBranchNA      = "n/a"
)

type Builder struct {
	factory                *agentfactory.Factory
	normaCfg               runtimeconfig.RuntimeConfig
	workingDir             string
	workspaceEnabled       bool
	workspaceBaseBranch    string
	baldaGlobalInstruction string
	telegramFormattingMode string
	sessionSvc             adksession.Service
	memoryStore            *memory.Store
}

type sessionStateFactory interface {
	BuildSessionState(agentID, workspaceDir string) (map[string]any, error)
}

type baldaPromptData struct {
	SessionID         string
	ChannelType       string
	ConfigPath        string
	WorkspaceDir      string
	WorkspaceEnabled  bool
	SessionBranch     string
	WorkspaceMode     string
	BaseBranch        string
	RepoBranchAtStart string
	FormattingMode    string
	FormattingRule    string
	FormattingExample string
	MemoryEnabled     bool
	GlobalInstruction string
	Instruction       string
}

func (b *Builder) buildBaldaInstruction(
	sessionID,
	channelType,
	agentName,
	sessionBranch,
	workspaceDir,
	repoBranchAtStart string,
) string {
	normalizedAgentName := strings.TrimSpace(agentName)
	repoBranch := strings.TrimSpace(repoBranchAtStart)
	if !b.workspaceEnabled {
		repoBranch = workspaceBranchNA
	} else if repoBranch == "" {
		repoBranch = workspaceBranchUnknown
	}

	baseBranch := strings.TrimSpace(b.workspaceBaseBranch)
	if b.workspaceEnabled && baseBranch == "" {
		if repoBranch != "" && repoBranch != workspaceBranchUnknown {
			baseBranch = repoBranch
		} else {
			baseBranch = workspaceBranchUnknown
		}
	}
	if !b.workspaceEnabled && baseBranch == "" {
		baseBranch = workspaceBranchNA
	}

	workspaceMode := "direct"
	if b.workspaceEnabled {
		workspaceMode = "git-worktree"
	}

	data := baldaPromptData{
		SessionID:         sessionID,
		ChannelType:       strings.TrimSpace(channelType),
		ConfigPath:        paths.ConfigPath(b.workingDir),
		WorkspaceDir:      workspaceDir,
		WorkspaceEnabled:  b.workspaceEnabled,
		SessionBranch:     sessionBranch,
		WorkspaceMode:     workspaceMode,
		BaseBranch:        baseBranch,
		RepoBranchAtStart: repoBranch,
		MemoryEnabled:     b.memoryStore.MemoryEnabled(),
	}
	mode := telegramfmt.NormalizeMode(b.telegramFormattingMode)
	rule, example := telegramfmt.PromptRuleAndExample(mode)
	data.FormattingMode = mode
	data.FormattingRule = rule
	data.FormattingExample = example

	agentInstruction := ""
	if agentCfg, ok := b.normaCfg.Providers[normalizedAgentName]; ok {
		agentInstruction = agentCfg.SystemInstructions
	}
	data.GlobalInstruction = strings.TrimSpace(b.baldaGlobalInstruction)
	data.Instruction = strings.TrimSpace(agentInstruction)

	var buf bytes.Buffer
	tmpl := template.Must(template.New("balda").Parse(baldaInstructionTmpl))
	if err := tmpl.Execute(&buf, data); err != nil {
		return baldaInstructionTmpl
	}
	return buf.String()
}

type BuilderParams struct {
	fx.In

	Factory                *agentfactory.Factory
	NormaCfg               runtimeconfig.RuntimeConfig
	WorkingDir             string
	WorkspaceEnabled       bool               `name:"balda_workspace_enabled"`
	WorkspaceBaseBranch    string             `name:"balda_workspace_base_branch"`
	BaldaGlobalInstruction string             `name:"balda_global_instruction"`
	TelegramFormattingMode string             `name:"balda_telegram_formatting_mode"`
	SessionService         adksession.Service `name:"balda_adk_session_service"`
	MemoryStore            *memory.Store
}

// NewBuilder creates a Builder with the given factory and config.
func NewBuilder(params BuilderParams) *Builder {
	return &Builder{
		factory:                params.Factory,
		normaCfg:               params.NormaCfg,
		workingDir:             strings.TrimSpace(params.WorkingDir),
		workspaceEnabled:       params.WorkspaceEnabled,
		workspaceBaseBranch:    strings.TrimSpace(params.WorkspaceBaseBranch),
		baldaGlobalInstruction: strings.TrimSpace(params.BaldaGlobalInstruction),
		telegramFormattingMode: telegramfmt.NormalizeMode(params.TelegramFormattingMode),
		sessionSvc:             params.SessionService,
		memoryStore:            params.MemoryStore,
	}
}

// ValidateAgent checks if an agent with the given name can be created.
// It returns an error if the agent is not found or its type is unsupported.
func (b *Builder) ValidateAgent(agentName string) error {
	return b.factory.ValidateAgent(agentName)
}

type BuiltAgent struct {
	Agent      agent.Agent
	Runner     *runner.Runner
	SessionSvc adksession.Service
	Session    adksession.Session
}

type BuiltRuntime struct {
	Agent      agent.Agent
	Runner     *runner.Runner
	SessionSvc adksession.Service
	AppName    string
}

type AgentMetadata struct {
	Type       string
	Model      string
	MCPServers []string
}

func (b *Builder) Build(ctx context.Context, sessionID, userID string, chatID int64, topicID int, agentName, workspaceDir string) (*BuiltAgent, error) {
	return b.BuildWithMCPServerIDs(ctx, sessionID, userID, chatID, topicID, agentName, workspaceDir, nil, nil)
}

func (b *Builder) BuildRuntime(ctx context.Context, agentName, workspaceDir string) (*BuiltRuntime, error) {
	return b.BuildRuntimeWithMCPServerIDs(ctx, agentName, workspaceDir, nil, nil)
}

func (b *Builder) BuildRuntimeWithMCPServerIDs(
	ctx context.Context,
	agentName, workspaceDir string,
	bundledMCPServerIDs []string,
	extraMCPServerIDs []string,
) (*BuiltRuntime, error) {
	const appName = "norma-balda"

	req := agentfactory.BuildRequest{
		AgentID:          agentName,
		Name:             agentName,
		Description:      b.buildAgentDescription(agentName),
		WorkingDirectory: workspaceDir,
		Instruction:      b.buildBaldaInstruction("balda-runtime", "telegram", agentName, "norma/balda/balda-runtime", workspaceDir, b.currentRepoBranch(ctx)),
		MCPServerIDs:     b.buildAgentMCPServerIDs(agentName, bundledMCPServerIDs, extraMCPServerIDs),
	}

	ag, err := b.factory.Build(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("creating agent %q: %w", agentName, err)
	}

	sessionSvc := b.sessionSvc
	if sessionSvc == nil {
		sessionSvc = adksession.InMemoryService()
	}
	r, err := runner.New(runner.Config{
		AppName:        appName,
		Agent:          ag,
		SessionService: sessionSvc,
	})
	if err != nil {
		if closer, ok := ag.(io.Closer); ok {
			_ = closer.Close()
		}
		return nil, fmt.Errorf("creating runner: %w", err)
	}

	return &BuiltRuntime{
		Agent:      ag,
		Runner:     r,
		SessionSvc: sessionSvc,
		AppName:    appName,
	}, nil
}

func (b *Builder) CreateRuntimeSession(
	ctx context.Context,
	runtime *BuiltRuntime,
	agentName string,
	userID string,
	sessionID string,
	workspaceDir string,
) (adksession.Session, error) {
	if runtime == nil {
		return nil, fmt.Errorf("runtime is required")
	}
	if strings.TrimSpace(userID) == "" {
		return nil, fmt.Errorf("user id is required")
	}
	if strings.TrimSpace(sessionID) == "" {
		return nil, fmt.Errorf("session id is required")
	}
	if strings.TrimSpace(agentName) == "" {
		return nil, fmt.Errorf("agent name is required")
	}
	if runtime.SessionSvc == nil {
		return nil, fmt.Errorf("session service is required")
	}
	if strings.TrimSpace(workspaceDir) == "" {
		return nil, fmt.Errorf("workspace dir is required")
	}
	state, err := b.buildSessionState(ctx, agentName, workspaceDir)
	if err != nil {
		return nil, err
	}

	appName := strings.TrimSpace(runtime.AppName)
	if appName == "" {
		appName = "norma-balda"
	}
	req := &adksession.CreateRequest{
		AppName:   appName,
		UserID:    strings.TrimSpace(userID),
		SessionID: strings.TrimSpace(sessionID),
	}
	if len(state) > 0 {
		req.State = state
	}
	getResp, getErr := runtime.SessionSvc.Get(ctx, &adksession.GetRequest{
		AppName:   req.AppName,
		UserID:    req.UserID,
		SessionID: req.SessionID,
	})
	if getErr == nil {
		if updater, ok := runtime.SessionSvc.(interface {
			UpdateSessionState(ctx context.Context, appName string, userID string, sessionID string, state map[string]any) (adksession.Session, error)
		}); ok {
			return updater.UpdateSessionState(ctx, req.AppName, req.UserID, req.SessionID, state)
		}
		return getResp.Session, nil
	}
	sess, err := runtime.SessionSvc.Create(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("creating session: %w", err)
	}

	return sess.Session, nil
}

func (b *Builder) BuildWithMCPServerIDs(
	ctx context.Context,
	sessionID string,
	userID string,
	chatID int64,
	topicID int,
	agentName, workspaceDir string,
	bundledMCPServerIDs []string,
	extraMCPServerIDs []string,
) (*BuiltAgent, error) {
	if strings.TrimSpace(userID) == "" {
		return nil, fmt.Errorf("user id is required")
	}
	sessionBranch := fmt.Sprintf("norma/balda/%s", sessionID)
	repoBranchAtStart := b.currentRepoBranch(ctx)
	state, err := b.buildSessionState(ctx, agentName, workspaceDir)
	if err != nil {
		return nil, err
	}
	req := agentfactory.BuildRequest{
		AgentID:          agentName,
		Name:             agentName,
		Description:      b.buildAgentDescription(agentName),
		WorkingDirectory: workspaceDir,
		Instruction:      b.buildBaldaInstruction(sessionID, "telegram", agentName, sessionBranch, workspaceDir, repoBranchAtStart),
		MCPServerIDs:     b.buildAgentMCPServerIDs(agentName, bundledMCPServerIDs, extraMCPServerIDs),
		SessionID:        sessionID,
	}

	ag, err := b.factory.Build(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("creating agent %q: %w", agentName, err)
	}

	sessionSvc := adksession.InMemoryService()
	sess, err := sessionSvc.Create(ctx, &adksession.CreateRequest{
		AppName:   fmt.Sprintf("norma-balda-topic-%d", topicID),
		UserID:    strings.TrimSpace(userID),
		SessionID: sessionID,
		State:     state,
	})
	if err != nil {
		if closer, ok := ag.(io.Closer); ok {
			_ = closer.Close()
		}
		return nil, fmt.Errorf("creating session: %w", err)
	}

	r, err := runner.New(runner.Config{
		AppName:        fmt.Sprintf("norma-balda-topic-%d", topicID),
		Agent:          ag,
		SessionService: sessionSvc,
	})
	if err != nil {
		if closer, ok := ag.(io.Closer); ok {
			_ = closer.Close()
		}
		return nil, fmt.Errorf("creating runner: %w", err)
	}

	return &BuiltAgent{
		Agent:      ag,
		Runner:     r,
		SessionSvc: sessionSvc,
		Session:    sess.Session,
	}, nil
}

func (b *Builder) currentRepoBranch(ctx context.Context) string {
	if !b.workspaceEnabled {
		return workspaceBranchNA
	}
	if strings.TrimSpace(b.workingDir) == "" {
		return workspaceBranchUnknown
	}
	branch, err := git.CurrentBranch(ctx, b.workingDir)
	if err != nil {
		return workspaceBranchUnknown
	}
	trimmed := strings.TrimSpace(branch)
	if trimmed == "" {
		return workspaceBranchUnknown
	}
	return trimmed
}

// buildAgentDescription returns a human-readable description of the agent.
func (b *Builder) buildAgentDescription(agentName string) string {
	agentCfg, ok := b.normaCfg.Providers[agentName]
	if !ok {
		return agentName
	}
	return agentCfg.Description(agentName)
}

// GetAgentInfo returns the description and list of MCP server names for an agent.
func (b *Builder) GetAgentInfo(agentName string) (description string, mcpServers []string) {
	agentCfg, ok := b.normaCfg.Providers[agentName]
	if !ok {
		return agentName, bundledMCPServerIDs(b.workspaceEnabled)
	}
	return agentCfg.Description(agentName), mergeMCPServerIDs(agentCfg.MCPServers, nil, b.workspaceEnabled)
}

// GetAgentMetadata returns provider type/model and provider-scoped MCP server IDs.
func (b *Builder) GetAgentMetadata(agentName string) AgentMetadata {
	agentCfg, ok := b.normaCfg.Providers[agentName]
	if !ok {
		return AgentMetadata{}
	}

	return AgentMetadata{
		Type:       strings.TrimSpace(agentCfg.Type),
		Model:      modelFromAgentConfig(agentCfg),
		MCPServers: mergeMCPServerIDs(agentCfg.MCPServers, nil, b.workspaceEnabled),
	}
}

// ProviderIDs returns configured runtime provider IDs sorted lexicographically.
func (b *Builder) ProviderIDs() []string {
	if b == nil || len(b.normaCfg.Providers) == 0 {
		return nil
	}
	providerIDs := make([]string, 0, len(b.normaCfg.Providers))
	for id := range b.normaCfg.Providers {
		trimmedID := strings.TrimSpace(id)
		if trimmedID == "" {
			continue
		}
		providerIDs = append(providerIDs, trimmedID)
	}
	sort.Strings(providerIDs)
	return providerIDs
}

func (b *Builder) buildAgentMCPServerIDs(agentName string, bundled, extra []string) []string {
	agentCfg, ok := b.normaCfg.Providers[agentName]
	if !ok {
		return mergeMCPServerIDsWithBase(defaultBundledMCPServerIDs(b.workspaceEnabled, bundled), nil, extra)
	}
	return mergeMCPServerIDsWithBase(defaultBundledMCPServerIDs(b.workspaceEnabled, bundled), agentCfg.MCPServers, extra)
}

func bundledMCPServerIDs(workspaceEnabled bool) []string {
	return []string{"balda"}
}

func defaultBundledMCPServerIDs(workspaceEnabled bool, override []string) []string {
	if override != nil {
		return append([]string(nil), override...)
	}
	return bundledMCPServerIDs(workspaceEnabled)
}

func mergeMCPServerIDs(explicit, extra []string, workspaceEnabled bool) []string {
	return mergeMCPServerIDsWithBase(bundledMCPServerIDs(workspaceEnabled), explicit, extra)
}

func mergeMCPServerIDsWithBase(base, explicit, extra []string) []string {
	out := make([]string, 0, len(base)+len(explicit)+len(extra))
	seen := make(map[string]struct{}, len(base)+len(explicit)+len(extra))

	appendUnique := func(id string) {
		trimmed := strings.TrimSpace(id)
		if trimmed == "" {
			return
		}
		if _, ok := seen[trimmed]; ok {
			return
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}

	for _, id := range base {
		appendUnique(id)
	}
	for _, id := range explicit {
		appendUnique(id)
	}
	for _, id := range extra {
		appendUnique(id)
	}

	return out
}

func (b *Builder) buildSessionState(ctx context.Context, agentName, workspaceDir string) (map[string]any, error) {
	if strings.TrimSpace(agentName) == "" {
		return nil, fmt.Errorf("agent name is required")
	}
	if strings.TrimSpace(workspaceDir) == "" {
		return nil, fmt.Errorf("workspace dir is required")
	}
	if stateFactory, ok := any(b.factory).(sessionStateFactory); ok {
		state, err := stateFactory.BuildSessionState(agentName, workspaceDir)
		if err != nil {
			return nil, err
		}
		return b.addMemorySnapshot(ctx, state)
	}
	state, err := buildSessionStateFallback(workspaceDir)
	if err != nil {
		return nil, err
	}
	return b.addMemorySnapshot(ctx, state)
}

func (b *Builder) addMemorySnapshot(ctx context.Context, state map[string]any) (map[string]any, error) {
	if state == nil {
		state = make(map[string]any)
	}
	state[memory.MemoryStateKey] = ""
	state[memory.SoulStateKey] = ""
	if b.memoryStore == nil {
		return state, nil
	}
	snapshot, err := b.memoryStore.Snapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("read balda memory snapshot: %w", err)
	}
	state[memory.MemoryStateKey] = snapshot.Memory
	state[memory.SoulStateKey] = snapshot.Soul
	return state, nil
}

func buildSessionStateFallback(workspaceDir string) (map[string]any, error) {
	cwd := strings.TrimSpace(workspaceDir)
	if cwd == "" {
		return nil, fmt.Errorf("session cwd is empty")
	}

	absCWD, err := filepath.Abs(cwd)
	if err != nil {
		return nil, fmt.Errorf("resolve session cwd %q: %w", cwd, err)
	}
	info, err := os.Stat(absCWD)
	if err != nil {
		return nil, fmt.Errorf("stat session cwd %q: %w", absCWD, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("session cwd %q is not a directory", absCWD)
	}

	return map[string]any{
		sessionstate.CWDKey: absCWD,
	}, nil
}

func modelFromAgentConfig(cfg agentconfig.Config) string {
	switch strings.TrimSpace(cfg.Type) {
	case agentconfig.AgentTypeGenericACP:
		if cfg.GenericACP != nil {
			return strings.TrimSpace(cfg.GenericACP.Model)
		}
	case agentconfig.AgentTypeGeminiACP:
		if cfg.GeminiACP != nil {
			return strings.TrimSpace(cfg.GeminiACP.Model)
		}
	case agentconfig.AgentTypeCodexACP:
		if cfg.CodexACP != nil {
			return strings.TrimSpace(cfg.CodexACP.Model)
		}
	case agentconfig.AgentTypeOpenCodeACP:
		if cfg.OpenCodeACP != nil {
			return strings.TrimSpace(cfg.OpenCodeACP.Model)
		}
	case agentconfig.AgentTypeCopilotACP:
		if cfg.CopilotACP != nil {
			return strings.TrimSpace(cfg.CopilotACP.Model)
		}
	case agentconfig.AgentTypeClaudeCodeACP:
		if cfg.ClaudeCodeACP != nil {
			return strings.TrimSpace(cfg.ClaudeCodeACP.Model)
		}
	}
	return ""
}
