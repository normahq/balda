package agent

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	defaultRuntimeAppName = "norma-balda"

	workspaceBranchUnknown = "unknown"
	workspaceBranchNA      = "n/a"

	BaldaSessionIDStateKey         = "balda_session_id"
	BaldaSessionBranchStateKey     = "balda_session_branch"
	BaldaRepoBranchAtStartStateKey = "balda_repo_branch_at_start"

	baldaSessionIDPlaceholder         = "{" + BaldaSessionIDStateKey + "}"
	baldaSessionBranchPlaceholder     = "{" + BaldaSessionBranchStateKey + "}"
	baldaRepoBranchAtStartPlaceholder = "{" + BaldaRepoBranchAtStartStateKey + "}"
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

type RuntimeSessionContext struct {
	BaldaSessionID string
	SessionBranch  string
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
	SessionService         adksession.Service `name:"balda_runtime_session_service"`
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

func (b *Builder) BuildRuntimeWithMCPServerIDs(
	ctx context.Context,
	agentName, workspaceDir string,
	bundledMCPServerIDs []string,
	extraMCPServerIDs []string,
) (*BuiltRuntime, error) {
	const appName = defaultRuntimeAppName

	req := agentfactory.BuildRequest{
		AgentID:          agentName,
		Name:             agentName,
		Description:      b.buildAgentDescription(agentName),
		WorkingDirectory: workspaceDir,
		Instruction:      b.buildRootRuntimeInstruction(agentName, workspaceDir),
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
	sessionCtx RuntimeSessionContext,
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
	state, err := b.buildSessionState(ctx, agentName, workspaceDir, sessionCtx)
	if err != nil {
		return nil, err
	}

	appName := strings.TrimSpace(runtime.AppName)
	if appName == "" {
		appName = defaultRuntimeAppName
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

// GetAgentMetadata returns provider type/model and provider-scoped MCP server IDs.
func (b *Builder) GetAgentMetadata(agentName string) AgentMetadata {
	agentCfg, ok := b.normaCfg.Providers[agentName]
	if !ok {
		return AgentMetadata{}
	}

	model := ""
	switch strings.TrimSpace(agentCfg.Type) {
	case agentconfig.AgentTypeGenericACP:
		if agentCfg.GenericACP != nil {
			model = strings.TrimSpace(agentCfg.GenericACP.Model)
		}
	case agentconfig.AgentTypeGeminiACP:
		if agentCfg.GeminiACP != nil {
			model = strings.TrimSpace(agentCfg.GeminiACP.Model)
		}
	case agentconfig.AgentTypeCodexACP:
		if agentCfg.CodexACP != nil {
			model = strings.TrimSpace(agentCfg.CodexACP.Model)
		}
	case agentconfig.AgentTypeOpenCodeACP:
		if agentCfg.OpenCodeACP != nil {
			model = strings.TrimSpace(agentCfg.OpenCodeACP.Model)
		}
	case agentconfig.AgentTypeCopilotACP:
		if agentCfg.CopilotACP != nil {
			model = strings.TrimSpace(agentCfg.CopilotACP.Model)
		}
	case agentconfig.AgentTypeClaudeCodeACP:
		if agentCfg.ClaudeCodeACP != nil {
			model = strings.TrimSpace(agentCfg.ClaudeCodeACP.Model)
		}
	}

	return AgentMetadata{
		Type:       strings.TrimSpace(agentCfg.Type),
		Model:      model,
		MCPServers: mergeMCPServerIDsWithBase([]string{"balda"}, agentCfg.MCPServers, nil),
	}
}

func (b *Builder) buildAgentMCPServerIDs(agentName string, bundled, extra []string) []string {
	base := []string{"balda"}
	if bundled != nil {
		base = append([]string(nil), bundled...)
	}
	agentCfg, ok := b.normaCfg.Providers[agentName]
	if !ok {
		return mergeMCPServerIDsWithBase(base, nil, extra)
	}
	return mergeMCPServerIDsWithBase(base, agentCfg.MCPServers, extra)
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

func (b *Builder) buildSessionState(ctx context.Context, agentName, workspaceDir string, sessionCtx RuntimeSessionContext) (map[string]any, error) {
	if strings.TrimSpace(agentName) == "" {
		return nil, fmt.Errorf("agent name is required")
	}
	if strings.TrimSpace(workspaceDir) == "" {
		return nil, fmt.Errorf("workspace dir is required")
	}
	absCWD, err := resolveSessionWorkspaceDir(workspaceDir)
	if err != nil {
		return nil, err
	}
	baldaSessionID := strings.TrimSpace(sessionCtx.BaldaSessionID)
	if baldaSessionID == "" {
		return nil, fmt.Errorf("balda session id is required")
	}
	sessionBranch := strings.TrimSpace(sessionCtx.SessionBranch)
	if sessionBranch == "" {
		if b.workspaceEnabled {
			sessionBranch = workspaceBranchUnknown
		} else {
			sessionBranch = workspaceBranchNA
		}
	}
	repoBranchAtStart := b.currentRepoBranch(ctx)

	var state map[string]any
	if stateFactory, ok := any(b.factory).(sessionStateFactory); ok {
		state, err = stateFactory.BuildSessionState(agentName, workspaceDir)
		if err != nil {
			return nil, err
		}
	} else {
		state = make(map[string]any, 4)
	}
	if state == nil {
		state = make(map[string]any, 4)
	}
	state[sessionstate.CWDKey] = absCWD
	state[BaldaSessionIDStateKey] = baldaSessionID
	state[BaldaSessionBranchStateKey] = sessionBranch
	state[BaldaRepoBranchAtStartStateKey] = repoBranchAtStart
	return b.addMemorySnapshot(ctx, state)
}

func (b *Builder) buildRootRuntimeInstruction(agentName, workspaceDir string) string {
	return b.buildBaldaInstruction(
		baldaSessionIDPlaceholder,
		"telegram",
		agentName,
		baldaSessionBranchPlaceholder,
		"{"+sessionstate.CWDKey+"}",
		baldaRepoBranchAtStartPlaceholder,
	)
}

func resolveSessionWorkspaceDir(workspaceDir string) (string, error) {
	cwd := strings.TrimSpace(workspaceDir)
	if cwd == "" {
		return "", fmt.Errorf("session cwd is empty")
	}
	absCWD, err := filepath.Abs(cwd)
	if err != nil {
		return "", fmt.Errorf("resolve session cwd %q: %w", cwd, err)
	}
	info, err := os.Stat(absCWD)
	if err != nil {
		return "", fmt.Errorf("stat session cwd %q: %w", absCWD, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("session cwd %q is not a directory", absCWD)
	}
	return absCWD, nil
}

func (b *Builder) addMemorySnapshot(ctx context.Context, state map[string]any) (map[string]any, error) {
	if state == nil {
		state = make(map[string]any)
	}
	state[memory.MemoryStateKey] = ""
	if b.memoryStore == nil {
		return state, nil
	}
	memoryText, err := b.memoryStore.ReadMemory(ctx)
	if err != nil {
		return nil, fmt.Errorf("read balda memory: %w", err)
	}
	state[memory.MemoryStateKey] = strings.TrimSpace(memoryText)
	return state, nil
}
