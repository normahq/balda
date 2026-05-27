package swarm

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

const (
	AgentNamePlanner  = "planner"
	AgentNameExecutor = "executor"
	AgentNameReviewer = "reviewer"
	AgentNameMemory   = "memory"

	AgentToolWorkspace = "workspace"
	AgentToolShell     = "shell"
	AgentToolMCP       = "mcp"
	AgentToolMemory    = "memory"

	AgentShellPolicyNone           = "none"
	AgentShellPolicyReadOnly       = "read_only"
	AgentShellPolicyWorkspaceWrite = "workspace_write"
)

var supportedAgentTools = map[string]struct{}{
	AgentToolWorkspace: {},
	AgentToolShell:     {},
	AgentToolMCP:       {},
	AgentToolMemory:    {},
}

var agentToolOrder = []string{AgentToolWorkspace, AgentToolShell, AgentToolMCP, AgentToolMemory}

type AgentSpec struct {
	Name        string
	Role        string
	Tools       []string
	CostPenalty int
}

// ShellExecutionPolicy returns the actor shell policy derived from role defaults
// and, for custom agents, from requested tool capabilities.
func (s AgentSpec) ShellExecutionPolicy() string {
	switch NormalizeAgentName(s.Name) {
	case AgentNamePlanner, AgentNameMemory:
		return AgentShellPolicyNone
	case AgentNameReviewer:
		return AgentShellPolicyReadOnly
	case AgentNameExecutor:
		return AgentShellPolicyWorkspaceWrite
	}
	hasShell := false
	hasWorkspace := false
	for _, tool := range s.Tools {
		switch NormalizeAgentName(tool) {
		case AgentToolShell:
			hasShell = true
		case AgentToolWorkspace:
			hasWorkspace = true
		}
	}
	if !hasShell {
		return AgentShellPolicyNone
	}
	if hasWorkspace {
		return AgentShellPolicyWorkspaceWrite
	}
	return AgentShellPolicyReadOnly
}

type AgentRegistry struct {
	specs map[string]AgentSpec
}

type AgentAllocator struct {
	registry *AgentRegistry
}

type AgentAllocationRequest struct {
	Name              string
	Role              string
	Tools             []string
	WorkspaceAffinity bool
}

func NewAgentRegistry(cfg Config) (*AgentRegistry, error) {
	specs, err := NormalizeAgentSpecs(cfg.Agents)
	if err != nil {
		return nil, err
	}
	registry := &AgentRegistry{specs: make(map[string]AgentSpec, len(specs))}
	for _, spec := range specs {
		registry.specs[spec.Name] = spec
	}
	return registry, nil
}

func NewAgentAllocator(registry *AgentRegistry) (*AgentAllocator, error) {
	if registry == nil {
		return nil, fmt.Errorf("agent registry is required")
	}
	return &AgentAllocator{registry: registry}, nil
}

func NormalizeAgentSpecs(raw map[string]AgentSpec) ([]AgentSpec, error) {
	defaults := defaultAgentSpecs()
	merged := make(map[string]AgentSpec, len(defaults)+len(raw))
	for name, spec := range defaults {
		merged[name] = spec
	}
	for key, spec := range raw {
		name := NormalizeAgentName(firstNonEmpty(spec.Name, key))
		if name == "" {
			return nil, fmt.Errorf("agent name is required")
		}
		if strings.Contains(name, ":") {
			return nil, fmt.Errorf("agent name %q must not contain ':'", name)
		}
		if base, ok := merged[name]; ok {
			if strings.TrimSpace(spec.Role) == "" {
				spec.Role = base.Role
			}
			if len(spec.Tools) == 0 {
				spec.Tools = base.Tools
			}
		}
		spec.Name = name
		spec.Role = strings.TrimSpace(spec.Role)
		if spec.Role == "" {
			return nil, fmt.Errorf("agent %q role is required", name)
		}
		tools, err := NormalizeAgentTools(spec.Tools)
		if err != nil {
			return nil, fmt.Errorf("agent %q tools: %w", name, err)
		}
		spec.Tools = tools
		merged[name] = spec
	}

	names := make([]string, 0, len(merged))
	for name := range merged {
		names = append(names, name)
	}
	sort.Strings(names)
	specs := make([]AgentSpec, 0, len(names))
	for _, name := range names {
		specs = append(specs, merged[name])
	}
	return specs, nil
}

func NormalizeAgentName(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func NormalizeAgentTools(raw []string) ([]string, error) {
	seen := make(map[string]struct{}, len(raw))
	for _, value := range raw {
		tool := strings.ToLower(strings.TrimSpace(value))
		if tool == "" {
			continue
		}
		if _, ok := supportedAgentTools[tool]; !ok {
			return nil, fmt.Errorf("unsupported tool %q", value)
		}
		seen[tool] = struct{}{}
	}
	tools := make([]string, 0, len(seen))
	for _, tool := range agentToolOrder {
		if _, ok := seen[tool]; ok {
			tools = append(tools, tool)
		}
	}
	return tools, nil
}

func (r *AgentRegistry) Get(name string) (AgentSpec, bool) {
	if r == nil {
		return AgentSpec{}, false
	}
	spec, ok := r.specs[NormalizeAgentName(name)]
	return spec, ok
}

func (r *AgentRegistry) All() []AgentSpec {
	if r == nil {
		return nil
	}
	names := make([]string, 0, len(r.specs))
	for name := range r.specs {
		names = append(names, name)
	}
	sort.Strings(names)
	specs := make([]AgentSpec, 0, len(names))
	for _, name := range names {
		specs = append(specs, r.specs[name])
	}
	return specs
}

func (r *AgentRegistry) FindByRole(role string) []AgentSpec {
	if r == nil {
		return nil
	}
	var matches []AgentSpec
	for _, spec := range r.All() {
		if spec.MatchesRole(role) {
			matches = append(matches, spec)
		}
	}
	return matches
}

func (s AgentSpec) MatchesRole(role string) bool {
	want := NormalizeAgentName(role)
	if want == "" {
		return false
	}
	if NormalizeAgentName(s.Name) == want {
		return true
	}
	for _, alias := range agentRoleAliases(NormalizeAgentName(s.Name)) {
		if want == alias {
			return true
		}
	}
	return strings.Contains(agentRoleText(s.Role), want)
}

func (a *AgentAllocator) Allocate(ctx context.Context, req AgentAllocationRequest) (AgentSpec, error) {
	if a == nil || a.registry == nil {
		return AgentSpec{}, fmt.Errorf("agent allocator is required")
	}
	if name := NormalizeAgentName(req.Name); name != "" {
		spec, ok := a.registry.Get(name)
		if !ok {
			return AgentSpec{}, fmt.Errorf("agent %q is not configured", name)
		}
		return spec, nil
	}

	requestedTools, err := NormalizeAgentTools(req.Tools)
	if err != nil {
		return AgentSpec{}, err
	}
	candidates := a.registry.All()
	if role := NormalizeAgentName(req.Role); role != "" {
		candidates = filterAgentsByRole(candidates, role)
		if len(candidates) == 0 {
			return AgentSpec{}, fmt.Errorf("no configured agent matches role %q", req.Role)
		}
	}
	if len(candidates) == 0 {
		return AgentSpec{}, fmt.Errorf("no configured agents")
	}

	best := candidates[0]
	bestScore := a.score(ctx, best, req, requestedTools)
	for _, spec := range candidates[1:] {
		score := a.score(ctx, spec, req, requestedTools)
		if score > bestScore || (score == bestScore && spec.Name < best.Name) {
			best = spec
			bestScore = score
		}
	}
	return best, nil
}

func (a *AgentAllocator) score(ctx context.Context, spec AgentSpec, req AgentAllocationRequest, requestedTools []string) int {
	score := 0
	if spec.MatchesRole(req.Role) {
		score += 30
	}
	toolSet := make(map[string]struct{}, len(spec.Tools))
	for _, tool := range spec.Tools {
		toolSet[tool] = struct{}{}
	}
	for _, tool := range requestedTools {
		if _, ok := toolSet[tool]; ok {
			score += 10
		}
	}
	if req.WorkspaceAffinity {
		if _, ok := toolSet[AgentToolWorkspace]; ok {
			score += 5
		}
	}
	score -= spec.CostPenalty
	return score
}

func filterAgentsByRole(specs []AgentSpec, role string) []AgentSpec {
	out := make([]AgentSpec, 0, len(specs))
	for _, spec := range specs {
		if spec.MatchesRole(role) {
			out = append(out, spec)
		}
	}
	return out
}

func defaultAgentSpecs() map[string]AgentSpec {
	return map[string]AgentSpec{
		AgentNamePlanner: {
			Name: AgentNamePlanner,
			Role: "Plan work and split into subtasks",
		},
		AgentNameExecutor: {
			Name:  AgentNameExecutor,
			Role:  "Use project tools and make changes",
			Tools: []string{AgentToolWorkspace, AgentToolShell, AgentToolMCP},
		},
		AgentNameReviewer: {
			Name:  AgentNameReviewer,
			Role:  "Validate result and inspect risks",
			Tools: []string{AgentToolWorkspace, AgentToolShell},
		},
		AgentNameMemory: {
			Name:  AgentNameMemory,
			Role:  "Extract durable facts and summaries",
			Tools: []string{AgentToolMemory},
		},
	}
}

func agentRoleAliases(name string) []string {
	switch name {
	case AgentNameExecutor:
		return []string{"worker"}
	case AgentNameReviewer:
		return []string{"validator"}
	default:
		return nil
	}
}

func agentRoleText(raw string) string {
	text := strings.ToLower(strings.TrimSpace(raw))
	text = strings.NewReplacer("_", " ", "-", " ", ".", " ", ",", " ").Replace(text)
	return text
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
