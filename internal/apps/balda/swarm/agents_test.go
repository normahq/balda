package swarm

import (
	"context"
	"testing"
)

func TestNormalizeAgentSpecs_DefaultsAndOverrides(t *testing.T) {
	t.Parallel()

	specs, err := NormalizeAgentSpecs(map[string]AgentSpec{
		AgentNameExecutor: {Role: "Execute with project tools", Tools: []string{AgentToolShell, AgentToolWorkspace, AgentToolShell}},
	})
	if err != nil {
		t.Fatalf("NormalizeAgentSpecs() error = %v", err)
	}
	byName := specsByName(specs)
	if _, ok := byName[AgentNamePlanner]; !ok {
		t.Fatalf("planner default missing: %+v", specs)
	}
	if got := byName[AgentNameExecutor].Role; got != "Execute with project tools" {
		t.Fatalf("executor role = %q, want override", got)
	}
	wantTools := []string{AgentToolWorkspace, AgentToolShell}
	if !equalStrings(byName[AgentNameExecutor].Tools, wantTools) {
		t.Fatalf("executor tools = %+v, want %+v", byName[AgentNameExecutor].Tools, wantTools)
	}
}

func TestNormalizeAgentSpecs_RejectsInvalidTool(t *testing.T) {
	t.Parallel()

	_, err := NormalizeAgentSpecs(map[string]AgentSpec{
		"custom": {Role: "Custom", Tools: []string{"root"}},
	})
	if err == nil {
		t.Fatal("NormalizeAgentSpecs() error = nil, want non-nil")
	}
}

func TestAgentAllocator_SelectsRoleAndTieBreaksByName(t *testing.T) {
	t.Parallel()

	registry, err := NewAgentRegistry(Config{})
	if err != nil {
		t.Fatalf("NewAgentRegistry() error = %v", err)
	}
	allocator := &AgentAllocator{registry: registry}
	got, err := allocator.Allocate(context.Background(), AgentAllocationRequest{Role: "validator"})
	if err != nil {
		t.Fatalf("Allocate() error = %v", err)
	}
	if got.Name != AgentNameReviewer {
		t.Fatalf("allocated agent = %q, want %q", got.Name, AgentNameReviewer)
	}

	got, err = allocator.Allocate(context.Background(), AgentAllocationRequest{Tools: []string{AgentToolWorkspace}})
	if err != nil {
		t.Fatalf("Allocate(workspace) error = %v", err)
	}
	if got.Name != AgentNameExecutor {
		t.Fatalf("allocated agent = %q, want deterministic tie-break to %q", got.Name, AgentNameExecutor)
	}
}

func TestAgentSpecShellExecutionPolicy_DefaultRoles(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		spec AgentSpec
		want string
	}{
		{name: "planner", spec: AgentSpec{Name: AgentNamePlanner}, want: AgentShellPolicyNone},
		{name: "executor", spec: AgentSpec{Name: AgentNameExecutor}, want: AgentShellPolicyWorkspaceWrite},
		{name: "reviewer", spec: AgentSpec{Name: AgentNameReviewer}, want: AgentShellPolicyReadOnly},
		{name: "memory", spec: AgentSpec{Name: AgentNameMemory}, want: AgentShellPolicyNone},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.spec.ShellExecutionPolicy(); got != tc.want {
				t.Fatalf("ShellExecutionPolicy() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestAgentSpecShellExecutionPolicy_CustomByTools(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		spec AgentSpec
		want string
	}{
		{
			name: "workspace and shell",
			spec: AgentSpec{Name: "custom", Tools: []string{AgentToolWorkspace, AgentToolShell}},
			want: AgentShellPolicyWorkspaceWrite,
		},
		{
			name: "shell only",
			spec: AgentSpec{Name: "custom", Tools: []string{AgentToolShell}},
			want: AgentShellPolicyReadOnly,
		},
		{
			name: "no shell",
			spec: AgentSpec{Name: "custom", Tools: []string{AgentToolWorkspace}},
			want: AgentShellPolicyNone,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.spec.ShellExecutionPolicy(); got != tc.want {
				t.Fatalf("ShellExecutionPolicy() = %q, want %q", got, tc.want)
			}
		})
	}
}

func specsByName(specs []AgentSpec) map[string]AgentSpec {
	out := make(map[string]AgentSpec, len(specs))
	for _, spec := range specs {
		out[spec.Name] = spec
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
