package actors

import (
	"context"

	"github.com/normahq/balda/internal/apps/balda/actors/goalkeeper"
	baldaagent "github.com/normahq/balda/internal/apps/balda/agent"
)

type goalRuntimeBuilderAdapter struct {
	manager *baldaagent.RuntimeManager
}

func (a goalRuntimeBuilderAdapter) BuildGoalRuntime(ctx context.Context, cfg goalkeeper.GoalRuntimeConfig) (goalkeeper.GoalRuntime, error) {
	runtime, err := a.manager.BuildGoalRuntime(ctx, baldaagent.GoalRuntimeConfig{
		SourceSessionID: cfg.SourceSessionID,
		TaskID:          cfg.TaskID,
		UserID:          cfg.UserID,
		MaxIterations:   cfg.MaxIterations,
	})
	if err != nil {
		return nil, err
	}
	return goalRuntimeAdapter{runtime: runtime}, nil
}

type goalRuntimeAdapter struct {
	runtime *baldaagent.GoalRuntime
}

func (a goalRuntimeAdapter) Runner() goalkeeper.GoalRunner {
	return a.runtime.Runner
}

func (a goalRuntimeAdapter) SessionID() string {
	return a.runtime.SessionID
}

func (a goalRuntimeAdapter) WorkspaceDir() string {
	return a.runtime.WorkspaceDir
}

func (a goalRuntimeAdapter) BranchName() string {
	return a.runtime.BranchName
}

func (a goalRuntimeAdapter) Close() error {
	return a.runtime.Close()
}

func (a goalRuntimeAdapter) CleanupResources(ctx context.Context) error {
	return a.runtime.CleanupResources(ctx)
}

func (a goalRuntimeAdapter) BuildCommitMessage(ctx context.Context, objective string, workerOutput string, validatorOutput string) (string, error) {
	return a.runtime.BuildCommitMessage(ctx, objective, workerOutput, validatorOutput)
}

func (a goalRuntimeAdapter) ExportWorkspace(ctx context.Context, commitMessage string) error {
	return a.runtime.ExportWorkspace(ctx, commitMessage)
}
