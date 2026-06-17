package actors

import (
	"context"

	"github.com/normahq/balda/internal/apps/balda/actors/goalkeeper"
	baldaagent "github.com/normahq/balda/internal/apps/balda/agent"
)

type goalRunPreparerAdapter struct {
	manager *baldaagent.RuntimeManager
}

func (a goalRunPreparerAdapter) PrepareGoalRun(ctx context.Context, cfg goalkeeper.GoalRunConfig) (goalkeeper.GoalRun, error) {
	runtime, err := a.manager.PrepareGoalRun(ctx, baldaagent.GoalRunConfig{
		SourceSessionID: cfg.SourceSessionID,
		TaskID:          cfg.TaskID,
		UserID:          cfg.UserID,
		MaxIterations:   cfg.MaxIterations,
	})
	if err != nil {
		return nil, err
	}
	return goalRunAdapter{runtime: runtime}, nil
}

type goalRunAdapter struct {
	runtime *baldaagent.GoalRun
}

func (a goalRunAdapter) Runner() goalkeeper.GoalRunner {
	return a.runtime.Runner
}

func (a goalRunAdapter) SessionID() string {
	return a.runtime.SessionID
}

func (a goalRunAdapter) WorkspaceDir() string {
	return a.runtime.WorkspaceDir
}

func (a goalRunAdapter) BranchName() string {
	return a.runtime.BranchName
}

func (a goalRunAdapter) Close() error {
	return a.runtime.Close()
}

func (a goalRunAdapter) CleanupResources(ctx context.Context) error {
	return a.runtime.CleanupResources(ctx)
}

func (a goalRunAdapter) Finalize(
	ctx context.Context,
	objective string,
	workerOutput string,
	validatorOutput string,
) (goalkeeper.GoalFinalizationResult, error) {
	result, err := a.runtime.Finalize(ctx, objective, workerOutput, validatorOutput)
	return goalkeeper.GoalFinalizationResult{
		Status:        result.Status,
		CommitMessage: result.CommitMessage,
		Reason:        result.Reason,
		Error:         result.Error,
	}, err
}
