package goalkeeper

import (
	"context"
	"fmt"
	"strings"

	deliverycmd "github.com/normahq/balda/internal/apps/balda/deliverycmd"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	"github.com/normahq/balda/internal/apps/balda/progress"
	"github.com/normahq/balda/pkg/actorlayer"
	actortransport "github.com/normahq/balda/pkg/actorlayer/transport"
)

func newGoalProgressUpdate(
	payload goalTaskPayload,
	step string,
	iteration int,
	kind baldaexecution.GoalProgressKind,
	text string,
	plan *progress.PlanSnapshot,
	sequence int,
) baldaexecution.GoalProgressUpdate {
	return baldaexecution.GoalProgressUpdate{
		JobID:         strings.TrimSpace(payload.JobID),
		Locator:       normalizeGoalDeliveryLocator(payload.Locator),
		Profile:       normalizeGoalDeliveryProfile(payload.DeliveryProfile),
		Step:          strings.TrimSpace(step),
		Iteration:     normalizeGoalIteration(iteration),
		MaxIterations: normalizeGoalMaxIterations(payload.MaxIterations),
		Kind:          kind,
		Text:          strings.TrimSpace(text),
		Plan:          plan,
		Sequence:      sequence,
	}
}

func dispatchGoalProgress(ctx context.Context, dispatcher actortransport.Dispatcher, update baldaexecution.GoalProgressUpdate) error {
	if dispatcher == nil {
		return actorlayer.TransientError(fmt.Errorf("actor dispatcher is required"))
	}
	env, err := deliverycmd.GoalProgressEnvelope(update)
	if err != nil {
		return actorlayer.PermanentError(fmt.Errorf("build goal progress envelope: %w", err))
	}
	if env.ID == "" {
		return nil
	}
	if _, err := dispatcher.Dispatch(ctx, env); err != nil {
		return actorlayer.TransientError(err)
	}
	return nil
}

func goalProgressEventPayload(update baldaexecution.GoalProgressUpdate) map[string]any {
	payload := map[string]any{
		"step":      strings.TrimSpace(update.Step),
		"iteration": normalizeGoalIteration(update.Iteration),
		"kind":      string(update.Kind),
	}
	if text := redactSecrets(strings.TrimSpace(update.Text)); text != "" {
		payload["text"] = text
	}
	return payload
}

func renderGoalProgressText(update baldaexecution.GoalProgressUpdate) string {
	body := redactSecrets(strings.TrimSpace(update.Text))
	return renderGoalStepMessage(update.Profile, update.Iteration, update.MaxIterations, update.Step, renderGoalProgressAction(update.Kind), body)
}

func renderGoalProgressAction(kind baldaexecution.GoalProgressKind) string {
	switch kind {
	case baldaexecution.GoalProgressKindOutput:
		return "update"
	case baldaexecution.GoalProgressKindCompleted:
		return "completed"
	default:
		return ""
	}
}

func normalizeGoalIteration(iteration int) int {
	if iteration <= 0 {
		return 1
	}
	return iteration
}
