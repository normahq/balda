package deliverycmd

import (
	"fmt"
	"strings"

	"github.com/normahq/balda/internal/apps/balda/deliveryfmt"
	baldaexecution "github.com/normahq/balda/internal/apps/balda/execution"
	"github.com/normahq/balda/pkg/actorlayer"
)

func GoalProgressEnvelope(update baldaexecution.GoalProgressUpdate) (actorlayer.Envelope, error) {
	from := actorlayer.ActorAddress{Target: baldaexecution.ActorTypeGoalkeeper, Key: strings.TrimSpace(update.JobID)}
	switch update.Kind {
	case baldaexecution.GoalProgressKindPlan:
		message := strings.TrimSpace(update.Text)
		if message == "" {
			return actorlayer.Envelope{}, nil
		}
		return ProgressPlanUpdateEnvelope(
			strings.TrimSpace(update.JobID),
			from,
			update.Locator,
			deliveryfmt.ProgressPolicy{},
			true,
			0,
			update.Plan,
			message,
			goalProgressDedupeSuffix(update),
		)
	case baldaexecution.GoalProgressKindOutput, baldaexecution.GoalProgressKindCompleted:
		message := strings.TrimSpace(update.Text)
		if message == "" {
			return actorlayer.Envelope{}, nil
		}
		return AgentReplyEnvelopeWithProfile(
			strings.TrimSpace(update.JobID),
			from,
			update.Locator,
			update.Profile,
			message,
			goalProgressDedupeSuffix(update),
		)
	default:
		return actorlayer.Envelope{}, fmt.Errorf("unsupported goal progress kind %q", update.Kind)
	}
}

func goalProgressDedupeSuffix(update baldaexecution.GoalProgressUpdate) string {
	iteration := update.Iteration
	if iteration <= 0 {
		iteration = 1
	}
	return fmt.Sprintf("progress:%s:%s:%d:%03d", update.Kind, strings.TrimSpace(update.Step), iteration, update.Sequence)
}
